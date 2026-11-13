//! Wire protocol: JSON envelopes of `topic`, `key` and `value`. Retained
//! messages keep their newest value per key for clients connecting late;
//! ephemeral ones reach only the clients connected at the time. A request
//! carries an `id` and its reply goes to that `id` alone.

use {
    flate2::{Compression, write::ZlibEncoder},
    serde::{Deserialize, Serialize},
    solana_clock::Slot,
    std::{
        collections::BTreeMap,
        io::Write,
        sync::{
            Arc, Mutex, OnceLock,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
    tokio::sync::broadcast,
};

/// A ceiling the size tests hold server messages under; soketto limits only what it receives.
#[cfg(test)]
pub const MAX_MESSAGE: usize = 1024 * 1024;

/// Fails the test where `message` is not under `ceiling` bytes.
#[cfg(test)]
#[track_caller]
pub fn assert_under(what: &str, message: &str, ceiling: usize) {
    let length = message.len();
    assert!(
        length < ceiling,
        "{what} is {length} bytes against a {ceiling} byte ceiling"
    );
}

const BROADCAST_CAPACITY: usize = 8192;

/// JSON this long or longer also travels deflated to a client that offered the subprotocol; below
/// it the saving does not pay for the framing.
pub const DEFLATE_FROM: usize = 512;

pub const DEFLATE_PROTOCOL: &str = "deflate";

const TRAFFIC_REPORT: Duration = Duration::from_secs(60);

const TRAFFIC_TOP: usize = 8;

pub const TOPIC_SUMMARY: &str = "summary";
pub const TOPIC_EPOCH: &str = "epoch";
pub const TOPIC_SLOT: &str = "slot";
pub const TOPIC_PEERS: &str = "peers";

#[derive(Serialize)]
struct Envelope<'a, T> {
    topic: &'a str,
    key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    value: T,
}

/// Unknown fields are ignored, so arguments can be added later.
#[derive(Deserialize)]
pub struct Request {
    pub topic: String,
    pub key: String,
    pub id: Option<u64>,
    #[serde(default)]
    pub params: serde_json::Value,
}

/// What a queued message can be dropped for: a newer value of the same retained key, or a newer
/// update under the same key to the same slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Supersedes {
    Key(&'static str, &'static str),
    Slot(&'static str, &'static str, Slot),
}

/// Serialized once, on the publishing thread, and shared by every client.
#[derive(Clone)]
pub struct Message {
    text: Arc<str>,
    /// Made before the first send to a client that takes it, and shared by every copy.
    deflated: Arc<OnceLock<Option<Box<[u8]>>>>,
    /// So an older value still queued for a slow client can be dropped in its favour.
    supersedes: Option<Supersedes>,
}

pub enum Frame<'a> {
    Text(&'a str),
    Binary(&'a [u8]),
}

impl Message {
    fn new(json: String) -> Self {
        Self {
            text: Arc::from(json.as_str()),
            deflated: Arc::default(),
            supersedes: None,
        }
    }

    fn deflated(&self) -> Option<&[u8]> {
        self.deflated
            .get_or_init(|| {
                (self.text.len() >= DEFLATE_FROM)
                    .then(|| deflate(self.text.as_bytes()))
                    .flatten()
                    .map(Vec::into_boxed_slice)
            })
            .as_deref()
    }

    pub fn frame(&self, deflate: bool) -> Frame<'_> {
        match deflate.then(|| self.deflated()).flatten() {
            Some(bytes) => Frame::Binary(bytes),
            None => Frame::Text(&self.text),
        }
    }

    pub fn wire_len(&self, deflate: bool) -> usize {
        deflate
            .then(|| self.deflated())
            .flatten()
            .map_or(self.text.len(), <[u8]>::len)
    }

    pub fn supersedes(&self) -> Option<Supersedes> {
        self.supersedes
    }

    /// Copies sharing this message's text, this one included.
    #[cfg(test)]
    pub(crate) fn copies(&self) -> usize {
        Arc::strong_count(&self.text)
    }
}

impl std::ops::Deref for Message {
    type Target = str;

    fn deref(&self) -> &str {
        &self.text
    }
}

impl std::fmt::Display for Message {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

/// zlib-wrapped, the format a browser's `DecompressionStream` reads as "deflate".
fn deflate(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).ok()?;
    encoder.finish().ok()
}

/// A burst of queued messages with each retained key and each slot sent once, with its newest
/// value, in the newest one's place. Everything else keeps its order.
pub fn coalesce(burst: Vec<Message>) -> Vec<Message> {
    let mut seen = std::collections::HashSet::new();
    let mut kept: Vec<Message> = burst
        .into_iter()
        .rev()
        .filter(|message| match message.supersedes() {
            Some(key) => seen.insert(key),
            None => true,
        })
        .collect();
    kept.reverse();
    kept
}

pub fn encode<T: Serialize>(topic: &str, key: &str, value: &T) -> Message {
    encode_with_id(topic, key, None, value)
}

pub fn encode_with_id<T: Serialize>(topic: &str, key: &str, id: Option<u64>, value: &T) -> Message {
    let envelope = Envelope {
        topic,
        key,
        id,
        value,
    };
    // Falling back to null keeps a bug in one topic from taking the feed down.
    match serde_json::to_string(&envelope) {
        Ok(json) => Message::new(json),
        Err(err) => {
            log::error!("dashboard: failed to encode {topic}.{key}: {err}");
            encode_with_id(topic, key, id, &())
        }
    }
}

pub fn encode_json_with_id(topic: &str, key: &str, id: Option<u64>, value: &str) -> Message {
    let topic = serde_json::Value::from(topic);
    let key = serde_json::Value::from(key);
    let id = id.map(|id| format!(r#""id":{id},"#)).unwrap_or_default();
    Message::new(format!(
        r#"{{"topic":{topic},"key":{key},{id}"value":{value}}}"#
    ))
}

#[derive(Clone, Copy, Default)]
struct Volume {
    messages: usize,
    bytes: usize,
}

struct Traffic {
    since: Instant,
    by_key: BTreeMap<(&'static str, String), Volume>,
}

impl Traffic {
    fn new() -> Self {
        Self {
            since: Instant::now(),
            by_key: BTreeMap::new(),
        }
    }

    fn describe(self) -> String {
        let seconds = self.since.elapsed().as_secs();
        let total = self
            .by_key
            .values()
            .fold(0usize, |sum, volume| sum.saturating_add(volume.bytes));
        let mut keys: Vec<_> = self.by_key.into_iter().collect();
        keys.sort_by_key(|(_, volume)| std::cmp::Reverse(volume.bytes));
        let top = keys
            .iter()
            .take(TRAFFIC_TOP)
            .map(|((topic, key), volume)| {
                format!(
                    "{topic}.{key} {} in {}",
                    bytes_text(volume.bytes),
                    volume.messages
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("published {} in {seconds} s: {top}", bytes_text(total))
    }
}

fn bytes_text(bytes: usize) -> String {
    let bytes = bytes as f64;
    if bytes >= 1e6 {
        format!("{:.1} MB", bytes / 1e6)
    } else if bytes >= 1e3 {
        format!("{:.0} KB", bytes / 1e3)
    } else {
        format!("{bytes:.0} B")
    }
}

pub struct Publisher {
    retained: Mutex<BTreeMap<(&'static str, &'static str), Message>>,
    sender: broadcast::Sender<Message>,
    traffic: Mutex<Traffic>,
    /// Subscriptions ever made.
    joined: AtomicU64,
}

impl Publisher {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            retained: Mutex::new(BTreeMap::new()),
            sender,
            traffic: Mutex::new(Traffic::new()),
            joined: AtomicU64::new(0),
        }
    }

    pub fn publish<T: Serialize>(&self, topic: &'static str, key: &'static str, value: &T) {
        let mut message = encode(topic, key, value);
        message.supersedes = Some(Supersedes::Key(topic, key));
        self.retained
            .lock()
            .unwrap()
            .insert((topic, key), message.clone());
        self.note(topic, key, &message);
        // An error here only means nobody is listening yet.
        let _ = self.sender.send(message);
    }

    /// Not encoded at all while nobody is connected.
    pub fn publish_ephemeral<T: Serialize>(&self, topic: &'static str, key: &str, value: &T) {
        self.broadcast(topic, key, value, None);
    }

    /// Everything held for one slot, so an older one still queued for a slow client can be dropped.
    pub fn publish_update<T: Serialize>(
        &self,
        topic: &'static str,
        key: &'static str,
        slot: Slot,
        value: &T,
    ) {
        self.broadcast(topic, key, value, Some(Supersedes::Slot(topic, key, slot)));
    }

    fn broadcast<T: Serialize>(
        &self,
        topic: &'static str,
        key: &str,
        value: &T,
        supersedes: Option<Supersedes>,
    ) {
        if self.sender.receiver_count() == 0 {
            return;
        }
        let mut message = encode(topic, key, value);
        message.supersedes = supersedes;
        self.note(topic, key, &message);
        let _ = self.sender.send(message);
    }

    /// Only while this module's log is at debug.
    fn note(&self, topic: &'static str, key: &str, message: &Message) {
        if !log::log_enabled!(log::Level::Debug) {
            return;
        }
        let mut traffic = self.traffic.lock().unwrap();
        let volume = traffic.by_key.entry((topic, key.to_owned())).or_default();
        volume.messages = volume.messages.saturating_add(1);
        volume.bytes = volume.bytes.saturating_add(message.wire_len(true));
        if traffic.since.elapsed() < TRAFFIC_REPORT {
            return;
        }
        let report = std::mem::replace(&mut *traffic, Traffic::new());
        drop(traffic);
        log::debug!("dashboard: {}", report.describe());
    }

    /// For bulk snapshots whose incremental changes go out separately.
    pub fn retain_only<T: Serialize>(&self, topic: &'static str, key: &'static str, value: &T) {
        let message = encode(topic, key, value);
        self.retained.lock().unwrap().insert((topic, key), message);
    }

    /// Drops a retained value, so a client connecting is not handed it stale.
    pub fn forget(&self, topic: &'static str, key: &'static str) {
        self.retained.lock().unwrap().remove(&(topic, key));
    }

    pub fn snapshot(&self) -> Vec<Message> {
        self.retained.lock().unwrap().values().cloned().collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Message> {
        self.joined.fetch_add(1, Ordering::Relaxed);
        self.sender.subscribe()
    }

    pub fn joined(&self) -> u64 {
        self.joined.load(Ordering::Relaxed)
    }

    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

pub struct Debounced<T> {
    topic: &'static str,
    key: &'static str,
    last: Option<T>,
}

impl<T> Debounced<T> {
    pub fn new(topic: &'static str, key: &'static str) -> Self {
        Self {
            topic,
            key,
            last: None,
        }
    }

    pub fn last(&self) -> Option<&T> {
        self.last.as_ref()
    }
}

impl<T: Serialize + PartialEq> Debounced<T> {
    pub fn publish(&mut self, publisher: &Publisher, value: T) {
        if self.last.as_ref() == Some(&value) {
            return;
        }
        publisher.publish(self.topic, self.key, &value);
        self.last = Some(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Unserializable;

    impl Serialize for Unserializable {
        fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("this value cannot be encoded"))
        }
    }

    #[test]
    fn test_an_unencodable_value_is_sent_as_null_and_a_reply_keeps_its_id() {
        assert_eq!(
            &*encode("summary", "broken", &Unserializable),
            r#"{"topic":"summary","key":"broken","value":null}"#
        );
        let message = encode_with_id("sum\"mary", "bro\\ken", Some(5), &Unserializable);
        let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!({ "topic": "sum\"mary", "key": "bro\\ken", "id": 5, "value": null })
        );
    }

    #[test]
    fn test_a_value_encoded_ahead_is_enveloped_as_encode_would() {
        let value = serde_json::json!({ "rows": [1, 2], "name": "a \"quoted\" name" });
        let json = serde_json::to_string(&value).unwrap();
        for id in [None, Some(0), Some(u64::MAX)] {
            assert_eq!(
                &*encode_json_with_id("summary", "misses", id, &json),
                &*encode_with_id("summary", "misses", id, &value),
            );
        }
    }

    #[test]
    fn test_a_debounce_sends_a_value_once_and_remembers_it() {
        // Callers compare against it, so it holds the value, not a hash.
        let publisher = Publisher::new();
        let mut receiver = publisher.subscribe();
        let mut debounced: Debounced<u64> = Debounced::new("summary", "root_slot");
        assert_eq!(debounced.last(), None);

        debounced.publish(&publisher, 7);
        debounced.publish(&publisher, 7);
        assert_eq!(debounced.last(), Some(&7));
        assert!(receiver.try_recv().is_ok());
        assert!(receiver.try_recv().is_err());

        debounced.publish(&publisher, 9);
        assert_eq!(debounced.last(), Some(&9));
    }

    #[test]
    fn test_retained_snapshot_replays_latest_value_only() {
        let publisher = Publisher::new();
        publisher.publish("summary", "root_slot", &1u64);
        publisher.publish("summary", "root_slot", &2u64);
        let snapshot = publisher.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot[0].contains(r#""value":2"#));
    }

    #[test]
    fn test_ephemeral_messages_are_not_replayed() {
        let publisher = Publisher::new();
        publisher.publish_ephemeral("slot", "update", &1u64);
        assert!(publisher.snapshot().is_empty());
    }

    #[test]
    fn test_retain_only_updates_snapshot_without_broadcasting() {
        let publisher = Publisher::new();
        let mut receiver = publisher.subscribe();
        publisher.retain_only("peers", "all", &[1u64, 2]);
        assert_eq!(publisher.snapshot().len(), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn test_a_forgotten_value_leaves_the_snapshot() {
        let publisher = Publisher::new();
        publisher.retain_only("slot", "overview", &[1u64]);
        publisher.publish("summary", "root_slot", &2u64);
        publisher.forget("slot", "overview");
        publisher.forget("slot", "overview");
        let snapshot = publisher.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert!(snapshot[0].contains(r#""key":"root_slot""#));
    }

    #[test]
    fn test_subscribers_are_counted_while_they_are_attached() {
        let publisher = Publisher::new();
        assert_eq!(publisher.subscriber_count(), 0);
        let first = publisher.subscribe();
        let second = publisher.subscribe();
        assert_eq!(publisher.subscriber_count(), 2);
        drop(first);
        assert_eq!(publisher.subscriber_count(), 1);
        drop(second);
        assert_eq!(publisher.subscriber_count(), 0);
    }

    #[test]
    fn test_query_responses_carry_the_request_id() {
        let message = encode_with_id("summary", "displays", Some(42), &());
        assert_eq!(
            &*message,
            r#"{"topic":"summary","key":"displays","id":42,"value":null}"#
        );
    }

    #[test]
    fn test_a_long_message_is_also_kept_deflated_and_a_short_one_is_not() {
        let long = encode("summary", "host", &vec![7u64; 400]);
        assert!(long.len() >= DEFLATE_FROM);
        assert!(matches!(long.frame(true), Frame::Binary(_)));
        assert!(long.wire_len(true) < long.wire_len(false));
        assert!(matches!(long.frame(false), Frame::Text(_)));

        let short = encode("summary", "root_slot", &7u64);
        assert!(matches!(short.frame(true), Frame::Text(_)));
        assert_eq!(short.wire_len(true), short.len());
    }

    #[test]
    fn test_a_message_is_deflated_only_once_a_client_takes_it() {
        let message = encode("summary", "host", &vec![7u64; 400]);
        let copy = message.clone();
        assert!(message.deflated.get().is_none());
        assert!(matches!(message.frame(false), Frame::Text(_)));
        assert!(message.deflated.get().is_none());
        assert!(matches!(copy.frame(true), Frame::Binary(_)));
        assert!(message.deflated.get().is_some());
    }

    #[test]
    fn test_an_event_with_nobody_listening_is_dropped_unencoded() {
        struct Untouched;
        impl Serialize for Untouched {
            fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
                panic!("encoded with nobody listening");
            }
        }
        let publisher = Publisher::new();
        publisher.publish_ephemeral("slot", "update", &Untouched);
        let mut receiver = publisher.subscribe();
        publisher.publish_ephemeral("slot", "update", &1u64);
        assert_eq!(
            &*receiver.try_recv().unwrap(),
            r#"{"topic":"slot","key":"update","value":1}"#
        );
    }

    /// What a client is sent of everything queued for it.
    fn coalesced(receiver: &mut broadcast::Receiver<Message>) -> Vec<String> {
        coalesce(std::iter::from_fn(|| receiver.try_recv().ok()).collect())
            .iter()
            .map(Message::to_string)
            .collect()
    }

    #[test]
    fn test_a_burst_sends_a_retained_key_once_with_its_newest_value() {
        let publisher = Publisher::new();
        let mut receiver = publisher.subscribe();
        publisher.publish("summary", "root_slot", &1u64);
        publisher.publish_ephemeral("slot", "update", &10u64);
        publisher.publish("summary", "root_slot", &2u64);
        publisher.publish_ephemeral("slot", "update", &11u64);
        publisher.publish("summary", "cluster", &"testnet");
        assert_eq!(
            coalesced(&mut receiver),
            [
                r#"{"topic":"slot","key":"update","value":10}"#,
                r#"{"topic":"summary","key":"root_slot","value":2}"#,
                r#"{"topic":"slot","key":"update","value":11}"#,
                r#"{"topic":"summary","key":"cluster","value":"testnet"}"#,
            ]
        );
    }

    #[test]
    fn test_a_burst_sends_each_slot_once_with_its_newest_entry() {
        let publisher = Publisher::new();
        let mut receiver = publisher.subscribe();
        publisher.publish_update(TOPIC_SLOT, "update", 7, &"7 first shred");
        publisher.publish_update(TOPIC_SLOT, "update", 8, &"8 first shred");
        publisher.publish_ephemeral(TOPIC_SUMMARY, "tps_sample", &1u64);
        publisher.publish_update(TOPIC_SLOT, "update", 7, &"7 rooted");
        publisher.publish_ephemeral(TOPIC_SUMMARY, "tps_sample", &2u64);
        assert_eq!(
            coalesced(&mut receiver),
            [
                r#"{"topic":"slot","key":"update","value":"8 first shred"}"#,
                r#"{"topic":"summary","key":"tps_sample","value":1}"#,
                r#"{"topic":"slot","key":"update","value":"7 rooted"}"#,
                r#"{"topic":"summary","key":"tps_sample","value":2}"#,
            ]
        );
    }

    #[test]
    fn test_updates_under_different_keys_to_one_slot_are_all_sent() {
        let publisher = Publisher::new();
        let mut receiver = publisher.subscribe();
        publisher.publish_update(TOPIC_SLOT, "update", 7, &1u64);
        publisher.publish_update(TOPIC_SUMMARY, "produced_block", 7, &2u64);
        assert_eq!(coalesced(&mut receiver).len(), 2);
    }

    #[test]
    fn test_traffic_report_names_the_heaviest_keys_first() {
        let mut traffic = Traffic::new();
        traffic.by_key.insert(
            ("summary", "host".to_owned()),
            Volume {
                messages: 3,
                bytes: 1_500,
            },
        );
        traffic.by_key.insert(
            ("slot", "update".to_owned()),
            Volume {
                messages: 40,
                bytes: 2_500_000,
            },
        );
        let line = traffic.describe();
        assert!(line.starts_with("published 2.5 MB in "), "{line}");
        assert!(
            line.ends_with("slot.update 2.5 MB in 40, summary.host 2 KB in 3"),
            "{line}"
        );
    }
}
