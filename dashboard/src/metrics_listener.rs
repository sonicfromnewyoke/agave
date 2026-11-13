//! The validator's metrics as its InfluxDB writer posts them: every ten seconds one
//! `POST /write?db=..&u=..&p=..&precision=n` whose body is line protocol, a point a line. The
//! validator is pointed here with
//! `SOLANA_METRICS_CONFIG=host=http://<listen>,db=dashboard,u=dashboard,p=dashboard`. Each point
//! goes to the metrics tap with its own timestamp, and each batch, as it came, to the operator's
//! own InfluxDB where one is named, since the validator writes to one destination only.

use {
    crate::metrics_tap::{MetricsTap, number_named},
    solana_metrics::datapoint::DataPoint,
    solana_time_utils::AtomicInterval,
    std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::{Duration, UNIX_EPOCH},
    },
    tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    },
};

/// Far beyond a request line and a few headers.
const MAX_HEAD: usize = 16 * 1024;
/// A batch holds at most 40,000 points, each well under a kilobyte.
const MAX_BODY: usize = 64 * 1024 * 1024;
/// Distinct point, tag and field names; agave's own come to a few thousand.
const MAX_NAMES: usize = 16_384;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
/// The validator reports what its writer dropped with every batch; one warning a minute is enough.
const LOST_WARNING_MILLIS: u64 = 60_000;
/// The point the writer appends to each batch, about the batch itself.
const WRITER_STATS: &str = "metrics";

/// Where every batch is passed on: an InfluxDB's write endpoint, with its own database and
/// credentials.
#[derive(Clone, Debug)]
pub struct Forward {
    url: String,
}

impl Forward {
    /// From a `SOLANA_METRICS_CONFIG` string, `host=..,db=..,u=..,p=..`, all four parts required as
    /// the validator requires them.
    pub fn parse(config: &str) -> Result<Self, String> {
        let (mut host, mut db, mut user, mut password) = (None, None, None, None);
        for part in config.split(',') {
            let (key, value) = part
                .split_once('=')
                .ok_or_else(|| format!("'{part}' is not key=value"))?;
            let slot = match key {
                "host" => &mut host,
                "db" => &mut db,
                "u" => &mut user,
                "p" => &mut password,
                _ => return Err(format!("unknown key '{key}'")),
            };
            *slot = Some(value.to_string()).filter(|value| !value.is_empty());
        }
        match (host, db, user, password) {
            (Some(host), Some(db), Some(user), Some(password)) => Ok(Self {
                url: format!(
                    "{}/write?db={db}&u={user}&p={password}&precision=n",
                    host.trim_end_matches('/')
                ),
            }),
            _ => Err("host, db, u and p are all required".to_string()),
        }
    }
}

/// Point, tag and field names as `'static` strings, which a `DataPoint` holds: each distinct name
/// is leaked once, up to [`MAX_NAMES`].
#[derive(Default)]
struct Names(Mutex<HashSet<&'static str>>);

impl Names {
    fn intern(&self, name: &str) -> Option<&'static str> {
        let mut names = self.0.lock().unwrap();
        if let Some(held) = names.get(name) {
            return Some(held);
        }
        if names.len() >= MAX_NAMES {
            return None;
        }
        let leaked: &'static str = Box::leak(name.to_owned().into_boxed_str());
        names.insert(leaked);
        Some(leaked)
    }
}

/// The tokens of `text` between `separator`s that are not inside a quoted string; agave escapes
/// nothing but the quotes inside a string field.
fn split_outside_quotes(text: &str, separator: char) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (at, char) in text.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match char {
            '\\' if quoted => escaped = true,
            '"' => quoted = !quoted,
            _ if char == separator && !quoted => {
                tokens.push(&text[start..at]);
                start = at.saturating_add(char.len_utf8());
            }
            _ => {}
        }
    }
    tokens.push(&text[start..]);
    tokens
}

/// One line, `name[,tag=value...] field=value[,field=value...] <nanoseconds>`, as agave's
/// `serialize_points` writes it. Field values are kept as written (`1i`, `"text"`, `true`, `0.5`),
/// which is how the validator held them and how the tap reads them.
fn parse_line(line: &str, names: &Names) -> Option<DataPoint> {
    let line = line.trim_end_matches('\r');
    if line.is_empty() {
        return None;
    }
    let tokens = split_outside_quotes(line, ' ');
    let [head, fields, nanos] = tokens.as_slice() else {
        return None;
    };
    let nanos: u64 = nanos.parse().ok()?;
    let mut head = head.split(',');
    let name = names.intern(head.next()?)?;
    let mut point = DataPoint {
        name,
        timestamp: UNIX_EPOCH.checked_add(Duration::from_nanos(nanos))?,
        tags: Vec::new(),
        fields: Vec::new(),
    };
    for tag in head {
        let (key, value) = tag.split_once('=')?;
        point.tags.push((names.intern(key)?, value.to_string()));
    }
    for field in split_outside_quotes(fields, ',') {
        let (key, value) = field.split_once('=')?;
        point.fields.push((names.intern(key)?, value.to_string()));
    }
    Some(point)
}

/// What a batch held, for the log.
#[derive(Debug, Default, PartialEq, Eq)]
struct Batch {
    points: usize,
    unread: usize,
    /// What the validator's writer says it dropped before this batch.
    lost: u64,
}

fn feed(body: &str, names: &Names, tap: &MetricsTap) -> Batch {
    let mut batch = Batch::default();
    for line in body.lines() {
        let Some(point) = parse_line(line, names) else {
            batch.unread = batch
                .unread
                .saturating_add(usize::from(!line.trim().is_empty()));
            continue;
        };
        if point.name == WRITER_STATS {
            batch.lost = number_named(&point, "points_lost").unwrap_or_default();
        }
        tap.observe_external(&point);
        batch.points = batch.points.saturating_add(1);
    }
    batch
}

/// Serves the validator's metrics writer until the process ends.
pub async fn serve(listener: TcpListener, tap: Arc<MetricsTap>, forward: Option<Forward>) {
    let names = Arc::new(Names::default());
    let forwarder = forward.map(|forward| {
        let client = reqwest::Client::builder()
            .timeout(FORWARD_TIMEOUT)
            .build()
            .expect("an HTTP client");
        (client, forward)
    });
    let lost_warned = Arc::new(AtomicInterval::default());
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        log::debug!("dashboard: metrics writer connected from {peer}");
        tokio::spawn(connection(
            stream,
            tap.clone(),
            names.clone(),
            forwarder.clone(),
            lost_warned.clone(),
        ));
    }
}

/// One writer connection: requests one after another, each answered before its body is read into
/// the tap, so the writer's five-second timeout is never at stake.
async fn connection(
    stream: TcpStream,
    tap: Arc<MetricsTap>,
    names: Arc<Names>,
    forwarder: Option<(reqwest::Client, Forward)>,
    lost_warned: Arc<AtomicInterval>,
) {
    let mut stream = BufReader::new(stream);
    loop {
        let request = match read_request(&mut stream).await {
            Ok(Some(request)) => request,
            Ok(None) => return,
            Err(status) => {
                let _ = stream.write_all(response(status).as_bytes()).await;
                return;
            }
        };
        let status = if request.write { 204 } else { 404 };
        if stream.write_all(response(status).as_bytes()).await.is_err() {
            return;
        }
        if !request.write {
            continue;
        }
        if let Some((client, forward)) = &forwarder {
            let send = client
                .post(forward.url.as_str())
                .body(request.body.clone())
                .send();
            tokio::spawn(async move {
                match send.await {
                    Ok(answer) if !answer.status().is_success() => {
                        log::warn!("dashboard: forwarded metrics refused: {}", answer.status())
                    }
                    Err(err) => log::warn!("dashboard: forwarding metrics failed: {err}"),
                    Ok(_) => {}
                }
            });
        }
        let (tap, names, lost_warned) = (tap.clone(), names.clone(), lost_warned.clone());
        tokio::task::spawn_blocking(move || {
            let body = String::from_utf8_lossy(&request.body);
            let batch = feed(&body, &names, &tap);
            log::debug!("dashboard: metrics batch {batch:?}");
            if batch.lost > 0 && lost_warned.should_update_ext(LOST_WARNING_MILLIS, false) {
                log::warn!(
                    "dashboard: the validator's metrics writer dropped {} points, so some figures \
                     undercount: raise SOLANA_METRICS_MAX_POINTS_PER_SECOND",
                    batch.lost
                );
            }
        });
    }
}

struct Request {
    /// A `POST` to `/write`, the one request the writer makes.
    write: bool,
    body: Vec<u8>,
}

fn response(status: u16) -> String {
    let reason = http::StatusCode::from_u16(status)
        .ok()
        .and_then(|code| code.canonical_reason())
        .unwrap_or_default();
    format!("HTTP/1.1 {status} {reason}\r\nContent-Length: 0\r\n\r\n")
}

/// The next request on the connection; `None` once the writer closes it.
async fn read_request(stream: &mut BufReader<TcpStream>) -> Result<Option<Request>, u16> {
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let room = MAX_HEAD.saturating_sub(head.len());
        if room == 0 {
            return Err(400);
        }
        match (&mut *stream)
            .take(room as u64)
            .read_until(b'\n', &mut head)
            .await
        {
            Ok(0) | Err(_) => return Ok(None),
            Ok(_) => {}
        }
    }
    let head = String::from_utf8_lossy(&head);
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    let (method, target) = (
        parts.next().unwrap_or_default(),
        parts.next().unwrap_or_default(),
    );
    let length = lines
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.trim().eq_ignore_ascii_case("content-length"))
        .map(|(_, value)| value.trim().parse::<usize>())
        .transpose()
        .map_err(|_| 400u16)?
        .unwrap_or_default();
    if length > MAX_BODY {
        return Err(413);
    }
    let mut body = Vec::new();
    match (&mut *stream)
        .take(length as u64)
        .read_to_end(&mut body)
        .await
    {
        Ok(read) if read == length => Ok(Some(Request {
            write: method == "POST" && (target == "/write" || target.starts_with("/write?")),
            body,
        })),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_metrics::metrics::serialize_points};

    fn point(name: &'static str) -> DataPoint {
        let mut point = DataPoint::new(name);
        point.timestamp = UNIX_EPOCH
            .checked_add(Duration::from_nanos(1_700_000_000_123_456_789))
            .unwrap();
        point
    }

    #[test]
    fn test_a_line_reads_back_as_the_validator_wrote_it() {
        let mut written = point("replay-slot-stats");
        written
            .add_tag("is_leader", "true")
            .add_field_i64("slot", 42)
            .add_field_f64("rate", 0.5)
            .add_field_bool("full", true)
            .add_field_str("costliest_account", "a, b \"quoted\"");
        let body = serialize_points(&vec![written.clone()], "Host1d");
        let names = Names::default();
        let read = parse_line(body.lines().next().unwrap(), &names).unwrap();
        assert_eq!(read.name, written.name);
        assert_eq!(read.timestamp, written.timestamp);
        assert_eq!(read.fields, written.fields);
        assert_eq!(
            read.tags,
            vec![
                ("host_id", "Host1d".to_string()),
                ("is_leader", "true".to_string())
            ]
        );
    }

    #[test]
    fn test_malformed_lines_are_skipped() {
        let names = Names::default();
        assert!(parse_line("", &names).is_none());
        assert!(parse_line("name field=1i", &names).is_none());
        assert!(parse_line("name field=1i notanumber", &names).is_none());
        assert!(parse_line("name fieldwithoutvalue 1", &names).is_none());
    }

    #[test]
    fn test_names_are_interned_once_and_bounded() {
        let names = Names::default();
        let first = names.intern("slot").unwrap();
        let again = names.intern(&String::from("slot")).unwrap();
        assert!(std::ptr::eq(first, again));
        for index in 0..MAX_NAMES {
            names.intern(&format!("name-{index}"));
        }
        assert!(names.intern("one-too-many").is_none());
        assert!(names.intern("slot").is_some());
    }

    #[test]
    fn test_a_batch_feeds_the_tap_and_reports_what_the_writer_lost() {
        let mut stats = point(WRITER_STATS);
        stats.add_field_i64("points_lost", 7);
        let mut packets = point("shred_fetch_receiver");
        packets.add_field_i64("packets_count", 5);
        let body = serialize_points(&vec![packets, stats], "Host1d") + "garbage\n";
        let tap = MetricsTap::default();
        let batch = feed(&body, &Names::default(), &tap);
        assert_eq!(
            batch,
            Batch {
                points: 2,
                unread: 1,
                lost: 7
            }
        );
        assert_eq!(tap.counters().network.shreds_turbine, 5);
    }

    #[test]
    fn test_a_forward_needs_every_part() {
        let forward =
            Forward::parse("host=https://metrics.example:8086/,db=mainnet-beta,u=me,p=secret")
                .unwrap();
        assert_eq!(
            forward.url,
            "https://metrics.example:8086/write?db=mainnet-beta&u=me&p=secret&precision=n"
        );
        assert!(Forward::parse("host=https://metrics.example:8086,db=x,u=me").is_err());
        assert!(Forward::parse("host=a,db=b,u=c,p=d,extra=e").is_err());
    }

    #[tokio::test]
    async fn test_a_write_is_answered_and_its_points_reach_the_tap() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let tap = Arc::new(MetricsTap::default());
        tokio::spawn(serve(listener, tap.clone(), None));
        let mut packets = point("shred_fetch_receiver");
        packets.add_field_i64("packets_count", 3);
        let body = serialize_points(&vec![packets], "Host1d");
        let client = reqwest::Client::new();
        for _ in 0..2 {
            let answer = client
                .post(format!(
                    "http://{address}/write?db=dashboard&u=a&p=b&precision=n"
                ))
                .body(body.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(answer.status(), 204);
        }
        let missing = client
            .post(format!("http://{address}/query"))
            .body("x")
            .send()
            .await
            .unwrap();
        assert_eq!(missing.status(), 404);
        for _ in 0..100 {
            if tap.counters().network.shreds_turbine == 6 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("the points did not reach the tap");
    }
}
