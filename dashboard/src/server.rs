//! One port serving the embedded single-page app over HTTP and the state feed
//! over a websocket at `/websocket`. Routing peeks at the request head, which
//! leaves the bytes in the socket for soketto's handshake.

use {
    crate::{
        collect::{EpochInfo, Replies},
        history::SlotHistory,
        proto::{
            DEFLATE_PROTOCOL, Frame, Message, Publisher, Request, coalesce, encode_json_with_id,
        },
        search::{SearchParams, future, leader_rows, search, summarize},
        validator_info::ValidatorInfoCache,
    },
    serde::{Serialize, de::DeserializeOwned},
    soketto::{
        Incoming,
        handshake::{Server, server},
    },
    solana_clock::Slot,
    solana_net_utils::token_bucket::TokenBucket,
    solana_time_utils::timestamp,
    std::{
        io,
        net::IpAddr,
        sync::{Arc, PoisonError, RwLock},
    },
    thiserror::Error,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        pin, select,
        sync::{
            Semaphore,
            broadcast::error::{RecvError, TryRecvError},
        },
        task::spawn_blocking,
        time::{Duration, sleep, timeout},
    },
    tokio_util::compat::{Compat, TokioAsyncReadCompatExt},
};

const WEBSOCKET_PATH: &str = "/websocket";

/// One search at a time across every connection, since each can read the whole history.
static SEARCHES: Semaphore = Semaphore::const_new(1);

const MAX_REQUEST_HEAD: usize = 8192;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Anything larger is a client bug or an attempt to make the server allocate.
const MAX_CLIENT_MESSAGE: usize = 4096;

/// Otherwise a viewer that stops reading holds a slot until TCP notices.
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);

/// Unsent bytes the kernel holds for a websocket. A slow viewer's backlog then waits in the
/// update queue, where a burst is coalesced, rather than in a socket buffer that is sent as is.
#[cfg(target_os = "linux")]
const UNSENT_LIMIT: u32 = 16 * 1024;

const MAX_WEBSOCKET_CLIENTS: usize = 64;

/// Requests answered at once; after those, one every quarter second.
const REPLY_BURST: u64 = 8;
const REPLIES_PER_SECOND: f64 = 4.0;

/// Out of descriptors, `accept` fails at once, so retrying straight away spins.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(100);

/// A ceiling, not a throttle: each connection holds a task, a socket and its buffers.
const MAX_CONNECTIONS: usize = 256;

#[derive(Clone)]
struct Limits {
    connections: Arc<Semaphore>,
    websockets: Arc<Semaphore>,
}

impl Limits {
    fn new() -> Self {
        Self {
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            websockets: Arc::new(Semaphore::new(MAX_WEBSOCKET_CLIENTS)),
        }
    }
}

const MISSING_FRONTEND: &str = include_str!("missing_frontend.html");

mod assets {
    include!(concat!(env!("OUT_DIR"), "/assets.rs"));
}

#[derive(Debug, Error)]
enum ConnectionError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("handshake error: {0}")]
    Handshake(#[from] soketto::handshake::Error),
    #[error("connection error: {0}")]
    Connection(#[from] soketto::connection::Error),
    #[error("client fell too far behind and was disconnected")]
    Lagged,
    #[error("client sent an oversized message of {0} bytes")]
    Oversized(usize),
}

#[derive(Clone)]
pub struct Handles {
    pub publisher: Arc<Publisher>,
    pub history: Arc<RwLock<SlotHistory>>,
    pub info_cache: Arc<RwLock<ValidatorInfoCache>>,
    pub epochs: Arc<RwLock<Vec<EpochInfo>>>,
    pub replies: Arc<Replies>,
}

pub async fn serve(listener: TcpListener, handles: Handles, allowed_hosts: Arc<[String]>) {
    let limits = Limits::new();
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(err) => {
                log::warn!("dashboard: accept failed: {err}");
                sleep(ACCEPT_BACKOFF).await;
                continue;
            }
        };
        let handles = handles.clone();
        let limits = limits.clone();
        let allowed_hosts = allowed_hosts.clone();
        tokio::spawn(async move {
            if let Err(err) = handle(socket, handles, limits, &allowed_hosts).await {
                log::debug!("dashboard: connection from {peer} ended: {err}");
            }
        });
    }
}

async fn handle(
    mut socket: TcpStream,
    handles: Handles,
    limits: Limits,
    allowed_hosts: &[String],
) -> Result<(), ConnectionError> {
    // Taken before the head is read, so a flood over the cap holds no task or buffer.
    let Ok(_connection) = limits.connections.try_acquire_owned() else {
        log::info!("dashboard: refusing a connection, {MAX_CONNECTIONS} already being served");
        socket.shutdown().await?;
        return Ok(());
    };

    let (head, head_len) =
        within(REQUEST_TIMEOUT, "request head", peek_request_head(&socket)).await??;

    // The peek stops short of the blank line only at its limit.
    if !head.contains("\r\n\r\n") {
        log::debug!("dashboard: refusing a request head of {MAX_REQUEST_HEAD} bytes or more");
        let reply = Reply::text(431, b"request head too large");
        return answer_http(socket, &head, head_len, reply).await;
    }

    // Checked before anything is served: a page served to a rebound name would be
    // same-origin with the dashboard.
    if !host_is_allowed(&head, allowed_hosts) {
        log::debug!(
            "dashboard: refusing host {:?}; add it with --allowed-host",
            for_logging(header(&head, "host").unwrap_or("(absent)"))
        );
        let reply = Reply::text(421, b"unrecognised host");
        return answer_http(socket, &head, head_len, reply).await;
    }

    if is_websocket_upgrade(&head) {
        if !origin_is_allowed(&head) {
            log::debug!(
                "dashboard: refusing websocket from origin {:?}",
                for_logging(header(&head, "origin").unwrap_or("(absent)"))
            );
            let reply = Reply::text(403, b"origin not allowed");
            return answer_http(socket, &head, head_len, reply).await;
        }
        let Ok(_permit) = limits.websockets.try_acquire_owned() else {
            log::info!(
                "dashboard: refusing a websocket, {MAX_WEBSOCKET_CLIENTS} clients already \
                 connected"
            );
            let reply = Reply::text(503, b"too many dashboard clients");
            return answer_http(socket, &head, head_len, reply).await;
        };
        let path = request_path(&head).to_string();
        serve_websocket(socket, handles, &path).await
    } else {
        let reply = http_reply(&head);
        answer_http(socket, &head, head_len, reply).await
    }
}

struct Reply {
    status: u16,
    content_type: &'static str,
    body: &'static [u8],
    immutable: bool,
    /// The content-encoding and vary lines, empty for a body with one form.
    coding: &'static str,
}

impl Reply {
    fn text(status: u16, body: &'static [u8]) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body,
            immutable: false,
            coding: "",
        }
    }
}

/// Reads the head off the socket, answers and closes. A HEAD request is sent the headers alone.
async fn answer_http(
    mut socket: TcpStream,
    head: &str,
    head_len: usize,
    reply: Reply,
) -> Result<(), ConnectionError> {
    // Closing with unread data sends RST rather than FIN, which truncates the response.
    let mut consumed = vec![0u8; head_len];
    socket.read_exact(&mut consumed).await?;
    let response = response_head(&reply);
    // Embedded assets are written from the binary, not copied.
    let body = if head.starts_with("HEAD ") {
        &[]
    } else {
        reply.body
    };
    within(WRITE_TIMEOUT, "http response", async {
        socket.write_all(response.as_bytes()).await?;
        socket.write_all(body).await?;
        socket.flush().await?;
        socket.shutdown().await
    })
    .await??;
    Ok(())
}

/// Pause between peeks of a head that has stopped arriving: `peek` never
/// clears readiness, so `readable` returns at once.
const HEAD_POLL: Duration = Duration::from_millis(20);

async fn peek_request_head(socket: &TcpStream) -> io::Result<(String, usize)> {
    let mut buffer = vec![0u8; MAX_REQUEST_HEAD];
    let mut last_peeked = 0;
    loop {
        socket.readable().await?;
        let peeked = match socket.peek(&mut buffer).await {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no request")),
            Ok(peeked) => peeked,
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => continue,
            Err(err) => return Err(err),
        };
        if peeked == last_peeked {
            sleep(HEAD_POLL).await;
            continue;
        }
        last_peeked = peeked;
        // `from_utf8_lossy` can change the byte count, so the length comes from
        // what was actually peeked rather than from the string.
        let head = String::from_utf8_lossy(&buffer[..peeked]);
        if head.contains("\r\n\r\n") || peeked == MAX_REQUEST_HEAD {
            return Ok((head.into_owned(), peeked));
        }
    }
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .skip(1) // the request line is not a header
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
}

/// Ports are dropped: a dashboard proxied on another port is the same machine.
fn host_of(value: &str) -> &str {
    let value = value
        .rsplit_once("//")
        .map(|(_, rest)| rest)
        .unwrap_or(value);
    // IPv6 authorities are bracketed, so the port colon is the one outside.
    if let Some(rest) = value.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    value.split(':').next().unwrap_or(value)
}

/// No `Host` at all is rejected; every HTTP/1.1 client sends one.
fn host_is_allowed(head: &str, allowed: &[String]) -> bool {
    let Some(host) = header(head, "host") else {
        return false;
    };
    let host = host_of(host);

    // An address literal is always accepted. Rebinding works by resolving a name
    // the attacker owns, so the browser sends that name, never a bare address.
    if host.parse::<IpAddr>().is_ok() {
        return true;
    }

    allowed
        .iter()
        .any(|candidate| host_of(candidate).eq_ignore_ascii_case(host))
}

/// No `Origin` is not a browser and is allowed; `null` is a sandboxed frame and is refused.
fn origin_is_allowed(head: &str) -> bool {
    let Some(origin) = header(head, "origin") else {
        return true;
    };
    if origin.eq_ignore_ascii_case("null") {
        return false;
    }
    match header(head, "host") {
        Some(host) => host_of(origin).eq_ignore_ascii_case(host_of(host)),
        None => false,
    }
}

fn is_websocket_upgrade(head: &str) -> bool {
    header(head, "upgrade").is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

fn request_path(head: &str) -> &str {
    head.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .map(|target| target.split(['?', '#']).next().unwrap_or("/"))
        .unwrap_or("/")
}

/// The page and its assets; anything but a read is refused.
fn http_reply(head: &str) -> Reply {
    if !(head.starts_with("GET ") || head.starts_with("HEAD ")) {
        return Reply::text(405, b"method not allowed");
    }
    if assets::ASSETS.is_empty() {
        return Reply {
            content_type: "text/html; charset=utf-8",
            ..Reply::text(200, MISSING_FRONTEND.as_bytes())
        };
    }
    let path = request_path(head);
    let gzip = accepts_gzip(head);
    match lookup(path, gzip) {
        // Hashed filenames cache forever; the entry document must not, or a redeploy is never
        // picked up.
        Some(found) => Reply {
            immutable: path.starts_with("/assets/"),
            ..found
        },
        // Unknown paths fall through to the SPA so client-side routes survive a hard refresh.
        None => lookup("/index.html", gzip).unwrap_or(Reply::text(404, b"not found")),
    }
}

/// Takes the gzip copy when `gzip` and the asset has one; such an asset sends `vary` either way.
fn lookup(path: &str, gzip: bool) -> Option<Reply> {
    let path = if path == "/" { "/index.html" } else { path };
    let &(_, content_type, raw, packed) =
        assets::ASSETS.iter().find(|(route, ..)| *route == path)?;
    let (body, coding) = match packed {
        Some(packed) if gzip => (packed, GZIP_HEADERS),
        Some(_) => (raw, VARY_HEADER),
        None => (raw, ""),
    };
    Some(Reply {
        content_type,
        coding,
        ..Reply::text(200, body)
    })
}

const VARY_HEADER: &str = "vary: accept-encoding\r\n";
const GZIP_HEADERS: &str = "content-encoding: gzip\r\nvary: accept-encoding\r\n";

/// `gzip;q=0` is a refusal.
fn accepts_gzip(head: &str) -> bool {
    header(head, "accept-encoding").is_some_and(|value| {
        value.split(',').any(|coding| {
            let mut parts = coding.split(';').map(str::trim);
            parts
                .next()
                .is_some_and(|name| name.eq_ignore_ascii_case("gzip"))
                && !parts.any(|param| {
                    param.split_once('=').is_some_and(|(name, q)| {
                        name.eq_ignore_ascii_case("q") && q.parse::<f32>().is_ok_and(|q| q <= 0.0)
                    })
                })
        })
    })
}

/// Sent with every response. `img-src` allows https for validator icons; `manifest-src` lets a
/// phone install the page; the inline script and style are index.html's theme stamp and splash.
const SECURITY_HEADERS: &str = concat!(
    "content-security-policy:",
    " default-src 'none';",
    " script-src 'self' 'unsafe-inline';",
    " style-src 'self' 'unsafe-inline';",
    " img-src 'self' data: https:;",
    " manifest-src 'self';",
    " connect-src 'self';",
    " font-src 'self';",
    " base-uri 'none';",
    " form-action 'none';",
    " frame-ancestors 'self'\r\n",
    "x-content-type-options: nosniff\r\n",
    "referrer-policy: no-referrer\r\n",
);

/// The length is the body's, also for a HEAD request, which is sent no body.
fn response_head(reply: &Reply) -> String {
    let Reply {
        status,
        content_type,
        body,
        immutable,
        coding,
    } = reply;
    let reason = http::StatusCode::from_u16(*status)
        .ok()
        .and_then(|code| code.canonical_reason())
        .unwrap_or_default();
    let cache = if *immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let len = body.len();
    format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: \
         {len}\r\ncache-control: {cache}\r\n{coding}{SECURITY_HEADERS}connection: close\r\n\r\n"
    )
}

async fn within<T>(
    limit: Duration,
    what: &'static str,
    future: impl Future<Output = T>,
) -> io::Result<T> {
    timeout(limit, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, what))
}

/// Sends each message, deflated for a client that takes it, then flushes. Taken by value, so each
/// message is released once sent. Cancelling a partly written frame leaves the stream
/// indeterminate, so a timeout is fatal.
async fn send_all(
    sender: &mut soketto::Sender<Compat<TcpStream>>,
    messages: impl IntoIterator<Item = Message>,
    deflate: bool,
    what: &'static str,
) -> Result<(), ConnectionError> {
    for message in messages {
        let send = async {
            match message.frame(deflate) {
                Frame::Text(text) => sender.send_text(text).await,
                Frame::Binary(bytes) => sender.send_binary(bytes).await,
            }
        };
        within(WRITE_TIMEOUT, what, send).await??;
    }
    within(WRITE_TIMEOUT, what, sender.flush()).await??;
    Ok(())
}

/// Limits unsent data only, so a fast link keeps its full window in flight.
#[cfg(target_os = "linux")]
fn limit_unsent(socket: &TcpStream) {
    if let Err(err) = socket2::SockRef::from(socket).set_tcp_notsent_lowat(UNSENT_LIMIT) {
        log::debug!("dashboard: could not limit a websocket's unsent bytes: {err}");
    }
}

#[cfg(not(target_os = "linux"))]
fn limit_unsent(_socket: &TcpStream) {}

async fn serve_websocket(
    socket: TcpStream,
    handles: Handles,
    path: &str,
) -> Result<(), ConnectionError> {
    limit_unsent(&socket);
    let mut server = Server::new(socket.compat());
    // Only a protocol the server lists is reported back from the request.
    server.add_protocol(DEFLATE_PROTOCOL);
    // Bounded like the peek.
    let (key, deflate) = within(REQUEST_TIMEOUT, "websocket request", async {
        let request = server.receive_request().await?;
        let deflate = request
            .protocols()
            .any(|protocol| protocol == DEFLATE_PROTOCOL);
        Ok::<_, soketto::handshake::Error>((request.key(), deflate))
    })
    .await??;

    let response = if path == WEBSOCKET_PATH {
        server::Response::Accept {
            key,
            protocol: deflate.then_some(DEFLATE_PROTOCOL),
        }
    } else {
        server::Response::Reject { status_code: 404 }
    };
    within(
        WRITE_TIMEOUT,
        "websocket response",
        server.send_response(&response),
    )
    .await??;
    if path != WEBSOCKET_PATH {
        return Ok(());
    }

    // Subscribing before taking the snapshot means a value that changes between
    // the two arrives as an update instead of going missing.
    let mut updates = handles.publisher.subscribe();
    let snapshot = handles.publisher.snapshot();

    // soketto applies these to what it receives only, so a frame over the bound is refused from
    // its header, before its payload is read.
    let mut builder = server.into_builder();
    builder.set_max_message_size(MAX_CLIENT_MESSAGE);
    builder.set_max_frame_size(MAX_CLIENT_MESSAGE);
    let (mut sender, mut receiver) = builder.finish();

    send_all(&mut sender, snapshot, deflate, "snapshot send")
        .await
        .inspect_err(|err| log::warn!("dashboard: snapshot send failed: {err}"))?;

    let mut incoming = Vec::new();
    let pace = TokenBucket::new(REPLY_BURST, REPLY_BURST, REPLIES_PER_SECOND);
    loop {
        // soketto's `receive` is not cancel safe, so it is polled to
        // completion in an inner loop rather than dropped by `select!`.
        let complete = {
            let receive = receiver.receive(&mut incoming);
            pin!(receive);
            loop {
                select! {
                    // Client frames are handled first so that pings and closes
                    // are not starved by a busy update stream.
                    biased;

                    received = &mut receive => match received {
                        Ok(Incoming::Data(_)) => break true,
                        Ok(Incoming::Pong(_)) => break false,
                        Ok(Incoming::Closed(_)) | Err(soketto::connection::Error::Closed) => {
                            return Ok(());
                        }
                        Err(err) => return Err(err.into()),
                    },

                    update = updates.recv() => match update {
                        Ok(message) => {
                            // A retained key queued twice goes once, with its newer value.
                            let mut burst = vec![message];
                            loop {
                                match updates.try_recv() {
                                    Ok(message) => burst.push(message),
                                    Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
                                    Err(TryRecvError::Lagged(_)) => {
                                        return Err(ConnectionError::Lagged);
                                    }
                                }
                            }
                            send_all(&mut sender, coalesce(burst), deflate, "websocket send")
                                .await?;
                        }
                        Err(RecvError::Lagged(_)) => return Err(ConnectionError::Lagged),
                        Err(RecvError::Closed) => return Ok(()),
                    },
                }
            }
        };

        // soketto restarts its count after a pong but keeps the bytes, so the bound is checked
        // on every return.
        if incoming.len() > MAX_CLIENT_MESSAGE {
            return Err(ConnectionError::Oversized(incoming.len()));
        }
        if !complete {
            continue;
        }
        while pace.consume_tokens(1).is_err() {
            let wait = pace.us_to_have_tokens(1).unwrap_or_default();
            sleep(Duration::from_micros(wait)).await;
        }
        if let Some(reply) = respond(&incoming, &handles).await {
            send_all(&mut sender, [reply], deflate, "websocket send").await?;
        }
        incoming.clear();
    }
}

/// No forged lines, terminal escapes or 8 KB entries.
fn for_logging(value: &str) -> String {
    const LIMIT: usize = 48;
    let mut out: String = value
        .chars()
        .take(LIMIT)
        .map(|c| match c {
            ' '..='~' => c,
            _ => '?',
        })
        .collect();
    if value.chars().nth(LIMIT).is_some() {
        out.push_str("...");
    }
    out
}

#[derive(serde::Deserialize)]
struct EpochParams {
    epoch: u64,
}

#[derive(serde::Deserialize)]
struct FiguresParams {
    /// The page below this slot; the newest without it.
    before: Option<Slot>,
}

#[derive(serde::Deserialize)]
struct DetailParams {
    first: Slot,
    last: Slot,
}

#[derive(serde::Deserialize)]
struct HandoffBinParams {
    /// Inclusive, in milliseconds; none for no lower bound.
    from: Option<i32>,
    /// Exclusive; none for no upper bound.
    to: Option<i32>,
}

#[derive(serde::Deserialize)]
struct SlotRangeParams {
    first_slot: Slot,
    /// Clamped by the history rather than refused.
    count: usize,
}

/// The requests that scan: a page of turns, the matches summed up, or a page of the turns to come.
#[derive(Clone, Copy)]
enum Scan {
    Search,
    Summary,
    Future,
}

/// Why a request is answered without its value.
enum Unanswered {
    /// What was wrong with the request, sent back as `{"error": ...}`.
    Bad(&'static str),
    /// Not readable or not gathered yet; sent as null.
    Missing,
    /// Sent as null, and logged.
    Unencodable(serde_json::Error),
}

impl<T> From<PoisonError<T>> for Unanswered {
    fn from(_: PoisonError<T>) -> Self {
        Self::Missing
    }
}

impl From<serde_json::Error> for Unanswered {
    fn from(err: serde_json::Error) -> Self {
        Self::Unencodable(err)
    }
}

fn parse<T: DeserializeOwned>(
    params: serde_json::Value,
    error: &'static str,
) -> Result<T, Unanswered> {
    serde_json::from_value(params).map_err(|_| Unanswered::Bad(error))
}

fn json(value: &impl Serialize) -> serde_json::Result<Arc<str>> {
    serde_json::to_string(value).map(Arc::from)
}

/// Even unknown requests get an answer, so no client waits on an id that never comes back.
async fn respond(payload: &[u8], handles: &Handles) -> Option<Message> {
    let Request {
        topic,
        key,
        id,
        params,
    } = serde_json::from_slice(payload).ok()?;
    let answered = match (topic.as_str(), key.as_str()) {
        ("slot", "search") => answer_scan(Scan::Search, params, handles).await,
        ("slot", "summary") => answer_scan(Scan::Summary, params, handles).await,
        ("slot", "future") => answer_scan(Scan::Future, params, handles).await,
        (topic, key) => answer(topic, key, params, handles),
    };
    let value = answered.unwrap_or_else(|unanswered| match unanswered {
        Unanswered::Bad(error) => serde_json::json!({ "error": error }).to_string().into(),
        Unanswered::Missing => "null".into(),
        Unanswered::Unencodable(err) => {
            log::error!("dashboard: failed to encode {topic}.{key}: {err}");
            "null".into()
        }
    });
    Some(encode_json_with_id(&topic, &key, id, &value))
}

/// Run on the blocking pool and one at a time, so a scan of the history holds up no other
/// client's updates and at most one core.
async fn answer_scan(
    scan: Scan,
    params: serde_json::Value,
    handles: &Handles,
) -> Result<Arc<str>, Unanswered> {
    let params: SearchParams = parse(params, "a scan needs a before slot")?;
    let _permit = SEARCHES.acquire().await.map_err(|_| Unanswered::Missing)?;
    let Handles {
        history,
        info_cache: info,
        epochs,
        replies,
        ..
    } = handles.clone();
    spawn_blocking(move || {
        // Held for the scan; the slot feed writes the epoch records once an epoch.
        let epochs = epochs.read()?;
        let leaders = &replies.leader_traits;
        Ok(match scan {
            Scan::Search => json(&search(&history, &info, leaders, &epochs, &params)),
            Scan::Summary => json(&summarize(&history, &info, leaders, &epochs, &params)),
            Scan::Future => json(&future(&history, &info, leaders, &epochs, &params)),
        }?)
    })
    .await
    .map_err(|_| Unanswered::Missing)?
}

fn answer(
    topic: &str,
    key: &str,
    params: serde_json::Value,
    handles: &Handles,
) -> Result<Arc<str>, Unanswered> {
    let Handles {
        history,
        info_cache,
        epochs,
        replies,
        ..
    } = handles;
    Ok(match (topic, key) {
        ("summary", "displays") => info_cache.read()?.displays_json(),
        ("summary", key @ ("misses" | "written")) => {
            replies.misses.ask(timestamp());
            let held = replies.misses.held.read()?;
            if key == "misses" {
                held.misses.clone()
            } else {
                held.written.clone()
            }
        }
        ("peers", "gossip") => {
            replies.gossip_peers.ask(timestamp());
            replies.gossip_peers.held.read()?.0.clone()
        }
        ("peers", "leaders") => json(&leader_rows(&*replies.leader_traits.read()?))?,
        ("cluster", "charts") => replies
            .ask_cluster_charts(timestamp())
            .ok_or(Unanswered::Missing)?,
        ("epoch", "query") => {
            let params: EpochParams = parse(params, "query needs an epoch")?;
            let epochs = epochs.read()?;
            json(&epochs.iter().find(|held| held.epoch == params.epoch))?
        }
        ("produced", "figures") => {
            let params: FiguresParams = parse(params, "figures takes an optional before slot")?;
            let page = replies.produced.read()?.figures(params.before);
            json(&page)?
        }
        ("produced", "detail") => {
            let params: DetailParams = parse(params, "detail needs a first and a last slot")?;
            json(&replies.produced_detail(params.first, params.last))?
        }
        ("handoff", "bin") => {
            let params: HandoffBinParams = parse(params, "bin takes an optional from and to")?;
            json(&replies.handoff_leaders_between(params.from, params.to))?
        }
        ("slot", "range") => {
            // A client waiting on an id cannot tell silence from slowness.
            let params: SlotRangeParams = parse(params, "range needs a first_slot and a count")?;
            let range = history.read()?.range(params.first_slot, params.count);
            json(&range)?
        }
        (topic, key) => {
            log::debug!(
                "dashboard: unhandled request {:?}.{:?}",
                for_logging(topic),
                for_logging(key)
            );
            return Err(Unanswered::Bad("unsupported request"));
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            collect::Encoded,
            proto::{encode, encode_with_id},
            search::LeaderTraits,
        },
        flate2::read::{GzDecoder, ZlibDecoder},
        soketto::handshake::{Client, ServerResponse},
        solana_pubkey::Pubkey,
        std::io::Read,
    };

    fn handles() -> Handles {
        Handles {
            publisher: Arc::new(Publisher::new()),
            history: Arc::new(RwLock::new(SlotHistory::new(16))),
            info_cache: Arc::default(),
            epochs: Arc::default(),
            replies: Arc::default(),
        }
    }

    fn epoch_record(epoch: u64) -> EpochInfo {
        EpochInfo {
            epoch,
            start_slot: epoch.saturating_mul(432_000),
            end_slot: epoch.saturating_mul(432_000).saturating_add(431_999),
            slots_in_epoch: 432_000,
            my_leader_slots: Vec::new(),
            leaders: vec!["LEADER".to_string()],
            turns: vec![0],
            block_cost_limit: 60_000_000,
            account_cost_limit: 12_000_000,
        }
    }

    /// The reply to a request of `topic.key` with `params`, which carries the request's id.
    async fn ask(handles: &Handles, (topic, key): (&str, &str), params: &str) -> String {
        let request = format!(r#"{{"topic":"{topic}","key":"{key}","id":7,"params":{params}}}"#);
        let reply = respond(request.as_bytes(), handles).await.unwrap();
        assert!(reply.contains(r#""id":7,"#), "{reply}");
        reply.to_string()
    }

    #[track_caller]
    fn just_asked(asked: u64) {
        assert!(
            timestamp().saturating_sub(asked) < 5_000,
            "asked at {asked}"
        );
    }

    #[tokio::test]
    async fn test_an_epoch_held_is_answered_with_its_arrays_and_one_dropped_with_nothing() {
        let handles = Handles {
            epochs: Arc::new(RwLock::new(vec![epoch_record(841), epoch_record(842)])),
            ..handles()
        };
        let held = ask(&handles, ("epoch", "query"), r#"{"epoch":841}"#).await;
        assert!(held.contains(r#""epoch":841"#), "{held}");
        assert!(held.contains(r#""LEADER""#), "{held}");
        // A validator that has not been up that long has no schedule for it.
        let dropped = ask(&handles, ("epoch", "query"), r#"{"epoch":700}"#).await;
        assert!(dropped.contains(r#""value":null"#), "{dropped}");
    }

    #[tokio::test]
    async fn test_the_display_reply_is_what_encoding_the_table_gives() {
        use crate::validator_info::ValidatorInfo;
        let handles = handles();
        let table = [
            (1u8, Some("Lantern"), Some("https://l/i.png")),
            (2, Some("a \"quoted\" name"), None),
            (3, None, Some("https://i/only.png")),
            (4, None, None),
        ]
        .map(|(byte, name, icon)| {
            (
                Pubkey::new_from_array([byte; 32]),
                ValidatorInfo {
                    name: name.map(str::to_string),
                    icon_url: icon.map(str::to_string),
                },
            )
        });
        handles.info_cache.write().unwrap().replace(table.into());
        let unnamed = Pubkey::new_from_array([4; 32]).to_string();
        for (request, id) in [
            (
                &br#"{"topic":"summary","key":"displays","id":4}"#[..],
                Some(4),
            ),
            (&br#"{"topic":"summary","key":"displays"}"#[..], None),
        ] {
            let reply = respond(request, &handles).await.unwrap();
            let expected = encode_with_id(
                "summary",
                "displays",
                id,
                &handles.info_cache.read().unwrap().displays(),
            );
            assert_eq!(&*reply, &*expected);
            assert!(reply.contains(r#""Lantern""#), "{reply}");
            assert!(reply.contains(r#""https://l/i.png""#), "{reply}");
            assert!(!reply.contains(&unnamed), "{reply}");
        }
    }

    #[tokio::test]
    async fn test_a_range_request_is_answered_and_a_bad_one_told_why() {
        let handles = handles();
        let reply = ask(&handles, ("slot", "range"), r#"{"first_slot":4,"count":2}"#).await;
        assert!(reply.contains(r#""first_slot":4"#), "{reply}");
        assert!(reply.contains(r#""rows":[null,null]"#), "{reply}");
        let bad = ask(&handles, ("slot", "range"), r#"{"first_slot":"soon"}"#).await;
        assert!(bad.contains("error"), "{bad}");
    }

    #[tokio::test]
    async fn test_a_bin_lists_the_leaders_whose_median_falls_in_it() {
        let handles = handles();
        let leader = |name: &str, p50: i32| crate::collect::LeaderFigures {
            leader: name.to_string(),
            country: Some("DE"),
            n: 20,
            p25: p50,
            p50,
            p75: p50,
            late: false,
        };
        *handles.replies.handoff_leaders.write().unwrap() = vec![
            leader("a", -5),
            leader("b", 42),
            leader("c", 47),
            leader("d", 48),
            leader("e", 400),
        ];
        let bin = async |params: &str| ask(&handles, ("handoff", "bin"), params).await;
        let middle = bin(r#"{"from":42,"to":48}"#).await;
        assert!(
            middle.contains(r#""leader":"b""#) && middle.contains(r#""leader":"c""#),
            "{middle}"
        );
        assert!(!middle.contains(r#""leader":"d""#), "{middle}");
        // The first bin takes everything below it, the last everything above.
        assert!(
            bin(r#"{"from":null,"to":6}"#)
                .await
                .contains(r#""leader":"a""#)
        );
        assert!(
            bin(r#"{"from":294,"to":null}"#)
                .await
                .contains(r#""leader":"e""#)
        );
        assert!(bin(r#"{"to":"x"}"#).await.contains("error"));
    }

    #[tokio::test]
    async fn test_figures_are_paged_newest_first() {
        let handles = handles();
        for slot in [40, 41, 42] {
            handles
                .replies
                .produced
                .write()
                .unwrap()
                .insert(crate::produced::sample_block(slot));
        }
        let reply = ask(&handles, ("produced", "figures"), r#"{"before":42}"#).await;
        assert!(reply.contains(r#""figures":[[41,"#), "{reply}");
        assert!(reply.contains(r#""held":3"#), "{reply}");
        assert!(reply.contains(r#""next":null"#), "{reply}");

        let newest = ask(&handles, ("produced", "figures"), "{}").await;
        assert!(newest.contains(r#""figures":[[42,"#), "{newest}");
    }

    #[tokio::test]
    async fn test_detail_carries_the_blocks_asked_for() {
        let handles = handles();
        handles
            .replies
            .produced
            .write()
            .unwrap()
            .insert(crate::produced::sample_block(40));
        let detail = ("produced", "detail");
        let reply = ask(&handles, detail, r#"{"first":40,"last":40}"#).await;
        assert!(reply.contains(r#""blockhash":"hash40""#), "{reply}");

        let bad = ask(&handles, detail, r#"{"first":40}"#).await;
        assert!(bad.contains("error"), "{bad}");
    }

    #[test]
    fn test_detects_a_websocket_upgrade() {
        assert!(is_websocket_upgrade(
            "GET /websocket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n"
        ));
        assert!(is_websocket_upgrade(
            "GET / HTTP/1.1\r\nupgrade:  WebSocket \r\n\r\n"
        ));
        assert!(!is_websocket_upgrade("GET / HTTP/1.1\r\nHost: x\r\n\r\n"));
    }

    fn req(headers: &str) -> String {
        format!("GET /websocket HTTP/1.1\r\n{headers}\r\n\r\n")
    }

    fn allowed() -> Vec<String> {
        vec![
            "localhost".into(),
            "127.0.0.1".into(),
            "dash.example.com".into(),
        ]
    }

    #[test]
    fn test_page_this_dashboard_served_may_open_a_socket() {
        assert!(origin_is_allowed(&req(
            "Host: dash.example.com\r\nOrigin: https://dash.example.com"
        )));
        // Behind a proxy the visitor's port and the dashboard's differ.
        assert!(origin_is_allowed(&req(
            "Host: dash.example.com\r\nOrigin: https://dash.example.com:443"
        )));
    }

    #[test]
    fn test_another_site_may_not() {
        // A websocket is exempt from the same-origin policy, so without this any page a browser
        // visits could read the feed.
        assert!(!origin_is_allowed(&req(
            "Host: dash.example.com\r\nOrigin: https://evil.example"
        )));
        assert!(!origin_is_allowed(&req(
            "Host: 127.0.0.1:10999\r\nOrigin: https://evil.example"
        )));
    }

    #[test]
    fn test_sandboxed_frame_is_refused() {
        assert!(!origin_is_allowed(&req(
            "Host: dash.example.com\r\nOrigin: null"
        )));
    }

    #[test]
    fn test_client_that_is_not_a_browser_is_allowed() {
        // curl and monitoring send no Origin, and cannot be acting for a page.
        assert!(origin_is_allowed(&req("Host: dash.example.com")));
    }

    #[test]
    fn test_only_known_hosts_are_answered() {
        assert!(host_is_allowed(&req("Host: localhost:10999"), &allowed()));
        assert!(host_is_allowed(&req("Host: DASH.EXAMPLE.COM"), &allowed()));
        assert!(!host_is_allowed(&req("Host: rebind.evil"), &allowed()));
        assert!(!host_is_allowed(&req("Origin: x"), &allowed()));
    }

    #[test]
    fn test_address_literal_needs_no_configuration() {
        // An address cannot be rebound, so accepting one relaxes nothing.
        assert!(host_is_allowed(&req("Host: 111.1.1.1:10999"), &allowed()));
        assert!(host_is_allowed(&req("Host: 127.0.0.1:10999"), &allowed()));
        assert!(host_is_allowed(&req("Host: [::1]:10999"), &allowed()));
        assert!(host_is_allowed(&req("Host: [2001:db8::1]"), &allowed()));
        assert!(host_is_allowed(&req("Host: 111.1.1.1"), &[]));
    }

    #[test]
    fn test_name_still_has_to_be_named() {
        // The rebinding defence survives the concession above: what an
        // attacker controls is a name, and a name is still checked.
        assert!(!host_is_allowed(&req("Host: rebind.evil"), &[]));
        assert!(!host_is_allowed(
            &req("Host: 111.1.1.1.evil.com"),
            &allowed()
        ));
        assert!(!host_is_allowed(&req("Host: 999.999.999.999"), &allowed()));
    }

    #[test]
    fn test_more_than_one_name_can_be_allowed() {
        let hosts = vec!["a.example.com".to_string(), "b.example.com".to_string()];
        assert!(host_is_allowed(&req("Host: a.example.com"), &hosts));
        assert!(host_is_allowed(&req("Host: b.example.com"), &hosts));
        assert!(!host_is_allowed(&req("Host: c.example.com"), &hosts));
    }

    #[test]
    fn test_rebinding_is_caught_by_the_host_check() {
        // Rebinding defeats the origin check and is caught by the host check.
        let rebound = req("Host: rebind.evil:10999\r\nOrigin: http://rebind.evil");
        assert!(
            origin_is_allowed(&rebound),
            "origin matches host under rebinding, which is why the host check exists"
        );
        assert!(!host_is_allowed(&rebound, &allowed()));
    }

    #[test]
    fn test_hosts_are_compared_without_scheme_or_port() {
        assert_eq!(host_of("https://dash.example.com:443"), "dash.example.com");
        assert_eq!(host_of("dash.example.com:10999"), "dash.example.com");
        assert_eq!(host_of("[::1]:10999"), "::1");
        assert_eq!(host_of("http://[::1]"), "::1");
    }

    #[test]
    fn test_headers_are_case_insensitive_and_end_at_body() {
        let head = "GET / HTTP/1.1\r\nHOST: x\r\n\r\nHost: injected\r\n";
        assert_eq!(header(head, "host"), Some("x"));
        assert_eq!(header(head, "missing"), None);
    }

    #[test]
    fn test_extracts_the_request_path() {
        assert_eq!(
            request_path("GET /assets/app.js HTTP/1.1\r\n"),
            "/assets/app.js"
        );
        assert_eq!(request_path("GET /?x=1 HTTP/1.1\r\n"), "/");
        assert_eq!(request_path("garbage"), "/");
    }

    /// The reply with the connection cap full, or else the websocket cap.
    async fn request_at_cap(request: &[u8], connections: bool) -> String {
        let limits = Limits {
            connections: Arc::new(Semaphore::new(1)),
            websockets: Arc::new(Semaphore::new(1)),
        };
        let full = if connections {
            &limits.connections
        } else {
            &limits.websockets
        };
        let _held = full.clone().try_acquire_owned().unwrap();
        request_with(request, limits).await
    }

    #[track_caller]
    fn assert_status(reply: &str, status: &str) {
        assert_eq!(
            reply.lines().next(),
            Some(format!("HTTP/1.1 {status}").as_str()),
            "{:?}",
            &reply[..reply.len().min(80)]
        );
    }

    #[tokio::test]
    async fn test_unrecognised_host_is_turned_away() {
        let reply = request(b"GET / HTTP/1.1\r\nHost: rebind.evil\r\n\r\n").await;
        assert_status(&reply, "421 Misdirected Request");
        // The page itself must not have gone out: serving it would make the
        // attacker's origin same-origin with the dashboard.
        assert!(!reply.contains("<!doctype"), "document was served anyway");
    }

    #[tokio::test]
    async fn test_websocket_from_another_origin_is_refused() {
        let reply = request(
            b"GET /websocket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nOrigin: https://evil.example\r\n\r\n",
        )
        .await;
        assert_status(&reply, "403 Forbidden");
    }

    #[tokio::test]
    async fn test_websocket_over_the_cap_is_refused_with_a_status() {
        let reply = request_at_cap(
            b"GET /websocket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n",
            false,
        )
        .await;
        assert_status(&reply, "503 Service Unavailable");
    }

    #[tokio::test]
    async fn test_http_is_still_served_when_the_websocket_cap_is_full() {
        // The point of capping websockets alone: a full pool must not stop the
        // page itself from loading.
        let reply = request_at_cap(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", false).await;
        assert_status(&reply, "200 OK");
    }

    #[tokio::test]
    async fn test_full_connection_cap_closes_without_reading() {
        // Over the cap the request is never read: reading it is the cost the
        // cap exists to bound.
        let reply = request_at_cap(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n", true).await;
        assert!(reply.is_empty(), "expected a closed socket, got {reply:?}");
    }

    #[tokio::test]
    async fn test_connection_cap_covers_websockets_too() {
        // A websocket is a connection. If it were exempt, the cheapest way past
        // the cap would be to open the long-lived kind.
        let reply = request_at_cap(
            b"GET /websocket HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n\r\n",
            true,
        )
        .await;
        assert!(reply.is_empty(), "expected a closed socket, got {reply:?}");
    }

    #[tokio::test]
    async fn test_a_head_arriving_in_two_pieces_is_still_served() {
        let (addr, server) = serve_one(Arc::new(Publisher::new())).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(b"GET / HTTP/1.1\r\nHo").await.unwrap();
        sleep(Duration::from_millis(100)).await;
        client.write_all(b"st: x\r\n\r\n").await.unwrap();
        let mut reply = String::new();
        client.read_to_string(&mut reply).await.unwrap();
        server.await.unwrap().unwrap();
        assert_status(&reply, "200 OK");
    }

    #[test]
    fn test_only_hashed_assets_are_cached() {
        // Asset filenames carry a content hash; index.html does not, and caching it
        // would hide a redeploy.
        let asset = Reply {
            immutable: true,
            ..Reply::text(200, b"x")
        };
        assert!(
            response_head(&asset).contains("cache-control: public, max-age=31536000, immutable")
        );
        assert!(response_head(&Reply::text(200, b"x")).contains("cache-control: no-cache"));
    }

    #[test]
    fn test_policy_still_permits_validator_icons() {
        assert!(SECURITY_HEADERS.contains("img-src 'self' data: https:"));
        assert!(!SECURITY_HEADERS.contains("img-src 'self'\r\n"));
    }

    #[test]
    fn test_header_block_is_well_formed() {
        for coding in ["", VARY_HEADER, GZIP_HEADERS] {
            let out = response_head(&Reply {
                coding,
                ..Reply::text(200, b"body")
            });
            let head = out.strip_suffix("\r\n\r\n").expect("no header terminator");
            assert!(!head.contains("\r\n\r\n"), "blank line inside the headers");
            for line in head.split("\r\n").skip(1) {
                assert!(line.contains(':'), "malformed header line: {line:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_each_scan_is_answered_and_no_other_request_scans() {
        let handles = handles();
        let scan = async |key: &str, params: &str| ask(&handles, ("slot", key), params).await;
        let search = scan("search", r#"{"query":"x","before":64}"#).await;
        assert!(search.contains(r#""turns":[],"next":"#), "{search}");
        assert!(scan("search", r#"{"query":"x"}"#).await.contains("error"));
        let future = scan("future", r#"{"before":64}"#).await;
        assert!(future.contains(r#""turns":[],"leaders":[]"#), "{future}");
        let summary = scan(
            "summary",
            r#"{"before":64,"where":[{"field":"base_fee","op":"gt","value":5000}]}"#,
        )
        .await;
        assert!(summary.contains(r#""clients":[]"#), "{summary}");
        let other = ask(&handles, ("summary", "search"), r#"{"before":64}"#).await;
        assert!(other.contains("unsupported request"), "{other}");
    }

    #[tokio::test]
    async fn test_the_miss_list_is_answered_and_the_ask_remembered() {
        let handles = handles();
        let reply = ask(&handles, ("summary", "misses"), "null").await;
        assert!(reply.contains(r#""rows":[]"#), "{reply}");
        assert!(
            !reply.contains("written"),
            "served on its own route: {reply}"
        );
        just_asked(handles.replies.misses.asked_at());
    }

    #[tokio::test]
    async fn test_the_gossip_peers_are_answered_and_the_ask_remembered() {
        let handles = handles();
        let reply = ask(&handles, ("peers", "gossip"), "null").await;
        assert!(
            reply.contains(r#""value":null"#),
            "not gathered yet: {reply}"
        );
        just_asked(handles.replies.gossip_peers.asked_at());
    }

    #[tokio::test]
    async fn test_every_leader_is_answered_with_what_a_turn_shows() {
        let handles = handles();
        let identity = Pubkey::new_unique();
        handles.replies.leader_traits.write().unwrap().insert(
            identity,
            LeaderTraits {
                client: Some("Agave".to_string()),
                version: Some("4.4.1".to_string()),
                ip: Some("8.8.8.8".to_string()),
                country: Some("US"),
                stake: 7,
                delinquent: false,
                last_vote: None,
            },
        );
        let reply = ask(&handles, ("peers", "leaders"), "null").await;
        assert!(reply.contains(&identity.to_string()), "{reply}");
        for field in [
            r#""client":"Agave""#,
            r#""version":"4.4.1""#,
            r#""ip":"8.8.8.8""#,
            r#""country":"US""#,
            r#""stake":7"#,
        ] {
            assert!(reply.contains(field), "{field} in {reply}");
        }
    }

    #[tokio::test]
    async fn test_the_cluster_charts_are_answered_and_the_ask_remembered() {
        let handles = handles();
        let reply = ask(&handles, ("cluster", "charts"), "null").await;
        assert!(
            reply.contains(r#""value":null"#),
            "not gathered yet: {reply}"
        );
        just_asked(handles.replies.cluster_charts.asked_at());
    }

    #[tokio::test]
    async fn test_cluster_charts_held_from_an_earlier_viewing_are_not_served() {
        let handles = handles();
        *handles.replies.cluster_charts.held.write().unwrap() =
            Encoded(Arc::from(r#"{"epoch":7}"#));
        let charts = async || ask(&handles, ("cluster", "charts"), "null").await;
        assert!(charts().await.contains(r#""value":null"#));
        *handles.replies.cluster_charts.held.write().unwrap() =
            Encoded(Arc::from(r#"{"epoch":8}"#));
        assert!(charts().await.contains(r#""value":{"epoch":8}"#));
    }

    #[tokio::test]
    async fn test_the_written_list_is_answered_and_the_ask_remembered() {
        let handles = handles();
        let reply = ask(&handles, ("summary", "written"), "null").await;
        assert!(reply.contains(r#""certificates":0"#), "{reply}");
        assert!(reply.contains(r#""rows":[]"#), "{reply}");
        just_asked(handles.replies.misses.asked_at());
    }

    #[tokio::test]
    async fn test_unknown_requests_still_get_a_reply() {
        let reply = ask(&handles(), ("nope", "nope"), "null").await;
        assert!(reply.contains("unsupported request"));
    }

    #[tokio::test]
    async fn test_malformed_requests_are_ignored() {
        assert!(respond(b"not json", &handles()).await.is_none());
    }

    #[test]
    fn test_log_text_cannot_forge_a_second_line() {
        // A newline would end the entry and let the rest pose as one the
        // validator wrote itself.
        assert_eq!(
            for_logging("summary\n[INFO] dashboard: all good"),
            "summary?[INFO] dashboard: all good"
        );
        // An escape sequence would reach the terminal of whoever tails the log,
        // and an override would reverse the text printed after it.
        assert_eq!(for_logging("\u{1b}[2Jwiped"), "?[2Jwiped");
        assert_eq!(for_logging("nice\u{202e}gnp.exe"), "nice?gnp.exe");
    }

    #[test]
    fn test_log_text_is_bounded() {
        let logged = for_logging(&"a".repeat(4096));
        assert!(
            logged.len() < 64,
            "unbounded log line: {} bytes",
            logged.len()
        );
        assert!(
            logged.ends_with("..."),
            "truncation is not visible: {logged:?}"
        );
        assert_eq!(for_logging("summary"), "summary");
    }

    #[tokio::test]
    async fn test_an_unknown_path_falls_through_to_the_app() {
        let reply = request(b"GET /some/client/route HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_status(&reply, "200 OK");
        assert!(reply.contains("content-type: text/html"));
    }

    #[tokio::test]
    async fn test_robots_is_served_as_text() {
        if assets::ASSETS.is_empty() {
            return;
        }
        let reply = request(b"GET /robots.txt HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(reply.contains("content-type: text/plain"));
        assert!(reply.contains("Disallow: /*?"));
    }

    #[tokio::test]
    async fn test_a_method_that_would_write_is_refused() {
        // Nothing here accepts input over HTTP, and a server that quietly
        // ignores a POST reads as one that took it.
        let reply = request(b"POST / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_status(&reply, "405 Method Not Allowed");
    }

    #[tokio::test]
    async fn test_a_head_request_is_sent_the_headers_alone() {
        let (get_head, get_body) = get_split(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        let (head, body) = get_split(b"HEAD / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert_status(&head, "200 OK");
        assert_eq!(head, get_head, "the same status and headers as a GET");
        assert!(!get_body.is_empty());
        assert!(body.is_empty(), "{} bytes of body", body.len());
    }

    #[tokio::test]
    async fn test_a_head_that_fills_the_peek_is_refused_as_too_large() {
        let mut head = b"GET / HTTP/1.1\r\nHost: x\r\nX-Padding: ".to_vec();
        head.resize(MAX_REQUEST_HEAD, b'a');
        let reply = request(&head).await;
        assert_status(&reply, "431 Request Header Fields Too Large");
        assert!(!reply.contains("<!doctype"), "the page was served anyway");
    }

    type Served = (
        std::net::SocketAddr,
        tokio::task::JoinHandle<Result<(), ConnectionError>>,
    );

    /// One connection served, answering the host `x` the requests here name.
    async fn serve_one_with(publisher: Arc<Publisher>, limits: Limits) -> Served {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let handles = Handles {
                publisher,
                ..handles()
            };
            handle(socket, handles, limits, &["x".to_string()]).await
        });
        (addr, server)
    }

    async fn serve_one(publisher: Arc<Publisher>) -> Served {
        serve_one_with(publisher, Limits::new()).await
    }

    /// The reply read to the end; a reset reads as an empty reply.
    async fn request_with(request: &[u8], limits: Limits) -> String {
        let (addr, server) = serve_one_with(Arc::new(Publisher::new()), limits).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(request).await.unwrap();
        let mut reply = String::new();
        let _ = client.read_to_string(&mut reply).await;
        server.await.unwrap().unwrap();
        reply
    }

    async fn request(request: &[u8]) -> String {
        request_with(request, Limits::new()).await
    }

    async fn connect(
        addr: std::net::SocketAddr,
        deflate: bool,
    ) -> Client<'static, Compat<TcpStream>> {
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut client = Client::new(stream.compat(), "x", WEBSOCKET_PATH);
        if deflate {
            client.add_protocol(DEFLATE_PROTOCOL);
        }
        let ServerResponse::Accepted { protocol } = client.handshake().await.unwrap() else {
            panic!("the server refused a well-formed handshake");
        };
        assert_eq!(protocol.as_deref(), deflate.then_some(DEFLATE_PROTOCOL));
        client
    }

    /// The next message's text, and whether it came deflated.
    async fn receive(receiver: &mut soketto::Receiver<Compat<TcpStream>>) -> (bool, String) {
        let mut data = Vec::new();
        if receiver.receive_data(&mut data).await.unwrap().is_text() {
            return (false, String::from_utf8(data).unwrap());
        }
        let mut text = String::new();
        ZlibDecoder::new(&data[..])
            .read_to_string(&mut text)
            .unwrap();
        assert!(
            data.len() < text.len(),
            "deflated, so shorter than its text"
        );
        (true, text)
    }

    #[tokio::test]
    async fn test_a_client_offering_the_subprotocol_gets_long_messages_deflated() {
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "cluster", &"testnet");
        publisher.publish("summary", "host", &vec!["a host row"; 200]);
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, true).await.into_builder().finish();

        let texts = [receive(&mut receiver).await, receive(&mut receiver).await];
        assert!(
            texts
                .iter()
                .any(|(binary, text)| !binary && text.contains(r#""key":"cluster""#)),
            "the short message should stay text: {texts:?}"
        );
        assert!(
            texts
                .iter()
                .any(|(binary, text)| *binary && text.contains(r#""key":"host""#)),
            "the long message should arrive deflated: {texts:?}"
        );

        sender.close().await.unwrap();
        server.await.unwrap().unwrap();
    }

    fn long_rows() -> Vec<&'static str> {
        vec!["a host row"; 8_000]
    }

    #[tokio::test]
    async fn test_long_snapshot_and_update_messages_arrive_as_their_text() {
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "host", &long_rows());
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, true).await.into_builder().finish();

        assert_eq!(
            receive(&mut receiver).await,
            (true, encode("summary", "host", &long_rows()).to_string())
        );
        publisher.publish("summary", "rows", &long_rows());
        assert_eq!(
            receive(&mut receiver).await,
            (true, encode("summary", "rows", &long_rows()).to_string())
        );

        sender.close().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_a_client_request_is_answered_on_the_same_socket() {
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "cluster", &"testnet");
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, false).await.into_builder().finish();
        receive(&mut receiver).await;

        sender
            .send_text(r#"{"topic":"summary","key":"displays","id":7}"#)
            .await
            .unwrap();
        sender.flush().await.unwrap();

        let (_, reply) = receive(&mut receiver).await;
        assert!(
            reply.contains(r#""id":7"#),
            "the reply did not carry the request's id: {reply}"
        );

        sender.close().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_an_oversized_client_message_ends_the_connection() {
        // Refused from the frame's header, before its payload is read.
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "cluster", &"testnet");
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, false).await.into_builder().finish();
        receive(&mut receiver).await;

        let oversized = "x".repeat(MAX_CLIENT_MESSAGE + 1);
        sender.send_text(&oversized).await.unwrap();
        sender.flush().await.unwrap();

        match server.await.unwrap() {
            Err(ConnectionError::Connection(soketto::connection::Error::Codec(
                soketto::base::Error::PayloadTooLarge { .. },
            ))) => {}
            other => panic!("expected the connection to be dropped, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_pongs_between_fragments_do_not_lift_the_bound() {
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "cluster", &"testnet");
        let (addr, server) = serve_one(publisher.clone()).await;
        let mut stream = connect(addr, false).await.into_inner().into_inner();

        // An unfinished text frame of 4,000 bytes, then an empty pong, both masked with a zero key.
        let mut fragment = vec![0x01, 0x80 | 126, 0x0f, 0xa0, 0, 0, 0, 0];
        fragment.resize(4008, b'x');
        let pong = [0x8a, 0x80, 0, 0, 0, 0];
        for _ in 0..2 {
            stream.write_all(&fragment).await.unwrap();
            stream.write_all(&pong).await.unwrap();
        }

        match timeout(Duration::from_secs(10), server).await {
            Ok(Ok(Err(ConnectionError::Oversized(len)))) => {
                assert!(len > MAX_CLIENT_MESSAGE, "reported {len} bytes")
            }
            other => panic!("expected the connection to be dropped, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_a_connected_client_does_not_hold_a_replaced_snapshot_value() {
        let publisher = Arc::new(Publisher::new());
        publisher.retain_only("summary", "history", &[1u64, 2]);
        let held = publisher.snapshot();
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, false).await.into_builder().finish();
        assert_eq!(receive(&mut receiver).await, (false, held[0].to_string()));

        publisher.retain_only("summary", "history", &[3u64]);
        // Sent from the update loop, so the snapshot loop has finished.
        publisher.publish("summary", "done", &true);
        assert!(receive(&mut receiver).await.1.contains(r#""key":"done""#));
        assert_eq!(
            held[0].copies(),
            1,
            "the connection still holds the replaced value"
        );

        sender.close().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_a_websocket_limits_what_the_kernel_holds_unsent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let _client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        limit_unsent(&socket);
        assert_eq!(
            socket2::SockRef::from(&socket).tcp_notsent_lowat().unwrap(),
            UNSENT_LIMIT
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_a_client_that_stops_reading_gets_each_slot_once_it_resumes() {
        const FILLER: u64 = 128;
        const WATCHED: u64 = FILLER;
        const REVISIONS: u64 = 50;
        let publisher = Arc::new(Publisher::new());
        publisher.publish("summary", "cluster", &"testnet");
        let (addr, server) = serve_one(publisher.clone()).await;
        let (mut sender, mut receiver) = connect(addr, false).await.into_builder().finish();
        receive(&mut receiver).await;

        // More than the socket buffers hold while the client is not reading, so the server's
        // sends stall and later updates queue behind them.
        let pad = "x".repeat(4096);
        for slot in 0..FILLER {
            publisher.publish_update(crate::proto::TOPIC_SLOT, "update", slot, &(slot, &pad));
            sleep(Duration::from_millis(1)).await;
        }
        for revision in 0..REVISIONS {
            publisher.publish_update(
                crate::proto::TOPIC_SLOT,
                "update",
                WATCHED,
                &(WATCHED, revision),
            );
            sleep(Duration::from_millis(1)).await;
        }
        publisher.publish("summary", "done", &true);

        let mut revisions = Vec::new();
        loop {
            let frame: serde_json::Value =
                serde_json::from_str(&receive(&mut receiver).await.1).unwrap();
            if frame["key"] == "done" {
                break;
            }
            if frame["key"] == "update" && frame["value"][0] == WATCHED {
                revisions.push(frame["value"][1].as_u64().unwrap());
            }
        }
        assert!(
            revisions.len() < REVISIONS as usize,
            "every revision was sent: {revisions:?}"
        );
        assert_eq!(revisions.last(), Some(&(REVISIONS - 1)));

        sender.close().await.unwrap();
        server.await.unwrap().unwrap();
    }

    #[test]
    fn test_root_serves_the_entry_document() {
        if !assets::ASSETS.is_empty() {
            assert!(lookup("/", false).is_some());
        }
    }

    #[test]
    fn test_a_head_with_one_form_of_body_is_unchanged() {
        assert_eq!(
            response_head(&Reply::text(404, b"not found")),
            format!(
                "HTTP/1.1 404 Not Found\r\ncontent-type: text/plain; \
                 charset=utf-8\r\ncontent-length: 9\r\ncache-control: \
                 no-cache\r\n{SECURITY_HEADERS}connection: close\r\n\r\n"
            )
        );
    }

    #[test]
    fn test_gzip_is_taken_only_when_offered() {
        let offers = |value: &str| accepts_gzip(&req(&format!("Accept-Encoding: {value}")));
        assert!(offers("gzip, deflate, br, zstd"));
        assert!(offers("deflate;q=1, GZIP;q=0.5"));
        assert!(!offers("deflate, br"));
        assert!(!offers("gzip;q=0"));
        assert!(!offers("br, gzip; q=0.000"));
        assert!(!offers("gzip;Q=0"));
        assert!(!accepts_gzip(&req("Host: x")));
    }

    #[test]
    fn test_every_gzip_copy_inflates_to_its_asset() {
        for (route, _, raw, packed) in assets::ASSETS {
            if [".html", ".js", ".css"]
                .iter()
                .any(|suffix| route.ends_with(suffix))
            {
                assert!(packed.is_some(), "{route} has no gzip copy");
            }
            let Some(packed) = packed else {
                continue;
            };
            assert!(
                packed.len() < raw.len(),
                "{route}'s gzip copy is no smaller"
            );
            let mut inflated = Vec::new();
            GzDecoder::new(*packed).read_to_end(&mut inflated).unwrap();
            assert!(inflated == *raw, "{route}'s gzip copy is not the asset");
        }
    }

    async fn get_split(request: &[u8]) -> (String, Vec<u8>) {
        let (addr, server) = serve_one(Arc::new(Publisher::new())).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(request).await.unwrap();
        // Closing on unread bytes sends a reset that discards the response.
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).await.unwrap();
        server.await.unwrap().unwrap();
        let split = reply
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .expect("no header terminator");
        let body = reply.split_off(split.saturating_add(4));
        (String::from_utf8(reply).unwrap(), body)
    }

    #[tokio::test]
    async fn test_the_entry_document_is_gzipped_only_for_a_client_that_asks() {
        let Some(Reply { body: raw, .. }) = lookup("/", false) else {
            return;
        };

        let (head, body) =
            get_split(b"GET / HTTP/1.1\r\nHost: x\r\nAccept-Encoding: gzip, deflate\r\n\r\n").await;
        assert!(head.contains("content-encoding: gzip\r\n"), "{head}");
        assert!(head.contains("vary: accept-encoding\r\n"), "{head}");
        assert!(head.contains(&format!("content-length: {}\r\n", body.len())));
        let mut inflated = Vec::new();
        GzDecoder::new(body.as_slice())
            .read_to_end(&mut inflated)
            .unwrap();
        assert!(
            inflated == raw,
            "the inflated page is not the entry document"
        );

        let (head, body) = get_split(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await;
        assert!(!head.contains("content-encoding"), "{head}");
        assert!(head.contains("vary: accept-encoding\r\n"), "{head}");
        assert!(head.contains(&format!("content-length: {}\r\n", body.len())));
        assert!(body == raw, "the page is not the entry document");
    }
}
