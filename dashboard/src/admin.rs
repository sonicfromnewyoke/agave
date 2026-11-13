//! The validator's admin socket, `<ledger>/admin.rpc`: JSON-RPC 2.0 over a unix socket. The server
//! splits requests by brace depth and ends each response with a newline, so one connection carries
//! one request and its line.

use {
    crate::meter_source::IngestAddresses,
    serde::{Deserialize, de::DeserializeOwned},
    std::{
        io,
        net::SocketAddr,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
    thiserror::Error,
    tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::UnixStream,
        time::timeout,
    },
};

/// The socket answers from the validator's own process; a slow answer means it is not answering.
const CALL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("unreadable answer: {0}")]
    Json(#[from] serde_json::Error),
    /// The method refused, as `-32602` does until startup is complete.
    #[error("{message} ({code})")]
    Rpc { code: i64, message: String },
    #[error("no answer within {CALL_TIMEOUT:?}")]
    Timeout,
}

pub struct AdminClient {
    path: PathBuf,
    next_id: AtomicU64,
}

impl AdminClient {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            next_id: AtomicU64::new(1),
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// `method` without parameters, its result read as a `T`.
    pub async fn call<T: DeserializeOwned>(&self, method: &str) -> Result<T, AdminError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let request = serde_json::json!({"jsonrpc": "2.0", "id": id, "method": method});
        let line = timeout(CALL_TIMEOUT, async {
            let mut stream = UnixStream::connect(&self.path).await?;
            stream.write_all(&serde_json::to_vec(&request)?).await?;
            let mut line = String::new();
            BufReader::new(stream).read_line(&mut line).await?;
            Ok::<_, AdminError>(line)
        })
        .await
        .map_err(|_| AdminError::Timeout)??;
        parse_response(&line)
    }
}

/// A result, which may be `null`, or the error the server sent instead.
fn parse_response<T: DeserializeOwned>(line: &str) -> Result<T, AdminError> {
    let mut response: serde_json::Value = serde_json::from_str(line)?;
    if let Some(error) = response.get("error") {
        return Err(AdminError::Rpc {
            code: error
                .get("code")
                .and_then(serde_json::Value::as_i64)
                .unwrap_or_default(),
            message: error
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string(),
        });
    }
    let result = response
        .get_mut("result")
        .map(serde_json::Value::take)
        .unwrap_or_default();
    Ok(serde_json::from_value(result)?)
}

/// What `contactInfo` answers that the dashboard reads: the validator's own advertised addresses.
/// An address the node does not advertise comes as `0.0.0.0:0`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AdminContactInfo {
    pub id: String,
    pub gossip: SocketAddr,
    pub tvu: SocketAddr,
    pub tpu_quic: Option<SocketAddr>,
    pub tpu_vote: SocketAddr,
    pub serve_repair: SocketAddr,
    pub shred_version: u16,
}

impl AdminContactInfo {
    /// The admin socket names the UDP ports and the TPU's QUIC port. The QUIC ports of the TPU's
    /// forwards and votes, and Alpenglow's, are not among them.
    pub fn ingest_addresses(&self) -> IngestAddresses {
        IngestAddresses {
            tvu_udp: advertised(self.tvu),
            tpu_quic: self.tpu_quic.and_then(advertised),
            tpu_forwards_quic: None,
            tpu_vote_udp: advertised(self.tpu_vote),
            gossip: advertised(self.gossip),
            serve_repair_udp: advertised(self.serve_repair),
        }
    }
}

fn advertised(address: SocketAddr) -> Option<SocketAddr> {
    (address.port() != 0).then_some(address)
}

#[cfg(test)]
mod tests {
    use {super::*, solana_core::validator::ValidatorStartProgress, std::time::SystemTime};

    #[test]
    fn test_a_result_is_read_and_an_error_is_returned() {
        let pid: u32 = parse_response(r#"{"jsonrpc":"2.0","result":4242,"id":1}"#).unwrap();
        assert_eq!(pid, 4242);
        let none: Option<SocketAddr> =
            parse_response(r#"{"jsonrpc":"2.0","result":null,"id":2}"#).unwrap();
        assert_eq!(none, None);
        let refused = parse_response::<u32>(
            r#"{"jsonrpc":"2.0","error":{"code":-32602,"message":"Retry once validator start up is complete"},"id":3}"#,
        );
        assert!(matches!(refused, Err(AdminError::Rpc { code: -32602, .. })));
    }

    #[test]
    fn test_start_progress_and_start_time_read_as_the_validator_writes_them() {
        let running: ValidatorStartProgress =
            parse_response(r#"{"jsonrpc":"2.0","result":"Running","id":1}"#).unwrap();
        assert_eq!(running, ValidatorStartProgress::Running);
        let waiting: ValidatorStartProgress = parse_response(
            r#"{"jsonrpc":"2.0","result":{"WaitingForSupermajority":{"slot":12,"gossip_stake_percent":70}},"id":2}"#,
        )
        .unwrap();
        assert_eq!(
            waiting,
            ValidatorStartProgress::WaitingForSupermajority {
                slot: 12,
                gossip_stake_percent: 70
            }
        );
        let started: SystemTime = parse_response(
            r#"{"jsonrpc":"2.0","result":{"secs_since_epoch":1700000000,"nanos_since_epoch":5},"id":3}"#,
        )
        .unwrap();
        assert_eq!(
            started,
            SystemTime::UNIX_EPOCH + Duration::new(1_700_000_000, 5)
        );
    }

    #[test]
    fn test_unadvertised_addresses_are_left_out() {
        let contact: AdminContactInfo = parse_response(
            r#"{"jsonrpc":"2.0","result":{"id":"11111111111111111111111111111111","gossip":"10.0.0.1:8001","tvu":"10.0.0.1:8002","serve_repair_quic":"0.0.0.0:0","tpu":"10.0.0.1:8003","tpu_quic":"10.0.0.1:8009","tpu_forwards":"10.0.0.1:8004","tpu_vote":"0.0.0.0:0","rpc":"0.0.0.0:0","rpc_pubsub":"0.0.0.0:0","serve_repair":"10.0.0.1:8005","last_updated_timestamp":1,"shred_version":50093},"id":1}"#,
        )
        .unwrap();
        let addresses = contact.ingest_addresses();
        assert_eq!(addresses.tvu_udp, Some("10.0.0.1:8002".parse().unwrap()));
        assert_eq!(addresses.tpu_quic, Some("10.0.0.1:8009".parse().unwrap()));
        assert_eq!(addresses.tpu_vote_udp, None);
        assert_eq!(addresses.tpu_forwards_quic, None);
        assert_eq!(addresses.gossip, Some("10.0.0.1:8001".parse().unwrap()));
        assert_eq!(contact.shred_version, 50093);
    }
}
