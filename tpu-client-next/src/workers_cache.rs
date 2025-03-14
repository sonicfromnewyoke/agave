//! This module defines [`WorkersCache`] along with aux struct [`WorkerInfo`]. These
//! structures provide mechanisms for caching workers, sending transaction
//! batches, and gathering send transaction statistics.

use {
    crate::transaction_batch::TransactionBatch,
    log::*,
    lru::LruCache,
    std::net::SocketAddr,
    thiserror::Error,
    tokio::{
        sync::mpsc::{self, error::TrySendError},
        task::JoinHandle,
    },
    tokio_util::sync::CancellationToken,
};

/// [`WorkerInfo`] holds information about a worker responsible for sending
/// transaction batches.
pub struct WorkerInfo {
    sender: mpsc::Sender<TransactionBatch>,
    handle: JoinHandle<()>,
    cancel: CancellationToken,
}

impl WorkerInfo {
    pub fn new(
        sender: mpsc::Sender<TransactionBatch>,
        handle: JoinHandle<()>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            sender,
            handle,
            cancel,
        }
    }

    fn try_send_transactions(&self, txs_batch: TransactionBatch) -> Result<(), WorkersCacheError> {
        self.sender.try_send(txs_batch).map_err(|err| match err {
            TrySendError::Full(_) => WorkersCacheError::FullChannel,
            TrySendError::Closed(_) => WorkersCacheError::ReceiverDropped,
        })?;
        Ok(())
    }

    async fn send_transactions(
        &self,
        txs_batch: TransactionBatch,
    ) -> Result<(), WorkersCacheError> {
        self.sender
            .send(txs_batch)
            .await
            .map_err(|_| WorkersCacheError::ReceiverDropped)?;
        Ok(())
    }

    /// Closes the worker by dropping the sender and awaiting the worker's
    /// statistics.
    async fn shutdown(self) -> Result<(), WorkersCacheError> {
        self.cancel.cancel();
        drop(self.sender);
        self.handle
            .await
            .map_err(|_| WorkersCacheError::TaskJoinFailure)?;
        Ok(())
    }
}

/// [`WorkersCache`] manages and caches workers. It uses an LRU cache to store and
/// manage workers. It also tracks transaction statistics for each peer.
pub struct WorkersCache {
    workers: LruCache<SocketAddr, WorkerInfo>,

    /// Indicates that the `WorkersCache` is been `shutdown()`, interrupting any outstanding
    /// `send_transactions_to_address()` invocations.
    cancel: CancellationToken,
}

#[derive(Debug, Error, PartialEq)]
pub enum WorkersCacheError {
    /// typically happens when the client could not establish the connection.
    #[error("Work receiver has been dropped unexpectedly.")]
    ReceiverDropped,

    #[error("Worker's channel is full.")]
    FullChannel,

    #[error("Task failed to join.")]
    TaskJoinFailure,

    #[error("The WorkersCache is being shutdown.")]
    ShutdownError,
}

impl WorkersCache {
    pub(crate) fn new(capacity: usize, cancel: CancellationToken) -> Self {
        Self {
            workers: LruCache::new(capacity),
            cancel,
        }
    }

    pub fn contains(&self, peer: &SocketAddr) -> bool {
        self.workers.contains(peer)
    }

    pub(crate) fn push(
        &mut self,
        leader: SocketAddr,
        peer_worker: WorkerInfo,
    ) -> Option<ShutdownWorker> {
        if let Some((leader, popped_worker)) = self.workers.push(leader, peer_worker) {
            return Some(ShutdownWorker {
                leader,
                worker: popped_worker,
            });
        }
        None
    }

    pub fn pop(&mut self, leader: SocketAddr) -> Option<ShutdownWorker> {
        if let Some(popped_worker) = self.workers.pop(&leader) {
            return Some(ShutdownWorker {
                leader,
                worker: popped_worker,
            });
        }
        None
    }

    /// Attempts to send immediately a batch of transactions to the worker for a
    /// given peer.
    ///
    /// This method returns immediately if the channel of worker corresponding
    /// to this peer is full returning error [`WorkersCacheError::FullChannel`].
    /// If it happens that the peer's worker is stopped, it returns
    /// [`WorkersCacheError::ShutdownError`]. In case if the worker is not
    /// stopped but it's channel is unexpectedly dropped, it returns
    /// [`WorkersCacheError::ReceiverDropped`].
    pub fn try_send_transactions_to_address(
        &mut self,
        peer: &SocketAddr,
        txs_batch: TransactionBatch,
    ) -> Result<(), WorkersCacheError> {
        let Self {
            workers, cancel, ..
        } = self;
        if cancel.is_cancelled() {
            return Err(WorkersCacheError::ShutdownError);
        }

        let current_worker = workers.get(peer).expect(
            "Failed to fetch worker for peer {peer}.\n\
             Peer existence must be checked before this call using `contains` method.",
        );
        let send_res = current_worker.try_send_transactions(txs_batch);

        if let Err(WorkersCacheError::ReceiverDropped) = send_res {
            debug!(
                "Failed to deliver transaction batch for leader {}, drop batch.",
                peer.ip()
            );
            maybe_shutdown_worker(workers.pop(peer).map(|current_worker| ShutdownWorker {
                leader: *peer,
                worker: current_worker,
            }));
        }

        send_res
    }

    /// Sends a batch of transactions to the worker for a given peer.
    ///
    /// If the worker for the peer is disconnected or fails, it
    /// is removed from the cache.
    #[allow(
        dead_code,
        reason = "This method will be used in the upcoming changes to implement optional backpressure on the sender."
    )]
    pub async fn send_transactions_to_address(
        &mut self,
        peer: &SocketAddr,
        txs_batch: TransactionBatch,
    ) -> Result<(), WorkersCacheError> {
        let Self {
            workers, cancel, ..
        } = self;

        let body = async move {
            let current_worker = workers.get(peer).expect(
                "Failed to fetch worker for peer {peer}.\n\
                 Peer existence must be checked before this call using `contains` method.",
            );
            let send_res = current_worker.send_transactions(txs_batch).await;
            if let Err(WorkersCacheError::ReceiverDropped) = send_res {
                // Remove the worker from the cache, if the peer has disconnected.
                maybe_shutdown_worker(workers.pop(peer).map(|current_worker| ShutdownWorker {
                    leader: *peer,
                    worker: current_worker,
                }));
            }

            send_res
        };

        cancel
            .run_until_cancelled(body)
            .await
            .unwrap_or(Err(WorkersCacheError::ShutdownError))
    }

    /// Closes and removes all workers in the cache. This is typically done when
    /// shutting down the system.
    pub(crate) async fn shutdown(&mut self) {
        // Interrupt any outstanding `send_transactions()` calls.
        self.cancel.cancel();

        while let Some((leader, worker)) = self.workers.pop_lru() {
            let res = worker.shutdown().await;
            if let Err(err) = res {
                debug!("Error while shutting down worker for {leader}: {err}");
            }
        }
    }
}

/// [`ShutdownWorker`] takes care of stopping the worker. It's method
/// `shutdown()` should be executed in a separate task to hide the latency of
/// finishing worker gracefully.
pub struct ShutdownWorker {
    leader: SocketAddr,
    worker: WorkerInfo,
}

impl ShutdownWorker {
    pub(crate) fn leader(&self) -> SocketAddr {
        self.leader
    }

    pub(crate) async fn shutdown(self) -> Result<(), WorkersCacheError> {
        self.worker.shutdown().await
    }
}

pub fn maybe_shutdown_worker(worker: Option<ShutdownWorker>) {
    let Some(worker) = worker else {
        return;
    };
    tokio::spawn(async move {
        let leader = worker.leader();
        let res = worker.shutdown().await;
        if let Err(err) = res {
            debug!("Error while shutting down worker for {leader}: {err}");
        }
    });
}
