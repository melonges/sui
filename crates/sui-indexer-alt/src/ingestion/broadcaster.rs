// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use backoff::backoff::Constant;
use futures::{future::try_join_all, TryStreamExt};
use mysten_metrics::spawn_monitored_task;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use sui_types::full_checkpoint_content::CheckpointData;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::{ingestion::error::Error, metrics::IndexerMetrics};

use super::{client::IngestionClient, IngestionConfig};

/// The broadcaster task is responsible for taking a stream of checkpoint sequence numbers from
/// `checkpoint_rx`, fetching them using the `client` and disseminating them to all subscribers in
/// `subscribers`.
///
/// The task will shut down if the `cancel` token is signalled, or if the `checkpoint_rx` channel
/// closes.
pub(super) fn broadcaster(
    config: IngestionConfig,
    client: IngestionClient,
    metrics: Arc<IndexerMetrics>,
    checkpoint_rx: mpsc::Receiver<u64>,
    subscribers: Vec<mpsc::Sender<Arc<CheckpointData>>>,
    cancel: CancellationToken,
) -> JoinHandle<()> {
    spawn_monitored_task!(async move {
        info!("Starting ingestion broadcaster");
        let latest_ingested_checkpoint = Arc::new(AtomicU64::new(0));

        match ReceiverStream::new(checkpoint_rx)
            .map(Ok)
            .try_for_each_concurrent(/* limit */ config.ingest_concurrency, |cp| {
                let client = client.clone();
                let metrics = metrics.clone();
                let metrics_clone = metrics.clone();
                let subscribers = subscribers.clone();
                let latest_ingested_checkpoint = latest_ingested_checkpoint.clone();

                // One clone is for the supervisor to signal a cancel if it detects a
                // subscriber that wants to wind down ingestion, and the other is to pass to
                // each worker to detect cancellation.
                let supervisor_cancel = cancel.clone();
                let cancel = cancel.clone();

                // Repeatedly retry if the checkpoint is not found, assuming that we are at the
                // tip of the network and it will become available soon.
                let backoff = Constant::new(config.retry_interval);
                let fetch = move || {
                    let client = client.clone();
                    let metrics = metrics_clone.clone();
                    let cancel = cancel.clone();

                    async move {
                        use backoff::Error as BE;
                        if cancel.is_cancelled() {
                            return Err(BE::permanent(Error::Cancelled));
                        }

                        client.fetch(cp, &cancel).await.map_err(|e| match e {
                            Error::NotFound(checkpoint) => {
                                debug!(checkpoint, "Checkpoint not found, retrying...");
                                metrics.total_ingested_not_found_retries.inc();
                                BE::transient(e)
                            }
                            e => BE::permanent(e),
                        })
                    }
                };

                async move {
                    let checkpoint = backoff::future::retry(backoff, fetch).await?;
                    let new_seq = checkpoint.checkpoint_summary.sequence_number;
                    let old_seq = latest_ingested_checkpoint.fetch_max(new_seq, Ordering::Relaxed);
                    if new_seq > old_seq {
                        metrics.latest_ingested_checkpoint.set(new_seq as i64);
                        metrics.latest_ingested_checkpoint_timestamp_lag_ms.set(
                            chrono::Utc::now().timestamp_millis()
                                - checkpoint.checkpoint_summary.timestamp_ms as i64,
                        );
                    }
                    let futures = subscribers.iter().map(|s| s.send(checkpoint.clone()));

                    if try_join_all(futures).await.is_err() {
                        info!("Subscription dropped, signalling shutdown");
                        supervisor_cancel.cancel();
                        Err(Error::Cancelled)
                    } else {
                        Ok(())
                    }
                }
            })
            .await
        {
            Ok(()) => {
                info!("Checkpoints done, stopping ingestion broadcaster");
            }

            Err(Error::Cancelled) => {
                info!("Shutdown received, stopping ingestion broadcaster");
            }

            Err(e) => {
                error!("Ingestion broadcaster failed: {}", e);
                cancel.cancel();
            }
        }
    })
}
