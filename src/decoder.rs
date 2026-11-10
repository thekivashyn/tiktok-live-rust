//! One in-flight decode per session. Only bounded, uncompressed small frames run inline.
use crate::{
    engine::{Code, Failure, Report},
    protocol::{self, BatchView, FrameInfo},
};
use prost::bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinHandle;

const INLINE_MAX_BYTES: usize = 4096;
const INLINE_YIELD_EVERY: u32 = 32;
type Decoded = anyhow::Result<(FrameInfo, Option<BatchView>)>;

pub(crate) struct Decoder {
    cancelled: Arc<AtomicBool>,
    pending: Option<JoinHandle<Decoded>>,
    inline_since_yield: u32,
}
impl Default for Decoder {
    fn default() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            pending: None,
            inline_since_yield: 0,
        }
    }
}
impl Decoder {
    pub(crate) async fn decode(
        &mut self,
        bytes: Bytes,
        report: &mut Report,
    ) -> Result<Decoded, Failure> {
        if bytes.len() <= INLINE_MAX_BYTES {
            let prepared = protocol::prepare_shared(bytes);
            if prepared.as_ref().is_ok_and(|frame| frame.is_compressed()) {
                report.decode_worker_frames += 1;
                let cancelled = self.cancelled.clone();
                self.pending = Some(tokio::task::spawn_blocking(move || {
                    protocol::finish_prepared(prepared?, &|| cancelled.load(Ordering::Relaxed))
                }));
            } else {
                report.decode_inline_frames += 1;
                self.inline_since_yield += 1;
                if self.inline_since_yield == INLINE_YIELD_EVERY {
                    self.inline_since_yield = 0;
                    tokio::task::yield_now().await;
                }
                return Ok(prepared.and_then(|frame| protocol::finish_prepared(frame, &|| false)));
            }
        } else {
            report.decode_worker_frames += 1;
            let cancelled = self.cancelled.clone();
            self.pending = Some(tokio::task::spawn_blocking(move || {
                protocol::decode_cancellable(bytes, &|| cancelled.load(Ordering::Relaxed))
            }));
        }
        // Keep ownership in self while awaiting. Dropping the session future cannot detach
        // this handle: the supervisor still owns it and will drain it during shutdown.
        let result = self.pending.as_mut().expect("worker created").await;
        self.pending.take();
        result.map_err(|_| Failure::new(Code::Decode, "decoder_worker", false))
    }
    pub(crate) async fn shutdown(&mut self) -> bool {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(job) = self.pending.take() {
            job.abort(); // cancels a queued job; a running job checks the flag between chunks
            let _ = job.await;
            return true;
        }
        false
    }
}
impl Drop for Decoder {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
        if let Some(job) = &self.pending {
            job.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::{io::Write, time::Duration};
    #[tokio::test]
    async fn tiny_compressed_bomb_is_never_decoded_inline() {
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
        gzip.write_all(&vec![0; 1024 * 1024]).unwrap();
        let bytes: Bytes = protocol::packet("msg", gzip.finish().unwrap(), 1).into();
        assert!(bytes.len() < INLINE_MAX_BYTES);
        let mut report = Report::default();
        let mut decoder = Decoder::default();
        assert!(decoder.decode(bytes, &mut report).await.unwrap().is_err());
        assert_eq!(
            (report.decode_inline_frames, report.decode_worker_frames),
            (0, 1)
        );
        assert!(!decoder.shutdown().await);
    }
    #[tokio::test]
    async fn routing_has_a_hard_wire_bound_and_preserves_decoding() {
        let mut decoder = Decoder::default();
        let mut report = Report::default();
        for (kind, payload) in [
            ("hb", vec![]),
            (
                "msg",
                protocol::Batch {
                    messages: vec![],
                    internal_ext: String::new(),
                    need_ack: false,
                }
                .encode_to_vec(),
            ),
            ("future", vec![0; INLINE_MAX_BYTES]),
        ] {
            let bytes: Bytes = protocol::packet(kind, payload, 1).into();
            let expected = protocol::decode_shared(bytes.clone()).unwrap();
            let actual = decoder.decode(bytes, &mut report).await.unwrap().unwrap();
            assert_eq!(actual.0.kind, expected.0.kind);
            assert_eq!(actual.1.is_some(), expected.1.is_some());
        }
        assert_eq!(
            (report.decode_inline_frames, report.decode_worker_frames),
            (2, 1)
        );
    }
    #[tokio::test]
    async fn shutdown_drains_a_running_job_after_its_waiter_is_cancelled() {
        let mut decoder = Decoder::default();
        let flag = decoder.cancelled.clone();
        let finished = Arc::new(AtomicBool::new(false));
        let observed = finished.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        decoder.pending = Some(tokio::task::spawn_blocking(move || {
            let _ = tx.send(());
            while !flag.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            finished.store(true, Ordering::SeqCst);
            Err(anyhow::anyhow!("cancelled"))
        }));
        rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(1), decoder.pending.as_mut().unwrap())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), decoder.shutdown())
                .await
                .unwrap()
        );
        assert!(observed.load(Ordering::SeqCst));
        assert!(decoder.pending.is_none());
    }
    #[tokio::test]
    async fn dropping_owner_signals_running_job_instead_of_leaving_it_uncontrolled() {
        let mut decoder = Decoder::default();
        let flag = decoder.cancelled.clone();
        let (started, rx) = tokio::sync::oneshot::channel();
        let (done, finished) = tokio::sync::oneshot::channel();
        decoder.pending = Some(tokio::task::spawn_blocking(move || {
            let _ = started.send(());
            while !flag.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
            let _ = done.send(());
            Err(anyhow::anyhow!("cancelled"))
        }));
        rx.await.unwrap();
        drop(decoder);
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .unwrap()
            .unwrap();
    }
}
