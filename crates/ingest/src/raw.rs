//! The append-only raw log.
//!
//! "Raw before smart": every event is durably appended before anything
//! computes on it (PLAN.md standing rule 3), so every future detector version
//! can be replayed against history.
//!
//! PLAN.md Phase 1 takes the cut line — one growing file rather than hourly
//! rotation. Records are written as concatenated gzip members, which is a
//! valid gzip stream: `zcat raw/events.jsonl.gz` reads the whole history.
//! ARCHITECTURE.md §6's `raw/dt=.../hh=.../part-*` partitioning and the push
//! to object storage stay with the archive stretch.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;

/// Buffers JSONL lines and appends them to the log as gzip members.
pub struct RawLog {
    path: PathBuf,
    buf: Vec<String>,
    /// Lines appended since process start — reported in the heartbeat.
    pub written: u64,
}

impl RawLog {
    /// Open (creating parents as needed). Does not truncate: the log outlives
    /// the process, and a redeploy that loses it is the reason §6 pushes to
    /// object storage.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir)
            .with_context(|| format!("creating raw log dir {}", dir.display()))?;
        let path = dir.join("events.jsonl.gz");
        // Touch it so an operator can see the file before the first flush.
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("opening raw log {}", path.display()))?;
        Ok(Self {
            path,
            buf: Vec::new(),
            written: 0,
        })
    }

    /// Queue one raw JSON line. Cheap — no I/O until `flush`.
    pub fn push(&mut self, line: &str) {
        self.buf.push(line.to_string());
    }

    pub fn pending(&self) -> usize {
        self.buf.len()
    }

    /// Compress and append everything buffered. A no-op when empty.
    pub fn flush(&mut self) -> Result<usize> {
        if self.buf.is_empty() {
            return Ok(0);
        }
        let mut payload = Vec::with_capacity(self.buf.len() * 512);
        for line in &self.buf {
            payload.extend_from_slice(line.as_bytes());
            payload.push(b'\n');
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&payload).context("gzip encode")?;
        let member = encoder.finish().context("gzip finish")?;

        let mut file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .with_context(|| format!("appending to {}", self.path.display()))?;
        file.write_all(&member)
            .with_context(|| format!("writing {} bytes", member.len()))?;
        // fsync: the append must survive an ungraceful container kill, which is
        // exactly the failure the Phase 1 demo induces on purpose.
        file.sync_data().context("fsync raw log")?;

        let n = self.buf.len();
        self.buf.clear();
        self.written += n as u64;
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn read_all(path: &Path) -> String {
        let mut raw = Vec::new();
        File::open(path).unwrap().read_to_end(&mut raw).unwrap();
        let mut out = String::new();
        flate2::read::MultiGzDecoder::new(&raw[..])
            .read_to_string(&mut out)
            .unwrap();
        out
    }

    #[test]
    fn appends_across_flushes_and_stays_readable_as_one_stream() {
        let dir = std::env::temp_dir().join(format!("pulse-raw-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut log = RawLog::open(&dir).unwrap();

        log.push(r#"{"n":1}"#);
        log.push(r#"{"n":2}"#);
        assert_eq!(log.pending(), 2);
        assert_eq!(log.flush().unwrap(), 2);
        assert_eq!(log.pending(), 0);

        // A second flush writes a second gzip member to the same file.
        log.push(r#"{"n":3}"#);
        assert_eq!(log.flush().unwrap(), 1);
        assert_eq!(log.written, 3);

        // MultiGzDecoder reads concatenated members as a single stream.
        let text = read_all(&dir.join("events.jsonl.gz"));
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines, vec![r#"{"n":1}"#, r#"{"n":2}"#, r#"{"n":3}"#]);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn flushing_nothing_is_a_noop() {
        let dir = std::env::temp_dir().join(format!("pulse-raw-noop-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut log = RawLog::open(&dir).unwrap();
        assert_eq!(log.flush().unwrap(), 0);
        assert_eq!(log.written, 0);
        fs::remove_dir_all(&dir).unwrap();
    }
}
