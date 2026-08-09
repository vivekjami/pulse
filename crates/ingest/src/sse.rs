//! A minimal SSE frame parser.
//!
//! We parse the protocol ourselves rather than take a client library, because
//! the gapless-resume guarantee (ARCHITECTURE.md §2) hinges on owning exactly
//! what we do with the `id:` field: Wikimedia's id is a JSON array of Kafka
//! offsets, and it is the token we persist and replay from.

/// One dispatched SSE frame.
#[derive(Debug, Default, Clone)]
pub struct Frame {
    /// Contents of the `id:` field — opaque; goes straight back as `Last-Event-ID`.
    pub id: Option<String>,
    /// `event:` type. Wikimedia sends `message`.
    pub event: Option<String>,
    /// Concatenated `data:` lines.
    pub data: String,
}

impl Frame {
    fn is_empty(&self) -> bool {
        self.data.is_empty() && self.id.is_none()
    }
}

/// Incremental parser: feed it bytes, drain complete frames.
#[derive(Default)]
pub struct Parser {
    /// Bytes received but not yet terminated by a newline.
    line_buf: Vec<u8>,
    /// The frame under construction.
    current: Frame,
}

impl Parser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed a chunk; returns every frame completed by it.
    pub fn feed(&mut self, chunk: &[u8]) -> Vec<Frame> {
        let mut out = Vec::new();
        for &byte in chunk {
            if byte == b'\n' {
                let line = std::mem::take(&mut self.line_buf);
                // Tolerate CRLF.
                let line = if line.last() == Some(&b'\r') {
                    &line[..line.len() - 1]
                } else {
                    &line[..]
                };
                if let Some(frame) = self.consume_line(line) {
                    out.push(frame);
                }
            } else {
                self.line_buf.push(byte);
            }
        }
        out
    }

    /// Returns a frame when the line is the blank dispatch delimiter.
    fn consume_line(&mut self, line: &[u8]) -> Option<Frame> {
        // Blank line dispatches the accumulated frame.
        if line.is_empty() {
            let frame = std::mem::take(&mut self.current);
            return if frame.is_empty() { None } else { Some(frame) };
        }

        // Lines starting with ':' are comments / keep-alives.
        if line[0] == b':' {
            return None;
        }

        let (field, value) = match line.iter().position(|&b| b == b':') {
            Some(i) => {
                let value = &line[i + 1..];
                // A single leading space after the colon is stripped by spec.
                let value = value.strip_prefix(b" ").unwrap_or(value);
                (&line[..i], value)
            }
            // A field with no colon is a field name with an empty value.
            None => (line, &[][..]),
        };

        let value = String::from_utf8_lossy(value).into_owned();
        match field {
            b"id" => self.current.id = Some(value),
            b"event" => self.current.event = Some(value),
            b"data" => {
                if !self.current.data.is_empty() {
                    self.current.data.push('\n');
                }
                self.current.data.push_str(&value);
            }
            // `retry:` and unknown fields are ignored — our backoff is our own.
            _ => {}
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_wikimedia_shaped_frame() {
        let mut p = Parser::new();
        let frames = p.feed(
            b"event: message\nid: [{\"topic\":\"rc\",\"partition\":0,\"offset\":42}]\ndata: {\"a\":1}\n\n",
        );
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event.as_deref(), Some("message"));
        assert_eq!(frames[0].data, "{\"a\":1}");
        assert!(frames[0].id.as_deref().unwrap().contains("offset"));
    }

    #[test]
    fn reassembles_frames_split_across_chunks() {
        let mut p = Parser::new();
        assert!(p.feed(b"data: {\"par").is_empty());
        assert!(p.feed(b"tial\":true}").is_empty());
        let frames = p.feed(b"\n\n");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "{\"partial\":true}");
    }

    #[test]
    fn joins_multiline_data_and_skips_comments() {
        let mut p = Parser::new();
        let frames = p.feed(b":keep-alive\ndata: one\ndata: two\n\n");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "one\ntwo");
    }

    #[test]
    fn handles_crlf_and_back_to_back_frames() {
        let mut p = Parser::new();
        let frames = p.feed(b"data: a\r\n\r\ndata: b\r\n\r\n");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, "a");
        assert_eq!(frames[1].data, "b");
    }

    #[test]
    fn blank_line_without_content_dispatches_nothing() {
        let mut p = Parser::new();
        assert!(p.feed(b"\n\n\n").is_empty());
    }
}
