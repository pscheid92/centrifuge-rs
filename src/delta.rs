//! Fossil delta apply algorithm.
//!
//! Applies a Fossil SCM delta to a source byte array, producing the target.
//! Only the apply direction is needed — the server creates deltas, the client
//! applies them.
//!
//! Format: `<size>\n<commands>;`
//! Commands: `<count>@<offset>,` (copy from source), `<count>:<bytes>` (insert literal)
//! Terminator: `<checksum>;`

/// Errors that can occur when applying a Fossil delta.
#[derive(Debug, thiserror::Error)]
pub enum DeltaError {
    #[error("unexpected end of delta")]
    UnexpectedEnd,
    #[error("invalid byte in delta integer")]
    InvalidByte,
    #[error("size integer not terminated by newline")]
    MissingNewline,
    #[error("copy command not terminated by comma")]
    MissingComma,
    #[error("copy extends past end of source (offset {offset}, count {count}, source len {source_len})")]
    CopyPastSource {
        offset: usize,
        count: usize,
        source_len: usize,
    },
    #[error("copy exceeds declared output size")]
    CopyExceedsLimit,
    #[error("insert exceeds declared output size")]
    InsertExceedsLimit,
    #[error("insert count exceeds remaining delta size")]
    InsertPastDelta,
    #[error("bad checksum: expected {expected:#X}, got {actual:#X}")]
    BadChecksum { expected: u32, actual: u32 },
    #[error("output size {actual} does not match declared size {expected}")]
    SizeMismatch { expected: usize, actual: usize },
    #[error("unknown delta operator: '{0}'")]
    UnknownOperator(char),
    #[error("unterminated delta (missing ';')")]
    Unterminated,
}

// Base64-like character value table used by Fossil delta encoding.
const Z_VALUE: [i8; 128] = {
    let mut t = [-1i8; 128];
    t[b'0' as usize] = 0;
    t[b'1' as usize] = 1;
    t[b'2' as usize] = 2;
    t[b'3' as usize] = 3;
    t[b'4' as usize] = 4;
    t[b'5' as usize] = 5;
    t[b'6' as usize] = 6;
    t[b'7' as usize] = 7;
    t[b'8' as usize] = 8;
    t[b'9' as usize] = 9;
    t[b'A' as usize] = 10;
    t[b'B' as usize] = 11;
    t[b'C' as usize] = 12;
    t[b'D' as usize] = 13;
    t[b'E' as usize] = 14;
    t[b'F' as usize] = 15;
    t[b'G' as usize] = 16;
    t[b'H' as usize] = 17;
    t[b'I' as usize] = 18;
    t[b'J' as usize] = 19;
    t[b'K' as usize] = 20;
    t[b'L' as usize] = 21;
    t[b'M' as usize] = 22;
    t[b'N' as usize] = 23;
    t[b'O' as usize] = 24;
    t[b'P' as usize] = 25;
    t[b'Q' as usize] = 26;
    t[b'R' as usize] = 27;
    t[b'S' as usize] = 28;
    t[b'T' as usize] = 29;
    t[b'U' as usize] = 30;
    t[b'V' as usize] = 31;
    t[b'W' as usize] = 32;
    t[b'X' as usize] = 33;
    t[b'Y' as usize] = 34;
    t[b'Z' as usize] = 35;
    t[b'_' as usize] = 36;
    t[b'a' as usize] = 37;
    t[b'b' as usize] = 38;
    t[b'c' as usize] = 39;
    t[b'd' as usize] = 40;
    t[b'e' as usize] = 41;
    t[b'f' as usize] = 42;
    t[b'g' as usize] = 43;
    t[b'h' as usize] = 44;
    t[b'i' as usize] = 45;
    t[b'j' as usize] = 46;
    t[b'k' as usize] = 47;
    t[b'l' as usize] = 48;
    t[b'm' as usize] = 49;
    t[b'n' as usize] = 50;
    t[b'o' as usize] = 51;
    t[b'p' as usize] = 52;
    t[b'q' as usize] = 53;
    t[b'r' as usize] = 54;
    t[b's' as usize] = 55;
    t[b't' as usize] = 56;
    t[b'u' as usize] = 57;
    t[b'v' as usize] = 58;
    t[b'w' as usize] = 59;
    t[b'x' as usize] = 60;
    t[b'y' as usize] = 61;
    t[b'z' as usize] = 62;
    t[b'~' as usize] = 63;
    t
};

struct DeltaReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> DeltaReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn have_bytes(&self) -> bool {
        self.pos < self.data.len()
    }

    fn get_byte(&mut self) -> Result<u8, DeltaError> {
        if self.pos >= self.data.len() {
            return Err(DeltaError::UnexpectedEnd);
        }
        let b = self.data[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn get_char(&mut self) -> Result<char, DeltaError> {
        Ok(self.get_byte()? as char)
    }

    /// Read a base64-encoded unsigned integer.
    fn get_int(&mut self) -> Result<u32, DeltaError> {
        let mut v: u32 = 0;
        while self.have_bytes() {
            let b = self.get_byte()? as usize;
            if b >= 128 {
                return Err(DeltaError::InvalidByte);
            }
            let c = Z_VALUE[b];
            if c < 0 {
                self.pos -= 1; // put back the terminator
                break;
            }
            v = (v << 6) + c as u32;
        }
        Ok(v)
    }
}

/// Compute the Fossil checksum of a byte array.
fn checksum(data: &[u8]) -> u32 {
    let mut sums = [0u32; 4];

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        sums[0] = sums[0].wrapping_add(chunk[0] as u32);
        sums[1] = sums[1].wrapping_add(chunk[1] as u32);
        sums[2] = sums[2].wrapping_add(chunk[2] as u32);
        sums[3] = sums[3].wrapping_add(chunk[3] as u32);
    }

    let mut result = sums[3]
        .wrapping_add(sums[2] << 8)
        .wrapping_add(sums[1] << 16)
        .wrapping_add(sums[0] << 24);

    if remainder.len() >= 3 {
        result = result.wrapping_add((remainder[2] as u32) << 8);
    }
    if remainder.len() >= 2 {
        result = result.wrapping_add((remainder[1] as u32) << 16);
    }
    if !remainder.is_empty() {
        result = result.wrapping_add((remainder[0] as u32) << 24);
    }

    result
}

/// Apply a Fossil delta to a source byte array, producing the target.
pub fn apply_delta(source: &[u8], delta: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let mut reader = DeltaReader::new(delta);

    let limit = reader.get_int()? as usize;
    if reader.get_char()? != '\n' {
        return Err(DeltaError::MissingNewline);
    }

    let mut output = Vec::with_capacity(limit);
    let mut total = 0;

    while reader.have_bytes() {
        let cnt_raw = reader.get_int()?;
        let cnt = cnt_raw as usize;

        match reader.get_char()? {
            '@' => {
                let ofst = reader.get_int()? as usize;
                if reader.have_bytes() && reader.get_char()? != ',' {
                    return Err(DeltaError::MissingComma);
                }
                total += cnt;
                if total > limit {
                    return Err(DeltaError::CopyExceedsLimit);
                }
                if ofst + cnt > source.len() {
                    return Err(DeltaError::CopyPastSource {
                        offset: ofst,
                        count: cnt,
                        source_len: source.len(),
                    });
                }
                output.extend_from_slice(&source[ofst..ofst + cnt]);
            }
            ':' => {
                total += cnt;
                if total > limit {
                    return Err(DeltaError::InsertExceedsLimit);
                }
                if reader.pos + cnt > delta.len() {
                    return Err(DeltaError::InsertPastDelta);
                }
                output.extend_from_slice(&delta[reader.pos..reader.pos + cnt]);
                reader.pos += cnt;
            }
            ';' => {
                let actual = checksum(&output);
                if cnt_raw != actual {
                    return Err(DeltaError::BadChecksum {
                        expected: cnt_raw,
                        actual,
                    });
                }
                if total != limit {
                    return Err(DeltaError::SizeMismatch {
                        expected: limit,
                        actual: total,
                    });
                }
                return Ok(output);
            }
            c => {
                return Err(DeltaError::UnknownOperator(c));
            }
        }
    }

    Err(DeltaError::Unterminated)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_delta_insert_only() {
        let source = b"";
        let hello = b"hello";
        let cksum = checksum(hello);

        let mut delta = vec![b'5', b'\n', b'5', b':'];
        delta.extend_from_slice(hello);
        delta.extend_from_slice(encode_int(cksum).as_bytes());
        delta.push(b';');

        let result = apply_delta(source, &delta).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn test_apply_delta_copy_only() {
        let source = b"hello world";
        let expected = b"hello";
        let cksum = checksum(expected);

        let mut delta = vec![b'5', b'\n', b'5', b'@', b'0', b','];
        delta.extend_from_slice(encode_int(cksum).as_bytes());
        delta.push(b';');

        let result = apply_delta(source, &delta).unwrap();
        assert_eq!(result, b"hello");
    }

    #[test]
    fn test_apply_delta_copy_and_insert() {
        let source = b"hello";
        let expected = b"hello world";
        let cksum = checksum(expected);

        let mut delta = Vec::new();
        delta.extend_from_slice(encode_int(11).as_bytes());
        delta.extend_from_slice(b"\n5@0,6:");
        delta.extend_from_slice(b" world");
        delta.extend_from_slice(encode_int(cksum).as_bytes());
        delta.push(b';');

        let result = apply_delta(source, &delta).unwrap();
        assert_eq!(result, b"hello world");
    }

    #[test]
    fn test_apply_delta_bad_checksum() {
        let source = b"";
        let mut delta = vec![b'2', b'\n', b'2', b':'];
        delta.extend_from_slice(b"hi");
        delta.extend_from_slice(encode_int(99999).as_bytes());
        delta.push(b';');

        assert!(matches!(
            apply_delta(source, &delta),
            Err(DeltaError::BadChecksum { .. })
        ));
    }

    #[test]
    fn test_checksum() {
        assert_eq!(checksum(b""), 0);
        assert_eq!(checksum(b"hello"), checksum(b"hello"));
        assert_ne!(checksum(b"hello"), checksum(b"world"));
    }

    #[test]
    fn test_apply_delta_missing_newline() {
        assert!(matches!(apply_delta(b"", b"5\x01"), Err(DeltaError::MissingNewline)));
    }

    #[test]
    fn test_apply_delta_copy_past_source() {
        let mut d = vec![b'A', b'\n', b'A', b'@', b'0', b','];
        d.extend_from_slice(b"0;");
        assert!(matches!(
            apply_delta(b"abc", &d),
            Err(DeltaError::CopyPastSource { .. })
        ));
    }

    #[test]
    fn test_apply_delta_copy_exceeds_limit() {
        let mut d = vec![b'1', b'\n', b'5', b'@', b'0', b','];
        d.extend_from_slice(b"0;");
        assert!(matches!(apply_delta(b"hello", &d), Err(DeltaError::CopyExceedsLimit)));
    }

    #[test]
    fn test_apply_delta_insert_exceeds_limit() {
        let mut d = vec![b'1', b'\n', b'5', b':'];
        d.extend_from_slice(b"hello");
        d.extend_from_slice(b"0;");
        assert!(matches!(apply_delta(b"", &d), Err(DeltaError::InsertExceedsLimit)));
    }

    #[test]
    fn test_apply_delta_unknown_operator() {
        let d = vec![b'1', b'\n', b'1', b'!'];
        assert!(matches!(apply_delta(b"", &d), Err(DeltaError::UnknownOperator('!'))));
    }

    #[test]
    fn test_apply_delta_unterminated() {
        let mut d = vec![b'5', b'\n', b'5', b':'];
        d.extend_from_slice(b"hello");
        assert!(matches!(apply_delta(b"", &d), Err(DeltaError::Unterminated)));
    }

    #[test]
    fn test_apply_delta_size_mismatch() {
        let hello = b"hello";
        let cksum = checksum(hello);
        let mut d = vec![b'A', b'\n', b'5', b':'];
        d.extend_from_slice(hello);
        d.extend_from_slice(encode_int(cksum).as_bytes());
        d.push(b';');
        assert!(matches!(apply_delta(b"", &d), Err(DeltaError::SizeMismatch { .. })));
    }

    #[test]
    fn test_apply_delta_empty() {
        assert!(apply_delta(b"", b"").is_err());
    }

    /// Encode a u32 as a Fossil base64 integer (for test delta construction).
    fn encode_int(mut v: u32) -> String {
        const DIGITS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~";
        if v == 0 {
            return "0".into();
        }
        let mut buf = Vec::new();
        while v > 0 {
            buf.push(DIGITS[(v & 0x3f) as usize]);
            v >>= 6;
        }
        buf.reverse();
        String::from_utf8(buf).unwrap()
    }
}
