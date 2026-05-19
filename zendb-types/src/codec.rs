//! Encoding and decoding helpers shared across the crate.
//!
//! Currently provides variable-length unsigned integer encoding (LEB128-like).

/// Maximum bytes a u64 can occupy in varint encoding.
pub const MAX_VARINT_BYTES: usize = 10; // ceil(64 / 7) = 10 for u64

/// Encode a u64 as a variable-length integer, appending to `out`.
///
/// Uses a LEB128-like scheme: 7 bits of data per byte, MSB set on all bytes
/// except the last. This is compact for small values (most lengths and counts
/// fit in 1–2 bytes).
pub fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

/// Decode a variable-length integer from `bytes`, returning the value and the
/// number of bytes consumed.
///
/// Returns `None` if the input is truncated (ends with a continuation byte)
/// or if the encoding would overflow a u64 (more than 9 bytes).
pub fn decode_varint(bytes: &[u8]) -> Option<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;
    for (i, &byte) in bytes.iter().enumerate() {
        if i >= MAX_VARINT_BYTES {
            // Would overflow u64
            return None;
        }
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
        shift += 7;
    }
    // Truncated: ended with a continuation byte
    None
}

/// Read exactly N bytes into a fixed-size array.
pub fn read_fixed<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    if bytes.len() < N {
        return None;
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&bytes[..N]);
    Some(arr)
}

/// Encode a length-prefixed UTF-8 string.
pub fn encode_string(out: &mut Vec<u8>, s: &str) {
    encode_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Decode a length-prefixed UTF-8 string. Returns string and bytes consumed.
pub fn decode_string(bytes: &[u8]) -> Option<(String, usize)> {
    let (len, n) = decode_varint(bytes)?;
    let end = n + len as usize;
    if bytes.len() < end {
        return None;
    }
    let s = String::from_utf8(bytes[n..end].to_vec()).ok()?;
    Some((s, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_zero() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 0);
        assert_eq!(buf, &[0x00]);
        assert_eq!(decode_varint(&buf), Some((0, 1)));
    }

    #[test]
    fn varint_small() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 127);
        assert_eq!(buf, &[0x7F]);
        assert_eq!(decode_varint(&buf), Some((127, 1)));
    }

    #[test]
    fn varint_two_bytes() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, 128);
        assert_eq!(buf, &[0x80, 0x01]);
        assert_eq!(decode_varint(&buf), Some((128, 2)));
    }

    #[test]
    fn varint_u64_max() {
        let mut buf = Vec::new();
        encode_varint(&mut buf, u64::MAX);
        let (val, _len) = decode_varint(&buf).unwrap();
        assert_eq!(val, u64::MAX);
    }

    #[test]
    fn varint_roundtrip() {
        for &val in &[
            0,
            1,
            127,
            128,
            255,
            256,
            1000,
            1_000_000,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut buf = Vec::new();
            encode_varint(&mut buf, val);
            let (decoded, consumed) = decode_varint(&buf).unwrap();
            assert_eq!(decoded, val, "failed for {}", val);
            assert_eq!(consumed, buf.len(), "wrong consumed for {}", val);
        }
    }

    #[test]
    fn varint_truncated() {
        // Byte with continuation bit set, but nothing after
        assert_eq!(decode_varint(&[0x80]), None);
        assert_eq!(decode_varint(&[0x80, 0x80]), None);
    }
}
