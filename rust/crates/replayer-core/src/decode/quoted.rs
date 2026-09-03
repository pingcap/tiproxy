// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(crate) fn parse_go_quoted(input: &str) -> Result<String, String> {
    let bytes = input.as_bytes();
    if bytes.len() < 2 || bytes.first() != Some(&b'"') || bytes.last() != Some(&b'"') {
        return Err("value must be a Go-quoted string".to_owned());
    }
    let mut output = Vec::with_capacity(bytes.len().saturating_sub(2));
    let mut index = 1;
    while index + 1 < bytes.len() {
        if bytes[index] != b'\\' {
            output.push(bytes[index]);
            index += 1;
            continue;
        }
        index += 1;
        let escaped = *bytes
            .get(index)
            .ok_or_else(|| "value has a trailing escape".to_owned())?;
        match escaped {
            b'a' => output.push(0x07),
            b'b' => output.push(0x08),
            b'f' => output.push(0x0c),
            b'n' => output.push(b'\n'),
            b'r' => output.push(b'\r'),
            b't' => output.push(b'\t'),
            b'v' => output.push(0x0b),
            b'\\' | b'"' | b'\'' => output.push(escaped),
            b'x' => {
                output.push(
                    u8::try_from(parse_hex(bytes, index + 1, 2)?)
                        .map_err(|_| "hex escape is larger than one byte".to_owned())?,
                );
                index += 2;
            }
            b'u' => {
                append_unicode(&mut output, parse_hex(bytes, index + 1, 4)?)?;
                index += 4;
            }
            b'U' => {
                append_unicode(&mut output, parse_hex(bytes, index + 1, 8)?)?;
                index += 8;
            }
            b'0'..=b'7' => {
                let end = index
                    .checked_add(3)
                    .ok_or_else(|| "octal escape overflow".to_owned())?;
                let digits = bytes
                    .get(index..end)
                    .ok_or_else(|| "short octal escape".to_owned())?;
                let mut value = 0_u16;
                for digit in digits {
                    if !(b'0'..=b'7').contains(digit) {
                        return Err("invalid octal escape".to_owned());
                    }
                    value = value * 8 + u16::from(*digit - b'0');
                }
                if value > u16::from(u8::MAX) {
                    return Err("octal escape is larger than one byte".to_owned());
                }
                output.push(
                    u8::try_from(value)
                        .map_err(|_| "octal escape is larger than one byte".to_owned())?,
                );
                index += 2;
            }
            other => return Err(format!("unsupported escape \\{}", char::from(other))),
        }
        index += 1;
    }
    String::from_utf8(output).map_err(|_| "quoted value is not valid UTF-8".to_owned())
}

fn parse_hex(bytes: &[u8], start: usize, count: usize) -> Result<u32, String> {
    let end = start
        .checked_add(count)
        .ok_or_else(|| "hex escape overflow".to_owned())?;
    let digits = bytes
        .get(start..end)
        .ok_or_else(|| "short hex escape".to_owned())?;
    let mut value = 0_u32;
    for digit in digits {
        value = value
            .checked_mul(16)
            .and_then(|current| char::from(*digit).to_digit(16).map(|next| current + next))
            .ok_or_else(|| "invalid hex escape".to_owned())?;
    }
    Ok(value)
}

fn append_unicode(output: &mut Vec<u8>, value: u32) -> Result<(), String> {
    let value = char::from_u32(value).ok_or_else(|| "invalid Unicode escape".to_owned())?;
    let mut encoded = [0_u8; 4];
    output.extend_from_slice(value.encode_utf8(&mut encoded).as_bytes());
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn accepts_go_specific_escapes() {
        assert_eq!(
            parse_go_quoted(r#""a\x00\u4e2d\U0001f60a\141""#).expect("valid"),
            "a\0中😊a"
        );
    }

    #[test]
    fn rejects_invalid_utf8_and_short_escapes() {
        assert!(parse_go_quoted(r#""\xff""#).is_err());
        assert!(parse_go_quoted(r#""\x0""#).is_err());
    }
}
