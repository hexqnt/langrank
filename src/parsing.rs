pub fn parse_u32(value: &str) -> Option<u32> {
    let mut parsed = 0_u32;
    let mut saw_digit = false;

    for byte in value.bytes() {
        if byte.is_ascii_digit() {
            let digit = u32::from(byte - b'0');
            parsed = parsed.checked_mul(10)?.checked_add(digit)?;
            saw_digit = true;
        }
    }

    saw_digit.then_some(parsed)
}

pub fn parse_percent(value: &str) -> Option<f64> {
    let mut buf = String::with_capacity(value.len());
    let mut saw_digit = false;
    let mut saw_decimal = false;

    for ch in value.chars() {
        if ch.is_ascii_digit() {
            buf.push(ch);
            saw_digit = true;
        } else if matches!(ch, '.' | ',') {
            if !saw_decimal {
                buf.push('.');
                saw_decimal = true;
            }
        } else if matches!(ch, '-' | '\u{2212}' | '\u{2013}' | '\u{2014}') {
            if buf.is_empty() {
                buf.push('-');
            }
        } else if matches!(
            ch,
            '+' | '%' | ' ' | '\t' | '\n' | '\r' | '\u{00a0}' | '\u{202f}'
        ) {
            // Ignore separators and whitespace.
        }
    }

    if !saw_digit {
        return None;
    }

    buf.parse::<f64>().ok()
}
