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
    let mut sign = 1.0_f64;
    let mut integer = 0.0_f64;
    let mut fraction = 0.0_f64;
    let mut divisor = 1.0_f64;
    let mut saw_digit = false;
    let mut saw_decimal = false;

    for ch in value.chars() {
        if ch.is_ascii_digit() {
            let digit = f64::from(ch as u8 - b'0');
            if saw_decimal {
                divisor *= 10.0;
                fraction += digit / divisor;
            } else {
                integer = integer.mul_add(10.0, digit);
            }
            saw_digit = true;
        } else if matches!(ch, '.' | ',') {
            saw_decimal = true;
        } else if matches!(ch, '-' | '\u{2212}' | '\u{2013}' | '\u{2014}') {
            if !saw_digit && !saw_decimal {
                sign = -1.0;
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

    Some(sign * (integer + fraction))
}

#[cfg(test)]
mod tests {
    #[test]
    fn parse_percent_keeps_existing_separator_behavior() {
        assert_eq!(super::parse_percent(" +12,345.67 % "), Some(12.34567));
    }

    #[test]
    fn parse_percent_handles_signs_and_unicode_spacing() {
        assert_eq!(super::parse_percent("\u{202f}-0.25%"), Some(-0.25));
        assert_eq!(super::parse_percent("\u{2212}.5%"), Some(-0.5));
        assert_eq!(super::parse_percent("no digits"), None);
    }

    #[test]
    fn parse_u32_ignores_non_digits_and_checks_overflow() {
        assert_eq!(super::parse_u32("Rank #12345"), Some(12345));
        assert_eq!(super::parse_u32("rank"), None);
        assert_eq!(super::parse_u32("999999999999999999999"), None);
    }
}
