pub mod client;
pub mod error;
pub mod message;

// https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/MQTT_V3.1_Protocol_Specific.pdf

type PublishHandler = Box<dyn Fn(Vec<u8>) -> Option<Vec<u8>> + Send>;

#[allow(clippy::cast_possible_truncation)]
fn encode_length(mut length: usize) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4);
    while length > 0 {
        let mut byte = length % 128;
        length /= 128;
        if length > 0 {
            byte |= 0x80;
        }
        bytes.push(byte as u8);
    }
    bytes
}

fn decode_length(header: &[u8]) -> (usize, usize) {
    let mut multiplier = 1;
    let mut len: usize = 0;
    let mut digit = 128;
    let mut i = 0;
    while digit & 128 != 0 {
        i += 1;
        digit = header[i];
        len += (digit as usize & 127) * multiplier;
        multiplier *= 128
    }
    (len, i + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length() {
        assert_eq!(encode_length(16), vec![16]);
        assert_eq!(decode_length(&[0, 16]), (16, 2));

        assert_eq!(encode_length(97), vec![97]);
        assert_eq!(decode_length(&[0, 97]), (97, 2));

        assert_eq!(encode_length(127), vec![127]);
        assert_eq!(decode_length(&[0, 127]), (127, 2));

        assert_eq!(encode_length(128), vec![0x80, 1]);
        assert_eq!(decode_length(&[0, 0x80, 1]), (128, 3));

        assert_eq!(encode_length(321), vec![0xC1, 2]);
        assert_eq!(decode_length(&[0, 0xC1, 2]), (321, 3));

        assert_eq!(encode_length(16383), vec![0xFF, 0x7F]);
        assert_eq!(decode_length(&[0, 0xFF, 0x7F]), (16383, 3));

        assert_eq!(encode_length(16384), vec![0x80, 0x80, 1]);
        assert_eq!(decode_length(&[0, 0x80, 0x80, 1]), (16384, 4));
    }
}
