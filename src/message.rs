use simple_error::bail;
use std::error::Error;
use std::fmt;

pub const CONNACK: [u8; 4] = [0x20, 2, 0, 0];
pub const DISCONNECT: [u8; 2] = [0xE0, 0];
pub const PINGREQ: [u8; 2] = [0xC0, 0];
pub const PINGRESP: [u8; 2] = [0xD0, 0];

#[derive(PartialEq)]
pub enum Message {
    Pingresp,
    Connack,
    Publish {
        id: Vec<u8>,
        topic: String,
        qos: u8,
        payload: Vec<u8>,
    },
    Suback(Vec<u8>),
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pingresp => write!(f, "PINGRESP"),
            Self::Connack => write!(f, "CONNACK"),
            Self::Publish { topic, .. } => write!(f, "PUBLISH {}", topic),
            Self::Suback(msg) => write!(f, "SUBACK {}", msg[3]),
        }
    }
}

pub fn parse_slice(msg: &[u8]) -> Result<Message, Box<dyn Error>> {
    if msg.len() < 2 {
        bail!("Message too short to be valid");
    }

    match msg[0] >> 4 {
        2 => {
            if msg[0..4] != CONNACK {
                bail!("Error in CONNACK, expected [32, 2, 0, 0], got {:?}", &msg);
            }
            Ok(Message::Connack)
        }
        3 => parse_publish(msg),
        9 => Ok(Message::Suback(msg.to_vec())),
        13 => {
            if msg[0..2] != PINGRESP {
                bail!("Error in PINGRESP, expected [12, 0], got {:?}", &msg);
            }
            Ok(Message::Pingresp)
        }
        _ => bail!(
            "Unrecognized message type {} in message {:?}",
            &msg[0] >> 4,
            &msg
        ),
    }
}

pub fn parse_publish(publish: &[u8]) -> Result<Message, Box<dyn Error>> {
    let qos = match publish[0] & 6 {
        0 => 0,
        2 => 1,
        4 => bail!("Can't handle PUBLISH QoS 2"),
        _ => bail!("Unexpected QoS value {}", publish[0] & 0x0F),
    };

    let (len, offset) = super::decode_length(publish);
    if len == 0 {
        bail!("Empty publish");
    }
    let topic_len = ((u16::from(publish[offset]) << 8) + u16::from(publish[offset + 1])) as usize;
    let topic = String::from_utf8(publish[offset + 2..topic_len + offset + 2].to_vec())?;

    let mut payload_offset = topic_len + offset + 2;
    let mut id = Vec::new();
    if qos == 1 {
        id.extend_from_slice(&publish[payload_offset..payload_offset + 2]);
        payload_offset += 2; // message ID after topic
    }
    let payload = publish[payload_offset..].to_vec();

    Ok(Message::Publish {
        id,
        topic,
        qos,
        payload,
    })
}

#[allow(clippy::cast_possible_truncation)]
pub fn make_publish(topic: &str, payload: &str) -> Vec<u8> {
    let topic_len = topic.len();
    if topic_len > 127 {
        panic!("Topic length must be less than 127 chars");
    }
    let len = topic_len + payload.len() + 2;
    let topic_len = topic_len as u8;
    let mut len_bytes = super::encode_length(len);

    let mut msg = vec![0x30];
    msg.append(&mut len_bytes);
    msg.extend_from_slice(&[0, topic_len]);
    msg.append(&mut Vec::from(topic));
    msg.append(&mut Vec::from(payload));

    msg
}

pub fn make_puback(msg_id: &[u8]) -> Vec<u8> {
    vec![0x40, 2, msg_id[0], msg_id[1]]
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_connack() {
        assert_eq!(Message::Connack, parse_slice(&CONNACK).unwrap());

        let mut connack_err = CONNACK;
        connack_err[3] = 1;

        assert!(parse_slice(&connack_err).is_err());
    }

    #[test]
    fn parse_pingresp() {
        assert_eq!(Message::Pingresp, parse_slice(&PINGRESP).unwrap());

        let pingresp_err = [PINGRESP[0], 1, 0];

        assert!(parse_slice(&pingresp_err).is_err());
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_slice(&[PINGRESP[0]]).is_err());
        assert!(parse_slice(&[0, 1, 1]).is_err());
    }

    #[test]
    fn puback_gen() {
        assert_eq!(make_puback(&[0, 27]), vec![0x40, 2, 0, 27]);
        assert_eq!(make_puback(&[12, 14]), vec![0x40, 2, 12, 14]);
    }

    #[test]
    fn publish() {
        match parse_publish(&[0x32, 7, 0, 3, b'a', b'/', b'b', 0, 27]).unwrap() {
            Message::Publish {
                id,
                topic,
                qos,
                payload,
            } => {
                assert_eq!(id, vec![0, 27]);
                assert_eq!(topic, "a/b");
                assert_eq!(qos, 1);
                assert_eq!(payload, Vec::<u8>::new());
            }
            _ => panic!("Received non-publish from parse"),
        };

        match parse_publish(&[0x30, 5, 0, 3, b'a', b'/', b'b']).unwrap() {
            Message::Publish {
                id,
                topic,
                qos,
                payload,
            } => {
                assert_eq!(id, Vec::<u8>::new());
                assert_eq!(topic, "a/b");
                assert_eq!(qos, 0);
                assert_eq!(payload, Vec::<u8>::new());
            }
            _ => panic!("Received non-publish from parse"),
        };

        assert!(parse_publish(&[0x34, 7, 0, 3, b'a', b'/', b'b', 0, 27]).is_err())
    }

    #[test]
    fn short_publish() {
        assert_eq!(
            make_publish("test/topic", "this is a payload"),
            vec![
                0x30, 29, 0, 10, 116, 101, 115, 116, 47, 116, 111, 112, 105, 99, 116, 104, 105,
                115, 32, 105, 115, 32, 97, 32, 112, 97, 121, 108, 111, 97, 100
            ]
        );
    }
}
