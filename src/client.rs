use super::message::{self, Message};
use simple_error::bail;
use std::collections::HashMap;
use std::error::Error;
use std::io::{self, prelude::*};
use std::net::TcpStream;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time;

struct ConnectedClient {
    tx: mpsc::Sender<Vec<u8>>,
    o_stream_thread: Option<JoinHandle<()>>,
}

pub struct Client {
    client_id: Vec<u8>,
    username: Vec<u8>,
    password: Vec<u8>,
    connected: Option<ConnectedClient>,
    keep_alive_secs: u8,
    pending_subscribe_ids: Arc<Mutex<Vec<u8>>>,
    next_message_id: u8,
    publish_functions: Arc<Mutex<HashMap<String, super::PublishHandler>>>,
}

impl Client {
    pub fn new(client_id: &str, username: &str, password: &str, keep_alive_secs: u8) -> Self {
        let client_id_len = client_id.len();
        if !(1..=23).contains(&client_id_len) {
            panic!("Client ID must be between 1 and 23 characters in length");
        }

        let username_len = username.len();
        if !(1..=12).contains(&username_len) {
            panic!("Username should be between 1 and 23 characters in length");
        }

        let password_len = password.len();
        if !(1..=12).contains(&password_len) {
            panic!("Password should be between 1 and 23 characters in length");
        }

        Self {
            client_id: Vec::from(client_id),
            username: Vec::from(username),
            password: Vec::from(password),
            connected: None,
            keep_alive_secs,
            pending_subscribe_ids: Arc::new(Mutex::new(Vec::new())),
            next_message_id: 1,
            publish_functions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    fn make_connect(&self) -> Vec<u8> {
        let client_id_len = self.client_id.len() as u8;

        let username_len = self.username.len() as u8;

        let password_len = self.password.len() as u8;

        let len = 20 + client_id_len + username_len + password_len;

        if len > 127 {
            panic!("We don't support sending large messages yet");
        }

        let mut connect_msg = Vec::<u8>::with_capacity(len as usize);
        connect_msg.extend_from_slice(&[
            0x10,    // CONNECT
            len - 2, // message length (-2 for first 2 fixed bytes)
            0,       // protocol name len
            6,       // protocol name len
            b'M',    // protocol name
            b'Q',
            b'I',
            b's',
            b'd',
            b'p',
            3,                    // protocol version
            0xC2,                 // connect flags (username, password, clean session)
            0,                    // keep alive
            self.keep_alive_secs, // keep alive 60 seconds
            0,                    // client ID len
            client_id_len,        // client ID len
        ]);

        connect_msg.extend_from_slice(&self.client_id[..]); // client_id

        // no will topic or will message

        connect_msg.extend_from_slice(&[0, username_len]); // username length
        connect_msg.extend_from_slice(&self.username[..]); // username

        connect_msg.extend_from_slice(&[0, password_len]); // password length
        connect_msg.extend_from_slice(&self.password[..]); // password

        connect_msg
    }

    #[allow(clippy::cast_possible_truncation)]
    fn make_subscribe(&mut self, topic: &str) -> Vec<u8> {
        let topic_len = topic.len();
        if topic_len > 127 {
            panic!("Topic length too long");
        }
        let len = topic_len + 5; // 2 bytes for variable header, 2 bytes for topic len, topic, 1 byte for QoS

        let mut subscribe_msg = Vec::<u8>::with_capacity(len + 2); // 2 bytes for fixed header
        subscribe_msg.extend_from_slice(&[
            0x82, // 8 - SUBSCRIBE, 2 - QoS 1
            len as u8,
            0,                    // message ID
            self.next_message_id, // message ID
            0,                    // topic length
            topic_len as u8,      // topic length
        ]);

        subscribe_msg.append(&mut Vec::from(topic));
        subscribe_msg.push(1); // QoS 1

        self.next_message_id += 1;

        subscribe_msg
    }

    pub fn connect(&mut self, addr: &str) -> Result<(), Box<dyn Error>> {
        let msg = self.make_connect();

        // TCP init

        let mut stream = TcpStream::connect(addr)?;
        stream.set_read_timeout(Some(time::Duration::from_secs(
            u64::from(self.keep_alive_secs) * 2,
        )))?;
        stream.set_nodelay(true)?;

        // CONNECT

        println!("Connecting...");

        stream.write_all(&msg[..])?;
        stream.flush()?;

        // CONNACK

        let mut buf = [0; 4];
        stream.read_exact(&mut buf)?;
        let connack = message::parse_slice(&buf).unwrap();
        match connack {
            Message::Connack => (),
            _ => bail!(
                "Expected {:?} from server, got {:?}",
                Message::Connack,
                connack
            ),
        };

        println!("Connected!");

        let (tx, rx): (mpsc::Sender<Vec<u8>>, mpsc::Receiver<Vec<u8>>) = mpsc::channel();

        let o_stream_thread =
            start_out_thread(stream.try_clone()?, rx).expect("Failed to created o_stream thread");

        self.start_in_thread(stream.try_clone()?, tx.clone())
            .expect("Failed to create i_stream thread");

        self.start_ping_thread(tx.clone())
            .expect("Failed to create ping thread");

        self.connected = Some(ConnectedClient {
            tx,
            o_stream_thread: Some(o_stream_thread),
        });

        Ok(())
    }

    pub fn subscribe(
        &mut self,
        topic: &str,
        f: super::PublishHandler,
    ) -> Result<(), Box<dyn Error>> {
        let sub_msg = self.make_subscribe(topic);

        println!("Subscribing...");

        self.publish_functions
            .lock()
            .expect("Error locking on publish functions")
            .insert(String::from(topic), f);

        let tx = match self.connected.as_ref() {
            Some(c) => &c.tx,
            None => bail!("Client not connected"),
        };

        self.pending_subscribe_ids
            .lock()
            .expect("Error locking on pending subscribe IDs")
            .push(sub_msg[3]);

        tx.send(sub_msg)?;

        Ok(())
    }

    pub fn disconnect(&mut self) {
        println!("Disconnecting...");

        let mut connected = self
            .connected
            .take()
            .expect("Attempt to disconnect while not connected");

        connected.tx.send(message::DISCONNECT.to_vec()).unwrap();
        connected
            .o_stream_thread
            .take()
            .expect("Error getting ostream thread on connected client")
            .join()
            .expect("Error joining ostream thread");
        drop(connected.tx);
    }

    pub fn publish(&mut self, topic: &str, payload: &str) {
        let msg = message::make_publish(topic, payload);

        self.connected
            .as_ref()
            .expect("Can't publish before connect")
            .tx
            .send(msg)
            .unwrap();
    }

    fn start_in_thread(
        &self,
        mut stream: TcpStream,
        tx: mpsc::Sender<Vec<u8>>,
    ) -> io::Result<JoinHandle<()>> {
        let pending_subscribe_ids = Arc::clone(&self.pending_subscribe_ids);
        let publish_functions = Arc::clone(&self.publish_functions);

        thread::Builder::new()
            .name("i_stream".into())
            .spawn(move || loop {
                let mut header = [0; 5];
                if stream.read(&mut header[..5]).unwrap() == 0 {
                    break;
                }
                let (len, bytes_read) = super::decode_length(&header);

                let mut buf = Vec::with_capacity(bytes_read + len);
                buf.extend_from_slice(&header);
                buf.resize(bytes_read + len, 0);

                if len > 3 {
                    if let Some(e) = stream.read_exact(&mut buf[5..]).err() {
                        eprintln!("Error reading istream {}", e);
                        continue;
                    }
                }
                match message::parse_slice(&buf) {
                    Ok(message) => match message {
                        Message::Pingresp => {}
                        Message::Suback(msg) => {
                            let mut pending_subscribe_ids = pending_subscribe_ids
                                .lock()
                                .expect("Error locking on subscribe IDs");
                            handle_suback(&msg, &mut pending_subscribe_ids);
                        }
                        Message::Publish {
                            id,
                            topic,
                            qos,
                            payload,
                        } => {
                            let publish_functions = publish_functions
                                .lock()
                                .expect("Error locking on publish functions");
                            let handler = publish_functions.get(&topic);
                            let mut responses = handle_publish(&id, &topic, qos, payload, handler);
                            for res in responses.drain(..) {
                                tx.send(res).unwrap();
                            }
                        }
                        _ => eprintln!("Unexpected message type: {:?}", message),
                    },
                    Err(e) => eprintln!("Error parsing message: {}", e),
                };
            })
    }

    fn start_ping_thread(&self, tx: mpsc::Sender<Vec<u8>>) -> io::Result<JoinHandle<()>> {
        let keep_alive_secs = self.keep_alive_secs;

        thread::Builder::new().name("ping".into()).spawn(move || {
            let interval = time::Duration::from_secs(u64::from(keep_alive_secs));
            loop {
                tx.send(message::PINGREQ.to_vec()).unwrap();
                thread::sleep(interval);
            }
        })
    }
}

fn handle_suback(suback: &[u8], pending_subscribe_ids: &mut Vec<u8>) {
    println!("Suback {}", suback[3]);
    if let Some(pos) = pending_subscribe_ids.iter().position(|&x| x == suback[3]) {
        pending_subscribe_ids.remove(pos);
    } else {
        eprintln!("Received suback for unknown ID {}", suback[3]);
    }
}

fn handle_publish(
    id: &[u8],
    topic: &str,
    qos: u8,
    payload: Vec<u8>,
    f: Option<&super::PublishHandler>,
) -> Vec<Vec<u8>> {
    println!("Publish topic {}", topic);

    let mut messages = Vec::with_capacity(2);
    if qos == 1 {
        messages.push(message::make_puback(id));
    }

    if let Some(f) = f {
        if let Some(response) = f(payload) {
            messages.push(response);
        }
    }

    messages
}

fn start_out_thread(
    mut stream: TcpStream,
    rx: mpsc::Receiver<Vec<u8>>,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("o_stream".into())
        .spawn(move || {
            while let Ok(msg) = rx.recv() {
                stream.write_all(&msg[..]).unwrap();
                stream.flush().unwrap();
                if msg == message::DISCONNECT.to_vec() {
                    stream.shutdown(std::net::Shutdown::Both).unwrap();
                    break;
                }
            }
        })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn short_connect() {
        let client = Client::new("iden", "username", "password", 15);
        assert_eq!(
            client.make_connect(),
            vec![
                16, 38, 0, 6, 77, 81, 73, 115, 100, 112, 3, 194, 0, 15, 0, 4, 105, 100, 101, 110,
                0, 8, 117, 115, 101, 114, 110, 97, 109, 101, 0, 8, 112, 97, 115, 115, 119, 111,
                114, 100
            ]
        );
    }

    #[test]
    fn short_subscribe() {
        let mut client = Client::new("iden", "username", "password", 15);
        assert_eq!(
            client.make_subscribe("test/topic"),
            vec![130, 15, 0, 1, 0, 10, 116, 101, 115, 116, 47, 116, 111, 112, 105, 99, 1]
        );
    }

    #[test]
    fn publish() {
        {
            assert_eq!(
                handle_publish(&[0, 27], "a/b", 1, Vec::new(), None),
                vec![vec![0x40, 2, 0, 27]]
            );

            assert_eq!(
                handle_publish(&[0, 27], "a/b", 0, Vec::new(), None),
                Vec::<Vec<u8>>::new()
            );
        };
    }
}
