#![allow(dead_code)]

use std::boxed::Box;
use std::result;
use std::error;
use std::default::Default;
use std::option::Option;


use bytes::{Buf, ByteBuf};
use mio::{self, Token};

use util::codec;

mod conn;
mod server;
mod run;
mod handler;

pub type Result<T> = result::Result<T, Box<error::Error + Send + Sync>>;

const SERVER_TOKEN: Token = Token(0);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);

const DEFAULT_RAFT_INTERVAL_TICK_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct Config {
    pub addr: String,
}

pub enum MsgType {
    None,
    // Quit event loop.
    Quit,
    // Read data from connection.
    ReadData,
    // Write data to connection.
    WriteData,
    // Close special connection.
    CloseConn,

    // Tick is for base raft internal tick message.
    Tick,
    // Timeout is for custom timeout message.
    Timeout,
}

pub struct ConnData {
    msg_id: u64,
    data: ByteBuf,
}

impl ConnData {
    pub fn encode_to_buf(&self) -> ByteBuf {
        let mut buf = ByteBuf::mut_with_capacity(codec::MSG_HEADER_LEN + self.data.bytes().len());

        // Must ok here
        codec::encode_data(&mut buf, self.msg_id, self.data.bytes()).unwrap();

        buf.flip()
    }
}

pub struct Msg {
    pub msg_type: MsgType,
    pub token: Token,
    pub conn_data: Option<ConnData>,
}

impl Default for Msg {
    fn default() -> Msg {
        Msg {
            msg_type: MsgType::None,
            token: Token(!0),
            conn_data: None,
        }
    }
}

#[derive(Debug)]
pub struct Sender {
    sender: mio::Sender<Msg>,
}

impl Clone for Sender {
    fn clone(&self) -> Sender {
        Sender { sender: self.sender.clone() }
    }
}

impl Sender {
    pub fn new(sender: mio::Sender<Msg>) -> Sender {
        Sender { sender: sender }
    }

    pub fn kill(&self) -> Result<()> {
        try!(self.sender.send(Msg { msg_type: MsgType::Quit, ..Default::default() }));
        Ok(())
    }

    pub fn write_data(&self, token: Token, data: ConnData) -> Result<()> {
        try!(self.sender.send(Msg {
            msg_type: MsgType::WriteData,
            token: token,
            conn_data: Some(data),
        }));

        Ok(())
    }
}
