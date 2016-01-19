#![allow(dead_code)]

use std::boxed::Box;
use std::result;
use std::error;

use bytes::{Buf, ByteBuf};
use mio::{self, Token};

use util::codec;

mod conn;
mod server;
mod run;
mod handler;
mod bench;

pub type Result<T> = result::Result<T, Box<error::Error + Send + Sync>>;

const SERVER_TOKEN: Token = Token(0);
const FIRST_CUSTOM_TOKEN: Token = Token(1024);

const DEFAULT_BASE_TICK_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct Config {
    pub addr: String,
}

pub struct ConnData {
    token: Token,
    msg_id: u64,
    data: ByteBuf,
}

pub enum TimerMsg {
    // None is just for test, we will remove this later.
    None,
}

pub struct TimerData {
    delay: u64,
    msg: TimerMsg,
}

impl ConnData {
    pub fn encode_to_buf(&self) -> ByteBuf {
        let mut buf = ByteBuf::mut_with_capacity(codec::MSG_HEADER_LEN + self.data.bytes().len());

        // Must ok here
        codec::encode_data(&mut buf, self.msg_id, self.data.bytes()).unwrap();

        buf.flip()
    }
}

pub enum Msg {
    // Quit event loop.
    Quit,
    // Read data from connection.
    ReadData(ConnData),
    // Write data to connection.
    WriteData(ConnData),
    // Tick is for base internal tick message.
    Tick,
    // Timer is for custom timeout message.
    Timer(TimerData),
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
        try!(self.sender.send(Msg::Quit));
        Ok(())
    }

    pub fn write_data(&self, data: ConnData) -> Result<()> {
        try!(self.sender.send(Msg::WriteData(data)));

        Ok(())
    }

    pub fn timeout_ms(&self, delay: u64, m: TimerMsg) -> Result<()> {
        try!(self.sender.send(Msg::Timer(TimerData {
            delay: delay,
            msg: m,
        })));

        Ok(())
    }
}
