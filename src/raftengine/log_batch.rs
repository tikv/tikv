
use std::u32;
use std::io::BufRead;

use byteorder::{ReadBytesExt, WriteBytesExt};
use byteorder::BigEndian;
use kvproto::eraftpb::Entry;
use protobuf::Message as PbMsg;

use util::codec::number::{NumberDecoder, NumberEncoder};

use super::Result;
use super::Error;
use super::util::calc_crc32;

const BATCH_MIN_SIZE: usize = 20; // 8 bytes total length + 8 bytes item count + 4 checksum

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const CMD_CLEAN: u8 = 0x01;

type SliceReader<'a> = &'a [u8];

#[derive(Clone, Copy)]
pub enum LogItemType {
    Entries, // entries
    CMD,     // admin command, eg. clean a region
}

impl LogItemType {
    pub fn from_byte(t: u8) -> LogItemType {
        if t == TYPE_ENTRIES {
            LogItemType::Entries
        } else if t == TYPE_COMMAND {
            LogItemType::CMD
        } else {
            panic!("Invalid item type: {}", t)
        }
    }

    pub fn to_byte(&self) -> u8 {
        match *self {
            LogItemType::Entries => TYPE_ENTRIES,
            LogItemType::CMD => TYPE_COMMAND,
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) {
        vec.push(self.to_byte());
    }
}

pub struct Entries {
    pub region_id: u64,
    pub entries: Vec<Entry>,
}

impl Entries {
    pub fn new(region_id: u64, entries: Vec<Entry>) -> Entries {
        Entries {
            region_id: region_id,
            entries: entries,
        }
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<Entries> {
        let region_id = buf.decode_u64()?;
        let mut count = buf.decode_u64()? as usize;
        let mut entries = Vec::with_capacity(count);
        loop {
            if count == 0 {
                break;
            }

            let len = buf.decode_u64()? as usize;
            let mut e = Entry::new();
            e.merge_from_bytes(&buf[..len])?;
            buf.consume(len);
            entries.push(e);

            count -= 1;
        }
        Ok(Entries::new(region_id, entries))
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        // layout = { 8 bytes region_id | 8 bytes entries count | multiple entries }
        // entries layout = { entry layout | ... | entry layout }
        // entry layout = { 8 bytes len | entry content }
        vec.encode_u64(self.region_id)?;
        vec.encode_u64(self.entries.len() as u64)?;
        for e in &self.entries {
            let content = e.write_to_bytes()?;
            vec.encode_u64(content.len() as u64)?;
            vec.extend_from_slice(&content);
        }
        Ok(())
    }
}

pub enum Command {
    Clean { region_id: u64 },
}

impl Command {
    pub fn encode_to(&self, vec: &mut Vec<u8>) {
        match *self {
            Command::Clean { region_id } => {
                vec.push(CMD_CLEAN);
                vec.encode_u64(region_id).unwrap();
            }
        }
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<Command> {
        let command_type = buf.read_u8()?;
        if command_type == CMD_CLEAN {
            let region_id = buf.decode_u64()?;
            Ok(Command::Clean {
                region_id: region_id,
            })
        } else {
            panic!("Unsupported command type: {:?}", command_type)
        }
    }
}

pub struct LogItem {
    pub item_type: LogItemType,
    pub entries: Option<Entries>,
    pub command: Option<Command>,
}

impl LogItem {
    pub fn new(item_type: LogItemType) -> LogItem {
        LogItem {
            item_type: item_type,
            entries: None,
            command: None,
        }
    }

    pub fn from_entries(region_id: u64, entries: Vec<Entry>) -> LogItem {
        LogItem {
            item_type: LogItemType::Entries,
            entries: Some(Entries::new(region_id, entries)),
            command: None,
        }
    }

    pub fn from_command(command: Command) -> LogItem {
        LogItem {
            item_type: LogItemType::CMD,
            entries: None,
            command: Some(command),
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { 1 byte type | item layout }
        self.item_type.encode_to(vec);
        match self.item_type {
            LogItemType::Entries => {
                self.entries.as_ref().unwrap().encode_to(vec)?;
            }
            LogItemType::CMD => {
                self.command.as_ref().unwrap().encode_to(vec);
            }
        }
        Ok(())
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<LogItem> {
        let item_type = LogItemType::from_byte(buf.read_u8()?);
        let mut item = LogItem::new(item_type);
        match item_type {
            LogItemType::Entries => {
                let entries = Entries::from_bytes(buf)?;
                item.entries = Some(entries);
            }
            LogItemType::CMD => {
                let command = Command::from_bytes(buf)?;
                item.command = Some(command);
            }
        }
        Ok(item)
    }
}

pub struct LogBatch {
    pub items: Vec<LogItem>,
}

impl Default for LogBatch {
    fn default() -> LogBatch {
        LogBatch {
            items: Vec::with_capacity(16),
        }
    }
}

impl LogBatch {
    pub fn from_bytes(buf: &mut SliceReader) -> Result<Option<(LogBatch, usize)>> {
        if buf.is_empty() {
            return Ok(None);
        }

        if buf.len() < BATCH_MIN_SIZE {
            return Err(Error::TooShort);
        }

        let batch_len = buf.decode_u64()? as usize;
        if buf.len() < batch_len {
            return Err(Error::TooShort);
        }

        // verify checksum
        let offset = batch_len - 4;
        let old_checksum = (&buf[offset..offset + 4]).decode_u32_le()?;
        let new_checksum = calc_crc32(&buf[..offset]);
        if old_checksum != new_checksum {
            return Err(Error::CheckSumError);
        }

        let mut items_count = buf.decode_u64()? as usize;
        if items_count == 0 || buf.is_empty() {
            panic!("Empty log event is not supported");
        }

        let mut log_batch = LogBatch::default();
        loop {
            if items_count == 0 {
                // checksum
                buf.consume(4);
                break;
            }

            let item = LogItem::from_bytes(buf)?;
            log_batch.items.push(item);

            items_count -= 1;
        }
        Ok(Some((log_batch, batch_len + 8)))
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<Entry>) {
        let item = LogItem::from_entries(region_id, entries);
        self.items.push(item);
    }

    pub fn add_command(&mut self, cmd: Command) {
        let item = LogItem::from_command(cmd);
        self.items.push(item);
    }

    pub fn to_vec(&self) -> Option<Vec<u8>> {
        if self.items.is_empty() {
            return None;
        }

        // layout = { 8 bytes len | 8 bytes item count | multiple items | 4 bytes checksum }
        let mut vec = Vec::with_capacity(4096);
        vec.encode_u64(0).unwrap();
        vec.encode_u64(self.items.len() as u64).unwrap();
        for item in &self.items {
            item.encode_to(&mut vec).unwrap();
        }
        let checksum = calc_crc32(&vec.as_slice()[8..]);
        vec.encode_u32_le(checksum).unwrap();
        let len = vec.len() as u64 - 8;
        vec.as_mut_slice().write_u64::<BigEndian>(len).unwrap();

        Some(vec)
    }
}
