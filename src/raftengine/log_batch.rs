
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
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;

type SliceReader<'a> = &'a [u8];

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LogItemType {
    Entries, // entries
    CMD,     // admin command, eg. clean a region
    KV,      // key/value pair
}

impl LogItemType {
    pub fn from_byte(t: u8) -> LogItemType {
        // likely
        if t == TYPE_ENTRIES {
            LogItemType::Entries
        } else if t == TYPE_COMMAND {
            LogItemType::CMD
        } else if t == TYPE_KV {
            LogItemType::KV
        } else {
            panic!("Invalid item type: {}", t)
        }
    }

    pub fn to_byte(&self) -> u8 {
        match *self {
            LogItemType::Entries => TYPE_ENTRIES,
            LogItemType::CMD => TYPE_COMMAND,
            LogItemType::KV => TYPE_KV,
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) {
        vec.push(self.to_byte());
    }
}

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
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

#[derive(Debug, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl KeyValue {
    pub fn new(key: &[u8], value: &[u8]) -> KeyValue {
        KeyValue {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<KeyValue> {
        let k_len = buf.decode_u64()? as usize;
        let key = &buf[..k_len];
        buf.consume(k_len);
        let v_len = buf.decode_u64()? as usize;
        let value = &buf[..v_len];
        buf.consume(v_len);
        Ok(KeyValue::new(key, value))
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { 8 bytes k_len | key | 8 bytes v_len | value }
        vec.encode_u64(self.key.len() as u64)?;
        vec.extend_from_slice(self.key.as_slice());
        vec.encode_u64(self.value.len() as u64)?;
        vec.extend_from_slice(self.value.as_slice());
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct LogItem {
    pub item_type: LogItemType,
    pub entries: Option<Entries>,
    pub command: Option<Command>,
    pub kv: Option<KeyValue>,
}

impl LogItem {
    pub fn new(item_type: LogItemType) -> LogItem {
        LogItem {
            item_type: item_type,
            entries: None,
            command: None,
            kv: None,
        }
    }

    pub fn from_entries(region_id: u64, entries: Vec<Entry>) -> LogItem {
        LogItem {
            item_type: LogItemType::Entries,
            entries: Some(Entries::new(region_id, entries)),
            command: None,
            kv: None,
        }
    }

    pub fn from_command(command: Command) -> LogItem {
        LogItem {
            item_type: LogItemType::CMD,
            entries: None,
            command: Some(command),
            kv: None,
        }
    }

    pub fn from_kv(key: &[u8], value: &[u8]) -> LogItem {
        LogItem {
            item_type: LogItemType::KV,
            entries: None,
            command: None,
            kv: Some(KeyValue::new(key, value)),
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
            LogItemType::KV => {
                self.kv.as_ref().unwrap().encode_to(vec)?;
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
            LogItemType::KV => {
                let kv = KeyValue::from_bytes(buf)?;
                item.kv = Some(kv);
            }
        }
        Ok(item)
    }
}

#[derive(Debug, PartialEq)]
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

    pub fn add_kv(&mut self, key: &[u8], value: &[u8]) {
        let item = LogItem::from_kv(key, value);
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


#[cfg(test)]
mod tests {
    use super::*;

    use kvproto::eraftpb::Entry;

    #[test]
    fn test_log_item_type() {
        let item_types = vec![LogItemType::Entries, LogItemType::CMD, LogItemType::KV];
        let item_types_byte = vec![TYPE_ENTRIES, TYPE_COMMAND, TYPE_KV];

        for (pos, item_type) in item_types.iter().enumerate() {
            assert_eq!(item_type.to_byte(), item_types_byte[pos]);
            assert_eq!(&LogItemType::from_byte(item_types_byte[pos]), item_type);

            let mut vec = vec![];
            item_type.encode_to(&mut vec);
            assert_eq!(vec, vec![item_types_byte[pos]]);
        }
    }

    #[test]
    fn test_entries_enc_dec() {
        let pb_entries = vec![Entry::new(); 10];
        let region_id = 8;
        let entries = Entries::new(region_id, pb_entries);

        let mut encoded = vec![];
        entries.encode_to(&mut encoded).unwrap();
        let mut s = encoded.as_slice();
        let decode_entries = Entries::from_bytes(&mut s).unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(entries.region_id, decode_entries.region_id);
        assert_eq!(entries.entries, decode_entries.entries);
    }

    #[test]
    fn test_command_enc_dec() {
        let cmd = Command::Clean { region_id: 8 };
        let mut encoded = vec![];
        cmd.encode_to(&mut encoded);
        let mut bytes_slice = encoded.as_slice();
        let decoded_cmd = Command::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(cmd, decoded_cmd);
    }

    #[test]
    fn test_kv_enc_dec() {
        let (key, value) = (b"key", b"value");
        let kv = KeyValue::new(key, value);
        let mut encoded = vec![];
        kv.encode_to(&mut encoded).unwrap();
        let mut bytes_slice = encoded.as_slice();
        let decoded_kv = KeyValue::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(kv, decoded_kv);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let (key, value) = (b"key", b"value");
        let items = vec![LogItem::from_entries(8, vec![Entry::new(); 10]),
        LogItem::from_command(Command::Clean { region_id: 8 }),
        LogItem::from_kv(key, value)];

        for item in items {
            let mut encoded = vec![];
            item.encode_to(&mut encoded).unwrap();
            let mut s = encoded.as_slice();
            let decoded_item = LogItem::from_bytes(&mut s).unwrap();
            assert_eq!(s.len(), 0);
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let mut batch = LogBatch::default();
        batch.add_entries(8, vec![Entry::new(); 10]);
        batch.add_command(Command::Clean { region_id: 8 });
        batch.add_kv(b"key", b"value");

        let encoded = batch.to_vec().unwrap();
        let mut s = encoded.as_slice();
        let (decoded_batch, _) = LogBatch::from_bytes(&mut s).unwrap().unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(batch, decoded_batch);
    }
}
