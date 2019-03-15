use std::cell::RefCell;
use std::io::BufRead;
use std::mem;

// crc32c implement Castagnoli Polynomial algorithm
// which also is used in iSCSI and SCTP. It is 50x
// faster than crc as we test.
use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use crc32c::crc32c;
use protobuf;
use protobuf::Message as PbMsg;
use raft::eraftpb::Entry;

use crate::util::codec::number::{self, NumberEncoder};

use super::memtable::EntryIndex;
use super::Error;
use super::Result;

const BATCH_MIN_SIZE: usize = 12; // 8 bytes total length + 4 checksum

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

    pub fn to_byte(self) -> u8 {
        match self {
            LogItemType::Entries => TYPE_ENTRIES,
            LogItemType::CMD => TYPE_COMMAND,
            LogItemType::KV => TYPE_KV,
        }
    }

    pub fn encode_to(self, vec: &mut Vec<u8>) {
        vec.push(self.to_byte());
    }
}

#[derive(Debug, PartialEq)]
pub struct Entries {
    pub region_id: u64,
    pub entries: Vec<Entry>,
    // EntryIndex may be update after write to file.
    pub entries_index: RefCell<Vec<EntryIndex>>,
}

impl Entries {
    pub fn new(
        region_id: u64,
        entries: Vec<Entry>,
        entries_index: Option<Vec<EntryIndex>>,
    ) -> Entries {
        let len = entries.len();
        Entries {
            region_id,
            entries,
            entries_index: match entries_index {
                Some(index) => RefCell::new(index),
                None => RefCell::new(vec![EntryIndex::default(); len]),
            },
        }
    }

    pub unsafe fn from_bytes(
        buf: &mut SliceReader,
        file_num: u64,
        fstart: *const u8,
    ) -> Result<Entries> {
        let region_id = number::decode_var_u64(buf)?;
        let mut count = number::decode_var_u64(buf)? as usize;
        let mut entries = Vec::with_capacity(count);
        let mut entries_index = Vec::with_capacity(count);
        loop {
            if count == 0 {
                break;
            }

            let len = number::decode_var_u64(buf)? as usize;
            let mut e = Entry::new();
            e.merge_from_bytes(&buf[..len])?;
            let mut entry_index = EntryIndex::default();
            entry_index.index = e.get_index();
            entry_index.file_num = file_num;
            entry_index.offset = buf.as_ptr().offset_from(fstart) as u64;
            entry_index.len = len as u64;
            buf.consume(len);
            entries.push(e);
            entries_index.push(entry_index);

            count -= 1;
        }
        Ok(Entries::new(region_id, entries, Some(entries_index)))
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        // layout = { region_id | entries count | multiple entries }
        // entries layout = { entry layout | ... | entry layout }
        // entry layout = { len | entry content }
        vec.encode_var_u64(self.region_id)?;
        vec.encode_var_u64(self.entries.len() as u64)?;
        for (i, e) in self.entries.iter().enumerate() {
            let content = e.write_to_bytes()?;
            vec.encode_var_u64(content.len() as u64)?;
            // file_num = 0 means entry index is not initialized.
            let mut entries_index = self.entries_index.borrow_mut();
            if entries_index[i].file_num == 0 {
                entries_index[i].index = e.get_index();
                entries_index[i].offset = vec.len() as u64;
                entries_index[i].len = content.len() as u64;
            }
            vec.extend_from_slice(&content);
        }
        Ok(())
    }

    pub fn update_offset_when_needed(&self, file_num: u64, base: u64) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            if idx.file_num == 0 {
                idx.offset += base;
                idx.file_num = file_num;
            }
        }
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
                vec.encode_var_u64(region_id).unwrap();
            }
        }
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<Command> {
        let command_type = number::read_u8(buf)?;
        if command_type == CMD_CLEAN {
            let region_id = number::decode_var_u64(buf)?;
            Ok(Command::Clean { region_id })
        } else {
            panic!("Unsupported command type: {:?}", command_type)
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpType {
    Put = 0x01,
    Del = 0x02,
}

impl OpType {
    pub fn encode_to(self, vec: &mut Vec<u8>) {
        vec.push(self as u8);
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<OpType> {
        let op_type = buf.read_u8()?;
        unsafe { Ok(mem::transmute(op_type)) }
    }
}

#[derive(Debug, PartialEq)]
pub struct KeyValue {
    pub op_type: OpType,
    pub region_id: u64,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl KeyValue {
    pub fn new(op_type: OpType, region_id: u64, key: &[u8], value: Option<&[u8]>) -> KeyValue {
        KeyValue {
            op_type,
            region_id,
            key: key.to_vec(),
            value: value.map(|v| v.to_vec()),
        }
    }

    pub fn from_bytes(buf: &mut SliceReader) -> Result<KeyValue> {
        let op_type = OpType::from_bytes(buf)?;
        let region_id = number::decode_var_u64(buf)?;
        let k_len = number::decode_var_u64(buf)? as usize;
        let key = &buf[..k_len];
        buf.consume(k_len);
        match op_type {
            OpType::Put => {
                let v_len = number::decode_var_u64(buf)? as usize;
                let value = &buf[..v_len];
                buf.consume(v_len);
                Ok(KeyValue::new(OpType::Put, region_id, key, Some(value)))
            }
            OpType::Del => Ok(KeyValue::new(OpType::Del, region_id, key, None)),
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { op_type | region_id | k_len | key | v_len | value }
        self.op_type.encode_to(vec);
        vec.encode_var_u64(self.region_id)?;
        vec.encode_var_u64(self.key.len() as u64)?;
        vec.extend_from_slice(self.key.as_slice());
        match self.op_type {
            OpType::Put => {
                vec.encode_var_u64(self.value.as_ref().unwrap().len() as u64)?;
                vec.extend_from_slice(self.value.as_ref().unwrap().as_slice());
            }
            OpType::Del => {}
        }
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
            item_type,
            entries: None,
            command: None,
            kv: None,
        }
    }

    pub fn from_entries(region_id: u64, entries: Vec<Entry>) -> LogItem {
        LogItem {
            item_type: LogItemType::Entries,
            entries: Some(Entries::new(region_id, entries, None)),
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

    pub fn from_kv(op_type: OpType, region_id: u64, key: &[u8], value: Option<&[u8]>) -> LogItem {
        LogItem {
            item_type: LogItemType::KV,
            entries: None,
            command: None,
            kv: Some(KeyValue::new(op_type, region_id, key, value)),
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

    pub unsafe fn from_bytes(
        buf: &mut SliceReader,
        file_num: u64,
        fstart: *const u8,
    ) -> Result<LogItem> {
        let item_type = LogItemType::from_byte(buf.read_u8()?);
        let mut item = LogItem::new(item_type);
        match item_type {
            LogItemType::Entries => {
                let entries = Entries::from_bytes(buf, file_num, fstart)?;
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
    pub items: RefCell<Vec<LogItem>>,
}

impl Default for LogBatch {
    fn default() -> Self {
        Self {
            items: RefCell::new(Vec::with_capacity(16)),
        }
    }
}

impl LogBatch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: RefCell::new(Vec::with_capacity(cap)),
        }
    }

    pub fn add_entries(&self, region_id: u64, entries: Vec<Entry>) {
        let item = LogItem::from_entries(region_id, entries);
        self.items.borrow_mut().push(item);
    }

    pub fn clean_region(&self, region_id: u64) {
        self.add_command(Command::Clean { region_id });
    }

    pub fn add_command(&self, cmd: Command) {
        let item = LogItem::from_command(cmd);
        self.items.borrow_mut().push(item);
    }

    pub fn delete(&self, region_id: u64, key: &[u8]) {
        let item = LogItem::from_kv(OpType::Del, region_id, key, None);
        self.items.borrow_mut().push(item);
    }

    pub fn put(&self, region_id: u64, key: &[u8], value: &[u8]) {
        let item = LogItem::from_kv(OpType::Put, region_id, key, Some(value));
        self.items.borrow_mut().push(item);
    }

    pub fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put(region_id, key, &value);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.borrow().is_empty()
    }

    pub unsafe fn from_bytes(
        buf: &mut SliceReader,
        file_num: u64,
        fstart: *const u8,
    ) -> Result<Option<LogBatch>> {
        if buf.is_empty() {
            return Ok(None);
        }

        if buf.len() < BATCH_MIN_SIZE {
            return Err(Error::TooShort);
        }

        let batch_len = number::decode_u64(buf)? as usize;
        if buf.len() < batch_len {
            return Err(Error::TooShort);
        }

        // verify checksum
        let offset = batch_len - 4;
        let mut s = &buf[offset..offset + 4];
        let old_checksum = number::decode_u32_le(&mut s)?;
        let new_checksum = crc32c(&buf[..offset]);
        if old_checksum != new_checksum {
            return Err(Error::CheckSumError);
        }

        let mut items_count = number::decode_var_u64(buf)? as usize;
        if items_count == 0 || buf.is_empty() {
            panic!("Empty log event is not supported");
        }

        let log_batch = LogBatch::new();
        loop {
            if items_count == 0 {
                // 4 bytes checksum
                buf.consume(4);
                break;
            }

            let item = LogItem::from_bytes(buf, file_num, fstart)?;
            log_batch.items.borrow_mut().push(item);

            items_count -= 1;
        }
        Ok(Some(log_batch))
    }

    pub fn encode_to_bytes(&self) -> Option<Vec<u8>> {
        if self.items.borrow().is_empty() {
            return None;
        }

        // layout = { 8 bytes len | item count | multiple items | 4 bytes checksum }
        let mut vec = Vec::with_capacity(4096);
        vec.encode_u64(0).unwrap();
        vec.encode_var_u64(self.items.borrow().len() as u64)
            .unwrap();
        for item in self.items.borrow_mut().iter_mut() {
            item.encode_to(&mut vec).unwrap();
        }
        let checksum = crc32c(&vec.as_slice()[8..]);
        vec.encode_u32_le(checksum).unwrap();
        let len = vec.len() as u64 - 8;
        vec.as_mut_slice().write_u64::<BigEndian>(len).unwrap();

        Some(vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use raft::eraftpb::Entry;

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
        let file_num = 1;
        let entries = Entries::new(region_id, pb_entries, None);

        let mut encoded = vec![];
        entries.encode_to(&mut encoded).unwrap();
        for idx in entries.entries_index.borrow_mut().iter_mut() {
            idx.file_num = file_num;
        }
        let mut s = encoded.as_slice();
        let fstart: *const u8 = encoded.as_ptr();
        let decode_entries = unsafe { Entries::from_bytes(&mut s, file_num, fstart).unwrap() };
        assert_eq!(s.len(), 0);
        assert_eq!(entries.region_id, decode_entries.region_id);
        assert_eq!(entries.entries, decode_entries.entries);
        assert_eq!(entries.entries_index, decode_entries.entries_index);
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
        let kv = KeyValue::new(OpType::Put, 8, b"key", Some(b"value"));
        let mut encoded = vec![];
        kv.encode_to(&mut encoded).unwrap();
        let mut bytes_slice = encoded.as_slice();
        let decoded_kv = KeyValue::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(kv, decoded_kv);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let region_id = 8;
        let file_num = 1;
        let items = vec![
            LogItem::from_entries(region_id, vec![Entry::new(); 10]),
            LogItem::from_command(Command::Clean {
                region_id: region_id,
            }),
            LogItem::from_kv(OpType::Put, region_id, b"key", Some(b"value")),
        ];

        for mut item in items {
            let mut encoded = vec![];
            item.encode_to(&mut encoded).unwrap();
            let mut s = encoded.as_slice();
            let fstart: *const u8 = encoded.as_ptr();
            let decoded_item = unsafe { LogItem::from_bytes(&mut s, file_num, fstart).unwrap() };
            assert_eq!(s.len(), 0);

            if item.item_type == LogItemType::Entries {
                item.entries
                    .as_mut()
                    .unwrap()
                    .update_offset_when_needed(file_num, 0);
            }
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let file_num = 1;
        let batch = LogBatch::new();
        batch.add_entries(region_id, vec![Entry::new(); 10]);
        batch.add_command(Command::Clean { region_id });
        batch.put(region_id, b"key", b"value");
        batch.delete(region_id, b"key2");

        let encoded = batch.encode_to_bytes().unwrap();
        let mut s = encoded.as_slice();
        let fstart: *const u8 = encoded.as_ptr();
        let decoded_batch = unsafe {
            LogBatch::from_bytes(&mut s, file_num, fstart)
                .unwrap()
                .unwrap()
        };
        assert_eq!(s.len(), 0);

        for item in batch.items.borrow_mut().iter_mut() {
            if item.item_type == LogItemType::Entries {
                item.entries
                    .as_ref()
                    .unwrap()
                    .update_offset_when_needed(file_num, 0);
            }
        }

        assert_eq!(batch, decoded_batch);
    }
}
