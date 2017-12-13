use std::mem;
use std::io::Write;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::sync::Arc;
use super::datum::Datum;
use super::mysql::{types, Duration, Time};
#[derive(Default)]
struct Column {
    length: usize,
    null_cnt: usize,
    null_bitmap: Vec<u8>,
    var_offsets: Vec<usize>,
    data: Vec<u8>,
    // if the data's length is fixed, fixed_len should be bigger than 0
    fixed_len: usize,
    ifaces: Vec<Datum>,
}

impl Column {
    fn new(tp: u8, init_cap: usize) -> Column {
        match tp {
            types::FLOAT => Column::new_fixed_column(4, init_cap),
            types::TINY |
            types::SHORT |
            types::INT24 |
            types::LONG |
            types::LONG_LONG |
            types::DOUBLE |
            types::YEAR => Column::new_fixed_column(8, init_cap),
            types::DURATION => Column::new_fixed_column(8, init_cap), //convert to i64
            // types::NEW_DECIMAL=>// TODO
            types::DATE | types::DATETIME | types::TIMESTAMP | types::JSON => {
                Column::new_interface_column(init_cap)
            }
            _ => Column::new_var_len_column(init_cap),
        }
    }

    fn new_fixed_column(fixed_len: usize, init_cap: usize) -> Column {
        Column {
            fixed_len: fixed_len,
            data: Vec::with_capacity(fixed_len * init_cap),
            null_bitmap: Vec::with_capacity(init_cap >> 3),
            ..Default::default()
        }
    }

    fn new_var_len_column(init_cap: usize) -> Column {
        let mut offsets = Vec::with_capacity(init_cap + 1);
        offsets.push(0);
        Column {
            var_offsets: offsets,
            data: Vec::with_capacity(4 * init_cap),
            null_bitmap: Vec::with_capacity(init_cap >> 3),
            ..Default::default()
        }
    }

    fn new_interface_column(init_cap: usize) -> Column {
        Column {
            ifaces: Vec::with_capacity(init_cap),
            null_bitmap: Vec::with_capacity(init_cap >> 3),
            ..Default::default()
        }
    }

    fn is_fixed(&self) -> bool {
        self.fixed_len > 0
    }

    fn is_varlen(&self) -> bool {
        self.var_offsets.len() > 0
    }

    fn is_interface(&self) -> bool {
        !self.is_fixed() && !self.is_varlen()
    }

    fn reset(&mut self) {
        self.length = 0;
        self.null_cnt = 0;
        self.null_bitmap.clear();
        if !self.var_offsets.is_empty() {
            // The first offset is always 0, it makes slicing the data easier, we need to keep it.
            self.var_offsets.truncate(1);
        }
        self.data.clear();
        self.ifaces.clear();
    }

    fn is_null(&self, row_idx: usize) -> bool {
        if let Some(null_byte) = self.null_bitmap.get(row_idx >> 3) {
            null_byte & (1 << ((row_idx) & 7)) == 0
        } else {
            false //TODO:tidb would panic here
        }
    }

    fn append_null_bitmap(&mut self, on: bool) {
        let idx = self.length >> 3; // /8
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if on {
            let pos = self.length & 7; // %8
            self.null_bitmap[idx] |= 1 << pos;
        } else {
            self.null_cnt += 1;
        }
    }

    fn append_null(&mut self) {
        self.append_null_bitmap(false);
        if self.is_fixed() {
            let len = self.fixed_len + self.data.len();
            self.data.resize(len, 0);
        } else if self.is_varlen() {
            let offset = self.var_offsets[self.length];
            self.var_offsets.push(offset);
        } else {
            self.ifaces.push(Datum::Null);
        }
        self.length += 1;
    }

    fn finish_append_fixed(&mut self) {
        self.append_null_bitmap(true);
        self.length += 1;
    }

    fn append_i64(&mut self, v: i64) {
        self.data.write_i64::<LittleEndian>(v).unwrap(); //.map_err(From::from)
        self.finish_append_fixed()
    }

    fn append_u64(&mut self, v: u64) {
        self.data.write_u64::<LittleEndian>(v).unwrap(); //.map_err(From::from)
        self.finish_append_fixed();
    }

    fn append_f32(&mut self, v: f32) {
        self.data.write_f32::<LittleEndian>(v).unwrap();
        self.finish_append_fixed();
    }

    fn append_f64(&mut self, v: f64) {
        self.data.write_f64::<LittleEndian>(v).unwrap();
        self.finish_append_fixed();
    }

    fn append_duration(&mut self, dur: Duration) {
        self.append_i64(dur.to_nanos());
    }

    fn append_time(&mut self, t: Time) {
        self.append_u64(t.to_packed_u64());
    }

    fn finished_append_var(&mut self) {
        self.append_null_bitmap(true);
        let offset = self.data.len();
        self.var_offsets.push(offset);
        self.length += 1;
    }

    fn append_str(&mut self, s: String) {
        self.data.write_all(s.as_bytes());
        self.finished_append_var();
    }

    fn append_bytes(&mut self, byte: &[u8]) {
        self.data.write_all(byte);
        self.finished_append_var();
    }

    fn append_name_value(&mut self, name: String, val: u64) {
        self.data.write_u64::<LittleEndian>(val).unwrap(); //.map_err(From::from)
        self.data.write_all(name.as_bytes());
        self.finished_append_var();
    }

    fn append_interface(&mut self, item: Datum) {
        self.ifaces.push(item);
        self.append_null_bitmap(true);
        self.length += 1;
    }
    //TODO: seems equal to append(row_col,row_idx,row_idx)?
    fn append_row(&mut self, row_col: &Column, row_idx: usize) {
        self.append_null_bitmap(!row_col.is_null(row_idx));
        if row_col.is_fixed() {
            let offset = row_idx * row_col.fixed_len;
            let end = offset + row_col.fixed_len;
            self.data.write_all(&row_col.data[offset..end]);
        } else if row_col.is_varlen() {
            let start = row_col.var_offsets[row_idx];
            let end = row_col.var_offsets[row_idx + 1];
            self.data.write_all(&row_col.data[start..end]);
            let len = self.data.len();
            self.var_offsets.push(len);
        } else {
            self.ifaces.push(row_col.ifaces[row_idx].clone());
        }
        self.length += 1;
    }

    // append appends data in [begin,end) in col to current column.
    fn append(&mut self, col: &Column, begin: usize, end: usize) {
        // TODO:should we check type before append?
        if col.is_fixed() {
            let from = col.fixed_len * begin;
            let to = col.fixed_len * end;
            self.data.write_all(&col.data[from..to]);
        } else if col.is_varlen() {
            let from = col.var_offsets[begin];
            let to = col.var_offsets[end];
            self.data.write_all(&col.data[from..to]);
            for id in begin..end {
                let offset = self.var_offsets.last().unwrap() + col.var_offsets[id + 1] -
                    col.var_offsets[id];
                self.var_offsets.push(offset);
            }
        } else {
            self.ifaces.extend_from_slice(&col.ifaces[begin..end])
        }

        for id in begin..end {
            self.append_null_bitmap(!col.is_null(id));
            self.length += 1;
        }
    }

    fn truncate_to(&mut self, num_rows: usize) {
        if self.is_fixed() {
            let to = self.fixed_len * num_rows;
            self.data.truncate(to);
        } else if self.is_varlen() {
            let to = self.var_offsets[num_rows];
            self.data.truncate(to);
            self.var_offsets.truncate(num_rows + 1);
        } else {
            self.ifaces.truncate(num_rows);
        }

        for id in num_rows..self.length {
            if self.is_null(id) {
                self.null_cnt -= 1;
            }
        }
        self.length = num_rows;
        self.null_bitmap.truncate(num_rows >> 3 + 1);
    }
}

// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
}

const CHUNK_INITIAL_CAPACITY: usize = 32;


impl Chunk {
    ///new_chunk creates a new chunk with field types.
    pub fn new_chunk(tps: &[i32]) -> Chunk {
        let mut columns = Vec::with_capacity(tps.len());
        for tp in tps {
            columns.push(Column::new(*tp as u8, CHUNK_INITIAL_CAPACITY));
        }
        Chunk { columns: columns }
    }

    /// swap_columns swaps columns with another Chunk.
    pub fn swap_columns(&mut self, other: &mut Chunk) {
        mem::swap(&mut self.columns, &mut other.columns)
    }

    // reset resets the chunk, so the memory it allocated can be reused.
    // Make sure all the data in the chunk is not used anymore before you reuse this chunk.
    pub fn reset(&mut self) {
        for column in self.columns.iter_mut() {
            column.reset();
        }
    }

    // num_cols returns the number of rows in the chunk.
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    // num_rows returns the number of rows in the chunk.
    pub fn num_rows(&self) -> usize {
        if self.columns.len() == 0 {
            0
        } else {
            self.columns[0].length
        }
    }

    //append_row appends a row to the chunk.
    pub fn append_row(&mut self, col_idx: usize, row: Row) {
        for (id, row_col) in row.c.columns.iter().enumerate() {
            let mut chk_col = self.columns.get_mut(col_idx + id).unwrap(); //TODO
            chk_col.append_row(row_col, row.idx)
        }
    }

    //append appends rows in [begin, end) in another Chunk to a Chunk.
    pub fn append(&mut self, other: &Chunk, begin: usize, end: usize) {
        for (col_id, src) in other.columns.iter().enumerate() {
            self.columns[col_id].append(&src, begin, end);
        }
    }

    // truncate to  truncates rows from tail to head in a Chunk to "num_rows" rows.
    pub fn truncate_to(&mut self, num_rows: usize) {
        for col in self.columns.iter_mut() {
            col.truncate_to(num_rows);
        }
    }

    /// appends a null value to the chunk.
    pub fn append_null(&mut self, col_idx: usize) {
        self.columns[col_idx].append_null();
    }

    // appends a int64 value to the chunk.
    pub fn append_i64(&mut self, col_idx: usize, v: i64) {
        self.columns[col_idx].append_i64(v);
    }

    // appends a uint64 value to the chunk.
    pub fn append_u64(&mut self, col_idx: usize, v: u64) {
        self.columns[col_idx].append_u64(v);
    }

    // appends a float32 value to the chunk.
    pub fn append_f32(&mut self, col_idx: usize, v: f32) {
        self.columns[col_idx].append_f32(v);
    }

    pub fn append_f64(&mut self, col_idx: usize, v: f64) {
        self.columns[col_idx].append_f64(v);
    }

    pub fn append_str(&mut self, col_idx: usize, v: String) {
        self.columns[col_idx].append_str(v);
    }

    pub fn append_bytes(&mut self, col_idx: usize, v: &[u8]) {
        self.columns[col_idx].append_bytes(v);
    }
}

struct ArcChunk {
    chunk: Arc<Chunk>,
}

impl ArcChunk {
    //get_row gets the Row in the chunk with the row index.
    pub fn get_row(&self, idx: usize) -> Row {
        Row::new(self.chunk.clone(), idx)
    }
    // begin returns the first valid Row in the Chunk.
    pub fn begin(&self) -> Row {
        Row::new(self.chunk.clone(), 0)
    }

    //end returns a Row referring to the past-the-end element in the Chunk.
    pub fn end(&self) -> Row {
        let num_rows = self.chunk.num_rows();
        Row::new(self.chunk.clone(), num_rows)
    }
}

pub struct Row {
    c: Arc<Chunk>,
    idx: usize,
}

impl Row {
    pub fn new(c: Arc<Chunk>, idx: usize) -> Row {
        Row { c: c, idx: idx }
    }
}
