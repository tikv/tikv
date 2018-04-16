use std::mem;
use std::io::Write;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::sync::Arc;
use tipb::expression::FieldType;
use super::datum::Datum;
use super::mysql::types;


// Chunk stores multiple rows of data in Apache Arrow format.
// See https://arrow.apache.org/docs/memory_layout.html
// Values are appended in compact format and can be directly accessed without decoding.
// When the chunk is done processing, we can reuse the allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
}

const CHUNK_INITIAL_CAPACITY: usize = 32;

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


pub struct Row {
    c: Arc<Chunk>,
    idx: usize,
}

impl Column {
    fn new(tp: &FieldType, init_cap: usize) -> Column {
        match tp.get_tp() as u8 {
            types::TINY |
            types::SHORT |
            types::INT24 |
            types::LONG |
            types::LONG_LONG |
            types::YEAR |
            types::FLOAT |
            types::DOUBLE => {
                //TODO:no Datum::F32
                Column::new_fixed_column(8, init_cap)
            }
            types::VARCHAR |
            types::VAR_STRING |
            types::STRING |
            types::BLOB |
            types::TINY_BLOB |
            types::MEDIUM_BLOB |
            types::LONG_BLOB => Column::new_var_len_column(init_cap),
            _ => Column::new_interface_column(init_cap),
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
        !self.var_offsets.is_empty()
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

    fn get_i64(&self, idx: usize) -> i64 {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let data = self.data[start..end].to_vec();
        data.as_slice().read_i64::<LittleEndian>().unwrap()
    }

    fn append_u64(&mut self, v: u64) {
        self.data.write_u64::<LittleEndian>(v).unwrap(); //.map_err(From::from)
        self.finish_append_fixed();
    }

    fn get_u64(&self, idx: usize) -> u64 {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let data = self.data[start..end].to_vec();
        data.as_slice().read_u64::<LittleEndian>().unwrap()
    }

    fn append_f64(&mut self, v: f64) {
        self.data.write_f64::<LittleEndian>(v).unwrap();
        self.finish_append_fixed();
    }

    fn get_f64(&self, idx: usize) -> f64 {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let data = self.data[start..end].to_vec();
        data.as_slice().read_f64::<LittleEndian>().unwrap()
    }

    fn finished_append_var(&mut self) {
        self.append_null_bitmap(true);
        let offset = self.data.len();
        self.var_offsets.push(offset);
        self.length += 1;
    }

    fn append_bytes(&mut self, byte: &[u8]) {
        self.data.write_all(byte).unwrap();
        self.finished_append_var();
    }

    fn get_bytes(&self, idx: usize) -> &[u8] {
        let start = self.var_offsets[idx];
        let end = self.var_offsets[idx + 1];
        &self.data[start..end]
    }

    fn append_interface(&mut self, item: Datum) {
        self.ifaces.push(item);
        self.append_null_bitmap(true);
        self.length += 1;
    }

    fn get_interface(&self, idx: usize) -> Datum {
        self.ifaces[idx].clone()
    }

    //TODO: seems equal to append(row_col,row_idx,row_idx)?
    fn append_row(&mut self, row_col: &Column, row_idx: usize) {
        self.append(row_col, row_idx, row_idx + 1);
    }

    // append appends data in [begin,end) in col to current column.
    fn append(&mut self, col: &Column, begin: usize, end: usize) {
        // TODO:should we check type before append?
        if col.is_fixed() {
            let from = col.fixed_len * begin;
            let to = col.fixed_len * end;
            self.data.write_all(&col.data[from..to]).unwrap();
        } else if col.is_varlen() {
            let from = col.var_offsets[begin];
            let to = col.var_offsets[end];
            self.data.write_all(&col.data[from..to]).unwrap();
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
        self.null_bitmap.truncate((num_rows >> 3) + 1);
    }
}

impl Chunk {
    ///new_chunk creates a new chunk with field types.
    pub fn new(tps: &[FieldType]) -> Chunk {
        let mut columns = Vec::with_capacity(tps.len());
        for tp in tps {
            columns.push(Column::new(tp, CHUNK_INITIAL_CAPACITY));
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
        for column in &mut self.columns {
            column.reset();
        }
    }

    // num_cols returns the number of rows in the chunk.
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    // num_rows returns the number of rows in the chunk.
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].length
        }
    }

    //append_row appends a row to the chunk.
    pub fn append_row(&mut self, col_idx: usize, row: Row) {
        for (id, row_col) in row.c.columns.iter().enumerate() {
            let mut chk_col = &mut self.columns[col_idx + id]; //TODO
            chk_col.append_row(row_col, row.idx)
        }
    }

    //append appends rows in [begin, end) in another Chunk to a Chunk.
    pub fn append(&mut self, other: &Chunk, begin: usize, end: usize) {
        for (col_id, src) in other.columns.iter().enumerate() {
            self.columns[col_id].append(src, begin, end);
        }
    }

    // truncate to  truncates rows from tail to head in a Chunk to "num_rows" rows.
    pub fn truncate_to(&mut self, num_rows: usize) {
        for col in &mut self.columns {
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

    pub fn append_f64(&mut self, col_idx: usize, v: f64) {
        self.columns[col_idx].append_f64(v);
    }

    pub fn append_bytes(&mut self, col_idx: usize, v: &[u8]) {
        self.columns[col_idx].append_bytes(v);
    }

    pub fn append_interface(&mut self, col_idx: usize, v: Datum) {
        self.columns[col_idx].append_interface(v);
    }
}

struct ArcChunk {
    chunk: Arc<Chunk>,
}

impl ArcChunk {
    pub fn new(chunk: Chunk) -> ArcChunk {
        ArcChunk {
            chunk: Arc::new(chunk),
        }
    }

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

impl Row {
    pub fn new(c: Arc<Chunk>, idx: usize) -> Row {
        Row { c: c, idx: idx }
    }

    //idx returns the row index of Chunk.
    pub fn idx(&self) -> usize {
        self.idx
    }

    //len returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.c.num_cols()
    }

    pub fn is_empty(&self) -> bool {
        self.c.num_cols() == 0
    }

    //next returns the next valid Row in the same Chunk.
    pub fn next(&self) -> Row {
        // TODO should we check the idx?
        Row {
            c: self.c.clone(),
            idx: self.idx + 1,
        }
    }

    // get_i64 returns the int64 value with the col_idx.
    pub fn get_i64(&self, col_idx: usize) -> i64 {
        let idx = self.idx;
        self.c.columns[col_idx].get_i64(idx)
    }

    /// get_i64 returns the u64 value with the col_idx.
    pub fn get_u64(&self, col_idx: usize) -> u64 {
        let idx = self.idx;
        self.c.columns[col_idx].get_u64(idx)
    }

    pub fn get_f64(&self, col_idx: usize) -> f64 {
        self.c.columns[col_idx].get_f64(self.idx)
    }

    pub fn get_bytes(&self, col_idx: usize) -> &[u8] {
        self.c.columns[col_idx].get_bytes(self.idx)
    }

    pub fn get_interface(&self, col_idx: usize) -> Datum {
        self.c.columns[col_idx].get_interface(self.idx)
    }

    pub fn get_datum(&self, col_idx: usize, fp: &FieldType) -> Datum {
        if self.is_null(col_idx) {
            return Datum::Null;
        }

        match fp.get_tp() as u8 {
            types::LONG_LONG | types::TINY | types::SHORT | types::LONG | types::YEAR => {
                if types::has_unsigned_flag(fp.get_flag()) {
                    Datum::U64(self.get_u64(col_idx))
                } else {
                    Datum::I64(self.get_i64(col_idx))
                }
            }
            types::FLOAT | types::DOUBLE => {
                //TODO:no Datum::F32
                Datum::F64(self.get_f64(col_idx))
            }
            types::VARCHAR |
            types::VAR_STRING |
            types::STRING |
            types::BLOB |
            types::TINY_BLOB |
            types::MEDIUM_BLOB |
            types::LONG_BLOB => Datum::Bytes(self.get_bytes(col_idx).to_vec()),
            //TODO
            _ => self.get_interface(col_idx),
        }
    }

    pub fn is_null(&self, col_idx: usize) -> bool {
        self.c.columns[col_idx].is_null(self.idx)
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::FieldType;
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::{Decimal, Json};
    use super::*;

    fn new_chunk(elem_len: &[i32]) -> Chunk {
        let mut cols = Vec::with_capacity(elem_len.len());
        for l in elem_len {
            let col = if *l > 0 {
                Column::new_fixed_column(*l as usize, 0)
            } else if *l == 0 {
                Column::new_var_len_column(0)
            } else {
                Column::new_interface_column(0)
            };
            cols.push(col);
        }
        Chunk { columns: cols }
    }

    fn assert_same_columns(left: &Column, right: &Column) {
        assert_eq!(left.length, right.length);
        assert_eq!(left.null_cnt, right.null_cnt);
        assert_eq!(left.null_bitmap, right.null_bitmap);
        assert_eq!(left.data, right.data);
        assert_eq!(left.fixed_len, right.fixed_len);
        assert_eq!(left.var_offsets, right.var_offsets);
        assert_eq!(left.ifaces, right.ifaces);
    }

    #[test]
    fn test_chunk() {
        let cols_cnt = 6;
        let rows_cnt = 10;
        let mut chunk = new_chunk(&[8, 8, 0, 0, -1, -1]);
        for i in 0..rows_cnt {
            chunk.append_null(0);
            chunk.append_i64(1, i as i64);
            let s = format!("{}.12345", i);
            chunk.append_bytes(2, s.clone().as_bytes());
            chunk.append_bytes(3, s.clone().as_bytes());
            let decimal = s.parse::<Decimal>().unwrap();
            chunk.append_interface(4, Datum::Dec(decimal));
            let json = Json::I64(i as i64);
            chunk.append_interface(5, Datum::Json(json));
        }

        assert_eq!(chunk.num_cols(), cols_cnt);
        assert_eq!(chunk.num_rows(), rows_cnt);
        let arc_chunk = ArcChunk::new(chunk);
        for i in 0..rows_cnt {
            let row = arc_chunk.get_row(i);
            //TODO:should not be zero?
            assert_eq!(row.get_i64(0), 0 as i64);
            assert!(row.is_null(0));

            //col 1
            assert_eq!(row.get_i64(1), i as i64);
            let s = format!("{}.12345", i);
            // col 2
            assert!(!row.is_null(2));
            assert_eq!(row.get_bytes(2), s.as_bytes());
            // col3
            assert!(!row.is_null(3));
            assert_eq!(row.get_bytes(3), s.as_bytes());
            // col4
            assert!(!row.is_null(4));
            let decimal = s.parse::<Decimal>().unwrap();
            assert_eq!(row.get_interface(4), Datum::Dec(decimal));
            //col5
            assert!(!row.is_null(5));
            let json = Json::I64(i as i64);
            assert_eq!(row.get_interface(5), Datum::Json(json));
        }

        // test append_row
        let mut chunk2 = new_chunk(&[8, 8, 0, 0, -1, -1]);
        for i in 0..rows_cnt {
            let row = arc_chunk.get_row(i);
            chunk2.append_row(0, row);
        }
        for i in 0..cols_cnt {
            assert_same_columns(&chunk2.columns[i], &arc_chunk.chunk.columns[i]);
        }
    }

    #[test]
    fn test_chunk_reset() {
        let mut chunk = new_chunk(&[0]);
        chunk.append_bytes(0, b"abcd");
        chunk.reset();
        chunk.append_bytes(0, b"def");
        let arc_chunk = ArcChunk::new(chunk);
        assert_eq!(arc_chunk.get_row(0).get_bytes(0), b"def");
    }

    fn field_type(tp: u8) -> FieldType {
        let mut fp = FieldType::new();
        fp.set_tp(tp as i32);
        fp
    }

    #[test]
    fn test_append() {
        let fields = vec![
            field_type(types::FLOAT),
            field_type(types::VARCHAR),
            field_type(types::JSON),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();

        let mut src = Chunk::new(&fields);
        let mut dst = Chunk::new(&fields);
        src.append_f64(0, 12.8);
        src.append_bytes(1, b"abc");
        src.append_interface(2, Datum::Json(json.clone()));
        src.append_null(0);
        src.append_null(1);
        src.append_null(2);

        for _ in 0..6 {
            dst.append(&src, 0, 2);
        }
        dst.append(&src, 1, 1);
        let row_cnt = 12;
        assert_eq!(dst.columns.len(), 3);
        for col in &dst.columns {
            assert_eq!(col.length, row_cnt);
            assert_eq!(col.null_cnt, 6);
            assert_eq!(col.null_bitmap, vec![0x55, 0x05]);
        }
        // check column 0
        assert!(!dst.columns[0].is_varlen());
        assert!(dst.columns[0].ifaces.is_empty());
        assert_eq!(dst.columns[0].fixed_len, 8);
        assert_eq!(dst.columns[0].data.len(), 8 * row_cnt);

        // check column 1,varchar
        assert_eq!(
            dst.columns[1].var_offsets,
            vec![0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18]
        );
        assert_eq!(dst.columns[1].data, b"abcabcabcabcabcabc".to_vec());
        assert!(!dst.columns[1].is_fixed());
        assert!(dst.columns[1].ifaces.is_empty());


        // check column 2, interface
        assert!(!dst.columns[2].is_varlen());
        assert!(!dst.columns[2].is_fixed());
        assert!(dst.columns[2].data.is_empty());
        assert_eq!(dst.columns[2].ifaces.len(), row_cnt);

        for i in 0..row_cnt {
            if i & 1 == 1 {
                assert_eq!(dst.columns[2].ifaces[i], Datum::Null)
            } else {
                assert_eq!(dst.columns[2].ifaces[i], Datum::Json(json.clone()));
            }
        }
    }

    #[test]
    fn test_truncate_to() {
        let fields = vec![
            field_type(types::FLOAT),
            field_type(types::VARCHAR),
            field_type(types::JSON),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();

        let mut chunk = Chunk::new(&fields);

        for _ in 0..8 {
            chunk.append_f64(0, 12.8);
            chunk.append_bytes(1, b"abc");
            chunk.append_interface(2, Datum::Json(json.clone()));
            chunk.append_null(0);
            chunk.append_null(1);
            chunk.append_null(2);
        }

        chunk.truncate_to(16);
        chunk.truncate_to(16);
        chunk.truncate_to(14);
        let row_cnt = 12;
        chunk.truncate_to(12);
        assert_eq!(chunk.columns.len(), 3);
        for col in &chunk.columns {
            assert_eq!(col.length, row_cnt);
            assert_eq!(col.null_cnt, 6);
            assert_eq!(col.null_bitmap, vec![0x55, 0x55]);
        }
        // check column 0
        assert!(!chunk.columns[0].is_varlen());
        assert!(chunk.columns[0].ifaces.is_empty());
        assert_eq!(chunk.columns[0].fixed_len, 8);
        assert_eq!(chunk.columns[0].data.len(), 8 * row_cnt);

        // check column 1,varchar
        assert_eq!(
            chunk.columns[1].var_offsets,
            vec![0, 3, 3, 6, 6, 9, 9, 12, 12, 15, 15, 18, 18]
        );
        assert_eq!(chunk.columns[1].data, b"abcabcabcabcabcabc".to_vec());
        assert!(!chunk.columns[1].is_fixed());
        assert!(chunk.columns[1].ifaces.is_empty());


        // check column 2, interface
        assert!(!chunk.columns[2].is_varlen());
        assert!(!chunk.columns[2].is_fixed());
        assert!(chunk.columns[2].data.is_empty());
        assert_eq!(chunk.columns[2].ifaces.len(), row_cnt);

        for i in 0..row_cnt {
            if i & 1 == 1 {
                assert_eq!(chunk.columns[2].ifaces[i], Datum::Null)
            } else {
                assert_eq!(chunk.columns[2].ifaces[i], Datum::Json(json.clone()));
            }
        }
    }

    #[test]
    fn test_row() {
        let fields = vec![
            field_type(types::FLOAT),
            field_type(types::VARCHAR),
            field_type(types::JSON),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();

        let mut chunk = Chunk::new(&fields);

        for _ in 0..8 {
            chunk.append_f64(0, 12.8);
            chunk.append_bytes(1, b"abc");
            chunk.append_interface(2, Datum::Json(json.clone()));
            chunk.append_null(0);
            chunk.append_null(1);
            chunk.append_null(2);
        }
        let arc_chunk = ArcChunk::new(chunk);
        // begin
        let row0 = arc_chunk.begin();
        assert!((row0.get_f64(0) - 12.8).abs() < 0.01);
        assert_eq!(row0.get_bytes(1), b"abc");
        assert_eq!(row0.get_interface(2), Datum::Json(json.clone()));
        assert_eq!(row0.len(), 3);
        assert_eq!(row0.idx(), 0);
        assert!(!row0.is_empty());

        let row1 = row0.next();
        assert!(row1.is_null(0));
        assert_eq!(row1.get_datum(1, &fields[1]), Datum::Null);

        for i in 0..arc_chunk.end().idx() {
            let row = arc_chunk.get_row(i);
            if i % 2 == 1 {
                assert!(row.is_null(0));
                assert!(row.is_null(1));
                assert!(row.is_null(2));
            } else {
                assert_eq!(row.get_datum(0, &fields[0]), Datum::F64(12.8));
                assert_eq!(row.get_datum(1, &fields[1]), Datum::Bytes(b"abc".to_vec()));
                assert_eq!(row.get_datum(2, &fields[2]), Datum::Json(json.clone()));
            }
        }

    }
}
