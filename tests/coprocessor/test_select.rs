use tikv::server::coprocessor::*;
use kvproto::kvrpcpb::Context;
use tikv::util::codec::{table, Datum, datum, number};
use tikv::storage::{Dsn, Mutation, Key};
use tikv::storage::engine::{self, Engine};
use tikv::storage::txn::TxnStore;
use kvproto::coprocessor::{Request, KeyRange};
use tipb::select::{SelectRequest, SelectResponse};
use tipb::schema::{self, ColumnInfo};

use std::ops::RangeFrom;
use std::sync::Arc;
use std::{slice, i64};
use protobuf::{RepeatedField, Message};

const TYPE_VAR_CHAR: u8 = 1;
const TYPE_LONG: u8 = 2;

struct TableInfo {
    t_id: i64,
    c_types: Vec<u8>,
    c_ids: Vec<i64>,
    indices: Vec<i32>,
    i_ids: Vec<i64>,
}

impl TableInfo {
    fn as_pb_table_info(&self) -> schema::TableInfo {
        let mut tb_info = schema::TableInfo::new();
        tb_info.set_table_id(self.t_id);
        let mut c_info = ColumnInfo::new();
        c_info.set_tp(TYPE_LONG as i32);
        c_info.set_column_id(tb_info.get_table_id());
        c_info.set_pk_handle(true);
        tb_info.mut_columns().push(c_info);
        for (&col_tp, &col_id) in self.c_types.iter().zip(&self.c_ids) {
            c_info = ColumnInfo::new();
            c_info.set_column_id(col_id);
            c_info.set_tp(col_tp as i32);
            c_info.set_pk_handle(false);
            tb_info.mut_columns().push(c_info);
        }
        tb_info
    }

    fn as_pb_index_info(&self, idx_off: i64) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.t_id);
        idx_info.set_index_id(idx_off);
        let col_off = self.indices[idx_off as usize];
        let mut c_info = ColumnInfo::new();
        c_info.set_tp(self.c_types[idx_off as usize] as i32);
        c_info.set_column_id(self.c_ids[col_off as usize]);
        c_info.set_pk_handle(false);
        idx_info.mut_columns().push(c_info);
        idx_info
    }
}

struct Store {
    store: TxnStore,
    ts_g: RangeFrom<u64>,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl Store {
    fn new(engine: Arc<Box<Engine>>) -> Store {
        Store {
            store: TxnStore::new(engine),
            ts_g: 1..,
            current_ts: 0,
            handles: vec![],
        }
    }

    fn begin(&mut self) {
        self.current_ts = self.ts_g.next().unwrap();
        self.handles.clear();
    }

    fn put(&mut self, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv.drain(..).map(|(k, v)| Mutation::Put((Key::from_raw(k), v))).collect();
        self.store.prewrite(Context::new(), kv, pk, self.current_ts).unwrap();
    }

    fn commit(&mut self) {
        let handles = self.handles.drain(..).map(Key::from_raw).collect();
        self.store
            .commit(Context::new(),
                    handles,
                    self.current_ts,
                    self.ts_g.next().unwrap())
            .unwrap();
    }
}

fn gen_values(h: i64, tbl: &TableInfo) -> Vec<Datum> {
    let mut values = Vec::with_capacity(tbl.c_types.len());
    for &tp in &tbl.c_types {
        match tp {
            TYPE_LONG => values.push(Datum::I64(h)),
            TYPE_VAR_CHAR => values.push(Datum::Bytes(format!("varchar:{}", h).into_bytes())),
            _ => values.push(Datum::Null),
        }
    }
    values
}

fn encode_col_kv(t_id: i64, h: i64, c_id: i64, v: &Datum) -> (Vec<u8>, Vec<u8>) {
    let key = table::encode_column_key(t_id, h, c_id);
    let vs = unsafe {
        let rp = v as *const Datum;
        slice::from_raw_parts(rp, 1)
    };
    let val = datum::encode_value(vs).unwrap();
    (key, val)
}

fn get_row(h: i64, tbl: &TableInfo) -> Vec<(Vec<u8>, Vec<u8>)> {
    let col_values = gen_values(h, tbl);
    let mut kvs = vec![];
    for (v, &id) in col_values.iter().zip(&tbl.c_ids) {
        let (k, v) = encode_col_kv(tbl.t_id, h, id, v);
        kvs.push((k, v));
    }
    for (&id, &c_id) in tbl.indices.iter().zip(&tbl.i_ids) {
        let v = col_values[id as usize].clone();

        let encoded = datum::encode_key(&[v, Datum::I64(h)]).unwrap();
        let idx_key = table::encode_index_seek_key(tbl.t_id, c_id, &encoded);
        kvs.push((idx_key, vec![0]));
    }
    kvs
}

fn prepare_table_data(store: &mut Store, tbl: &TableInfo, count: i64) {
    store.begin();
    for i in 1..count + 1 {
        let mut mutations = vec![];

        let mut buf = vec![0; 8];
        number::encode_i64(&mut buf, i).unwrap();
        let row_key = table::encode_row_key(tbl.t_id, &buf);

        mutations.push((row_key, vec![]));
        mutations.extend(get_row(i, tbl));
        store.put(mutations);
    }
    store.commit();
}

fn prepare_sel(store: &mut Store, tbl: &TableInfo) -> Request {
    let mut sel = SelectRequest::new();
    sel.set_table_info(tbl.as_pb_table_info());
    sel.set_start_ts(store.ts_g.next().unwrap());

    let mut req = Request::new();
    req.set_tp(REQ_TYPE_SELECT);
    req.set_data(sel.write_to_bytes().unwrap());

    let mut range = KeyRange::new();

    let mut buf = vec![0; 8];
    number::encode_i64(&mut buf, i64::MIN).unwrap();
    range.set_start(table::encode_row_key(tbl.t_id, &buf));

    number::encode_i64(&mut buf, i64::MAX).unwrap();
    range.set_end(table::encode_row_key(tbl.t_id, &buf));
    req.set_ranges(RepeatedField::from_vec(vec![range]));
    req
}

fn initial_data(count: i64) -> (Store, SnapshotEndPoint, TableInfo) {
    let engine = Arc::new(engine::new_engine(Dsn::Memory).unwrap());
    let mut store = Store::new(engine.clone());
    let tbl = TableInfo {
        t_id: 1,
        c_types: vec![TYPE_VAR_CHAR, TYPE_LONG],
        c_ids: vec![3, 4],
        indices: vec![0],
        i_ids: vec![5],
    };
    prepare_table_data(&mut store, &tbl, count);

    let end_point = SnapshotEndPoint::new(engine);
    (store, end_point, tbl)
}

#[test]
fn test_select() {
    let count = 10;
    let (mut store, end_point, tbl) = initial_data(count);
    let req = prepare_sel(&mut store, &tbl);
    let mut sel_req = SelectRequest::new();
    sel_req.merge_from_bytes(req.get_data()).unwrap();

    let resp = end_point.handle_select(req, sel_req).unwrap();

    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    assert_eq!(sel_resp.get_rows().len(), count as usize);

    for (i, row) in sel_resp.get_rows().iter().enumerate() {
        let handle = i as i64 + 1;
        let mut expected_datum = vec![Datum::I64(handle)];
        expected_datum.extend(gen_values(handle, &tbl));
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }
}

fn prepare_idx(store: &mut Store, tbl: &TableInfo) -> Request {
    let mut sel = SelectRequest::new();
    sel.set_index_info(tbl.as_pb_index_info(0));
    sel.set_start_ts(store.ts_g.next().unwrap());

    let mut req = Request::new();
    req.set_tp(REQ_TYPE_INDEX);
    req.set_data(sel.write_to_bytes().unwrap());

    let mut range = KeyRange::new();

    range.set_start(table::encode_index_seek_key(tbl.t_id, tbl.i_ids[0], &[0]));
    range.set_end(table::encode_index_seek_key(tbl.t_id, tbl.i_ids[0], &[255]));

    req.set_ranges(RepeatedField::from_vec(vec![range]));
    req
}

#[test]
fn test_index() {
    let count = 10;
    let (mut store, end_point, tbl) = initial_data(count);
    let req = prepare_idx(&mut store, &tbl);
    let mut sel_req = SelectRequest::new();
    sel_req.merge_from_bytes(req.get_data()).unwrap();

    let resp = end_point.handle_select(req, sel_req).unwrap();

    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    assert_eq!(sel_resp.get_rows().len(), count as usize);
    let mut handles = vec![];
    for row in sel_resp.get_rows() {
        let datums = datum::decode(row.get_handle()).unwrap().0;
        assert_eq!(datums.len(), 1);
        if let Datum::I64(h) = datums[0] {
            handles.push(h);
        } else {
            panic!("i64 expected, but got {:?}", datums[0]);
        }
    }
    handles.sort();
    for (i, &h) in handles.iter().enumerate() {
        assert_eq!(i as i64 + 1, h);
    }
}
