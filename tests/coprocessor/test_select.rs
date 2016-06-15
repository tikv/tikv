use tikv::server::coprocessor::*;
use tikv::server::coprocessor;
use kvproto::kvrpcpb::Context;
use tikv::util::codec::{table, Datum, datum};
use tikv::util::codec::datum::DatumDecoder;
use tikv::util::codec::number::*;
use tikv::storage::{Dsn, Mutation, Key};
use tikv::storage::engine::{self, Engine, TEMP_DIR};
use tikv::storage::txn::TxnStore;
use tikv::util;
use tikv::util::event::Event;
use tikv::util::worker::Worker;
use kvproto::coprocessor::{Request, KeyRange};
use tipb::select::{ByItem, SelectRequest, SelectResponse};
use tipb::schema::{self, ColumnInfo};
use tipb::expression::{Expr, ExprType};

use std::sync::Arc;
use std::i64;
use protobuf::{RepeatedField, Message};

use util::TsGenerator;

const TYPE_VAR_CHAR: u8 = 1;
const TYPE_LONG: u8 = 2;

struct TableInfo {
    t_id: i64,
    c_types: Vec<u8>,
    c_ids: Vec<i64>,
    idx_off: Vec<i32>,
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

    fn as_pb_index_info(&self, offset: i64) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.t_id);
        idx_info.set_index_id(offset);
        let col_off = self.idx_off[offset as usize];
        let mut c_info = ColumnInfo::new();
        c_info.set_tp(self.c_types[col_off as usize] as i32);
        c_info.set_column_id(self.c_ids[col_off as usize]);
        c_info.set_pk_handle(false);
        idx_info.mut_columns().push(c_info);
        idx_info
    }
}

struct Store {
    store: TxnStore,
    ts_g: TsGenerator,
    current_ts: u64,
    handles: Vec<Vec<u8>>,
}

impl Store {
    fn new(engine: Arc<Box<Engine>>) -> Store {
        Store {
            store: TxnStore::new(engine),
            ts_g: TsGenerator::new(),
            current_ts: 1,
            handles: vec![],
        }
    }

    fn begin(&mut self) {
        self.current_ts = self.ts_g.gen();
        self.handles.clear();
    }

    fn put(&mut self, mut kv: Vec<(Vec<u8>, Vec<u8>)>) {
        self.handles.extend(kv.iter().map(|&(ref k, _)| k.clone()));
        let pk = kv[0].0.clone();
        let kv = kv.drain(..).map(|(k, v)| Mutation::Put((Key::from_raw(&k), v))).collect();
        self.store.prewrite(Context::new(), kv, pk, self.current_ts).unwrap();
    }

    fn delete(&mut self, mut keys: Vec<Vec<u8>>) {
        self.handles.extend(keys.clone());
        let pk = keys[0].clone();
        let mutations = keys.drain(..).map(|k| Mutation::Delete(Key::from_raw(&k))).collect();
        self.store.prewrite(Context::new(), mutations, pk, self.current_ts).unwrap();
    }

    fn commit(&mut self) {
        let handles = self.handles.drain(..).map(|x| Key::from_raw(&x)).collect();
        self.store
            .commit(Context::new(), handles, self.current_ts, self.ts_g.gen())
            .unwrap();
    }
}

fn gen_values(h: i64, ti: &TableInfo) -> Vec<Datum> {
    let mut values = Vec::with_capacity(ti.c_types.len());
    for &tp in &ti.c_types {
        match tp {
            TYPE_LONG => values.push(Datum::I64(h)),
            TYPE_VAR_CHAR => values.push(Datum::Bytes(format!("varchar:{}", h / 2).into_bytes())),
            _ => values.push(Datum::Null),
        }
    }
    values
}

fn encode_col_kv(t_id: i64, h: i64, c_id: i64, v: &Datum) -> (Vec<u8>, Vec<u8>) {
    let key = table::encode_column_key(t_id, h, c_id);
    let val = datum::encode_value(util::as_slice(v)).unwrap();
    (key, val)
}

fn get_row(h: i64, ti: &TableInfo) -> Vec<(Vec<u8>, Vec<u8>)> {
    let col_values = gen_values(h, ti);
    let mut kvs = vec![];
    for (v, &id) in col_values.iter().zip(&ti.c_ids) {
        let (k, v) = encode_col_kv(ti.t_id, h, id, v);
        kvs.push((k, v));
    }
    for (&off, &i_id) in ti.idx_off.iter().zip(&ti.i_ids) {
        let v = col_values[off as usize].clone();

        let encoded = datum::encode_key(&[v, Datum::I64(h)]).unwrap();
        let idx_key = table::encode_index_seek_key(ti.t_id, i_id, &encoded);
        kvs.push((idx_key, vec![0]));
    }
    kvs
}

fn build_row_key(table_id: i64, id: i64) -> Vec<u8> {
    let mut buf = [0; 8];
    (&mut buf as &mut [u8]).encode_i64(id).unwrap();
    table::encode_row_key(table_id, &buf)
}

fn prepare_table_data(store: &mut Store, ti: &TableInfo, count: i64) {
    store.begin();
    for i in 1..count + 1 {
        let mut mutations = vec![];

        let row_key = build_row_key(ti.t_id, i as i64);

        mutations.push((row_key, vec![]));
        mutations.extend(get_row(i, ti));
        store.put(mutations);
    }
    store.commit();
}

fn build_basic_sel(store: &mut Store,
                   limit: Option<i64>,
                   desc: Option<bool>,
                   aggrs: Vec<Expr>,
                   group_by: Vec<i64>)
                   -> SelectRequest {
    let mut sel = SelectRequest::new();
    sel.set_start_ts(store.ts_g.gen());

    if let Some(limit) = limit {
        sel.set_limit(limit);
    }

    if let Some(desc) = desc {
        let mut item = ByItem::new();
        item.set_desc(desc);
        sel.mut_order_by().push(item);
    }

    if !aggrs.is_empty() {
        sel.set_aggregates(RepeatedField::from_vec(aggrs));
    }

    for col in group_by {
        let mut item = ByItem::new();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        expr.mut_val().encode_i64(col).unwrap();
        item.set_expr(expr);
        sel.mut_group_by().push(item);
    }

    sel
}

fn prepare_sel(store: &mut Store,
               ti: &TableInfo,
               limit: Option<i64>,
               desc: Option<bool>,
               aggrs: Vec<Expr>,
               group_by: Vec<i64>)
               -> Request {
    let mut sel = build_basic_sel(store, limit, desc, aggrs, group_by);
    sel.set_table_info(ti.as_pb_table_info());

    let mut req = Request::new();
    req.set_tp(REQ_TYPE_SELECT);
    req.set_data(sel.write_to_bytes().unwrap());

    let mut range = KeyRange::new();

    let mut buf = Vec::with_capacity(8);
    buf.encode_i64(i64::MIN).unwrap();
    range.set_start(table::encode_row_key(ti.t_id, &buf));

    buf.clear();
    buf.encode_i64(i64::MAX).unwrap();
    range.set_end(table::encode_row_key(ti.t_id, &buf));
    req.set_ranges(RepeatedField::from_vec(vec![range]));
    req
}

// This function will insert `count` rows data into table.
// each row contains 3 column, first column primary key, which
// id is 1. Second column's type is a varchar, id is 3, value is
// `varchar$((handle / 2))`. Third column's type is long, id is 4,
// value is the same as handle.
fn initial_data(count: i64) -> (Store, Worker<RequestTask>, TableInfo) {
    let engine = Arc::new(engine::new_engine(Dsn::RocksDBPath(TEMP_DIR)).unwrap());
    let mut store = Store::new(engine.clone());
    let ti = TableInfo {
        t_id: 1,
        c_types: vec![TYPE_VAR_CHAR, TYPE_LONG],
        c_ids: vec![3, 4],
        idx_off: vec![0],
        i_ids: vec![5],
    };
    prepare_table_data(&mut store, &ti, count);

    let end_point = EndPointHost::new(engine);
    let mut worker = Worker::new("test select worker");
    worker.start_batch(end_point, 5).unwrap();
    (store, worker, ti)
}

#[test]
fn test_select() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);
    let req = prepare_sel(&mut store, &ti, None, None, vec![], vec![]);

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), count as usize);
    for (i, row) in resp.get_rows().iter().enumerate() {
        let handle = i as i64 + 1;
        let mut expected_datum = vec![Datum::I64(handle)];
        expected_datum.extend(gen_values(handle, &ti));
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_group_by() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);

    let req = prepare_sel(&mut store, &ti, None, None, vec![], vec![3]);
    let resp = handle_select(&end_point, req);
    assert_eq!(resp.get_rows().len(), 6);
    for (i, row) in resp.get_rows().iter().enumerate() {
        let gk = datum::encode_value(&[Datum::Bytes(format!("varchar:{}", i).into_bytes())]);
        let expected_encoded = datum::encode_value(&[Datum::Bytes(gk.unwrap())]).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_aggr_count() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);

    let mut expr = Expr::new();
    expr.set_tp(ExprType::Count);
    let req = prepare_sel(&mut store, &ti, None, None, vec![expr.clone()], vec![]);

    let resp = handle_select(&end_point, req);
    assert_eq!(resp.get_rows().len(), 1);
    let mut expected_encoded =
        datum::encode_value(&[Datum::Bytes(coprocessor::SINGLE_GROUP.to_vec()), Datum::U64(10)])
            .unwrap();
    assert_eq!(resp.get_rows()[0].get_data(), &*expected_encoded);

    let req = prepare_sel(&mut store, &ti, None, None, vec![expr], vec![3]);
    let resp = handle_select(&end_point, req);
    assert_eq!(resp.get_rows().len(), 6);
    for (i, row) in resp.get_rows().iter().enumerate() {
        let count = if i == 0 || i == 5 {
            1
        } else {
            2
        };
        let gk = datum::encode_value(&[Datum::Bytes(format!("varchar:{}", i).into_bytes())]);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::U64(count)];
        expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_aggr_first() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);

    let mut col = Expr::new();
    col.set_tp(ExprType::ColumnRef);
    col.mut_val().encode_i64(1).unwrap();
    let mut expr = Expr::new();
    expr.set_tp(ExprType::First);
    expr.mut_children().push(col);

    let req = prepare_sel(&mut store, &ti, None, None, vec![expr], vec![3]);
    let resp = handle_select(&end_point, req);
    assert_eq!(resp.get_rows().len(), 6);
    for (i, row) in resp.get_rows().iter().enumerate() {
        let idx = if i == 0 {
            1
        } else {
            i * 2
        };
        let gk = datum::encode_value(&[Datum::Bytes(format!("varchar:{}", i).into_bytes())]);
        let expected_datum = vec![Datum::Bytes(gk.unwrap()), Datum::I64(idx as i64)];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_limit() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);
    let req = prepare_sel(&mut store, &ti, Some(5), None, vec![], vec![]);

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), 5 as usize);

    for (i, row) in resp.get_rows().iter().enumerate() {
        let handle = i as i64 + 1;
        let mut expected_datum = vec![Datum::I64(handle)];
        expected_datum.extend(gen_values(handle, &ti));
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

#[test]
fn test_reverse() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);
    let req = prepare_sel(&mut store, &ti, Some(5), Some(true), vec![], vec![]);

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), 5 as usize);

    for (i, row) in resp.get_rows().iter().enumerate() {
        let handle = 10 - i as i64;
        let mut expected_datum = vec![Datum::I64(handle)];
        expected_datum.extend(gen_values(handle, &ti));
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        assert_eq!(row.get_data(), &*expected_encoded);
    }

    end_point.stop().unwrap();
}

fn prepare_idx(store: &mut Store,
               ti: &TableInfo,
               limit: Option<i64>,
               desc: Option<bool>)
               -> Request {
    let mut sel = build_basic_sel(store, limit, desc, vec![], vec![]);
    sel.set_index_info(ti.as_pb_index_info(0));

    let mut req = Request::new();
    req.set_tp(REQ_TYPE_INDEX);
    req.set_data(sel.write_to_bytes().unwrap());

    let mut range = KeyRange::new();

    range.set_start(table::encode_index_seek_key(ti.t_id, ti.i_ids[0], &[0]));
    range.set_end(table::encode_index_seek_key(ti.t_id, ti.i_ids[0], &[255]));

    req.set_ranges(RepeatedField::from_vec(vec![range]));
    req
}

fn handle_select(end_point: &Worker<RequestTask>, req: Request) -> SelectResponse {
    let finish = Event::new();
    let finish_clone = finish.clone();
    end_point.schedule(RequestTask::new(req,
                                   box move |r| {
                                       finish_clone.set(r);
                                   }))
        .unwrap();
    finish.wait_timeout(None);
    let resp = finish.take().unwrap().take_cop_resp();
    assert!(resp.has_data(), format!("{:?}", resp));
    let mut sel_resp = SelectResponse::new();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    sel_resp
}

#[test]
fn test_index() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);
    let req = prepare_idx(&mut store, &ti, None, None);

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), count as usize);
    let mut handles = vec![];
    for row in resp.get_rows() {
        let datums = row.get_handle().decode().unwrap();
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
    end_point.stop().unwrap();
}

#[test]
fn test_index_reverse_limit() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);
    let req = prepare_idx(&mut store, &ti, Some(5), Some(true));

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), 5);
    let mut handles = vec![];
    for row in resp.get_rows() {
        let datums = row.get_handle().decode().unwrap();
        assert_eq!(datums.len(), 1);
        if let Datum::I64(h) = datums[0] {
            handles.push(h);
        } else {
            panic!("i64 expected, but got {:?}", datums[0]);
        }
    }
    for (i, &h) in handles.iter().enumerate() {
        assert_eq!(10 - i as i64, h);
    }
    end_point.stop().unwrap();
}

#[test]
fn test_del_select() {
    let count = 10;
    let (mut store, mut end_point, ti) = initial_data(count);

    store.begin();
    let handle = count / 2;
    let row_key = build_row_key(ti.t_id, handle);
    store.delete(vec![row_key]);
    let mut rows = get_row(handle, &ti);
    store.delete(rows.drain(..).map(|(k, _)| k).collect());
    store.commit();

    let req = prepare_sel(&mut store, &ti, None, None, vec![], vec![]);

    let resp = handle_select(&end_point, req);

    assert_eq!(resp.get_rows().len(), count as usize - 1);

    end_point.stop().unwrap();
}
