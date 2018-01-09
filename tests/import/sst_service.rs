// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

fn must_new_cluster() -> (Cluster<ServerCluster>, Context) {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::new();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    (cluster, ctx)
}

fn must_new_cluster_and_import_client() -> (Cluster<ServerCluster>, ImportClient, Context) {
    let (cluster, ctx) = must_new_cluster();

    let env = Arc::new(Environment::new(1));
    let addr = cluster.sim.rl().get_addr(ctx.get_peer().get_store_id());
    let channel = ChannelBuilder::new(env).connect(&format!("{}", addr));
    let client = ImportClient::new(channel);

    (cluster, client, ctx)
}

#[test]
fn test_upload_sst() {
    let (_, client, _) = must_new_cluster_and_import_client();

    let data = vec![1; 1024];
    let mut hash = crc32::Digest::new(crc32::IEEE);
    hash.write(&data);
    let crc32 = hash.sum32();

    let mut upload = UploadSSTRequest::new();
    upload.set_data(data.clone());

    // Mismatch length
    let meta = make_sst_meta(0, crc32);
    upload.set_meta(meta);
    assert!(send_upload_sst(&client, upload.clone()).is_err());

    // Mismatch checksum
    let meta = make_sst_meta(data.len(), 0);
    upload.set_meta(meta);
    assert!(send_upload_sst(&client, upload.clone()).is_err());

    let meta = make_sst_meta(data.len(), crc32);
    upload.set_meta(meta);
    send_upload_sst(&client, upload.clone()).unwrap();

    // Can't upload the same uuid file again.
    assert!(send_upload_sst(&client, upload.clone()).is_err());
}

fn make_sst_meta(len: usize, crc32: u32) -> SSTMeta {
    let mut m = SSTMeta::new();
    m.set_len(len as u64);
    m.set_crc32(crc32);
    m.set_handle(make_sst_handle());
    m
}

fn make_sst_handle() -> SSTHandle {
    let mut h = SSTHandle::new();
    let uuid = Uuid::new_v4();
    h.set_uuid(uuid.as_bytes().to_vec());
    h.set_cf_name("default".to_owned());
    h.set_region_id(1);
    h.mut_region_epoch().set_conf_ver(2);
    h.mut_region_epoch().set_version(3);
    h
}

fn send_upload_sst(client: &ImportClient, m: UploadSSTRequest) -> Result<UploadSSTResponse> {
    let (tx, rx) = client.upload_sst();
    let stream = stream::once({ Ok((m, WriteFlags::default().buffer_hint(true))) });
    stream.forward(tx).and_then(|_| rx).wait()
}
