use j4rs::{Instance, Jvm, JvmBuilder, ClasspathEntry, InvocationArg};

pub fn send_kv(pd_address: &str, key: &str, value: &str) {
    let entry = ClasspathEntry::new("/home/tidb/workspace/tikv_raw_assembly-jar-with-dependencies.jar");

    let jvm = JvmBuilder::new().classpath_entry(entry).build().unwrap();

    let str_arg = InvocationArg::try_from(pd_address);
    jvm.invoke_static(
        "org.pingcap.tikv_raw_java.RawApiSender",
        "init",
        &[str_arg.unwrap()]
    );
    let sender = jvm.create_instance(
        "org.pingcap.tikv_raw_java.RawApiSender",
        &Vec::new(),
    ).unwrap();

    let key = InvocationArg::try_from(key).unwrap();
    let value = InvocationArg::try_from(value).unwrap();
    jvm.chain(&sender).unwrap().invoke("sendRawApi", &vec![key, value]);

}

pub fn send_kv_delete(pd_address: &str, key: &str) {
    let entry = ClasspathEntry::new("/home/tidb/workspace/tikv_raw_assembly-jar-with-dependencies.jar");

    let jvm = JvmBuilder::new().classpath_entry(entry).build().unwrap();

    let str_arg = InvocationArg::try_from(pd_address);
    jvm.invoke_static(
        "org.pingcap.tikv_raw_java.RawApiSender",
        "init",
        &[str_arg.unwrap()]
    );
    let sender = jvm.create_instance(
        "org.pingcap.tikv_raw_java.RawApiSender",
        &Vec::new(),
    ).unwrap();

    let key = InvocationArg::try_from(key).unwrap();
    jvm.chain(&sender).unwrap().invoke("sendRawApiDelete", &vec![key]);

}
