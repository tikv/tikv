use file_system::IoType;
use tikv_util::{resizable_threadpool::TokioRuntimeReplaceRule, sys::thread::ThreadBuildWrapper};
use tokio::{io::Result as TokioResult, runtime::Runtime};

pub struct ImportRuntimeCreator;

impl TokioRuntimeReplaceRule for ImportRuntimeCreator {
    fn create_tokio_runtime(thread_count: usize, thread_name: &str) -> TokioResult<Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .thread_name(thread_name)
            .enable_io()
            .enable_time()
            .with_sys_and_custom_hooks(
                || {
                    file_system::set_io_type(IoType::Export);
                },
                || {},
            )
            .worker_threads(thread_count)
            .build()
    }
}
