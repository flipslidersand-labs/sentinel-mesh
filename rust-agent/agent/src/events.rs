use std::time::Duration;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::pb::{ExecEvent, EventType, KernelEvent};

/// Mock event generator for testing without eBPF.
pub async fn mock_source(tx: Sender<KernelEvent>, rate: u64) {
    let interval = Duration::from_millis(1000 / rate.max(1));
    let mut ticker = tokio::time::interval(interval);
    let mut pid: u32 = 1000;

    loop {
        ticker.tick().await;
        pid = pid.wrapping_add(1);

        let event = KernelEvent {
            event_id: Uuid::new_v4().to_string(),
            node_id: String::new(), // filled by grpc::stream_to_collector
            timestamp: now_nanos(),
            r#type: EventType::Exec as i32,
            payload: Some(crate::pb::kernel_event::Payload::Exec(ExecEvent {
                pid,
                ppid: pid.saturating_sub(1),
                comm: "mock-proc".into(),
                cmdline: format!("/usr/bin/mock-proc --pid={pid}"),
                cwd: "/home/user".into(),
                uid: 1000,
            })),
        };

        let json = serde_json::to_string(&MockEvent::from(&event)).unwrap_or_default();
        println!("{json}");

        if tx.send(event).await.is_err() {
            break;
        }
    }
}

#[cfg(feature = "ebpf")]
pub async fn ebpf_source(tx: Sender<KernelEvent>) {
    use aya::{maps::RingBuf, programs::TracePoint, Bpf};
    use uuid::Uuid;

    let bytes = include_bytes_aligned!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../target/bpfel-unknown-none/debug/sentinel-agent-ebpf"
    ));

    let mut bpf = match Bpf::load(bytes) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("failed to load eBPF: {e}");
            return;
        }
    };

    let prog: &mut TracePoint = bpf
        .program_mut("sentinel_exec")
        .unwrap()
        .try_into()
        .unwrap();
    prog.load().unwrap();
    prog.attach("syscalls", "sys_enter_execve").unwrap();

    let ring_buf = RingBuf::try_from(bpf.map_mut("EVENTS").unwrap()).unwrap();
    let mut async_fd = tokio::io::unix::AsyncFd::new(ring_buf).unwrap();

    loop {
        let mut guard = async_fd.readable_mut().await.unwrap();
        let rb = guard.get_inner_mut();
        while let Some(item) = rb.next() {
            let data: &[u8] = &item;
            if data.len() < core::mem::size_of::<EbpfExecEvent>() {
                continue;
            }
            let raw = unsafe { &*(data.as_ptr() as *const EbpfExecEvent) };
            let comm = std::str::from_utf8(&raw.comm)
                .unwrap_or("")
                .trim_end_matches('\0')
                .to_string();

            let event = KernelEvent {
                event_id: Uuid::new_v4().to_string(),
                node_id: String::new(),
                timestamp: now_nanos(),
                r#type: EventType::Exec as i32,
                payload: Some(crate::pb::kernel_event::Payload::Exec(ExecEvent {
                    pid: raw.pid,
                    ppid: 0,
                    comm,
                    cmdline: String::new(),
                    cwd: String::new(),
                    uid: raw.uid,
                })),
            };
            let _ = tx.send(event).await;
        }
        guard.clear_ready();
    }
}

#[cfg(feature = "ebpf")]
macro_rules! include_bytes_aligned {
    ($path:expr) => {{
        #[repr(C, align(4096))]
        struct Aligned<const N: usize>([u8; N]);
        static DATA: Aligned<{ include_bytes!($path).len() }> =
            Aligned(*include_bytes!($path));
        &DATA.0
    }};
}

#[cfg(feature = "ebpf")]
#[repr(C)]
struct EbpfExecEvent {
    event_type: u8,
    pid: u32,
    uid: u32,
    comm: [u8; 16],
}

fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

// Lightweight JSON representation for stdout logging.
#[derive(serde::Serialize)]
struct MockEvent {
    event_id: String,
    r#type: String,
    pid: u32,
    comm: String,
}

impl From<&KernelEvent> for MockEvent {
    fn from(e: &KernelEvent) -> Self {
        let (type_str, pid, comm) = match &e.payload {
            Some(crate::pb::kernel_event::Payload::Exec(ex)) => {
                ("exec".into(), ex.pid, ex.comm.clone())
            }
            Some(crate::pb::kernel_event::Payload::Tcp(t)) => {
                ("tcp".into(), t.pid, t.comm.clone())
            }
            Some(crate::pb::kernel_event::Payload::File(f)) => {
                ("file".into(), f.pid, f.comm.clone())
            }
            None => ("unknown".into(), 0, String::new()),
        };
        MockEvent { event_id: e.event_id.clone(), r#type: type_str, pid, comm }
    }
}
