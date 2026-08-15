use std::time::Duration;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use crate::pb::{kernel_event::Payload, EventType, ExecEvent, FileEvent, KernelEvent, TcpEvent};

// Mock pools for realistic-looking events
static MOCK_IPS: &[&str] = &[
    "10.0.0.1", "10.0.0.2", "172.16.0.10", "192.168.1.100", "8.8.8.8", "1.1.1.1",
];
static MOCK_PORTS: &[(u32, &str)] = &[
    (80, "nginx"), (443, "curl"), (5432, "psql"), (6379, "redis-cli"), (8080, "app"),
];
static MOCK_PATHS: &[(&str, &str)] = &[
    ("/etc/passwd", "open"),
    ("/var/log/syslog", "read"),
    ("/tmp/data.tmp", "write"),
    ("/home/user/.bashrc", "open"),
    ("/proc/meminfo", "read"),
];
static MOCK_COMMS: &[&str] = &["bash", "python3", "curl", "systemd", "sshd", "nginx"];

/// Mock event generator — cycles Exec / TCP / File to exercise all event types.
pub async fn mock_source(tx: Sender<KernelEvent>, rate: u64) {
    let interval = Duration::from_millis(1000 / rate.max(1));
    let mut ticker = tokio::time::interval(interval);
    let mut counter: u64 = 0;
    let mut pid: u32 = 1000;

    loop {
        ticker.tick().await;
        pid = pid.wrapping_add(1);
        counter = counter.wrapping_add(1);

        let event_kind = counter % 3; // 0=exec, 1=tcp, 2=file
        let comm = MOCK_COMMS[(counter as usize) % MOCK_COMMS.len()].to_string();

        let (etype, payload) = match event_kind {
            0 => (
                EventType::Exec,
                Payload::Exec(ExecEvent {
                    pid,
                    ppid: pid.saturating_sub(1),
                    comm: comm.clone(),
                    cmdline: format!("/usr/bin/{comm} --pid={pid}"),
                    cwd: "/home/user".into(),
                    uid: 1000,
                }),
            ),
            1 => {
                let (dst_port, dst_comm) = MOCK_PORTS[(counter as usize) % MOCK_PORTS.len()];
                let src_ip = MOCK_IPS[(counter as usize) % MOCK_IPS.len()];
                let dst_ip = MOCK_IPS[(counter as usize + 3) % MOCK_IPS.len()];
                (
                    EventType::Tcp,
                    Payload::Tcp(TcpEvent {
                        pid,
                        comm: dst_comm.to_string(),
                        src_ip: src_ip.into(),
                        src_port: 32768 + (pid % 30000),
                        dst_ip: dst_ip.into(),
                        dst_port,
                        direction: "outbound".into(),
                    }),
                )
            }
            _ => {
                let (path, op) = MOCK_PATHS[(counter as usize) % MOCK_PATHS.len()];
                (
                    EventType::File,
                    Payload::File(FileEvent {
                        pid,
                        comm: comm.clone(),
                        path: path.into(),
                        op: op.into(),
                        ret: 0,
                    }),
                )
            }
        };

        let event = KernelEvent {
            event_id: Uuid::new_v4().to_string(),
            node_id: String::new(), // filled by grpc::stream_to_collector
            timestamp: now_nanos(),
            r#type: etype as i32,
            payload: Some(payload),
        };

        let json = serde_json::to_string(&MockEventLog::from(&event)).unwrap_or_default();
        println!("{json}");

        if tx.send(event).await.is_err() {
            break;
        }
    }
}

#[cfg(feature = "ebpf")]
pub async fn ebpf_source(tx: Sender<KernelEvent>) {
    use aya::{maps::RingBuf, programs::TracePoint, Bpf};
    use std::net::Ipv4Addr;

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

    // Attach exec tracepoint (sys_enter_execve)
    let exec_prog: &mut TracePoint = bpf
        .program_mut("sentinel_exec")
        .unwrap()
        .try_into()
        .unwrap();
    exec_prog.load().unwrap();
    exec_prog.attach("syscalls", "sys_enter_execve").unwrap();

    // Attach openat tracepoint (sys_enter_openat) — best-effort
    if let Some(prog) = bpf.program_mut("sentinel_openat") {
        let tp: &mut TracePoint = prog.try_into().unwrap();
        if let Err(e) = tp.load().and_then(|_| tp.attach("syscalls", "sys_enter_openat")) {
            eprintln!("warn: openat tracepoint: {e}");
        }
    }

    // Attach tcp_connect kprobe — best-effort
    if let Some(prog) = bpf.program_mut("sentinel_tcp_connect") {
        use aya::programs::KProbe;
        let kp: &mut KProbe = prog.try_into().unwrap();
        if let Err(e) = kp.load().and_then(|_| kp.attach("tcp_connect", 0)) {
            eprintln!("warn: tcp_connect kprobe: {e}");
        }
    }

    let ring_buf = RingBuf::try_from(bpf.map_mut("EVENTS").unwrap()).unwrap();
    let mut async_fd = tokio::io::unix::AsyncFd::new(ring_buf).unwrap();

    loop {
        let mut guard = async_fd.readable_mut().await.unwrap();
        let rb = guard.get_inner_mut();
        while let Some(item) = rb.next() {
            let data: &[u8] = &item;
            if data.is_empty() {
                continue;
            }
            let kernel_event = match data[0] {
                0 if data.len() >= core::mem::size_of::<EbpfExecEvent>() => {
                    let raw = unsafe { &*(data.as_ptr() as *const EbpfExecEvent) };
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::Exec as i32,
                        payload: Some(Payload::Exec(ExecEvent {
                            pid: raw.pid,
                            ppid: 0,
                            comm: bytes_to_str(&raw.comm),
                            cmdline: String::new(),
                            cwd: String::new(),
                            uid: raw.uid,
                        })),
                    }
                }
                1 if data.len() >= core::mem::size_of::<EbpfFileEvent>() => {
                    let raw = unsafe { &*(data.as_ptr() as *const EbpfFileEvent) };
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::File as i32,
                        payload: Some(Payload::File(FileEvent {
                            pid: raw.pid,
                            comm: bytes_to_str(&raw.comm),
                            path: bytes_to_str(&raw.path),
                            op: "open".into(),
                            ret: raw.ret,
                        })),
                    }
                }
                2 if data.len() >= core::mem::size_of::<EbpfTcpEvent>() => {
                    let raw = unsafe { &*(data.as_ptr() as *const EbpfTcpEvent) };
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::Tcp as i32,
                        payload: Some(Payload::Tcp(TcpEvent {
                            pid: raw.pid,
                            comm: bytes_to_str(&raw.comm),
                            src_ip: Ipv4Addr::from(u32::from_be(raw.src_ip)).to_string(),
                            src_port: raw.src_port as u32,
                            dst_ip: Ipv4Addr::from(u32::from_be(raw.dst_ip)).to_string(),
                            dst_port: raw.dst_port as u32,
                            direction: "outbound".into(),
                        })),
                    }
                }
                _ => continue,
            };
            let _ = tx.send(kernel_event).await;
        }
        guard.clear_ready();
    }
}

#[cfg(feature = "ebpf")]
fn bytes_to_str(b: &[u8]) -> String {
    std::str::from_utf8(b)
        .unwrap_or("")
        .trim_end_matches('\0')
        .to_string()
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

// Shared C structs mirroring agent-ebpf layout — must stay in sync with kernel side.
#[cfg(feature = "ebpf")]
#[repr(C)]
struct EbpfExecEvent {
    event_type: u8,
    pid: u32,
    uid: u32,
    comm: [u8; 16],
}

#[cfg(feature = "ebpf")]
#[repr(C)]
struct EbpfFileEvent {
    event_type: u8,
    pid: u32,
    uid: u32,
    comm: [u8; 16],
    path: [u8; 256],
    ret: i32,
}

#[cfg(feature = "ebpf")]
#[repr(C)]
struct EbpfTcpEvent {
    event_type: u8,
    pid: u32,
    comm: [u8; 16],
    src_ip: u32,
    dst_ip: u32,
    src_port: u16,
    dst_port: u16,
}

fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[derive(serde::Serialize)]
struct MockEventLog {
    event_id: String,
    r#type: &'static str,
    pid: u32,
    comm: String,
    detail: String,
}

impl From<&KernelEvent> for MockEventLog {
    fn from(e: &KernelEvent) -> Self {
        match &e.payload {
            Some(Payload::Exec(ex)) => MockEventLog {
                event_id: e.event_id.clone(),
                r#type: "exec",
                pid: ex.pid,
                comm: ex.comm.clone(),
                detail: ex.cmdline.clone(),
            },
            Some(Payload::Tcp(t)) => MockEventLog {
                event_id: e.event_id.clone(),
                r#type: "tcp",
                pid: t.pid,
                comm: t.comm.clone(),
                detail: format!("{}:{} -> {}:{}", t.src_ip, t.src_port, t.dst_ip, t.dst_port),
            },
            Some(Payload::File(f)) => MockEventLog {
                event_id: e.event_id.clone(),
                r#type: "file",
                pid: f.pid,
                comm: f.comm.clone(),
                detail: format!("{} {}", f.op, f.path),
            },
            None => MockEventLog {
                event_id: e.event_id.clone(),
                r#type: "unknown",
                pid: 0,
                comm: String::new(),
                detail: String::new(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{kernel_event::Payload, ExecEvent, FileEvent, KernelEvent, TcpEvent};

    fn ke(payload: Option<Payload>) -> KernelEvent {
        KernelEvent {
            event_id: "id-1".into(),
            node_id: String::new(),
            timestamp: 0,
            r#type: 0,
            payload,
        }
    }

    #[test]
    fn mock_event_log_exec() {
        let e = ke(Some(Payload::Exec(ExecEvent {
            pid: 42,
            ppid: 41,
            comm: "bash".into(),
            cmdline: "/usr/bin/bash -x".into(),
            cwd: "/root".into(),
            uid: 0,
        })));
        let log = MockEventLog::from(&e);
        assert_eq!(log.r#type, "exec");
        assert_eq!(log.pid, 42);
        assert_eq!(log.comm, "bash");
        assert_eq!(log.detail, "/usr/bin/bash -x");
        assert_eq!(log.event_id, "id-1");
    }

    #[test]
    fn mock_event_log_tcp() {
        let e = ke(Some(Payload::Tcp(TcpEvent {
            pid: 7,
            comm: "curl".into(),
            src_ip: "10.0.0.1".into(),
            src_port: 40000,
            dst_ip: "1.1.1.1".into(),
            dst_port: 443,
            direction: "outbound".into(),
        })));
        let log = MockEventLog::from(&e);
        assert_eq!(log.r#type, "tcp");
        assert_eq!(log.pid, 7);
        assert_eq!(log.comm, "curl");
        assert_eq!(log.detail, "10.0.0.1:40000 -> 1.1.1.1:443");
    }

    #[test]
    fn mock_event_log_file() {
        let e = ke(Some(Payload::File(FileEvent {
            pid: 9,
            comm: "python3".into(),
            path: "/etc/passwd".into(),
            op: "open".into(),
            ret: 0,
        })));
        let log = MockEventLog::from(&e);
        assert_eq!(log.r#type, "file");
        assert_eq!(log.pid, 9);
        assert_eq!(log.comm, "python3");
        assert_eq!(log.detail, "open /etc/passwd");
    }

    #[test]
    fn mock_event_log_none_is_unknown() {
        let log = MockEventLog::from(&ke(None));
        assert_eq!(log.r#type, "unknown");
        assert_eq!(log.pid, 0);
        assert!(log.comm.is_empty());
        assert!(log.detail.is_empty());
    }

    #[test]
    fn now_nanos_is_positive() {
        assert!(now_nanos() > 0);
    }

    fn pid_of(e: &KernelEvent) -> u32 {
        match &e.payload {
            Some(Payload::Exec(p)) => p.pid,
            Some(Payload::Tcp(p)) => p.pid,
            Some(Payload::File(p)) => p.pid,
            None => 0,
        }
    }

    fn kind_of(e: &KernelEvent) -> &'static str {
        match &e.payload {
            Some(Payload::Exec(_)) => "exec",
            Some(Payload::Tcp(_)) => "tcp",
            Some(Payload::File(_)) => "file",
            None => "unknown",
        }
    }

    #[tokio::test]
    async fn mock_source_emits_all_event_types_with_incrementing_pids() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let handle = tokio::spawn(mock_source(tx, 1000)); // ~1ms interval

        let mut events = Vec::new();
        for _ in 0..6 {
            events.push(rx.recv().await.expect("event"));
        }
        // Dropping rx makes the next tx.send fail, ending the task cleanly.
        drop(rx);
        let _ = handle.await;

        // All three payload variants appear within one full cycle.
        let kinds: std::collections::HashSet<_> = events.iter().map(kind_of).collect();
        assert!(kinds.contains("exec"), "exec missing: {kinds:?}");
        assert!(kinds.contains("tcp"), "tcp missing: {kinds:?}");
        assert!(kinds.contains("file"), "file missing: {kinds:?}");

        for e in &events {
            assert!(e.node_id.is_empty(), "node_id filled by grpc layer, not source");
            assert!(e.timestamp > 0);
            assert!(
                uuid::Uuid::parse_str(&e.event_id).is_ok(),
                "event_id must be a UUID: {}",
                e.event_id
            );
        }

        // pid increments by exactly 1 per event, starting at 1001.
        assert_eq!(pid_of(&events[0]), 1001);
        for w in events.windows(2) {
            assert_eq!(pid_of(&w[1]), pid_of(&w[0]) + 1);
        }
    }
}
