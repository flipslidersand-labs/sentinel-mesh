use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::{error::TrySendError, Sender};
use uuid::Uuid;

use crate::pb::{kernel_event::Payload, EventType, ExecEvent, FileEvent, KernelEvent, TcpEvent};

// Mock pools for realistic-looking events
static MOCK_IPS: &[&str] = &[
    "10.0.0.1",
    "10.0.0.2",
    "172.16.0.10",
    "192.168.1.100",
    "8.8.8.8",
    "1.1.1.1",
];
static MOCK_PORTS: &[(u32, &str)] = &[
    (80, "nginx"),
    (443, "curl"),
    (5432, "psql"),
    (6379, "redis-cli"),
    (8080, "app"),
];
static MOCK_PATHS: &[(&str, &str)] = &[
    ("/etc/passwd", "open"),
    ("/var/log/syslog", "read"),
    ("/tmp/data.tmp", "write"),
    ("/home/user/.bashrc", "open"),
    ("/proc/meminfo", "read"),
];
static MOCK_COMMS: &[&str] = &["bash", "python3", "curl", "systemd", "sshd", "nginx"];

/// Minimum gap between "events dropped" warnings, so a long outage does not
/// flood the log (#282).
const DROP_REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// Throttled cumulative counter: the first event is reported immediately;
/// later ones are batched and reported at most once per `interval`. Reports
/// are `(since_last, total)`. Shared by `DropTracker` and the gRPC reject
/// tracker.
pub(crate) struct ThrottledCounter {
    total: u64,
    pub(crate) unreported: u64,
    last_report: Option<Instant>,
    interval: Duration,
}

impl ThrottledCounter {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            total: 0,
            unreported: 0,
            last_report: None,
            interval,
        }
    }

    /// Record one event; returns `(since_last, total)` when it is time to log.
    pub(crate) fn record(&mut self, now: Instant) -> Option<(u64, u64)> {
        self.total += 1;
        self.unreported += 1;
        self.report_if_due(now)
    }

    /// Report if events are unreported and the interval has elapsed.
    fn report_if_due(&mut self, now: Instant) -> Option<(u64, u64)> {
        if self.unreported == 0 {
            return None;
        }
        let due = match self.last_report {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= self.interval,
        };
        if due {
            self.flush(now)
        } else {
            None
        }
    }

    /// Report anything not yet logged (e.g. when the source stops).
    pub(crate) fn flush(&mut self, now: Instant) -> Option<(u64, u64)> {
        if self.unreported == 0 {
            return None;
        }
        let r = (self.unreported, self.total);
        self.unreported = 0;
        self.last_report = Some(now);
        Some(r)
    }
}

/// Counts events dropped because the channel was full. `total` is cumulative
/// across reconnects (the tracker lives as long as the source task).
struct DropTracker {
    /// Mirror of `total`, readable from outside the task (tests, metrics).
    counter: Arc<AtomicU64>,
    core: ThrottledCounter,
}

/// What to log: drops since the previous report, and the cumulative total.
#[derive(Debug, PartialEq, Eq)]
struct DropReport {
    since_last: u64,
    total: u64,
}

fn to_drop_report(r: Option<(u64, u64)>) -> Option<DropReport> {
    r.map(|(since_last, total)| DropReport { since_last, total })
}

impl DropTracker {
    #[cfg(test)]
    fn new(interval: Duration) -> Self {
        Self::with_counter(interval, Arc::new(AtomicU64::new(0)))
    }

    fn with_counter(interval: Duration, counter: Arc<AtomicU64>) -> Self {
        Self {
            counter,
            core: ThrottledCounter::new(interval),
        }
    }

    /// Record one dropped event; returns a report when it is time to log.
    fn record(&mut self, now: Instant) -> Option<DropReport> {
        self.counter.fetch_add(1, Ordering::Relaxed);
        to_drop_report(self.core.record(now))
    }

    /// Call after a successful send: if drops are still unreported and the
    /// interval has elapsed, report them so they never stay unlogged after
    /// the channel recovers.
    fn on_success(&mut self, now: Instant) -> Option<DropReport> {
        to_drop_report(self.core.report_if_due(now))
    }

    /// Report any drops not yet logged (e.g. when the source stops).
    fn flush(&mut self, now: Instant) -> Option<DropReport> {
        to_drop_report(self.core.flush(now))
    }
}

/// Non-blocking send. Returns `false` when the receiver is gone (Closed).
/// A Full channel drops the event, counts it and logs (throttled).
fn try_forward(tx: &Sender<KernelEvent>, event: KernelEvent, drops: &mut DropTracker) -> bool {
    try_forward_at(tx, event, drops, Instant::now(), &mut log_drops)
}

/// `try_forward` with an injectable clock and report sink (for tests).
fn try_forward_at(
    tx: &Sender<KernelEvent>,
    event: KernelEvent,
    drops: &mut DropTracker,
    now: Instant,
    sink: &mut impl FnMut(&DropReport),
) -> bool {
    match tx.try_send(event) {
        Ok(()) => {
            if let Some(r) = drops.on_success(now) {
                sink(&r);
            }
            true
        }
        Err(TrySendError::Full(_)) => {
            if let Some(r) = drops.record(now) {
                sink(&r);
            }
            true
        }
        Err(TrySendError::Closed(_)) => {
            if let Some(r) = drops.flush(now) {
                sink(&r);
            }
            false
        }
    }
}

fn log_drops(r: &DropReport) {
    eprintln!(
        "warn: agent→collector channel full, dropped {} event(s) since last report ({} total)",
        r.since_last, r.total
    );
}

/// Mock event generator — cycles Exec / TCP / File to exercise all event types.
pub async fn mock_source(tx: Sender<KernelEvent>, rate: u64) {
    mock_source_counted(tx, rate, mock_dropped_counter()).await
}

/// Process-wide count of events `mock_source` dropped on a full channel
/// (shared by all `mock_source` tasks; also lets tests observe that
/// `mock_source` itself goes through `DropTracker`).
fn mock_dropped_counter() -> Arc<AtomicU64> {
    static COUNTER: std::sync::OnceLock<Arc<AtomicU64>> = std::sync::OnceLock::new();
    COUNTER.get_or_init(|| Arc::new(AtomicU64::new(0))).clone()
}

/// Same as `mock_source`, exposing the number of events dropped on a full
/// channel through `dropped`.
async fn mock_source_counted(tx: Sender<KernelEvent>, rate: u64, dropped: Arc<AtomicU64>) {
    let interval = Duration::from_millis(1000 / rate.max(1));
    let mut ticker = tokio::time::interval(interval);
    let mut counter: u64 = 0;
    let mut pid: u32 = 1000;
    let mut drops = DropTracker::with_counter(DROP_REPORT_INTERVAL, dropped);

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

        println!("{event:?}");

        // try_send (not send().await): while stream_to_collector is
        // reconnecting (#73), the channel can fill up — dropping new mock
        // events here is preferable to blocking this generator indefinitely.
        // Full drops are counted and logged (throttled), see DropTracker.
        if !try_forward(&tx, event, &mut drops) {
            break;
        }
    }
}

#[cfg(feature = "ebpf")]
pub async fn ebpf_source(tx: Sender<KernelEvent>) {
    use aya::{include_bytes_aligned, maps::RingBuf, programs::TracePoint, Ebpf};

    let bytes = include_bytes_aligned!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../target/bpfel-unknown-none/debug/sentinel-agent-ebpf"
    ));

    let mut bpf = match Ebpf::load(bytes) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("failed to load eBPF: {e}");
            return;
        }
    };

    // Attach exec tracepoint (sched_process_exec). This program is required —
    // without it there is nothing left to collect, so any failure here
    // (missing BPF object section, permission denial, unsupported kernel,
    // LSM rejection, ...) is fatal for this source and we return instead of
    // panicking the whole agent process.
    let exec_prog = match bpf.program_mut("sentinel_exec") {
        Some(prog) => prog,
        None => {
            eprintln!("error: eBPF object has no \"sentinel_exec\" program");
            return;
        }
    };
    let exec_prog: &mut TracePoint = match exec_prog.try_into() {
        Ok(tp) => tp,
        Err(e) => {
            eprintln!("error: \"sentinel_exec\" is not a tracepoint program: {e}");
            return;
        }
    };
    if let Err(e) = exec_prog.load() {
        eprintln!("error: failed to load sentinel_exec tracepoint: {e}");
        return;
    }
    if let Err(e) = exec_prog.attach("sched", "sched_process_exec") {
        eprintln!("error: failed to attach sentinel_exec to sched_process_exec: {e}");
        return;
    }

    // Attach openat tracepoints — best-effort: optional, a failure here should
    // not prevent exec/tcp collection. The exit program emits the event (with
    // ret), so the enter program (which stashes the filename pointer) is only
    // attached once the exit program is attached (#373).
    let mut openat_exit_ok = false;
    if let Some(prog) = bpf.program_mut("sentinel_openat_exit") {
        match TryInto::<&mut TracePoint>::try_into(prog) {
            Ok(tp) => match tp
                .load()
                .and_then(|_| tp.attach("syscalls", "sys_exit_openat"))
            {
                Ok(_) => openat_exit_ok = true,
                Err(e) => eprintln!("warn: openat exit tracepoint: {e}"),
            },
            Err(e) => eprintln!("warn: \"sentinel_openat_exit\" is not a tracepoint program: {e}"),
        }
    }
    if openat_exit_ok {
        if let Some(prog) = bpf.program_mut("sentinel_openat") {
            match TryInto::<&mut TracePoint>::try_into(prog) {
                Ok(tp) => {
                    if let Err(e) = tp
                        .load()
                        .and_then(|_| tp.attach("syscalls", "sys_enter_openat"))
                    {
                        eprintln!(
                            "warn: openat enter tracepoint failed, FILE events are disabled: {e}"
                        );
                    }
                }
                Err(e) => eprintln!("warn: \"sentinel_openat\" is not a tracepoint program: {e}"),
            }
        }
    }

    // Attach tcp_connect kprobe — best-effort, same rationale as openat above.
    if let Some(prog) = bpf.program_mut("sentinel_tcp_connect") {
        use aya::programs::KProbe;
        match TryInto::<&mut KProbe>::try_into(prog) {
            Ok(kp) => {
                if let Err(e) = kp.load().and_then(|_| kp.attach("tcp_connect", 0)) {
                    eprintln!("warn: tcp_connect kprobe: {e}");
                }
            }
            Err(e) => eprintln!("warn: \"sentinel_tcp_connect\" is not a kprobe program: {e}"),
        }
    }

    let events_map = match bpf.map_mut("EVENTS") {
        Some(m) => m,
        None => {
            eprintln!("error: eBPF object has no \"EVENTS\" map");
            return;
        }
    };
    let ring_buf = match RingBuf::try_from(events_map) {
        Ok(rb) => rb,
        Err(e) => {
            eprintln!("error: \"EVENTS\" map is not a ring buffer: {e}");
            return;
        }
    };
    // SAFETY: the map fd is owned by `bpf`; ring_buf (and so async_fd) borrows
    // it, so `bpf` cannot drop before async_fd and the fd stays open and
    // unchanged while the AsyncFd lives. Dropping AsyncFd only deregisters.
    let mut async_fd = match unsafe { tokio::io::unix::AsyncFd::register(ring_buf) } {
        Ok(fd) => fd,
        Err(e) => {
            eprintln!("error: failed to register EVENTS ring buffer with the async runtime: {e}");
            return;
        }
    };

    // Events dropped because the channel to grpc::stream_to_collector was
    // full (collector unreachable/slow) are counted and logged, throttled
    // (#70, #285).
    let mut drops = DropTracker::with_counter(DROP_REPORT_INTERVAL, Arc::new(AtomicU64::new(0)));

    loop {
        let mut guard = match async_fd.readable_mut().await {
            Ok(g) => g,
            Err(e) => {
                // The ring buffer fd itself failed (e.g. the underlying epoll
                // registration broke). There is nothing to recover from here,
                // so log and end this source rather than looping forever or
                // panicking the whole agent process.
                eprintln!("error: EVENTS ring buffer became unreadable, stopping eBPF source: {e}");
                return;
            }
        };
        let rb = guard.get_inner_mut();
        while let Some(item) = rb.next() {
            let data: &[u8] = &item;
            if data.is_empty() {
                continue;
            }
            let kernel_event = match data[0] {
                // read_unaligned copies the bytes out rather than casting `&[u8]`
                // (alignment=1) directly to a `&EbpfXxxEvent` reference
                // (alignment>=4) — that cast is UB in Rust regardless of the
                // actual runtime address (#69). An exact length match (not
                // `>=`) ensures the ring buffer record is really this struct,
                // not a same-tag record of a different size.
                0 if data.len() == core::mem::size_of::<EbpfExecEvent>() => {
                    let raw =
                        unsafe { core::ptr::read_unaligned(data.as_ptr() as *const EbpfExecEvent) };
                    // Userspace enrichment (#432); falls back to 0 / empty (cwd via /proc/<pid>/cwd, #422).
                    let (ppid, cmdline, cwd) = read_proc_exec_info(Path::new("/proc"), raw.pid);
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::Exec as i32,
                        payload: Some(Payload::Exec(ExecEvent {
                            pid: raw.pid,
                            ppid,
                            comm: bytes_to_str(&raw.comm),
                            cmdline,
                            cwd,
                            uid: raw.uid,
                        })),
                    }
                }
                1 if data.len() == core::mem::size_of::<EbpfFileEvent>() => {
                    let raw =
                        unsafe { core::ptr::read_unaligned(data.as_ptr() as *const EbpfFileEvent) };
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::File as i32,
                        payload: Some(Payload::File(FileEvent {
                            pid: raw.pid,
                            comm: bytes_to_str(&raw.comm),
                            path: bytes_to_str(&raw.path),
                            op: open_flags_to_op(raw.flags).into(),
                            ret: raw.ret,
                        })),
                    }
                }
                2 if data.len() == core::mem::size_of::<EbpfTcpEvent>() => {
                    let raw =
                        unsafe { core::ptr::read_unaligned(data.as_ptr() as *const EbpfTcpEvent) };
                    KernelEvent {
                        event_id: Uuid::new_v4().to_string(),
                        node_id: String::new(),
                        timestamp: now_nanos(),
                        r#type: EventType::Tcp as i32,
                        payload: Some(Payload::Tcp(TcpEvent {
                            pid: raw.pid,
                            comm: bytes_to_str(&raw.comm),
                            src_ip: tcp_addr_to_string(raw.family, &raw.src_addr),
                            src_port: raw.src_port as u32,
                            dst_ip: tcp_addr_to_string(raw.family, &raw.dst_addr),
                            dst_port: raw.dst_port as u32,
                            direction: "outbound".into(),
                        })),
                    }
                }
                _ => continue,
            };
            // try_send (not send().await): blocking here when the channel is
            // full would stall this drain loop, which in turn stalls draining
            // the kernel ring buffer — a full ring buffer makes the kernel
            // silently drop exec/connect events at the source (#70). Prefer
            // dropping downstream (counted/logged) over blocking upstream.
            if !try_forward(&tx, kernel_event, &mut drops) {
                // Receiver gone: nothing left to forward to.
                return;
            }
        }
        guard.clear_ready();
    }
}

#[cfg(feature = "ebpf")]
fn bytes_to_str(b: &[u8]) -> String {
    String::from_utf8_lossy(b)
        .trim_end_matches('\0')
        .to_string()
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

/// Maps open(2) flags to an op string (#419). O_CREAT -> "create"; any
/// write intent (O_WRONLY, O_RDWR, O_TRUNC, O_APPEND) -> "write"; else "open".
/// Constants are the Linux values (identical on x86_64 and aarch64).
#[cfg_attr(not(any(feature = "ebpf", test)), allow(dead_code))]
fn open_flags_to_op(flags: u32) -> &'static str {
    const O_ACCMODE: u32 = 0o3;
    const O_CREAT: u32 = 0o100;
    const O_TRUNC: u32 = 0o1000;
    const O_APPEND: u32 = 0o2000;
    if flags & O_CREAT != 0 {
        "create"
    } else if flags & O_ACCMODE != 0 || flags & (O_TRUNC | O_APPEND) != 0 {
        "write"
    } else {
        "open"
    }
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
    flags: u32,
}

#[cfg(feature = "ebpf")]
#[repr(C)]
struct EbpfTcpEvent {
    event_type: u8,
    family: u8,
    pid: u32,
    comm: [u8; 16],
    src_addr: [u8; 16],
    dst_addr: [u8; 16],
    src_port: u16,
    dst_port: u16,
}

/// Formats an address from the eBPF TcpEvent: AF_INET (2) uses the first 4
/// bytes, AF_INET6 (10) all 16 (both in network byte order) (#429).
#[cfg(any(feature = "ebpf", test))]
fn tcp_addr_to_string(family: u8, addr: &[u8; 16]) -> String {
    use std::net::{Ipv4Addr, Ipv6Addr};
    if family == 10 {
        Ipv6Addr::from(*addr).to_string()
    } else {
        Ipv4Addr::new(addr[0], addr[1], addr[2], addr[3]).to_string()
    }
}

fn now_nanos() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Max bytes of `ExecEvent.cmdline`. Must not exceed the collector's
/// `maxRegisterFieldLength` (go-collector/internal/receiver/grpc.go), which
/// rejects longer values (the whole event is lost).
#[cfg_attr(not(feature = "ebpf"), allow(dead_code))]
const CMDLINE_MAX_BYTES: usize = 256;

/// Extract the parent pid from the contents of /proc/<pid>/status.
#[cfg_attr(not(feature = "ebpf"), allow(dead_code))]
fn parse_ppid(status: &str) -> Option<u32> {
    status
        .lines()
        .find_map(|l| l.strip_prefix("PPid:"))
        .and_then(|v| v.trim().parse().ok())
}

/// Turn raw /proc/<pid>/cmdline (NUL-separated argv) into a space-joined
/// string that the collector always accepts: every byte outside printable
/// ASCII 0x20-0x7E (tabs, newlines, non-ASCII, invalid UTF-8) becomes '?',
/// then the result is capped at `CMDLINE_MAX_BYTES` (ASCII-only, so cutting
/// at a byte boundary is safe).
#[cfg_attr(not(feature = "ebpf"), allow(dead_code))]
fn parse_cmdline(raw: &[u8]) -> String {
    let raw = raw.strip_suffix(&[0]).unwrap_or(raw);
    raw.iter()
        .take(CMDLINE_MAX_BYTES)
        .map(|&b| match b {
            0 => ' ',
            0x20..=0x7e => b as char,
            _ => '?',
        })
        .collect()
}

/// Read (ppid, cmdline, cwd) for `pid` under `proc_root` (normally /proc).
///
/// Known limitation: this runs after the kernel event is queued, so a very
/// short-lived process may already be gone (or unreadable); then ppid is 0
/// and cmdline/cwd are empty. Capturing these in the kernel (CO-RE) would avoid
/// the race but needs real-hardware verification. The two small std::fs
/// reads of /proc are cheap and never fail the event.
#[cfg_attr(not(feature = "ebpf"), allow(dead_code))]
fn read_proc_exec_info(proc_root: &Path, pid: u32) -> (u32, String, String) {
    let dir = proc_root.join(pid.to_string());
    let ppid = std::fs::read_to_string(dir.join("status"))
        .ok()
        .and_then(|s| parse_ppid(&s))
        .unwrap_or(0);
    let cmdline = std::fs::read(dir.join("cmdline"))
        .map(|b| parse_cmdline(&b))
        .unwrap_or_default();
    let cwd = std::fs::read_link(dir.join("cwd"))
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    (ppid, cmdline, cwd)
}

#[cfg(test)]
mod tests {
    #[test]
    fn tcp_addr_to_string_formats_v4_and_v6() {
        use super::tcp_addr_to_string;
        let mut a = [0u8; 16];
        a[..4].copy_from_slice(&[192, 0, 2, 1]);
        assert_eq!(tcp_addr_to_string(2, &a), "192.0.2.1");
        let v6: [u8; 16] = [0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(tcp_addr_to_string(10, &v6), "2001:db8::1");
    }

    #[test]
    fn tcp_addr_to_string_v4_mapped_is_emitted_as_ipv4() {
        use super::tcp_addr_to_string;
        // The eBPF side reports family=AF_INET with the IPv4 in the first 4 bytes
        // for a v4-mapped connect on an AF_INET6 socket.
        let mut a = [0u8; 16];
        a[..4].copy_from_slice(&[192, 0, 2, 33]);
        assert_eq!(tcp_addr_to_string(2, &a), "192.0.2.33");
    }

    #[test]
    fn tcp_addr_to_string_unknown_family_falls_back_to_ipv4() {
        use super::tcp_addr_to_string;
        // Any family other than AF_INET6 (10) is read as IPv4 from the first 4 bytes
        // (the eBPF side never emits other families).
        let mut a = [0u8; 16];
        a[..4].copy_from_slice(&[192, 0, 2, 7]);
        assert_eq!(tcp_addr_to_string(99, &a), "192.0.2.7");
        assert_eq!(tcp_addr_to_string(0, &[0u8; 16]), "0.0.0.0");
    }

    use super::*;
    use crate::pb::{kernel_event::Payload, KernelEvent};

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
    fn open_flags_map_to_op() {
        assert_eq!(open_flags_to_op(0), "open"); // O_RDONLY
        assert_eq!(open_flags_to_op(0o2000000), "open"); // O_RDONLY|O_CLOEXEC
        assert_eq!(open_flags_to_op(0o1), "write"); // O_WRONLY
        assert_eq!(open_flags_to_op(0o2), "write"); // O_RDWR
        assert_eq!(open_flags_to_op(0o2000), "write"); // O_APPEND
        assert_eq!(open_flags_to_op(0o1000), "write"); // O_TRUNC
        assert_eq!(open_flags_to_op(0o101), "create"); // O_WRONLY|O_CREAT
        assert_eq!(open_flags_to_op(0o100), "create"); // O_CREAT
    }

    #[test]
    fn now_nanos_is_positive() {
        assert!(now_nanos() > 0);
    }

    #[cfg(feature = "ebpf")]
    #[test]
    fn bytes_to_str_preserves_non_utf8_content() {
        // 0xFF is not valid UTF-8 on its own; a naive from_utf8().unwrap_or("")
        // would silently drop the whole string instead of keeping the rest.
        let raw = b"evil\xffname\0\0\0";
        let s = bytes_to_str(raw);
        assert!(!s.is_empty());
        assert!(s.starts_with("evil"));
        assert!(s.ends_with("name"));
        assert!(s.contains('\u{FFFD}'));
    }

    #[cfg(feature = "ebpf")]
    #[test]
    fn bytes_to_str_trims_nul_terminator() {
        let raw = b"bash\0\0\0\0\0\0\0\0\0\0\0\0";
        assert_eq!(bytes_to_str(raw), "bash");
    }

    fn plain_event() -> KernelEvent {
        ke(None)
    }

    #[test]
    fn drop_tracker_reports_first_then_throttles() {
        let t0 = Instant::now();
        let mut d = DropTracker::new(Duration::from_secs(10));
        assert_eq!(
            d.record(t0),
            Some(DropReport {
                since_last: 1,
                total: 1
            })
        );
        // Within the interval: counted but not reported.
        assert_eq!(d.record(t0 + Duration::from_secs(1)), None);
        assert_eq!(d.record(t0 + Duration::from_secs(9)), None);
        assert_eq!(d.core.total, 3);
        // Boundary: exactly at the interval reports the batch with the total.
        assert_eq!(
            d.record(t0 + Duration::from_secs(10)),
            Some(DropReport {
                since_last: 3,
                total: 4
            })
        );
        // Next window starts from the last report.
        assert_eq!(d.record(t0 + Duration::from_secs(19)), None);
        assert_eq!(
            d.record(t0 + Duration::from_secs(20)),
            Some(DropReport {
                since_last: 2,
                total: 6
            })
        );
    }

    #[test]
    fn drop_tracker_success_flushes_pending_after_interval() {
        let t0 = Instant::now();
        let mut d = DropTracker::new(Duration::from_secs(10));
        assert!(d.record(t0).is_some());
        assert!(d.record(t0 + Duration::from_secs(1)).is_none());
        // Recovered: success within the interval keeps it pending...
        assert_eq!(d.on_success(t0 + Duration::from_secs(5)), None);
        // ...but once the interval passed, the next success reports it.
        assert_eq!(
            d.on_success(t0 + Duration::from_secs(10)),
            Some(DropReport {
                since_last: 1,
                total: 2
            })
        );
        assert_eq!(d.on_success(t0 + Duration::from_secs(30)), None);
    }

    #[tokio::test]
    async fn mock_source_counts_drops_on_full_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(1); // never drained
        let dropped = Arc::new(AtomicU64::new(0));
        let handle = tokio::spawn(mock_source_counted(tx, 1000, dropped.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while dropped.load(Ordering::Relaxed) < 3 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("mock_source must count drops on a full channel");
        drop(rx);
        let _ = handle.await;
    }

    #[test]
    fn drop_tracker_flush_reports_pending_only() {
        let t0 = Instant::now();
        let mut d = DropTracker::new(Duration::from_secs(10));
        assert_eq!(d.flush(t0), None);
        d.record(t0);
        d.record(t0);
        assert_eq!(
            d.flush(t0),
            Some(DropReport {
                since_last: 1,
                total: 2
            })
        );
        assert_eq!(d.flush(t0), None);
    }

    #[tokio::test]
    async fn try_forward_counts_drops_on_full_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut d = DropTracker::new(Duration::from_secs(3600));
        assert!(try_forward(&tx, plain_event(), &mut d)); // fills the channel
        for _ in 0..5 {
            assert!(try_forward(&tx, plain_event(), &mut d)); // Full: dropped
        }
        assert_eq!(d.core.total, 5);
        // Draining then sending works again; count is cumulative.
        rx.recv().await.unwrap();
        assert!(try_forward(&tx, plain_event(), &mut d));
        assert_eq!(d.core.total, 5);
    }

    #[tokio::test]
    async fn try_forward_closed_returns_false_and_flushes() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut d = DropTracker::new(Duration::from_secs(3600));
        assert!(try_forward(&tx, plain_event(), &mut d));
        assert!(try_forward(&tx, plain_event(), &mut d)); // Full, reported
        assert!(try_forward(&tx, plain_event(), &mut d)); // Full, pending
        assert_eq!(d.core.unreported, 1);
        drop(rx);
        assert!(!try_forward(&tx, plain_event(), &mut d));
        assert_eq!(d.core.unreported, 0);
        assert_eq!(d.core.total, 2); // Closed is not counted as a drop
    }

    #[test]
    fn try_forward_success_reports_pending_drops_after_interval() {
        let t0 = Instant::now();
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut d = DropTracker::new(Duration::from_secs(10));
        let mut reports: Vec<DropReport> = Vec::new();
        let mut sink = |r: &DropReport| {
            reports.push(DropReport {
                since_last: r.since_last,
                total: r.total,
            })
        };
        assert!(try_forward_at(&tx, plain_event(), &mut d, t0, &mut sink)); // fills
        assert!(try_forward_at(&tx, plain_event(), &mut d, t0, &mut sink)); // Full, reported
        assert!(try_forward_at(
            &tx,
            plain_event(),
            &mut d,
            t0 + Duration::from_secs(1),
            &mut sink
        )); // Full, pending
        rx.try_recv().unwrap();
        // Success inside the interval: still pending.
        assert!(try_forward_at(
            &tx,
            plain_event(),
            &mut d,
            t0 + Duration::from_secs(5),
            &mut sink
        ));
        rx.try_recv().unwrap();
        // Success after the interval: pending drop is reported by on_success.
        assert!(try_forward_at(
            &tx,
            plain_event(),
            &mut d,
            t0 + Duration::from_secs(10),
            &mut sink
        ));
        assert_eq!(
            reports,
            vec![
                DropReport {
                    since_last: 1,
                    total: 1
                },
                DropReport {
                    since_last: 1,
                    total: 2
                },
            ]
        );
    }

    #[tokio::test]
    async fn mock_source_does_not_block_on_full_channel() {
        // Never-drained channel of 1: a blocking send would park the task on
        // the 2nd event, so after the pause the next event would be pid 1002.
        // With try_send the generator keeps running and drops in between.
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let before = mock_dropped_counter().load(Ordering::Relaxed);
        let handle = tokio::spawn(mock_source(tx, 1000));
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            mock_dropped_counter().load(Ordering::Relaxed) > before,
            "drops via mock_source must be counted by DropTracker"
        );
        let first = rx.recv().await.unwrap();
        assert_eq!(pid_of(&first), 1001);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let next = rx.recv().await.unwrap();
        assert!(
            pid_of(&next) > 1010,
            "mock_source must keep generating (dropping) while full, got pid {}",
            pid_of(&next)
        );
        drop(rx);
        let _ = handle.await;
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
            assert!(
                e.node_id.is_empty(),
                "node_id filled by grpc layer, not source"
            );
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

    #[test]
    fn parse_ppid_cases() {
        assert_eq!(parse_ppid("Name:\tbash\nPPid:\t41\nUid:\t0\n"), Some(41));
        assert_eq!(parse_ppid(""), None);
        assert_eq!(parse_ppid("Name:\tbash\n"), None);
        assert_eq!(parse_ppid("PPid:\tabc\n"), None);
    }

    #[test]
    fn parse_cmdline_cases() {
        assert_eq!(parse_cmdline(b"/bin/ls\0-l\0/tmp\0"), "/bin/ls -l /tmp");
        assert_eq!(parse_cmdline(b"/bin/ls"), "/bin/ls");
        assert_eq!(parse_cmdline(b""), "");
        assert_eq!(parse_cmdline(b"a\0\xff\0"), "a ?");
        assert_eq!(parse_cmdline(b"a\tb\nc"), "a?b?c");
    }

    #[test]
    fn parse_cmdline_always_collector_safe() {
        let long = vec![b'x'; 5000];
        let inputs: Vec<Vec<u8>> = vec![
            b"/bin/ls\0-l\0".to_vec(),
            long,
            "caf\u{e9}\0\u{65e5}\u{672c}".as_bytes().to_vec(),
            b"a\tb\nc\r\x7f\x1b[0m".to_vec(),
            vec![0xff, 0xfe, 0x80, 0, 0xc3],
            b"\0\0\0".to_vec(),
        ];
        for raw in inputs {
            let out = parse_cmdline(&raw);
            assert!(out.len() <= 256, "too long: {}", out.len());
            assert!(out.bytes().all(|b| (0x20..=0x7e).contains(&b)), "{out:?}");
        }
        assert_eq!(parse_cmdline(&vec![b'x'; 5000]).len(), 256);
    }

    #[test]
    fn read_proc_exec_info_fake_proc() {
        let root = std::env::temp_dir().join(format!("sentinel-proc-test-{}", Uuid::new_v4()));
        let dir = root.join("1234");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("status"), "Name:\tx\nPPid:\t77\n").unwrap();
        std::fs::write(dir.join("cmdline"), b"/usr/bin/x\0--flag\0").unwrap();
        // cwd is a symlink in /proc/<pid>/cwd
        #[cfg(unix)]
        std::os::unix::fs::symlink("/srv/work", dir.join("cwd")).unwrap();
        #[cfg(unix)]
        assert_eq!(
            read_proc_exec_info(&root, 1234),
            (77, "/usr/bin/x --flag".to_string(), "/srv/work".to_string())
        );
        // missing pid falls back without error
        assert_eq!(
            read_proc_exec_info(&root, 9999),
            (0, String::new(), String::new())
        );
        // unreadable cwd (no symlink) leaves cwd empty
        let dir2 = root.join("4321");
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir2.join("status"), "PPid:\t5\n").unwrap();
        assert_eq!(
            read_proc_exec_info(&root, 4321),
            (5, String::new(), String::new())
        );
        std::fs::remove_dir_all(&root).unwrap();
    }
}
