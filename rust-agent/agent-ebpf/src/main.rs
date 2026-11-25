#![no_std]
#![no_main]

use aya_ebpf::{
    helpers::{bpf_get_current_comm, bpf_get_current_pid_tgid, bpf_get_current_uid_gid},
    macros::{kprobe, map, tracepoint},
    maps::{LruHashMap, PerCpuArray, RingBuf},
    programs::{ProbeContext, TracePointContext},
};

const ET_EXEC: u8 = 0;
const ET_FILE: u8 = 1;
const ET_TCP: u8 = 2;

// ─── Shared kernel↔userspace structs (repr(C), fixed layout) ─────────────

#[repr(C)]
pub struct ExecEvent {
    pub event_type: u8,
    pub pid: u32,
    pub uid: u32,
    pub comm: [u8; 16],
}

#[repr(C)]
pub struct FileEvent {
    pub event_type: u8,
    pub pid: u32,
    pub uid: u32,
    pub comm: [u8; 16],
    pub path: [u8; 256],
    pub ret: i32,
    /// open(2) flags from sys_enter_openat (#419).
    pub flags: u32,
}

#[repr(C)]
pub struct TcpEvent {
    pub event_type: u8,
    /// AF_INET (2) or AF_INET6 (10). For AF_INET only the first 4 bytes of
    /// src_addr/dst_addr are used (network byte order); the rest is zero (#429).
    pub family: u8,
    pub pid: u32,
    pub comm: [u8; 16],
    pub src_addr: [u8; 16],
    pub dst_addr: [u8; 16],
    pub src_port: u16,
    pub dst_port: u16,
}

#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(512 * 1024, 0);

/// Args saved at sys_enter_openat and consumed at exit (#373, #419, #578).
///
/// `path` holds the filename as read at entry so the event reports the path the
/// syscall was entered with, not whatever the user buffer holds by exit time
/// (TOCTOU, #578). `path_ok` is 1 when that read succeeded, 0 when it failed
/// (exit then falls back to re-reading `filename_ptr`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct OpenArgs {
    pub filename_ptr: u64,
    pub flags: u64,
    pub path_ok: u64,
    pub path: [u8; 256],
}

/// pid_tgid -> OpenArgs, stored at sys_enter_openat and
/// consumed at sys_exit_openat (#373). LRU so entries orphaned by a thread
/// that dies between enter and exit are evicted instead of filling the map.
#[map]
static OPEN_ARGS: LruHashMap<u64, OpenArgs> = LruHashMap::with_max_entries(10240, 0);

/// Per-CPU scratch for building an OpenArgs at entry. The 280-byte value does
/// not fit comfortably on the 512-byte BPF stack next to the other locals, so
/// the path is probe-read straight into this map value and then copied into
/// OPEN_ARGS by the kernel (insert takes a map-value pointer, not a stack copy).
#[map]
static OPEN_SCRATCH: PerCpuArray<OpenArgs> = PerCpuArray::with_max_entries(1, 0);

// ─── Exec: sched_process_exec ─────────────────────────────────────────────
// Attached to sched/sched_process_exec, which fires only after a successful
// exec, once the task's comm has been updated to the new binary name. (At
// syscalls/sys_enter_execve comm was still the caller's, e.g. "bash", #347.)
// No tracepoint args are read, so no field offsets are assumed.

#[tracepoint(name = "sentinel_exec", category = "sched")]
pub fn sentinel_exec(ctx: TracePointContext) -> u32 {
    match try_exec(ctx) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[inline(always)]
fn try_exec(_ctx: TracePointContext) -> Result<(), i64> {
    let pid_tgid = bpf_get_current_pid_tgid();
    let pid = (pid_tgid >> 32) as u32;
    let uid = (bpf_get_current_uid_gid() & 0xffff_ffff) as u32;
    let raw_comm = bpf_get_current_comm()?;

    let mut entry = unsafe { EVENTS.reserve::<ExecEvent>(0) }.ok_or(1i64)?;
    let ev = entry.as_mut_ptr();
    unsafe {
        // reserve() returns uninitialized memory (incl. struct padding); zero
        // it before writing fields so we never submit stale kernel bytes (#71).
        core::ptr::write_bytes(ev, 0, 1);
        (*ev).event_type = ET_EXEC;
        (*ev).pid = pid;
        (*ev).uid = uid;
        for i in 0..16 {
            (*ev).comm[i] = raw_comm[i] as u8;
        }
    }
    entry.submit(0);
    Ok(())
}

// ─── File open: sys_enter_openat (stash) + sys_exit_openat (emit) ────────

#[tracepoint(name = "sentinel_openat", category = "syscalls")]
pub fn sentinel_openat(ctx: TracePointContext) -> u32 {
    match try_openat_enter(ctx) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[inline(always)]
fn try_openat_enter(ctx: TracePointContext) -> Result<(), i64> {
    use aya_ebpf::helpers::bpf_probe_read_user_str_bytes;

    // sys_enter_openat tracepoint args: dfd@16, filename ptr@24, flags@32, mode@40
    // (common fields occupy 0-7, __syscall_nr is padded to 8 bytes at 8-15).
    // Offset 16 is dfd, not the filename pointer (#169).
    let filename_ptr: u64 = unsafe { ctx.read_at(24) }.map_err(|_| 1i64)?;
    let flags: u64 = unsafe { ctx.read_at(32) }.map_err(|_| 1i64)?;
    let pid_tgid = bpf_get_current_pid_tgid();

    let args = OPEN_SCRATCH.get_ptr_mut(0).ok_or(1i64)?;
    unsafe {
        (*args).filename_ptr = filename_ptr;
        (*args).flags = flags;
        // Snapshot the path now (#578). Reading it at exit would race with
        // another thread rewriting the user buffer between enter and exit.
        // bpf_probe_read_user_str NUL-terminates on success; bytes after the
        // NUL are stale and never copied out (the exit side copies up to NUL).
        //
        // Residual window: at sys_enter the string's page may not be faulted in
        // yet, so this read can fail (the helper cannot fault pages in). Then
        // path_ok stays 0 and exit re-reads the pointer as before, which is
        // still open to the TOCTOU race. This is not closed by this change.
        (*args).path_ok = match bpf_probe_read_user_str_bytes(
            filename_ptr as *const u8,
            &mut (*args).path,
        ) {
            Ok(_) => 1,
            Err(_) => 0,
        };
    }
    OPEN_ARGS.insert(&pid_tgid, unsafe { &*args }, 0)?;
    Ok(())
}

#[tracepoint(name = "sentinel_openat_exit", category = "syscalls")]
pub fn sentinel_openat_exit(ctx: TracePointContext) -> u32 {
    match try_openat_exit(ctx) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[inline(always)]
fn try_openat_exit(ctx: TracePointContext) -> Result<(), i64> {
    let pid_tgid = bpf_get_current_pid_tgid();
    let args = match OPEN_ARGS.get_ptr(&pid_tgid) {
        Some(a) => a,
        None => return Ok(()),
    };
    // Emit first, remove after: the entry's path is read from the map value, so
    // it must not be deleted (and its slot reused) before that copy.
    let res = emit_open_event(&ctx, pid_tgid, args);
    let _ = OPEN_ARGS.remove(&pid_tgid);
    res
}

#[inline(always)]
fn emit_open_event(
    ctx: &TracePointContext,
    pid_tgid: u64,
    args: *const OpenArgs,
) -> Result<(), i64> {
    use aya_ebpf::helpers::{bpf_probe_read_kernel_str_bytes, bpf_probe_read_user_str_bytes};

    // sys_exit_openat tracepoint args: __syscall_nr@8 (int), ret@16 (long).
    // Layout per the standard syscalls exit format (common fields 0-7,
    // __syscall_nr 8-11 padded to 16, long ret at 16). Could not be confirmed
    // against /sys/kernel/tracing/.../sys_exit_openat/format (not readable
    // here); verify on a real host. ret is the fd (>= 0) or -errno.
    let ret: i64 = unsafe { ctx.read_at(16) }.map_err(|_| 1i64)?;

    let pid = (pid_tgid >> 32) as u32;
    let uid = (bpf_get_current_uid_gid() & 0xffff_ffff) as u32;
    let raw_comm = bpf_get_current_comm()?;

    let mut entry = unsafe { EVENTS.reserve::<FileEvent>(0) }.ok_or(1i64)?;
    let ev = entry.as_mut_ptr();
    unsafe {
        // reserve() returns uninitialized memory; zero it first (#71).
        core::ptr::write_bytes(ev, 0, 1);
        (*ev).event_type = ET_FILE;
        (*ev).pid = pid;
        (*ev).uid = uid;
        (*ev).ret = if ret < 0 { ret as i32 } else { 0 };
        (*ev).flags = (*args).flags as u32;
        for i in 0..16 {
            (*ev).comm[i] = raw_comm[i] as u8;
        }
        let path_slice = &mut (*ev).path;
        if (*args).path_ok == 1 {
            // Path snapshotted at entry (#578). Copies up to the NUL only, so
            // the event's zeroed tail stays zero.
            let _ = bpf_probe_read_kernel_str_bytes((*args).path.as_ptr(), path_slice);
        } else {
            // Entry read failed (page not yet faulted in): fall back to the
            // user pointer, which is still valid at exit (same syscall). This
            // path remains subject to the TOCTOU race (#578).
            let _ = bpf_probe_read_user_str_bytes((*args).filename_ptr as *const u8, path_slice);
        }
    }
    entry.submit(0);
    Ok(())
}

// ─── TCP connect: kprobe/tcp_connect ─────────────────────────────────────

#[kprobe]
pub fn sentinel_tcp_connect(ctx: ProbeContext) -> u32 {
    match try_tcp_connect(ctx) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[inline(always)]
fn try_tcp_connect(ctx: ProbeContext) -> Result<(), i64> {
    use aya_ebpf::helpers::bpf_probe_read_kernel;

    let pid_tgid = bpf_get_current_pid_tgid();
    let pid = (pid_tgid >> 32) as u32;
    let raw_comm = bpf_get_current_comm()?;

    // arg(0) = struct sock * (struct sock_common at offset 0):
    //   skc_daddr      @0  __be32 — remote (dst) IP, network byte order
    //   skc_rcv_saddr  @4  __be32 — local (src) IP, network byte order
    //   skc_dport      @12 __be16 — remote (dst) port, network byte order
    //   skc_num        @14 __u16  — local (src) port, ALREADY host byte order
    //   skc_family     @16 __u16  — address family (AF_INET = 2, AF_INET6 = 10);
    //                   follows skc_portpair (@12..16) in struct sock_common
    // Raw offsets are BTF-fragile across kernel versions — CO-RE (with
    // vmlinux.h field offsets resolved by the loader) would be more robust;
    // tracked as a follow-up, out of scope for this fix (#72).
    let sk: *const u8 = ctx.arg(0).ok_or(1i64)?;
    // tcp_v6_connect() also reaches tcp_connect(); for AF_INET6 sockets offsets
    // 0/4 are not IPv4 addresses unless the socket connects to a v4-mapped
    // address (#369, #429). Verified against net/ipv6/tcp_ipv6.c (v6.12):
    //   - tcp_v6_connect sets sk_v6_daddr = usin->sin6_addr BEFORE the
    //     IPV6_ADDR_MAPPED branch, so for a v4-mapped target sk_v6_daddr is
    //     ::ffff:a.b.c.d here, and tcp_v4_connect fills skc_daddr/skc_rcv_saddr.
    //   - For a native IPv6 connect, inet_rcv_saddr is set to LOOPBACK4_IPV6
    //     (127.0.0.6) and sk_v6_rcv_saddr is already set (inet_bhash2_update_saddr).
    //     (The 127.0.0.6 marker is on rcv_saddr @4, not daddr as first guessed in #429.)
    // skc_v6_daddr @56 / skc_v6_rcv_saddr @72 (16 bytes each) follow skc_net
    // @48 in struct sock_common. These are raw offsets like the ones above and
    // assume x86_64 with CONFIG_NET_NS and CONFIG_IPV6; CO-RE would be more robust
    // (same follow-up as #72). NOT yet verified on a real host.
    const AF_INET: u16 = 2;
    const AF_INET6: u16 = 10;
    const LOOPBACK4_IPV6_BE: u32 = u32::from_ne_bytes([127, 0, 0, 6]);
    let family: u16 =
        unsafe { bpf_probe_read_kernel(sk.add(16) as *const u16) }.map_err(|_| 1i64)?;
    if family != AF_INET && family != AF_INET6 {
        return Ok(());
    }
    let src_ip: u32 =
        unsafe { bpf_probe_read_kernel(sk.add(4) as *const u32) }.map_err(|_| 1i64)?;
    let dst_ip: u32 = unsafe { bpf_probe_read_kernel(sk as *const u32) }.map_err(|_| 1i64)?;
    let mut src_addr = [0u8; 16];
    let mut dst_addr = [0u8; 16];
    let mut out_family = AF_INET as u8;
    // Native IPv6 only when rcv_saddr is the 127.0.0.6 marker AND sk_v6_daddr is
    // not v4-mapped; every other AF_INET6 socket is an IPv4 connection.
    if family == AF_INET6 && src_ip == LOOPBACK4_IPV6_BE {
        let v6_dst: [u8; 16] =
            unsafe { bpf_probe_read_kernel(sk.add(56) as *const [u8; 16]) }.map_err(|_| 1i64)?;
        let mapped = v6_dst[..10].iter().all(|b| *b == 0) && v6_dst[10] == 0xff && v6_dst[11] == 0xff;
        if !mapped {
            let v6_src: [u8; 16] =
                unsafe { bpf_probe_read_kernel(sk.add(72) as *const [u8; 16]) }
                    .map_err(|_| 1i64)?;
            src_addr = v6_src;
            dst_addr = v6_dst;
            out_family = AF_INET6 as u8;
        }
    }
    if out_family == AF_INET as u8 {
        src_addr[..4].copy_from_slice(&src_ip.to_ne_bytes());
        dst_addr[..4].copy_from_slice(&dst_ip.to_ne_bytes());
    }
    let src_port: u16 =
        unsafe { bpf_probe_read_kernel(sk.add(14) as *const u16) }.map_err(|_| 1i64)?;
    let dst_port_be: u16 =
        unsafe { bpf_probe_read_kernel(sk.add(12) as *const u16) }.map_err(|_| 1i64)?;
    // skc_dport is network byte order (__be16) — convert here so userspace
    // (and src_port, which is already host order) don't need to know which
    // of the two ports needs swapping (#72).
    let dst_port = u16::from_be(dst_port_be);

    let mut entry = unsafe { EVENTS.reserve::<TcpEvent>(0) }.ok_or(1i64)?;
    let ev = entry.as_mut_ptr();
    unsafe {
        // reserve() returns uninitialized memory (incl. struct padding); zero
        // it before writing fields so we never submit stale kernel bytes (#71).
        core::ptr::write_bytes(ev, 0, 1);
        (*ev).event_type = ET_TCP;
        (*ev).pid = pid;
        (*ev).family = out_family;
        (*ev).src_addr = src_addr;
        (*ev).dst_addr = dst_addr;
        (*ev).src_port = src_port;
        (*ev).dst_port = dst_port;
        for i in 0..16 {
            (*ev).comm[i] = raw_comm[i] as u8;
        }
    }
    entry.submit(0);
    Ok(())
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
