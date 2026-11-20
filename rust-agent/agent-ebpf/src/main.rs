#![no_std]
#![no_main]

use aya_ebpf::{
    helpers::{bpf_get_current_comm, bpf_get_current_pid_tgid, bpf_get_current_uid_gid},
    macros::{kprobe, map, tracepoint},
    maps::{LruHashMap, RingBuf},
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
}

#[repr(C)]
pub struct TcpEvent {
    pub event_type: u8,
    pub pid: u32,
    pub comm: [u8; 16],
    pub src_ip: u32,
    pub dst_ip: u32,
    pub src_port: u16,
    pub dst_port: u16,
}

#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(512 * 1024, 0);

/// pid_tgid -> userspace filename pointer, stored at sys_enter_openat and
/// consumed at sys_exit_openat (#373). LRU so entries orphaned by a thread
/// that dies between enter and exit are evicted instead of filling the map.
#[map]
static OPEN_ARGS: LruHashMap<u64, u64> = LruHashMap::with_max_entries(10240, 0);

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
    // sys_enter_openat tracepoint args: dfd@16, filename ptr@24, flags@32, mode@40
    // (common fields occupy 0-7, __syscall_nr is padded to 8 bytes at 8-15).
    // Offset 16 is dfd, not the filename pointer (#169).
    let filename_ptr: u64 = unsafe { ctx.read_at(24) }.map_err(|_| 1i64)?;
    let pid_tgid = bpf_get_current_pid_tgid();
    OPEN_ARGS.insert(&pid_tgid, &filename_ptr, 0)?;
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
    use aya_ebpf::helpers::bpf_probe_read_user_str_bytes;

    let pid_tgid = bpf_get_current_pid_tgid();
    let filename_ptr: u64 = match unsafe { OPEN_ARGS.get(&pid_tgid) } {
        Some(p) => *p,
        None => return Ok(()),
    };
    let _ = OPEN_ARGS.remove(&pid_tgid);

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
        for i in 0..16 {
            (*ev).comm[i] = raw_comm[i] as u8;
        }
        // The user pointer is still valid at exit time (same syscall).
        let path_slice = &mut (*ev).path;
        let _ = bpf_probe_read_user_str_bytes(filename_ptr as *const u8, path_slice);
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
    // 0/4 are not IPv4 addresses. Only IPv4 is supported — drop everything else
    // instead of recording bogus addresses (#369).
    // Known limitation: a dual-stack AF_INET6 socket connecting to a v4-mapped
    // address (tcp_v6_connect -> tcp_v4_connect) keeps sk_family = AF_INET6 and is
    // dropped here too, although offsets 0/4 hold a valid IPv4 pair; see #429.
    const AF_INET: u16 = 2;
    let family: u16 =
        unsafe { bpf_probe_read_kernel(sk.add(16) as *const u16) }.map_err(|_| 1i64)?;
    if family != AF_INET {
        return Ok(());
    }
    let src_ip: u32 =
        unsafe { bpf_probe_read_kernel(sk.add(4) as *const u32) }.map_err(|_| 1i64)?;
    let dst_ip: u32 = unsafe { bpf_probe_read_kernel(sk as *const u32) }.map_err(|_| 1i64)?;
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
        (*ev).src_ip = src_ip;
        (*ev).dst_ip = dst_ip;
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
