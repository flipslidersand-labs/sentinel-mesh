#![no_std]
#![no_main]

use aya_ebpf::{
    helpers::{bpf_get_current_comm, bpf_get_current_pid_tgid, bpf_get_current_uid_gid},
    macros::{kprobe, map, tracepoint},
    maps::RingBuf,
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

// ─── Exec: sys_enter_execve ───────────────────────────────────────────────

#[tracepoint(name = "sentinel_exec", category = "syscalls")]
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

// ─── File open: sys_enter_openat ──────────────────────────────────────────

#[tracepoint(name = "sentinel_openat", category = "syscalls")]
pub fn sentinel_openat(ctx: TracePointContext) -> u32 {
    match try_openat(ctx) {
        Ok(_) => 0,
        Err(_) => 1,
    }
}

#[inline(always)]
fn try_openat(ctx: TracePointContext) -> Result<(), i64> {
    use aya_ebpf::helpers::bpf_probe_read_user_str_bytes;

    let pid_tgid = bpf_get_current_pid_tgid();
    let pid = (pid_tgid >> 32) as u32;
    let uid = (bpf_get_current_uid_gid() & 0xffff_ffff) as u32;
    let raw_comm = bpf_get_current_comm()?;

    // sys_enter_openat tracepoint args: dfd(8), filename ptr(16), flags(24), mode(32)
    let filename_ptr: u64 = unsafe { ctx.read_at(16) }.map_err(|_| 1i64)?;

    let mut entry = unsafe { EVENTS.reserve::<FileEvent>(0) }.ok_or(1i64)?;
    let ev = entry.as_mut_ptr();
    unsafe {
        (*ev).event_type = ET_FILE;
        (*ev).pid = pid;
        (*ev).uid = uid;
        (*ev).ret = 0;
        for i in 0..16 {
            (*ev).comm[i] = raw_comm[i] as u8;
        }
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

    // arg(0) = struct sock *sk. skc_daddr is at offset 0, inet_dport at offset 12.
    let sk: *const u8 = ctx.arg(0).ok_or(1i64)?;
    let dst_ip: u32 =
        unsafe { bpf_probe_read_kernel(sk as *const u32) }.map_err(|_| 1i64)?;
    let dst_port: u16 =
        unsafe { bpf_probe_read_kernel(sk.add(12) as *const u16) }.map_err(|_| 1i64)?;

    let mut entry = unsafe { EVENTS.reserve::<TcpEvent>(0) }.ok_or(1i64)?;
    let ev = entry.as_mut_ptr();
    unsafe {
        (*ev).event_type = ET_TCP;
        (*ev).pid = pid;
        (*ev).src_ip = 0;
        (*ev).dst_ip = dst_ip;
        (*ev).src_port = 0;
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
