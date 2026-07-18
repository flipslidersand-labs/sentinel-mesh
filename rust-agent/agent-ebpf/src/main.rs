#![no_std]
#![no_main]

use aya_bpf::{
    helpers::{bpf_get_current_comm, bpf_get_current_pid_tgid, bpf_get_current_uid_gid},
    macros::{map, tracepoint},
    maps::RingBuf,
    programs::TracePointContext,
};

// Shared with userspace — keep #[repr(C)] and fixed layout.
#[repr(C)]
pub struct ExecEvent {
    pub event_type: u8, // 0 = exec
    pub pid: u32,
    pub uid: u32,
    pub comm: [u8; 16],
}

#[map]
static EVENTS: RingBuf = RingBuf::with_byte_size(256 * 1024, 0);

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
        (*ev).event_type = 0;
        (*ev).pid = pid;
        (*ev).uid = uid;
        // [i8; 16] → [u8; 16]
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
