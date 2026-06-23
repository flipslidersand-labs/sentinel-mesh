#![no_std]
#![no_main]

use aya_bpf::{macros::tracepoint, programs::TracePointContext};

// Phase 1: sys_enter_execve をフックしてプロセス起動を検出
#[tracepoint(name = "sentinel_exec")]
pub fn sentinel_exec(_ctx: TracePointContext) -> u32 {
    0
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
