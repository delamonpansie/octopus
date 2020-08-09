/*
 * Copyright (C) 2020 Yury Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */


use libc::{c_char, c_void, size_t};

extern {
    pub type PallocPool;
}

#[repr(C)]
union PallocCtx {
    ptr: *mut c_void,
    rc: usize,
}

#[repr(C)]
struct PallocConfig {
    name: *const c_char,
    ctx: PallocCtx,
    size: size_t,
    nomem_cb: Option<extern "C" fn(pool: *mut PallocPool, ctx: *mut c_void)>,
}

cfg_if::cfg_if! {
    if #[cfg(not(test))] {
        extern {
            fn palloc(pool: *mut PallocPool, size: size_t) -> *mut c_void;
            fn palloc_create_pool(config: PallocConfig) -> *mut PallocPool;
            fn palloc_destroy_pool(pool: *mut PallocPool);
            fn palloc_ctx(pool: *mut PallocPool, ctx: PallocCtx) -> PallocCtx;
            fn palloc_allocated(pool: *mut PallocPool) -> size_t;
        }
    } else {
        unsafe fn palloc(_pool: *mut PallocPool, _size: size_t) -> *mut c_void { panic!("not implemented") }
        unsafe fn palloc_create_pool(_config: PallocConfig) -> *mut PallocPool { 0 as *mut PallocPool }
        unsafe fn palloc_destroy_pool(_pool: *mut PallocPool) { }
        unsafe fn palloc_ctx(_pool: *mut PallocPool, _ctx: PallocCtx) -> PallocCtx { PallocCtx { rc: 2 } }
        unsafe fn palloc_allocated(_pool: *mut PallocPool) -> size_t { 1 }
    }
}

unsafe fn get_rc(pool: *mut PallocPool) -> usize {
    palloc_ctx(pool, PallocCtx { rc: 0 }).rc
}

unsafe fn set_rc(pool: *mut PallocPool, rc: usize) {
    assert!(rc != 0);
    palloc_ctx(pool, PallocCtx { rc });
}

#[no_mangle]
unsafe extern "C" fn palloc_ref(pool: *mut PallocPool) {
    let rc = get_rc(pool);
    set_rc(pool, rc + 1);
}

#[no_mangle]
unsafe extern "C" fn palloc_unref(pool: *mut PallocPool) {
    match get_rc(pool) {
        1 => palloc_destroy_pool(pool),
        n => set_rc(pool, n - 1),
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub struct Pool(*mut PallocPool);

impl Pool {
    pub fn new(name: *const c_char) -> Self {
        let inner = unsafe {
            palloc_create_pool(PallocConfig {
                name: name,
                ctx: PallocCtx { rc: 1 },
                size: 0,
                nomem_cb: None,
            })
        };
        Pool(inner)
    }

    pub fn palloc(&self, size: size_t) -> *mut c_void {
        unsafe { palloc(self.0, size) }
    }

    pub fn allocated(&self) -> usize {
        unsafe { palloc_allocated(self.0) as usize }
    }
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        unsafe { palloc_ref(self.0) };
        Pool(self.0)
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        unsafe { palloc_unref(self.0) };
    }
}
