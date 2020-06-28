use libc::{__errno_location, c_char, c_int, c_void, ssize_t, uintptr_t, writev};
use staticvec::StaticVec;
use std::cell::Cell;
use std::ops::RangeFrom;
use std::ptr;
use std::collections::VecDeque;

use crate::palloc;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct IoSlice {
    iov_base: *const c_void,
    iov_len: usize,
}
unsafe impl std::marker::Sync for IoSlice {}

impl IoSlice {
    fn new(base: *const c_void, len: usize) -> Self {
        Self {
            iov_base: base,
            iov_len: len,
        }
    }

    fn end(&self) -> *const c_void {
        self.iov_base.wrapping_add(self.iov_len)
    }

    fn advance<'a, 'b>(iov: &'a mut &'b mut [IoSlice], mut n: usize) {
        let mut slice = std::mem::take(iov);
        while let Some(v) = slice.first_mut() {
            if v.iov_len > n {
                v.iov_base = v.iov_base.wrapping_add(n);
                v.iov_len -= n;
                break;
            }
            n -= v.iov_len;
            slice = &mut slice[1..];
        }
        *iov = slice;
    }

    fn sum_iov_len(iov: &[IoSlice]) -> usize {
        iov.iter().map(|el| el.iov_len).sum()
    }
}

#[derive(Debug)]
pub struct Node {
    iov: StaticVec<IoSlice, 64>,
    refs: StaticVec<usize, 64>,
    pool: palloc::Pool,
}

impl Node {
    fn new(pool: palloc::Pool) -> Self {
        Node {
            iov: StaticVec::new(),
            refs: StaticVec::new(),
            pool,
        }
    }

    fn trim(&mut self, iov_range: RangeFrom<usize>, refs_range: RangeFrom<usize>) {
        let mut have_lua_refs = false;
        for &x in &self.refs[refs_range.clone()] {
            if x & 1 == 1 {
                have_lua_refs = true
            } else {
                unsafe { object_decr_ref(x) }
            }
        }
        if have_lua_refs {
            unsafe {
                let refs = self.refs.as_ptr();
                let from = refs_range.start as c_int;
                let count = self.refs.len() as c_int - from;
                __netmsg_unref(refs, from, count);
            }
        }

        self.iov.drain(iov_range);
        self.refs.drain(refs_range);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.trim(0.., 0..);
    }
}

static DUMMY: IoSlice = IoSlice {
    iov_base: std::ptr::null_mut(),
    iov_len: 0,
};

#[repr(C)]
struct PoolCtx {
    pool: Cell<palloc::Pool>,
    limit: usize,
    name: *const c_char,
}

impl PoolCtx {
    fn new(name: *const c_char, limit: usize) -> Self {
        Self {
            name,
            limit,
            pool: Cell::new(palloc::Pool::new(name)),
        }
    }

    fn with_pool<T, F: FnOnce(&palloc::Pool) -> T>(&self, f: F) -> T {
        let pool = unsafe { &*self.pool.as_ptr() };
        f(pool)
    }

    fn gc(&self) {
        if self.with_pool(|p| p.allocated()) > self.limit {
            self.pool.replace(palloc::Pool::new(self.name));
        }
    }

    fn pool(&self) -> palloc::Pool {
        self.with_pool(|p| p.clone())
    }
}

#[repr(C)]
pub struct Msg {
    bytes: usize,
    pool_ctx: &'static PoolCtx,
    last_used_iov: *mut IoSlice,
    node: VecDeque<Box<Node>>, // TODO: Vec result in double inderection (msg->vec->box) and expensive grow in Msg::node()
}

#[repr(C)]
#[derive(Debug)]
pub enum Mark {
    Node {
        idx: *const Node,
        iov_len: usize,
        refs_len: usize,
        last_iov_len: usize,
    },
    Empty,
}

impl Msg {
    fn new(pool_ctx: &'static PoolCtx) -> Self {
        Self {
            pool_ctx,
            bytes: 0,
            node: VecDeque::new(),
            last_used_iov: &raw const DUMMY as *mut IoSlice,
        }
    }

    fn node(&mut self) -> &mut Node {
        let need_grow = match &self.node.back() {
            None => true,
            Some(node) => !node.iov.is_not_full(),
        };
        if std::intrinsics::unlikely(need_grow) {
            self.node.push_back(box Node::new(self.pool_ctx.pool()));
        }
        match self.node.back_mut() {
            Some(node) => node,
            _ => unreachable!(),
        }
    }

    fn add(&mut self, base: *const c_void, len: usize) {
        self.bytes += len;
        let last = unsafe { &mut *self.last_used_iov }; // SAFE: Node is heap allocated and hence node.iov is immovable
        if last.end() == base {
            last.iov_len += len;
        } else {
            let iov = &mut self.node().iov;
            iov.push(IoSlice::new(base, len));
            match iov.last_mut() {
                Some(v) => self.last_used_iov = v,
                _ => unreachable!(),
            }
        }
    }

    fn add_ref(&mut self, obj: usize, base: *const c_void, len: usize) {
        self.bytes += len;
        let node = self.node();
        node.iov.push(IoSlice::new(base, len));
        node.refs.push(obj);
        self.last_used_iov = &raw const DUMMY as *mut _;
    }

    fn add_alloc(&mut self, len: usize) -> *mut c_void {
        let buf = self.node().pool.palloc(len);
        self.add(buf, len);
        buf
    }

    fn get_mark(&mut self) -> Mark {
        match self.node.back() {
            Some(node) => Mark::Node {
                idx: &**node,
                iov_len: node.iov.len(),
                refs_len: node.refs.len(),
                last_iov_len: match node.iov.last() {
                    Some(iov) => iov.iov_len,
                    None => 0,
                },
            },
            None => Mark::Empty,
        }
    }

    fn rewind(&mut self, mark: &Mark) {
        match *mark {
            Mark::Empty => {
                self.clear();
            }
            Mark::Node { idx, iov_len, refs_len, last_iov_len } => {
                while let Some(n) = self.node.back_mut() {
                    if &**n as *const _ == idx {
                        self.bytes -= IoSlice::sum_iov_len(&n.iov[iov_len..]);
                        n.trim(iov_len.., refs_len..);
                        if let Some(iov) = n.iov.last_mut() {
                            iov.iov_len = last_iov_len
                        }
                        break;
                    } else {
                        self.bytes -= IoSlice::sum_iov_len(&n.iov);
                        self.node.pop_back();
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.bytes = 0;
        self.last_used_iov = &raw const DUMMY as *mut IoSlice;
        self.node.clear();
    }

    fn flatten_into(&self, out: &mut StaticVec<IoSlice, 1024>) {
        for n in &self.node {
            let r = out.try_extend_from_slice(&n.iov);
            if r.is_err() {
                return;
            }
        }
    }

    fn writev(&mut self, fd: i32) -> isize {
        if self.bytes == 0 {
            return 0;
        }

        let mut result: usize = 0;
        let mut iovec: StaticVec<IoSlice, 1024> = StaticVec::new();
        self.flatten_into(&mut iovec);
        let total_len = iovec.len();

        let mut iov = &mut iovec[..];
        loop {
            let n = unsafe {
                let r = writev(fd, iov as *const _ as *const libc::iovec, iov.len() as i32);
                if r < 0 {
                    if *__errno_location() == libc::EINTR {
                        continue;
                    }
                    if result == 0 {
                        return r;
                    }
                    break;
                }
                r as usize
            };

            result += n;

            if self.bytes == result {
                self.clear();
                return result as isize;
            }

            IoSlice::advance(&mut iov, n);

            if iov.len() == 0 {
                break;
            }
        }

        self.bytes -= result;

        let mut iov_sent = total_len - iov.len();
        let mut node_sent = 0;
        for n in &mut self.node {
            if n.iov.len() > iov_sent {
                n.iov.drain(0..iov_sent);
                if iov.len() > 0 {
                    n.iov[0] = iov[0];
                }
                break;
            }
            node_sent += 1;
            iov_sent -= n.iov.len();
        }

        for _ in 0..node_sent {
            self.node.pop_front();
        }

        result as isize
    }
}

cfg_if::cfg_if! {
    if #[cfg(not(test))] {
        extern {
            fn object_decr_ref(obj: usize);
            fn object_incr_ref(obj: usize);
            fn __netmsg_unref(refs: *const uintptr_t, from: c_int, count: c_int);
        }
    } else {
        static REF_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        unsafe extern fn object_decr_ref(_obj: usize)  {
            REF_COUNT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
        unsafe extern fn object_incr_ref(_obj: usize)  {
            REF_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        unsafe extern fn __netmsg_unref(_refs: *const uintptr_t, _from: c_int, _count: c_int) {
            panic!("not implemented")
        }
    }
}

#[no_mangle]
unsafe extern "C" fn netmsg_head_init(msg: *mut Msg, pool_ctx: *const PoolCtx) {
    *msg = Msg::new(&*pool_ctx)
}

#[no_mangle]
unsafe extern "C" fn netmsg_head_dealloc(msg: *mut Msg) {
    ptr::drop_in_place(msg)
}

#[no_mangle]
unsafe extern "C" fn netmsg_pool_ctx_init(ctx: *mut PoolCtx, name: *const c_char, limit: c_int) {
    ctx.write(PoolCtx::new(name, limit as usize));
}

#[no_mangle]
unsafe extern "C" fn netmsg_pool_ctx_gc(ctx: *mut PoolCtx) {
    (*ctx).gc();
}

#[no_mangle]
unsafe extern "C" fn net_add_iov(msg: *mut Msg, buf: *const c_void, len: usize) {
    (*msg).add(buf, len)
}

#[no_mangle]
unsafe extern "C" fn net_add_alloc(msg: *mut Msg, len: usize) -> *mut c_void {
    (*msg).add_alloc(len) as *mut c_void
}

#[no_mangle]
unsafe extern "C" fn net_add_iov_dup(msg: *mut Msg, buf: *const c_void, len: usize) {
    let dst = (*msg).add_alloc(len);
    ptr::copy_nonoverlapping(buf, dst, len);
}

#[no_mangle]
unsafe extern "C" fn net_add_ref_iov(msg: *mut Msg, obj: uintptr_t, buf: *const c_void, len: usize) {
    (*msg).add_ref(obj, buf, len);
}

#[no_mangle]
unsafe extern "C" fn net_add_obj_iov(msg: *mut Msg, obj: uintptr_t, buf: *const c_void, len: usize) {
    assert!(obj & 1 == 0); // will work because sizeof(gc_oct_object->refs) == 4
    object_incr_ref(obj);
    (*msg).add_ref(obj, buf, len);
}

#[no_mangle]
unsafe extern "C" fn netmsg_getmark(msg: *mut Msg, mark: *mut Mark) {
    std::ptr::write(mark, (*msg).get_mark());
}

#[no_mangle]
unsafe extern "C" fn netmsg_rewind(msg: *mut Msg, mark: *const Mark) {
    (*msg).rewind(&*mark);
}

#[no_mangle]
unsafe extern "C" fn netmsg_reset(msg: *mut Msg) {
    (*msg).clear();
}

#[no_mangle]
unsafe extern "C" fn netmsg_writev(fd: c_int, msg: *mut Msg) -> ssize_t {
    (*msg).writev(fd)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_msg_size() {
        assert_eq!(std::mem::size_of::<Msg>(), 48); // do not forget to update net_io.h
    }
    #[test]
    fn test_msg_layout() {
        assert_eq!(0, memoffset::offset_of!(Msg, bytes));
        assert_eq!(
            std::mem::size_of::<usize>(),
            memoffset::offset_of!(Msg, pool_ctx)
        );
    }

    #[test]
    fn test_mark_size() {
        assert_eq!(std::mem::size_of::<Mark>(), 40); // do not forget to update net_io.h
    }

    #[test]
    fn test_poolctx_size() {
        assert_eq!(std::mem::size_of::<PoolCtx>(), 24); // do not forget to update net_io.h
    }

    #[test]
    fn test_rewind() {
        let ctx = PoolCtx::new("test_ctx".as_ptr() as *const _, 64 * 1024);
        let ctx = unsafe { &*(&ctx as *const _) };
        let v = vec![0; 512];
        let mut p = v.as_ptr();
        let mut msg = Msg::new(&ctx);
        assert!(msg.node.is_empty());

        for _ in 0..64 {
            msg.add(p as *const _, 1);
            p = p.wrapping_offset(2);
        }
        assert!(!msg.node.is_empty());

        let tail_node: *const Node = &**msg.node.back().unwrap();

        let mark = msg.get_mark();

        for _ in 0..512 {
            msg.add(p as *const _, 1);
            p = p.wrapping_offset(2);
        }
        assert!(!msg.node.is_empty());
        assert_ne!(tail_node, &**msg.node.back().unwrap());

        msg.rewind(&mark);
        assert_eq!(tail_node, &**msg.node.back().unwrap());
    }

    #[test]
    fn test_drop() {
        let ctx = PoolCtx::new("test_ctx".as_ptr() as *const _, 64 * 1024);
        let ctx = unsafe { &*(&ctx as *const _) };
        let mut msg = Msg::new(&ctx);

        for _ in 0..512 {
            msg.add_ref(1024, std::ptr::null(), 1);
        }

        REF_COUNT.store(512, std::sync::atomic::Ordering::SeqCst);
        msg.node.clear();
        assert_eq!(0, REF_COUNT.load(std::sync::atomic::Ordering::SeqCst))
    }
}
