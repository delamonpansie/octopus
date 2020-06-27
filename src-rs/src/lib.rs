#![allow(incomplete_features)]
#![feature(
    const_generics,
    maybe_uninit_uninit_array,
    maybe_uninit_slice_assume_init,
    maybe_uninit_extra,
    test,
    raw_ref_op,
    box_syntax,
    core_intrinsics
)]

pub mod net_io;
pub mod palloc;
