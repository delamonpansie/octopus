set history save on
set print pretty on
set pagination off
set confirm off

handle SIGPIPE nostop noprint pass
handle SIGUSR1 nostop noprint pass

def slab
  set $slab = (void *)((uintptr_t)($arg0) & ~(SLAB_SIZE - 1))
  p $slab
end

def xgetlen
  if *(char *)$ptr < 127
    set $len = *(char *)$ptr
    set $ptr = (char *)$ptr + 1
  else
    aaa
  end
end

def pfield
  set $ptr = $arg0
  xgetlen

  if $len > 0
    output *$ptr@$len
    set $ptr = (char *)$ptr + $len
  else
    printf "nil"
  end
  printf "\n"
end

def ptuple
  if $arg0->type != 1
    printf "bad tnt_obj type\n"
  end
  set $tuple = (struct box_tuple *)$arg0->data
  set $cardinlaity = $tuple->cardinality
  set $ptr = $tuple->data

  printf "["
  while $cardinlaity > 0
    xgetlen

    if $len > 0
      if $len == 4
        output *(i32 *)$ptr
      end
      if $len == 8
        output *(i64 *)$ptr
      end
      output *$ptr@$len
      set $ptr = (char *)$ptr + $len
    else
      printf "nil"
    end

    if $cardinlaity > 1
      printf ", "
    else
      printf "]\n"
    end
    set $cardinlaity--
  end
end

def x31_hash
  set $ptr = (unsigned char *)$arg0
  set $hash = (uint32_t)0
  xgetlen
  if $len
    while $len--
      set $hash = ($hash << 5) - $hash + *$ptr++
    end
  end
  printf "hash: %u\n", $hash
end

def murmur_hash
  set $data = (unsigned char *)$arg0
  set $len = (int)$arg1
  set $seed = (unsigned int)$arg2

  set $mur_m = (unsigned int)0x5bd1e995
  set $mur_r = (int)24

  set $hash = (unsigned int)(seed ^ len)

  while $len >= 4
    set $k = *(unsigned int *)$data

    set $k *= $mur_m
    set $k ^= $k >> $mur_r
    set $k *= $mur_m

    set $hash *= $mur_m
    set $hash ^= $k

    set $data += 4
    set $len -= 4
  end

  if $len == 3
    set $hash ^= $data[2] << 16
    set $hash ^= $data[1] << 8
    set $hash ^= $data[0]
    set $hash *= $mur_m
  end
  if $len == 2
    set $hash ^= $data[1] << 8
    set $hash ^= $data[0]
    set $hash *= $mur_m
  end
  if $len == 1
    set $hash ^= $data[0]
    set $hash *= $mur_m
  end

  set $hash ^= $hash >> 13
  set $hash *= $mur_m
  set $hash ^= $hash >> 15

  printf "hash: %u\n", $hash
end

def pnetmsg
  set $netmsg_count = $arg0->count
  set $netmsg_offt = 0
  while $netmsg_offt < $netmsg_count
    set $iov_len = $arg0->iov[$netmsg_offt].iov_len
    set $iov_offt = 0
    printf "iov[%i]: ", $netmsg_offt
    while $iov_offt < $iov_len
      set $iov_base = (char *)$arg0->iov[$netmsg_offt].iov_base
      printf "%02x ", $iov_base[$iov_offt]
      set $iov_offt = $iov_offt + 1
    end
    printf "\n"
    set $netmsg_offt = $netmsg_offt + 1
  end
end

def pfibers
  set $fiber = fibers.slh_first
  while $fiber != 0
    if $fiber->name != 0
      printf "%30s/%i  ", $fiber->name, $fiber->fid
    else
      printf "                        (none)/%i  ", $fiber->fid
    end
    p $fiber
    set $fiber = $fiber.link.sle_next
  end
end

def btfiber
  set $fib_coro = $arg0->coro
  set $fib_stack_top = $fib_coro->stack + $fib_coro->stack_size
  set $fib_sp = (void *)$fib_coro->ctx->sp
  set $fib_stack_len = $fib_stack_top - $fib_sp

  while $fib_stack_len > 0
    x/a $fib_sp
    set $fib_sp = $fib_sp + sizeof(void *)
    set $fib_stack_len = $fib_stack_len - sizeof(void *)
  end
end

def save_ctx
  set $save_rbp = $rbp
  set $save_rbx = $rbx
  set $save_r12 = $r12
  set $save_r13 = $r13
  set $save_r14 = $r14
  set $save_r15 = $r15
  set $save_rsp = $rsp
  set $save_rip = $rip
  set $save_fiber = fiber
end

def restore_ctx
  set $rbp = $save_rbp
  set $rbx = $save_rbx
  set $r12 = $save_r12
  set $r13 = $save_r13
  set $r14 = $save_r14
  set $r15 = $save_r15
  set $rsp = $save_rsp
  set $rip = $save_rip
  set fiber = $save_fiber
end

def switch_to_fiber_no
  frame 0
  set $fiber = fibers.slh_first
  while $fiber != 0 && $fiber->fid != $arg0 
    set $fiber = $fiber.link.sle_next
  end
  set fiber = $fiber
  set $sp = $fiber->coro.ctx.sp
  set $rbp = ((uintptr_t*)$sp)[5]
  set $rbx = ((uintptr_t*)$sp)[4]
  set $r12 = ((uintptr_t*)$sp)[3]
  set $r13 = ((uintptr_t*)$sp)[2]
  set $r14 = ((uintptr_t*)$sp)[1]
  set $r15 = ((uintptr_t*)$sp)[0]
  set $rip = ((uintptr_t*)$sp)[6]
end

def switch_to_fiber_ptr
  frame 0
  set fiber = $arg0
  set $sp = fiber->coro.ctx.sp
  set $rbp = ((uintptr_t*)$sp)[5]
  set $rbx = ((uintptr_t*)$sp)[4]
  set $r12 = ((uintptr_t*)$sp)[3]
  set $r13 = ((uintptr_t*)$sp)[2]
  set $r14 = ((uintptr_t*)$sp)[1]
  set $r15 = ((uintptr_t*)$sp)[0]
  set $rip = ((uintptr_t*)$sp)[6]
end
