set history save on
set print pretty on
set pagination off

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