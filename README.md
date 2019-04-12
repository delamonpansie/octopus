# Octopus project
in-memory storage framework

## Key features
 * extensible
 * fast
 * easy persistance
 * log streaming replication
 * hot standby
 * simple binary protocol
 * memcached protocol emulation
 * extensibility


## Branches

Octopus (ab)uses git branches in a specific way: the core of framework on the "master"
branch, modules are on "mod\_box" and "mod\_memcached" branches.


## Silverbox: Yet another in-memory key-value database

Silverbox is a bit more than a typical key-value database, instead of
storing just key-value pairs, it stores a whole tuple. That is, you
can have many different keys per values like `(key1, key2,
value)`. You can have many values per key also.

Silverbox protocol description can be found at
[doc/silverbox-protocol.txt](doc/silverbox-protocol.txt)

If protocol isn't flexible enough to suite your needs, you can
implement stored procedure in Lua and OCaml. Unforutantely, there is
no much documentation on that at the moment. Though, you can use
[mod/box/src-lua/box/example_proc.lua](../mod_box/src-lua/box/example_proc.lua)
as a starting point. OCaml interface documentation can be found at
[mod/box/src-ml/box1.mli](../mod_box/src-ml/box1.mli)


# How to run Octopus/Silverbox:

Compile (note GNU make and GNU Objective-C compiler are required)

    make

   note: on MacOSX

    CC='gcc -m32' ./configure
    make CORO_IMPL=SJLJ


Customize config

    cp cfg/octopus_box.cfg custom.cfg
	emacs custom.cfg


Initialize storage

    ./octopus --config custom.cfg --init-storage

Run

    ./octopus --config custom.cfg
