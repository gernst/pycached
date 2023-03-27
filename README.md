# pycached

`pycached` is a Python model of the memcache server created for https://verifythis.github.io/

## Quickstart

Single-threaded via stdin/stdout:

    python pycached.py

Alternatively, in one terminal run the server in network mode,
by specifying a hostname and a port such as:

    python pycached.py localhost 1234

In other terminals, connect to the server and perhaps run some tests:

    nc localhost 1234 < test.memcached

## Status

Infrastructure implented

- high-level cache model
- multithreaded TCP server
- no telnet specifics, end of line currently \n instead of \r\n

Operations implemented

- get/gets and gat/gats
- set/add/replace, delete, and touch
- incr/decr and append/prepend

Cache entries

- flags, exptime, unique CAS id are stored
- no honoring of exptime yet
- no eviction protocol yet
