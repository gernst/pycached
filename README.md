# pycached

`pycached` is a Python model of the memcache server created for https://verifythis.github.io/

## Quickstart

In one terminal run the server

    python pycached.py

In a second terminal, run some tests:

    nc localhost 8081 < test.memcached

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
