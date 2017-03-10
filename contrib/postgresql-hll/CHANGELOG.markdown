2014-01-10
==========
v2.10.0 - Merged [PR #17](https://github.com/aggregateknowledge/postgresql-hll/pull/17), cleaned up compiler warnings and test cruft, added binary IO type for hll.

2013-12-16
==========
v2.9.0 - Fixed [issue #16](https://github.com/aggregateknowledge/postgresql-hll/issues/16), `bitstream_pack` fixed to write one byte at a time to avoid writing to unallocated memory.

2013-07-16
==========
v2.8.0 - Fixed [issue #2](https://github.com/aggregateknowledge/postgresql-hll/issues/2), `hll_add_agg` now returns `hll_empty` on input of an empty set. (Thanks to @ozgune!)

2013-06-12
==========
v2.7.1 - Build fixes for OS X and Debian. Documentation fixes. Small changes to test format to improve stability across `psql` versions.

2013-02-04
==========
v2.7 - First open-source release.