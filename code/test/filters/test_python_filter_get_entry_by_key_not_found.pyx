#!python
#cython: language_level=3

from filter_lib cimport *
from cython.cimports.libc.stdlib import free
cimport Pando

cdef done = "DONE"
cdef fail = "FAIL"

cdef public void run_internal(const DBAccess* access):
    # get_entry_by_key
    search_key = dbkey_t(2,2,2)
    ret = Pando.get_entry_by_key(access, search_key)
    if ret:
        Pando.add_tag(access, fail)
    else:
        Pando.add_tag(access, "SUCCESS")

    done_tag = Pando.make_tag(done)
    Pando.add_tag(access, done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if done in tags or fail in tags:
        return False
    if "A" in tags:
        return True
    return False
