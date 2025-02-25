#!python
#cython: language_level=3

from filter_lib cimport *
from cython.cimports.libc.stdlib import free
cimport Pando

cdef done = "DONE"
cdef fail = "FAILURE"

cdef public void run_internal(const DBAccess* access):
    search_key = dbkey_t(2,2,2)
    ret = Pando.get_entry_by_key(access, search_key)    
    if ret:
        Pando.add_tag(access, "SUCCESS")
    else:
        Pando.add_tag(access, fail)
    Pando.add_tag(access, done)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if done in tags or fail in tags:
        return False
    if "A" in tags:
        return True
    return False
