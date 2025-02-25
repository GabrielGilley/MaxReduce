#!python
#cython: language_level=3

from filter_lib cimport *
from cython.cimports.libc.stdlib import free
cimport Pando

cdef filter_done = "test_python_filter_subscribe_to_entry:done"
cdef filter_inactive = "test_python_filter_subscribe_to_entry:inactive"  


cdef public void run_internal(const DBAccess* access):
    # Look for key, first time it should not exist
    search_key = dbkey_t(2,2,2)
    ret = Pando.get_entry_by_key(access, search_key)
    if ret:
        Pando.add_tag(access, filter_done)
    else:
        Pando.subscribe_to_entry(access, access.key, search_key, filter_inactive)
        Pando.add_tag(access, filter_inactive)

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)    
    if filter_done in tags or filter_inactive in tags:
        return False
    if "A" in tags:
        return True
    return False
