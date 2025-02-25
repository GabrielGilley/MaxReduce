#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef should_run = "should_run"


cdef public void run_internal(const DBAccess* access):
    # remove_tag
    Pando.remove_tag(access, should_run)

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if should_run in tags:
        return True
    return False    
