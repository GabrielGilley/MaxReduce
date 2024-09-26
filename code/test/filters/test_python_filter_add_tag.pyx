#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef filter_done_tag = "test_python_filter_add_tag:done"


cdef public void run_internal(const DBAccess* access): 
    Pando.add_tag(access, filter_done_tag);


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done_tag in tags:
        return False
    if "A" in tags:
        return True
    return False


