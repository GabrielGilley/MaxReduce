#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef filter_done = "DONE"


cdef public void run_internal(const DBAccess* access):
    # update_entry_val
    new_val = "my new value"
    Pando.update_entry_val(access, access.key, new_val)
    Pando.add_tag(access, filter_done)


cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if filter_done in tags:
        return False
    if "A" in tags:
        return True
    return False
