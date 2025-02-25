#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef done = "DONE"


cdef public void run_internal(const DBAccess* access):
    # update_entry_val
    new_val = "my new value"
    search_key = dbkey_t(2,2.,2)
    Pando.update_entry_val(access, search_key, new_val)
    Pando.add_tag(access, done)

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if done in tags:
        return False
    if "A" in tags:
        return True
    return False
