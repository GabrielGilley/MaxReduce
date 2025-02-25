#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef done = "DONE"

cdef public void run_internal(const DBAccess* access):
    new_key = dbkey_t(2,2,2)
    new_value = "test2"
    tags = ["B", "TESTING"]
    Pando.make_new_entry_lt_10_tags(access, tags, new_value, new_key)
    Pando.add_tag(access, done)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if done in tags:
        return False
    if "A" in tags:
        return True
    return False
