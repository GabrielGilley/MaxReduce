#!python
#cython: language_level=3

from filter_lib cimport *
cimport Pando

cdef filter_done = "test_python_filter_pack_chain_info:done"


cdef public void run_internal(const DBAccess* access):
    # add_tag
    chain_info = pack_chain_info(ZEC_KEY, TXTIME_KEY, 0)
    Pando.add_tag(access, str(chain_info))
    Pando.add_tag(access, filter_done)

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if filter_done in tags:
        return False
    if "A" in tags:
        return True
    return False
