#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando

cdef filter_done = "python_ZEC_get_block:done"
cdef filter_fail = "python_ZEC_get_block:fail"


cdef public void run_internal(const DBAccess* access):
    import json
    d = json.loads(access.value)
    new_tags = ["ZEC", "tx"]
    for tx in d['tx']:
        new_key = dbkey_t(1<<63, -1, -1)
        new_value = json.dumps(tx)
        Pando.make_new_entry_lt_10_tags(access, new_tags, new_value, new_key)

    # add_tag
    filter_done_tag = Pando.make_tag(filter_done)
    Pando.add_tag(access, filter_done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "ZEC" in tags and "block" in tags:
         return True
    return False
