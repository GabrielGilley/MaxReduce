#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando
import random

cdef filter_done = "python_BTC_get_block:done"
cdef filter_fail = "python_BTC_get_block:fail"


cdef public void run_internal(const DBAccess* access):
    import json
    d = json.loads(access.value)
    new_tags = ["BTC", "tx"]
    for tx in d['tx']:
        a_key = random.randint(1,1000000)  # Randomly get a key between 1 and a million to get big boi joins
        # Worth noting that the number of key collisions should probably also remain proportional in weak scalability tests. How doable is that?
        new_key = dbkey_t(a_key, -1, -1)
        new_value = json.dumps(tx)
        Pando.make_new_entry_lt_10_tags(access, new_tags, new_value, new_key)

    # add_tag
    filter_done_tag = Pando.make_tag(filter_done)
    Pando.add_tag(access, filter_done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "BTC" in tags and "block" in tags:
         return True
    return False
