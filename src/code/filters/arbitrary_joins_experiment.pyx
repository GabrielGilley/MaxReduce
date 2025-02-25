#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando
import random

cdef filter_done = "arbitrary_joins:done"
cdef filter_fail = "arbitrary_joins:fail"


cdef public void run_internal(const DBAccess* access):
    new_tags = ["arbitrary", "tags"]
    a_key = random.rand_range(100)
    new_key = dbkey_t(a, -1, -1)
    new_value = "Arbitrary value"
    Pando.make_new_entry_lt_10_tags(access, new_tags, new_value, new_key)

    # add_tag
    filter_done_tag = Pando.make_tag(filter_done)
    Pando.add_tag(access, filter_done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "arbitrary" in tags and "tags" in tags:
         return True
    return False
