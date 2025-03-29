#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando

# We don't need to specify short or long yet cause they won't be used together.
cdef filter_done = "read_to_qual:done"
cdef filter_fail = "read_to_qual:fail"
SHORT = 1  # temp db key for short reads
SEQ = 2 
QUALITY = 3

cdef public void run_internal(const DBAccess* access):
    new_tags = ["qual", "short"]
    read = access.value.decode('utf-8')
    lines = read.split("\n")
    identifier = lines[0]
    qual = lines[-2]
    # @chr1_15660191_15660680_0:0:0_0:0:0_1/1
    # Convert id to int
    all_parts = identifier.split("_")
    important_parts = all_parts[:3] + [all_parts[-1]]  # Remove the 0:0:0_0:0:0 because we need smaller digit and this is not relevant
    numeric_string = ''.join(filter(str.isdigit, ''.join(important_parts)))
    id_as_int = int(numeric_string)
    print("id:", identifier)
    print("int:", id_as_int)
    new_key = dbkey_t(QUALITY, id_as_int, 0)
    val = Pando.make_value(qual)
    Pando.make_new_entry_lt_3_tags(access, new_tags, val, new_key) 
    filter_done_tag = Pando.make_tag(filter_done) 
    Pando.add_tag(access, filter_done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "shortread" in tags:
         return True
    return False
