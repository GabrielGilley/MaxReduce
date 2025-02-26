#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando

# There may be a use case when kmers from multiple k values would ideally be extracted
# Could change name based on k variable or could have filter take a list of k's to extract
cdef filter_done = "extract_kmers_fastq:done"
cdef filter_fail = "extract_kmers_fastq:fail"

# For space efficiency we could encode nucleotides in as few as 2 bits, 3 with RNA support
# Instead of using an entire byte as a char

cdef public void run_internal(const DBAccess* access, const uint k):
    import json

    d = json.loads(access.value)
    new_tags = ["kmer"]
    seq = d['seq']  # Assumes a json object contains a read with id, seq, and quality

    length = len(seq)
    # Exit if k is too big
    if k > length:
        Pando.add_tag(access, filter_fail)
        return

    quality = d['quality']
    
    # Exit if quality and sequence aren't same size
    if length != len(quality):
        Pando.add_tag(access, filter_fail)
        return
 
    counter = 0

    # Make entry for every kmer in the sequence
    for i in range(length - k + 1):
        kmer = ""
        qmer = ""

        kmer += seq[i]
        qmer += quality[i]
        
        # We actually probably only want kmer as a key
        # kmer_ascii = [ord(char) for char in s] 
        # or maybe A = 0, C = 1, ..., T = 3
        # key of ACGT = 0123
        # above approaches would get really big. could do a 256 bit hash of kmer
        # convert it to int and use it as key
        new_key = dbkey_t(d["id"], k, counter)
        new_value = "kmer=" + kmer + "\n" + "quality=" + qmer + "\n"
        
        counter += 1
        Pando.make_new_entry_lt_10_tags(access, new_tags, new_value, new_key)

    # add_tag
    filter_done_tag = Pando.make_tag(filter_done)
    Pando.add_tag(access, filter_done_tag)


cdef public bint should_run_internal(const DBAccess* access):
    tags  = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "seq" in tags and "fastq" in tags:
         return True
    return False


