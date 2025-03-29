#!python
#cython: language_level=3

import os
import sys

from filter_lib cimport *
cimport Pando

# There may be a use case when kmers from multiple k values would ideally be extracted
# Could change name based on k variable or could have filter take a list of k's to extract
cdef filter_done = "seq_to_kmers:done"
cdef filter_fail = "seq_to_kmers:fail"
cdef k = 15
KMER_KEY = 15
# For space efficiency we could encode nucleotides in as few as 2 bits, 3 with RNA support
# Instead of using an entire byte as a char

cdef public void run_internal(const DBAccess* access):
    new_tags = ["kmer"]
    seq = access.value.decode('utf-8')

    length = len(seq)
    # Exit if k is too big
    if k > length:
        print("K is larger than seq")
        Pando.add_tag(access, filter_fail)
        return

    # Make entry for every kmer in the sequence
    kmers = []
    counts = []

    for i in range(length - k + 1):
        kmer = seq[i:i + k]
        if kmer in kmers:
            counts[kmers.index(kmer)] += 1
        else:
            kmers.append(kmer)
            counts.append(1)
    
    nucleotide_to_bits = {
        'A': 0b00,  # 00
        'C': 0b01,  # 01
        'G': 0b10,  # 10
        'T': 0b11   # 11
    }
    
    # Process each nucleotide in the k-mer
    for idx, kmer in enumerate(kmers):
        kmer_int = 0
        for nucleotide in kmer:
            kmer_int = (kmer_int << 2) | nucleotide_to_bits[nucleotide]
        new_key = dbkey_t(KMER_KEY, kmer_int, 0)
        new_value = str(counts[idx])
        Pando.make_new_entry_lt_3_tags(access, new_tags, new_value, new_key)
    Pando.add_tag(access, filter_done)


cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if filter_done in tags or filter_fail in tags:
         return False
    if "seq" in tags:
         return True
    return False


