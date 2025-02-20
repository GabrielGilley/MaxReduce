#!python
#cython: language_level=3

from filter_lib cimport *
from ETH_BASED_tx_edges cimport *

crypto_tag = "BNB"
filter_name = crypto_tag + "_tx_edges_python"
filter_done_tag = filter_name + ":done"
filter_fail_tag = filter_name + ":fail"

cdef public void run_internal(const DBAccess* access):
    eth_based_tx_edges(access, crypto_tag, filter_name, filter_fail_tag, filter_done_tag)

cdef public bint should_run_internal(const DBAccess* access):
    return should_run_eth_based_tx_edges(access, crypto_tag, filter_name, filter_fail_tag, filter_done_tag)
