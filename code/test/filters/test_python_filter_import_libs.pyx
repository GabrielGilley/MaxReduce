#!python
#cython: language_level=3

from filter_lib cimport *
#import math

cdef public void run_internal(const DBAccess* access):
    pass


cdef public bint should_run_internal(const DBAccess* access):
    # A few modules that caused issues during development
    import math
    import collections
    import random
    import json

    return False
