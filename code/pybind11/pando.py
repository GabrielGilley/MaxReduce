from pando_api import *
from multiprocessing import Pool

def _query_target(addr, tags):
    c_t = ParDBClient(addr)
    return c_t.query(tags)

def query(client, tags):
    if not isinstance(tags, list):
        tags = [tags]

    neighbors = client.neighbors()
    with Pool(32) as pool:
        entry_lists = pool.starmap(_query_target, zip(neighbors, tags*len(neighbors)))

    entries = []

    for entry_list in entry_lists:
        entries.extend(entry_list)

    return entries
