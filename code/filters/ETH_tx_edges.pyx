#!python
#cython: language_level=3

from filter_lib cimport *
import rlp
from Crypto.Hash import keccak
cimport Pando
import traceback
from cpython.bytes cimport PyBytes_AsString, PyBytes_FromString
from libc.stdlib cimport malloc

FILTER_NAME = "ETH_tx_edges_python"
filter_done_tag = FILTER_NAME + ":done"
filter_fail_tag = FILTER_NAME + ":fail"

def get_contract_addr(sender, nonce):
    """
    Args:
        sender (str): A hex string that is the "from" field
        nonce (int): An integer value representing the nonce
    """

    if sender.startswith('0x'):
        sender = sender[2:]
    sender = bytes.fromhex(sender)

    input_arr = [sender, nonce]
    rlp_encoded = rlp.encode(input_arr)

    contract_address_long = keccak.new(digest_bits=256, data=rlp_encoded).hexdigest()

    contract_address = contract_address_long[24:]  # Trim the first 24 characters

    contract_address = f"0x{contract_address}"
    return contract_address

cdef public void run_internal(const DBAccess* access):
    import json
    decoded_val = access.value.decode('utf-8')
    d = json.loads(decoded_val)
    tx = d["transactions"]
    tx_from = tx['from']
    tx_to = tx['to']

    if tx_to is None:
        # Contract creation
        nonce = tx['nonce']
        contract_address = get_contract_addr(tx_from, nonce);
        tx_to = contract_address

    from_sub = tx_from[:15]
    from_b_key = int(from_sub, 16)

    to_sub = tx_to[:15]
    to_c_key = int(to_sub, 16)

    tx_hash = tx["hash"];
    tx_hash_tag = f"hash={tx_hash}"

    from_tag = f"from={tx_from}"
    to_tag = f"to={tx_to}"

    new_val_str = f"{tx['value']},{tx['hash']}"
    new_val = bytes(new_val_str, 'utf-8')

    ci = pack_chain_info(ETH_KEY, TX_OUT_EDGE_KEY, 0);
    new_key = dbkey_t(ci, from_b_key, to_c_key)

    tags = ["ETH", "tx-out-edge", "tx-edge", from_tag, to_tag, "MERGE_STRATEGY=FORCE_MERGE", ""]
    Pando.make_new_entry_lt_10_tags(access, tags, new_val, new_key)
    Pando.add_tag(access, filter_done_tag);

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)
    if filter_done_tag in tags or filter_fail_tag in tags:
        return False
    if "ETH" in tags and "tx" in tags:
        return True

    return False
