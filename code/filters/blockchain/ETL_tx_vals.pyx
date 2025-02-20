#!python
#cython: language_level=3

import os
import sys
import traceback
from datetime import datetime, timezone

from filter_lib cimport *
from cython.cimports.libc.stdlib import free

cimport Pando

FILTER_NAME = "ETL_tx_vals"
cdef public void fail_(const DBAccess* access, message):
    """
    Print an out an output message and then add a fail tag to the filter.
    """
    print(f"[ERROR] unable to run filter `{FILTER_NAME}`: {message}")
    filter_fail_tag = f"{FILTER_NAME}:fail"
    Pando.add_tag(access, filter_fail_tag)


cdef public void run_internal(const DBAccess* access):
    """
    Provide the main logic for the `python_ETL_tx_vals` filter.
    Fundamentally, this filter has the following sections:
        1. Identify the transaction hash from the token transaction.
        2. Look up the timestamp of that transaction and convert it to midnight UTC.
        3. Create a new entry for the value of that token transaction by converting it to wei
        4. Identify the exchange rate of the token for the given day.
        5. Create a new entry for the USD value (`token_tx_value * exchange_rate`) of the token.
    """

    # First we should extract all the data from our passed in value
    # This will provide the transaction hash, token contract address, and value (in wei)
    data = {}
    acc_v = access.value.decode('utf-8').split('\n')
    for line in acc_v[:-1]:
        key, value = line.split(":")
        data[key] = value

    tx_hash = data["transaction_hash"]

    # Now we can find the associated transaction timestamp
    chain_info = pack_chain_info(ETH_KEY, TXTIME_KEY, 0)

    # Take the first 15 bytes of the TX hash string (including the 0x) and convert it to an int
    tx_hash_key = int(tx_hash[:15], 16)
    tx_key = dbkey_t(chain_info, tx_hash_key, 0)

    ret = Pando.get_entry_by_key(access, tx_key)

    # Now `ret` should have the value of the TX hash time
    # If ret is not null, we need to make it a timestamp
    # If it is null, than we should subscribe to the entry
    inactive_tag = bytes(f"{FILTER_NAME}:inactive", 'utf-8')
    if not ret:
        Pando.add_tag(access, inactive_tag)
        Pando.subscribe_to_entry(access, access.key, tx_key, inactive_tag)
        return

    # Once we have the transaction timestamp we need to get
    # the unix timestamp date as midnight (e.g., hour 0) UTC time
    full_time = datetime.fromtimestamp(int(ret))
    timestamp = int(datetime(full_time.year, full_time.month, full_time.day, 0, tzinfo=timezone.utc).timestamp())


    # If we are sure that the transaction has a timestamp, we can create
    # a new entry for the token value
    value = data["value"]

    value_wei = float(data["value"])
    wei_to_eth = float(pow(10, 18))
    value = value_wei / wei_to_eth

    new_tags = ["ETL", "out_val_etl"]
    ci = pack_chain_info(ETL_TX_TOKEN_KEY, OUT_VAL_KEY, 0)
    new_key = dbkey_t(ci, timestamp, tx_hash_key)
    Pando.make_new_entry_lt_3_tags(access, new_tags, str(value), new_key)

    ###########################################################################
    # Now that we have created a new entry for the TX token and the amount of
    # tokens transferred, we need to identify the correct Token identifier
    ###########################################################################

    token_info_key = pack_chain_info(ETH_KEY, ETL_TOKEN_KEY, 0)

    # Take the first 17 bytes of the Token address string (excluding the 0x)
    token_key = dbkey_t(int(token_info_key), int(data["token_address"][:17], 16), 0)

    token_ret = Pando.get_entry_by_key(access, token_key)
    if not token_ret:
        Pando.add_tag(access, inactive_tag)
        Pando.subscribe_to_entry(access, access.key, token_key, inactive_tag)
        return

    # Token Data
    token_data = {}

    token_ret_val = token_ret.decode('utf-8')
    for line in token_ret_val.split("\n"):
        try:
            key, tvalue = line.split(":")
            token_data[key] = tvalue
        except ValueError:
            # There was not a ":" in the string and we can ignore this entry
            pass

    # Now we have access to the token symbol, time stamp, and amount
    # We now need to look up the exchange rate
    # The key for this entry is the date without hour/min/sec as a Unix integer time stamp

    exchange_info = pack_chain_info(ETL_TOKEN_KEY, EXCHANGE_RATE_KEY, 0)
    exchange_key = dbkey_t(exchange_info, timestamp, 0)

    # Get the exchange entry
    usd_ret = Pando.get_entry_by_key(access, exchange_key)
    if not usd_ret:
        Pando.add_tag(access, inactive_tag)
        Pando.subscribe_to_entry(access, access.key, exchange_key, inactive_tag)
        return

    # Get the exchange rate value for the given token symbol
    usd_ex_val = 0
    usd_ret_str = usd_ret.decode('utf-8')
    for line in usd_ret_str.split("\n"):
        try:
            token, tvalue = line.split(":")
            if token == token_data["symbol"]:
                usd_ex_val = tvalue
                break
        except ValueError:
            # We can ignore non-conforming lines
            pass

    if not token_data:
        fail_(access, str(f"No token data identified on date {timestamp}."))
        return

    if not usd_ex_val:
        fail_(access, str(
            f"No value for: `{token_data['symbol']}` "
            f"on date {timestamp}.")
        )
        return

    # Calculate the value
    usd_val = float(usd_ex_val) * float(value)

    # Now we have the value for the token in USD
    new_tags = ["USD", "out_val_usd"]
    new_key = INITIAL_KEY
    Pando.make_new_entry_lt_3_tags(access, new_tags, str(usd_val), new_key)

    # add_tag to current token TX
    Pando.add_tag(access, "ETL_tx_vals:done")

cdef public bint should_run_internal(const DBAccess* access):
    tags = Pando.get_tags(access)

    if "ETL_tx_vals:done" in tags or "ETL_tx_vals:fail" in tags or "ETL_tx_vals:inactive" in tags:
        return False

    if "ethereum_etl_token_transfer" in tags:
        return True

    return False
