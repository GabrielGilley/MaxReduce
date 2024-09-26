#include "helpers.hpp"
#include <sstream>

using namespace std;
using namespace pando;
using namespace rapidjson;


void fail_(const DBAccess *access, const char* filter_name, const char* filter_fail_tag) {
    cerr << "[ERROR] unable to run filter: " << filter_name << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool rollback([[maybe_unused]] const DBAccess *access) {return true;}

bool should_run_btc_based_tx_in_edges(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_block = false;

    //make sure the entry includes "BTC" or "ZEC" and "tx" but not
    //"BTC_tx_in_edges:done" or "BTC_tx_in_edges:fail"
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_crypto = true;
        if (string(*tags) == "tx")
            found_block = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_crypto && found_block)
        return true;
    return false;
}

void btc_based_tx_in_edges(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_done_tag, const char* filter_fail_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);

    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, filter_name, filter_fail_tag);

    // Find the transactions in the block
    if (!d.HasMember("tx")) return fail_(access, filter_name, filter_fail_tag);
    auto &tx = d["tx"];
    if (!tx.IsObject()) return fail_(access, filter_name, filter_fail_tag);
    if (!tx.HasMember("txid")) return fail_(access, filter_name, filter_fail_tag);
    auto &txid = tx["txid"];
    if (!txid.IsString()) return fail_(access, filter_name, filter_fail_tag);

    const char * new_value = txid.GetString(); //TODO only a portion of the ID

    if (!tx.HasMember("vin")) return fail_(access, filter_name, filter_fail_tag);
    auto &vins = tx["vin"];
    if (!vins.IsArray()) return fail_(access, filter_name, filter_fail_tag);

    for (auto& vin : vins.GetArray()) {
        if (vin.HasMember("coinbase")) {
            //Coinbase transaction
            const char* coinbase_tags[] = {crypto_tag, "tx-in-edge", "from=COINBASE", "n=0", ""};
            access->make_new_entry.run(&access->make_new_entry, coinbase_tags, new_value, INITIAL_KEY);
        } else {
            //Non-coinbase transaction
            if (!vin["vout"].IsInt() || !vin["txid"].IsString()) {
                fail_(access, filter_name, filter_fail_tag);
                continue;
            }

            // Pull the txid and vout from the "vin" object
            auto vin_txid = vin["txid"].GetString();
            int n = vin["vout"].GetInt();
            string n_str = to_string(n);
            n_str = "n=" + n_str;

            string src_id;
            src_id.assign(vin_txid);
            string src_id_tag = "from=" + src_id;

            //set keys and make new entry
            const char* new_tags[] = {crypto_tag, "tx-in-edge", src_id_tag.c_str(), n_str.c_str(), ""};
            chain_info_t crypto_key = pack_chain_info(crypto_id, TX_IN_EDGE_KEY, 0);
            vtx_t n_key = n;
            vtx_t src_key;
            stringstream ss;
            ss << src_id.substr(0,15);
            ss >> hex >> src_key;
            dbkey_t new_key {crypto_key, src_key, n_key};
            access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

        }

    }

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}


