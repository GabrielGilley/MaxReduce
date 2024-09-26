#include "helpers.hpp"
#include <sstream>

using namespace std;
using namespace pando;
using namespace rapidjson;

template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_name, const char* filter_fail_tag, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

bool should_run_btc_based_tx_out_edges(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag, const char* filter_inactive_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_block = false;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_crypto = true;
        if (string(*tags) == "tx")
            found_block = true;
        if (string(*tags) == filter_inactive_tag)
            return false;
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

void btc_based_tx_out_edges(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_done_tag, const char* filter_fail_tag, const char* filter_inactive_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);
	
    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, filter_name, filter_fail_tag, "cannot parse");

    // Find the transactions in the block
    if (!d.HasMember("tx")) return fail_(access, filter_name, filter_fail_tag, "no member tx");
    auto &tx = d["tx"];
    if (!tx.IsObject()) return fail_(access, filter_name, filter_fail_tag, "tx is not an object");
    if (!tx.HasMember("txid")) return fail_(access, filter_name, filter_fail_tag, "txid not found");
    auto &txid = tx["txid"];
    if (!txid.IsString()) return fail_(access, filter_name, filter_fail_tag, "txid is not a string");

    // Get the src ID (stored in the txid DB entry)
    string src = txid.GetString();
    string src_id = "from=" + src;

    // In a BTC transaction, the txid is the source
    // We are only outputting "out" edges
    // So, we need to get and output each destination
    // We know that inputs have already been processed, so every edge already exists
    // We need to add values to them, and output unspent transactions
    if (!tx.HasMember("vout")) return fail_(access, filter_name, filter_fail_tag, "no vout object");
    auto &vouts = tx["vout"];
    if (!vouts.IsArray()) return fail_(access, filter_name, filter_fail_tag, "vout is not an array");

    bool has_utxo = false;

    for (auto & vout: vouts.GetArray()) {
        // Find the position
        if (!vout.HasMember("n")) return fail_(access, filter_name, filter_fail_tag, "no 'n' in vout");
        if (!vout["n"].IsInt()) return fail_(access, filter_name, filter_fail_tag, "'n' is not an int");
        int n = vout["n"].GetInt();
        string n_str = to_string(n);
        n_str = "n=" + n_str;

        // Find the existing edge
        // This will be tagged with "src","vout-n" value will be the "dst"
        chain_info_t search_chain_info = pack_chain_info(crypto_id, TX_IN_EDGE_KEY, 0);
        vtx_t search_c_key = n;
        vtx_t search_b_key;
        stringstream stream;
        stream << src.substr(0,15);
        stream >> hex >> search_b_key;
        dbkey_t search_key = {search_chain_info, search_b_key, search_c_key};

        char* dst_id_ret = nullptr;
        access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &dst_id_ret);
        string dst_id;
        bool is_utxo = false;
        if (dst_id_ret != nullptr) {
            dst_id.assign(dst_id_ret);
            free(dst_id_ret);
        } else {
            //Might be a UTXO, or the in edge may not be processed yet. Label
            //it as a UTXO, but mark it as inactive so that if/when the in-edge
            //gets processed, this runs again and gets updated.
            dst_id = "UTXO";
            has_utxo = true;
            is_utxo = true;
            access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, filter_inactive_tag);
        }
        string dst_id_tag = "to=" + dst_id;

        // Find the (currency) value
        if (!vout.HasMember("value")) return fail_(access, filter_name, filter_fail_tag, "no value found in vout");
        if (!vout["value"].IsDouble()) return fail_(access, filter_name, filter_fail_tag, "value is not an int");
        double value = vout["value"].GetDouble();
        string value_str = to_string(value);

        // Create the output edge
        // Create new DB entries for each transaction ID
        // FIXME does this need to be only the first 64 bits?
        const char* new_value = value_str.c_str();

        //create the new key
        chain_info_t crypto_key;
        vtx_t n_key;
        if (is_utxo) {
            crypto_key = pack_chain_info(crypto_id, TX_OUT_EDGE_KEY, UTXO_KEY);
            n_key = n;
        } else {
            crypto_key = pack_chain_info(crypto_id, TX_OUT_EDGE_KEY, NOT_UTXO_KEY);
            stringstream ss;
            ss << hex << dst_id.substr(0,15);
            ss >> n_key;
        }
        vtx_t src_key;
        stringstream ss;
        ss << src.substr(0,15);
        ss >> hex >> src_key;

        // "n_key" is the "n" value if UTXO, but dst_tx_id if not a utxo
        dbkey_t new_key {crypto_key, src_key, n_key};

        const char* new_tags[] = {crypto_tag, "tx-out-edge", src_id.c_str(), dst_id_tag.c_str(), ""};
        access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);

    }

    if (has_utxo) {
        access->add_tag.run(&access->add_tag, filter_inactive_tag);
    }
    else {
        // Add a tag indicating parsing was successful
        access->add_tag.run(&access->add_tag, filter_done_tag);
    }

}

