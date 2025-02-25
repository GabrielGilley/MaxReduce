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

bool should_run_btc_based_vout_addrs(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_tx = false;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_crypto = true;
        if (string(*tags) == "tx")
            found_tx = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_crypto && found_tx)
        return true;
    return false;
}

void btc_based_vout_addrs(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_done_tag, const char* filter_fail_tag) {
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
    auto &txid_raw = tx["txid"];
    if (!txid_raw.IsString()) return fail_(access, filter_name, filter_fail_tag, "txid is not a string");
    string txid = txid_raw.GetString();
    stringstream ss;
    ss << txid.substr(0,15);
    vtx_t txid_key;
    ss >> hex >> txid_key;

    if (!tx.HasMember("vout")) return fail_(access, filter_name, filter_fail_tag, "no vout object");
    auto &vouts = tx["vout"];
    if (!vouts.IsArray()) return fail_(access, filter_name, filter_fail_tag, "vout is not an array");

    for (auto & vout: vouts.GetArray()) {
        // Find the position
        if (!vout.HasMember("n")) return fail_(access, filter_name, filter_fail_tag, "no 'n' in vout");
        if (!vout["n"].IsInt()) return fail_(access, filter_name, filter_fail_tag, "'n' is not an int");
        int n = vout["n"].GetInt();
        vtx_t n_key = n;

        // Now, pull out address information to link an address to this tx_output
        // REMEMBER! Not all transactions have associated addresses, that's fine
        if (!vout.HasMember("scriptPubKey")) continue;
        auto &scriptPubKey = vout["scriptPubKey"];
        if (!scriptPubKey.IsObject()) return fail_(access, filter_name, filter_fail_tag, "\"scriptPubKey\" was not an object");
        if (!scriptPubKey.HasMember("addresses")) continue;
        auto &addrs = scriptPubKey["addresses"];
        if (!addrs.IsArray()) return fail_(access, filter_name, filter_fail_tag, "\"addresses\" was not an array");
        chain_info_t ci = pack_chain_info(crypto_id, VOUT_ADDR_KEY, 0);
        dbkey_t new_key {ci, txid_key, n_key};
        string new_val = "";
        bool first = true;
        for (auto & addr : addrs.GetArray()) {
            if (!first) {
                new_val += "\n";
            }
            if (!addr.IsString()) return fail_(access, filter_name, filter_fail_tag, "address was not a string");
            string address = addr.GetString();
            new_val += address;
            first = false;

        }

        const char* new_tags[] = {crypto_tag, "VOUT_ADDRESS", ""};
        access->make_new_entry.run(&access->make_new_entry, new_tags, new_val.c_str(), new_key);

    }

    access->add_tag.run(&access->add_tag, filter_done_tag);

}

