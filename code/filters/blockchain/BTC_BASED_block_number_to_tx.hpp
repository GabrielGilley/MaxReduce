#include <sstream>
#include "helpers.hpp"

using namespace std;
using namespace pando;
using namespace rapidjson;


template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_fail_tag, const char* filter_name, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void btc_based_block_number_to_tx(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_fail_tag, const char* filter_done_tag) {
    // Intended to be able to look up a block number by txid
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);

    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) {
        cout << access->value << endl;
        cout << "ERROR: " << GetParseError_En(d.GetParseError()) << endl;

        return fail_(access, filter_fail_tag, filter_name, "parse error");
    }

    // Extract the block number
    if (!d.HasMember("height")) return fail_(access, filter_fail_tag, filter_name, "no member \"height\"");
    auto &block_num_raw = d["height"];
    if (!block_num_raw.IsInt()) return fail_(access, filter_fail_tag, filter_name, "height was not an int");
    int block_number = block_num_raw.GetInt();
    string block_number_str = to_string(block_number);

    // Find the transactions in the block
    if (!d.HasMember("tx")) return fail_(access, filter_fail_tag, filter_name, "no member \"tx\"");
    auto &txs = d["tx"];
    if (!txs.IsArray()) return fail_(access, filter_fail_tag, filter_name, "txs was not an array");

    // Iterate through each transaction
    for (auto & tx : txs.GetArray()) {
        if (!tx.HasMember("txid")) return fail_(access, filter_fail_tag, filter_name, "no member \"txid\"");
        auto &txid_raw = tx["txid"];
        if (!txid_raw.IsString()) return fail_(access, filter_fail_tag, filter_name, "txid was not a string");
        string txid = txid_raw.GetString();

        stringstream ss;
        vtx_t txid_key;
        ss << txid.substr(0, 15);
        ss >> hex >> txid_key;

        // Create new DB entries for each transaction
        const char* new_tags[] = {crypto_tag, "BLOCK_NUMBER_LOOKUP", ""};
        chain_info_t ci = pack_chain_info(crypto_id, BLOCK_NUMBER_LOOKUP_KEY, 0);
        dbkey_t new_key {ci, txid_key, 0};

        access->make_new_entry.run(&access->make_new_entry, new_tags, block_number_str.c_str(), new_key);
    }

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}


bool should_run_btc_based_block_number_to_tx(const DBAccess* access, const char* crypto_tag, const char* filter_fail_tag, const char* filter_done_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_block = false;

    // make sure the entry includes the crypto name and "block" but not
    // the fail or done tags
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
	    found_crypto = true;
	if (string(*tags) == "block")
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
