#include "helpers.hpp"
#include <sstream>

using namespace std;
using namespace pando;
using namespace rapidjson;

template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_fail_tag, Args && ...args) {
    cerr << "[ERROR] unable to parse JSON in entry: ";
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void eth_based_block_to_txtime(const DBAccess *access, const char* crypto_tag, const char* filter_fail_tag, const char* filter_done_tag) {
    const uint32_t crypto_id = get_blockchain_key(crypto_tag);
    // the value should be a valid JSON string
    Document d;
    d.Parse(access->value);
    if (d.HasParseError()) return fail_(access, filter_fail_tag, "No member \"transactions\"");
    auto &txs = d["transactions"];
    if (!txs.IsArray()) return fail_(access, filter_fail_tag, "\"transactions\" was not an array");
    
    // Get the time
    if (!d.HasMember("timestamp")) return fail_(access, filter_fail_tag, "No member \"timestamp\"");

    // Timestamp for eth_based blockchains is a hex string
    if (!d["timestamp"].IsString()) return fail_(access, filter_fail_tag, "\"timestamp\" was not a string");
    string timestamp_str = d["timestamp"].GetString();
    stringstream timestamp_ss;
    timestamp_ss << hex << timestamp_str;
    uint32_t ts_int;
    timestamp_ss >> ts_int;
    string ts_str = to_string(ts_int);
    // Iterate through each transaction
    for (auto & tx : txs.GetArray()) {
        // Get the tx ID
	auto txid = tx["hash"].GetString();
        
        // Create new DB entries for each transaction
	const char* new_tags[] = {crypto_tag, "txtime", txid, ""};
        const char* new_value = ts_str.c_str();
        chain_info_t chain_info = pack_chain_info(crypto_id, TXTIME_KEY, 0);
        vtx_t src_key;
        string txid_s; txid_s.assign(txid);
        stringstream ss;
        ss << txid_s.substr(0,15);
	ss >> hex >> src_key;
	dbkey_t new_key { chain_info, src_key, 0 };
	access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, new_key);
    }
     access->add_tag.run(&access->add_tag, filter_done_tag);
}


bool should_run_eth_based_block_to_txtime(const DBAccess* access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    auto tags = access->tags;
    bool found_crypto = false;
    bool found_block = false;

    // make sure the entry includes the crypto tag and "block" but not
    // fail tags or done tags
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

