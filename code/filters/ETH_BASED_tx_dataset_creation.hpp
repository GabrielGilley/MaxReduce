#include <iostream>

#include "filter.hpp"
#include "dbkey.h"
#include <boost/algorithm/string.hpp>


#include <string>
#include <sstream>
#include <iomanip>

using namespace std;
using namespace pando;


template<typename ...Args>
void fail_(const DBAccess *access, const char* filter_fail_tag, const char* filter_name, Args && ...args) {
    cerr << "[ERROR] unable to run filter: " << filter_name;
    (cerr << " " << ... << args);
    cerr << endl;
    access->add_tag.run(&access->add_tag, filter_fail_tag);
}

void eth_based_tx_dataset_creation(const DBAccess *access, const char* crypto_tag, const char* filter_name, const char* filter_fail_tag, const char* filter_done_tag) {
    const uint32_t crypto_key = get_blockchain_key(crypto_tag);

    // define tags for all entry types that will be created
    const char* new_tags_addr_to_input[] = {crypto_tag, "address_to_tx_inputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_addr_to_output[] = {crypto_tag, "address_to_tx_outputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_input[] = {crypto_tag, "tx_inputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_output[] = {crypto_tag, "tx_outputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_input[] = {crypto_tag, "tx_to_inputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_tx_to_output[] = {crypto_tag, "tx_to_outputs.csv", "CROSSCHAIN_DATASET", ""};
    const char* new_tags_addr[] = {crypto_tag, "address.csv", "CROSSCHAIN_DATASET", ""};

    stringstream tx_input_id_ss;
    stringstream tx_output_id_ss;
    
    // the "b" key is the "from", the "c" key is the "to"
    tx_input_id_ss << access->key.a << access->key.b;
    tx_output_id_ss << access->key.a << access->key.c;
    string tx_input_id = tx_input_id_ss.str();
    string tx_output_id = tx_output_id_ss.str();

    // add the value as the second field. However, there could be multiple
    // lines of value, so iterate over and create a new entry for each
    vector<string> values;
    boost::split(values, access->value, boost::is_any_of("\n"));

    vector<tuple<string,string>> values_and_hashes;

    for (auto& line : values) {
        vector<string> val_parts;
        boost::split(val_parts, line, boost::is_any_of(","));
        string value = val_parts[0];

        values_and_hashes.emplace_back(val_parts[0], val_parts[1]);

        string tx_input_val = tx_input_id_ss.str() + "," + value;
        string tx_output_val = tx_output_id_ss.str() + "," + value;

        access->make_new_entry.run(&access->make_new_entry, new_tags_input, tx_input_val.c_str(), INITIAL_KEY);
        access->make_new_entry.run(&access->make_new_entry, new_tags_output, tx_output_val.c_str(), INITIAL_KEY);
        
    }

    // Get the "from" and "to" addresses and tx hash from the tags
    string from;    bool from_found = false;
    string to;      bool to_found = false;
    auto tags = access->tags;
    while ((*tags)[0] != '\0') {
        string tag = string(*tags);
        if (tag.size() > 3 && tag.substr(0, 3) == "to=") {
            to = tag.substr(3, tag.size());
            to_found = true;
        }
        if (tag.size() > 5) {
            if (tag.substr(0, 5) == "from=") {
                from = tag.substr(5, tag.size());
                from_found = true;
            }
        }
        ++tags;
    }
    if (!from_found) return fail_(access, filter_fail_tag, filter_name, "could not parse \"from\" field from tags");
    if (!to_found) return fail_(access, filter_fail_tag, filter_name, "could not parse \"to\" field from tags");

    // create the address.csv entries
    chain_info_t addr_ci = pack_chain_info(crypto_key, ADDR_KEY, 0);
    // to
    stringstream to_addr_ss;
    vtx_t to_key;
    to_addr_ss << to.substr(0, 15);
    to_addr_ss >> hex >> to_key;
    dbkey_t new_addr_to_key {addr_ci, to_key, 0};
    access->make_new_entry.run(&access->make_new_entry, new_tags_addr, to.c_str(), new_addr_to_key);
    // from
    stringstream from_addr_ss;
    vtx_t from_key;
    from_addr_ss << from.substr(0, 15);
    from_addr_ss >> hex >> from_key;
    dbkey_t new_addr_from_key {addr_ci, from_key, 0};
    access->make_new_entry.run(&access->make_new_entry, new_tags_addr, from.c_str(), new_addr_from_key);

    stringstream addr_to_input_ss;
    addr_to_input_ss << from << "," << tx_input_id;
    string addr_to_input = addr_to_input_ss.str();

    stringstream addr_to_output_ss;
    addr_to_output_ss << to << "," << tx_output_id;
    string addr_to_output = addr_to_output_ss.str();

    for (size_t i = 0; i < values.size(); ++i) {
        access->make_new_entry.run(&access->make_new_entry, new_tags_addr_to_input, addr_to_input.c_str(), INITIAL_KEY);
        access->make_new_entry.run(&access->make_new_entry, new_tags_addr_to_output, addr_to_output.c_str(), INITIAL_KEY);
    }

    for (auto & [val, tx_hash] : values_and_hashes) {
        stringstream tx_to_input_ss;
        stringstream tx_to_output_ss;
        tx_to_input_ss << tx_hash << "," << tx_input_id; 
        tx_to_output_ss << tx_hash << "," << tx_output_id; 
        string tx_to_input = tx_to_input_ss.str();
        string tx_to_output = tx_to_output_ss.str();

        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_input, tx_to_input.c_str(), INITIAL_KEY);
        access->make_new_entry.run(&access->make_new_entry, new_tags_tx_to_output, tx_to_output.c_str(), INITIAL_KEY);

    }

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
}
bool should_run_eth_based_tx_dataset_creation(const DBAccess *access, const char* crypto_tag, const char* filter_fail_tag, const char* filter_done_tag) {
    auto tags = access->tags;
    bool found_eth = false;
    bool found_edge = false;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
            found_eth = true;
        if (string(*tags) == "tx-edge")
            found_edge = true;
        if (string(*tags) == filter_done_tag)
            return false;
        if (string(*tags) == filter_fail_tag)
            return false;
        ++tags;
    }
    if (found_eth && found_edge)
        return true;
    return false;
}
