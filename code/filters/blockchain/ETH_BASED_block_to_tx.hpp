// Abstracted version of ETH_block_to_tx
#include "helpers.hpp"

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

bool should_run_eth_based_block_to_tx(const DBAccess* access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {
    auto tags = access->tags;
    bool found_name = false;
    bool found_block = false;

    //make sure the entry includes the abbreviation and "block" but not done or fail tags
    while ((*tags)[0] != '\0') {
        if (string(*tags) == crypto_tag)
	    found_name = true;
	if (string(*tags) == "block")
	    found_block = true;
	if (string(*tags) == filter_done_tag)
	    return false;
	if (string(*tags) == filter_fail_tag)
	    return false;
	++tags;
    }
    if (found_block && found_name)
	return true;
    return false;
}


void eth_based_block_to_tx(const DBAccess *access, const char* crypto_tag, const char* filter_done_tag, const char* filter_fail_tag) {    
    // The value should be a valid JSON string
    Document d;
    d.Parse(access->value);

    if (d.HasParseError()) return fail_(access, filter_fail_tag, "Parse Error");

    // Find the transactions in the block
    if (!d.HasMember("transactions")) {
        return fail_(access, filter_fail_tag, "No member \"transactions\"");
    }
    auto &txs = d["transactions"];
    if (!txs.IsArray()) return fail_(access, filter_fail_tag, "\"transactions\" was not an array");
    
	// Iterate through each transaction
    for (auto & tx : txs.GetArray()) {
        Document n;
        n.SetObject();

        Document::AllocatorType& allocator = n.GetAllocator();

        n.AddMember("transactions", tx, allocator);

        StringBuffer buf;
        PrettyWriter<StringBuffer> writer { buf };
        n.Accept(writer);

        // Create new DB entries for each transaction
        const char* new_tags[] = {crypto_tag, "tx", ""};
        const char* new_value = buf.GetString();

        access->make_new_entry.run(&access->make_new_entry, new_tags, new_value, INITIAL_KEY);
    }

    // Add a tag indicating parsing was successful
    access->add_tag.run(&access->add_tag, filter_done_tag);
    
    return;
}

