#include <iostream>
#include <string>
#include <sstream>
#include <cctype>
#include <vector>
#include <algorithm>
#include "filter.hpp"
#include "dbkey.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/error/en.h"

using namespace std;

float get_exchange_rate(time_t timestamp, const DBAccess *access, const char* inactive_tag, vtx_t bc_key) {
    //generate a tm object from the timestamp so we can floor the hours,
    //minute, etc. because we only care about exchange rate at the fidelity of
    //a given day
    struct tm *tm;
    tm = gmtime(&timestamp);
    tm->tm_sec = 0;
    tm->tm_min = 0;
    tm->tm_hour = 0;
    time_t ts_key = mktime(tm) - timezone;

    //create the search key
    chain_info_t ci = pack_chain_info(bc_key, EXCHANGE_RATE_KEY, 0);
    dbkey_t search_key = {ci, ts_key, 0};
    char* exchange_rate_ret = nullptr;
    string exchange_rate_str;
    access->get_entry_by_key.run(&access->get_entry_by_key, search_key, &exchange_rate_ret);
    if (exchange_rate_ret == nullptr) {
        access->add_tag.run(&access->add_tag, inactive_tag);
        access->subscribe_to_entry.run(&access->subscribe_to_entry, access->key, search_key, inactive_tag);
        return -1;
    } else {
        exchange_rate_str.assign(exchange_rate_ret);
        free(exchange_rate_ret);
    }
    float exchange_rate = stof(exchange_rate_str);
    return exchange_rate;
}

// NOTE: This isn't a great function to use in the should_run, as it would
// iterate over the tags multiple times. However, it can be useful if you're
// just checking for the existence of a single tag
bool has_tag(const DBAccess* access, string tag) {
    auto tags = access->tags;

    while ((*tags)[0] != '\0') {
        if (string(*tags) == tag)
            return true;
        ++tags;
    }
    return false;
}

void split_by_newline(string input, vector<string> * lines)
{
    size_t pos = 0;
    size_t newline_pos;

    // Split the string at newline characters
    while ((newline_pos = input.find('\n', pos)) != string::npos) {
        lines->push_back(input.substr(pos, newline_pos - pos));
        pos = newline_pos + 1; // Move past the newline character
    }
    // Add the last line (after the last newline)
    lines->push_back(input.substr(pos));
}

string convert_wgsim_id_to_key(const string & input)
{
    stringstream ss(input.substr(1));
    string part;
    vector<string> all_parts;
    while (getline(ss, part, '_'))
    {
        all_parts.push_back(part);
    }

    string important_parts = all_parts[1] + all_parts[2] + all_parts[5];
    string numeric_string;
    for (char c : important_parts)
    {
        if (isdigit(c))
        {
            numeric_string += c;
        }
    }
    return numeric_string;
}

int nucleotide_to_bits(char nucleotide)
{
    switch (nucleotide)
    {
        case 'A': return 0b00; // 00
        case 'C': return 0b01; // 01
        case 'G': return 0b10; // 10
        case 'T': return 0b11; // 11
        case 'U': return 0b11; // account for RNA
        default: {cerr << "invalid nucleotide" << endl; return -1;}
    }
}

long long sequence_to_bits(const string & sequence)
{
    long long bit_representation = 0;
    int single_bp = 0;
    for (char nucleotide : sequence)
    {
        single_bp = nucleotide_to_bits(nucleotide);
        if (single_bp == -1)
            return -1;
        bit_representation = (bit_representation << 2) | single_bp;
    }

    return bit_representation;
}
