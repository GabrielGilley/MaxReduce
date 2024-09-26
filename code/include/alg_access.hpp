#pragma once

#include "types.h"

#include "pack.hpp"

#include <string>
#include <unordered_map>
#include <memory>
#include <vector>
#include <cstdint>

using namespace std;

namespace pando {

/** @brief Holds data used in messages
 *
 * This consists of the size and data for a message. It can be easily used with
 * a few simple types, but the raw data can be set to any value.
 *
 * Size is expected to match the size of the data.
 *
 * Note that this is copy-on-write after resizing.
 */
class MData {
    private:
        /** Reallocate to hold the given size */
        void allocate_() {
            data_ = shared_ptr<char>(new char[size_], default_delete<char[]>());
        }
        /** Hold the size */
        size_t size_;
        /** Hold the actual data */
        shared_ptr<char> data_;
    public:
        /** @brief Construct a new empty data storage */
        MData() : size_(0) {}
        /** @brief Construct a data storage from an object
         *
         * @tparam T the object to copy in
         * */
        template<class T>
        MData(T t) : size_(sizeof(T)) {
            allocate_();
            T* v = (T*)get();
            *v = t;
        }
        /** @brief Replace from a string and copy in the string's contents */
        void from_string(const string s) {
            resize(s.size());
            s.copy(get(), s.size());
        }
        /** @brief Resize to the new given size */
        void resize(size_t s) {
            size_ = s;
            allocate_();
        }
        /** @brief Return the size of the message data */
        size_t size() { return size_; }
        /** @brief Return the size of the serialized message */
        size_t serialized_size() { return sizeof(size())+size(); }
        /** @brief Return a pointer to the message data */
        char* get() { return data_.get(); }
        /** @brief Serialize into an existing character array */
        void serialize(char*& buffer) {
            size_t s = size();
            elga::pack_single(buffer, s);

            char* data = get();
            memcpy(buffer, data, s);
            buffer += s;    // FIXME test
        }
        /** @brief Deserialize into a new MData entry */
        void from_serialized(const char*& ser) {
            size_t s;
            elga::unpack_single(ser, s);

            resize(s);

            char* data = get();
            memcpy(data, ser, s); // FIXME test
            ser += s;
        }
        /** @brief Return the value */
        template<class T>
        T getval() { return *(T*)get(); }
};
typedef size_t itkey_t;
typedef unordered_map<vtx_t, vector<MData>> itmsg_t;
typedef unordered_map<itkey_t, itmsg_t> msg_t;
typedef chain_info_t graph_t;

}

using namespace pando;

struct DBAccess;

enum GroupState {
    ACTIVE,     // Run when visited
    INACTIVE    // Do not run when visited
};

class GraphEdge {
    public:
        vtx_t n;
        const DBAccess* acc;
        GraphEdge(vtx_t new_n, const DBAccess* new_acc) : n(new_n), acc(new_acc) {}
};

//FIXME no need for this to be in its own class anymore
class GraphEntry {
    public:
        vector<GraphEdge> out;
};

class GroupAccess {
    public:
        vtx_t vtx;
        GraphEntry &entry;

        msg_t* in_msgs;
        msg_t* out_msgs;
        enum GroupState state = ACTIVE;
        itkey_t iter = 0;

        GroupAccess(vtx_t v, GraphEntry& e, msg_t* im, msg_t* om) : vtx(v), entry(e), in_msgs(im), out_msgs(om) {}
};

