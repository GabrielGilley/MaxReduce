#pragma once

#include "par_db_participant.hpp"

#include "pack.hpp"
#include "chatterbox.hpp"
#include "random_key_gen.hpp"

using namespace std;
using namespace elga;

namespace pando {

/** @brief Contains a parallel database proxy
 *
 * This does not participate as an agent, but can proxy into the mesh
 **/
class PandoProxy : public PandoParticipant {
    private:
        RandomKeyGen random_key_gen_;
    public:
        /** @brief Initialize the proxy without joining as an agent */
        PandoProxy(ZMQAddress addr) : PandoParticipant(addr, false), random_key_gen_(addr_ser_) {}
        PandoProxy(ZMQAddress addr, size_t) : PandoProxy(addr) {}

        /** @brief Handle specific, custom messages */
        virtual void process_msg(msg_type_t type, zmq_socket_t sock, const char *data, [[maybe_unused]] const char* end) {
            if (type == ADD_ENTRY)
                recv_add_entry(sock, data, end);
            else recv_unknown_msg();
        }

        /** @brief Insert an entry into the internal DB */
        void recv_add_entry(zmq_socket_t sock, const char* data, [[maybe_unused]] const char* end) {
            // Parse the entry to determine the owner
            allocator<char> alloc;
            DBEntry<> e {alloc, data};

            // NOTE: this can change the key, so we will rebuild the message
            // Optimization for special cases could be to only rebuild if key changes
            random_key_gen_.verify_entry_key(&e);

            // Send to the owner
            auto req = find_req(e.get_key());
            // Rebuild a new message
            size_t msg_size = sizeof(msg_type_t)+e.serialize_size();
            char* msg = new char[msg_size];
            char* msg_ptr = msg;

            elga::pack_msg(msg_ptr, ADD_ENTRY);
            e.serialize(msg_ptr);

            req->send(msg, msg_size);

            delete [] msg;

            // If necessary, respond with an acknowledgement
            if (ZMQRequester::is_reqrep_sock(sock))
                ack(sock);
        }
};

}
