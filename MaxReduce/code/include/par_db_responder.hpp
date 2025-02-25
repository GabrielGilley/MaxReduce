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
class PandoResponder : public PandoParticipant {
    private:
        RandomKeyGen random_key_gen_;
        PandoMapClient db_;
    public:
        /** @brief Initialize the proxy without joining as an agent */
        PandoResponder(ZMQAddress addr) : 
                PandoParticipant(addr, false),
                random_key_gen_(addr_ser_), 
                db_(addr+MAP_LOCALNUM_OFFSET-RESPONDER_LOCALNUM_OFFSET, addr) {}

        PandoResponder(ZMQAddress addr, size_t) : PandoResponder(addr) {}

        /** @brief Handle specific, custom messages */
        virtual void process_msg(msg_type_t type, zmq_socket_t sock, const char *data, [[maybe_unused]] const char* end) {
            if (type == GET_ENTRY_BY_KEY)
                recv_get_entry_by_key(sock, data, end);
            else recv_unknown_msg();
        }

        void recv_get_entry_by_key(zmq_socket_t sock, [[maybe_unused]] const char* data, [[maybe_unused]] const char* end) {
            dbkey_t key;
            unpack_single(data, key);

            string value = "";
            if (db_.key_exist(key)) {
                auto entry = db_.retrieve(key);
                value = entry.value();
            }

            size_t msg_size = value.size(); //add 1 for null terminator
            char* msg = new char[msg_size];
            char *msg_ptr = msg;

            pack_string(msg_ptr, value);

            send(sock, msg, msg_size);

            delete [] msg;
        }
};

}
