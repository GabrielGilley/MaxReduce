#pragma once

elga::ZMQAddress get_zmq_addr(string s) {
    size_t split_point = s.find(',');
    if (split_point == string::npos) throw runtime_error("Unable to parse address");

    string ipv4 = s.substr(0, split_point);
    if (split_point+2 > s.size())
        throw runtime_error("Invalid address");
    char *end = nullptr;
    long off = strtoul(s.c_str() + split_point + 1, &end, 10);
    if (end != s.c_str() + s.size())
        throw runtime_error("ID not parsed");
    if (off < 0 || off >= local_max)
        throw runtime_error("Address ID is invalid");
    localnum_t ln = off;

    elga::ZMQAddress ret { ipv4, ln };
    return ret;
}

elga::ZMQAddress get_zmq_addr(char* s_raw) {
    string s { s_raw };
    return get_zmq_addr(s);
}
