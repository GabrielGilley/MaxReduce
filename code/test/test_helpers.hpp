void wait(vector<ZMQAddress> addrs) {
    size_t sleep_time = 100;
    this_thread::sleep_for(chrono::milliseconds(sleep_time));

    for (auto & addr : addrs) {
        ParDBClient c { addr };
        while (c.processing()) {
            this_thread::sleep_for(chrono::milliseconds(sleep_time));
        }
    }
}
