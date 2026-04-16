#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <memory>

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

int main() {
    rafty::utils::init_logger();
    auto logger = spdlog::default_logger(); // Use default or create a specific one

    // 1. Generate configs for a 3-replica cluster
    int num_nodes = 3;
    uint64_t start_port = 50050;
    auto insts = toolings::ConfigGen::gen_local_instances(num_nodes, start_port);
    
    std::vector<rafty::Config> configs;
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    for (const auto& inst : insts) {
        std::map<uint64_t, std::string> peer_addrs;
        for (const auto& peer : insts) {
            if (peer.id == inst.id) continue;
            peer_addrs[peer.id] = peer.external_addr;
        }
        configs.push_back({inst.id, inst.listening_addr, peer_addrs});
        node_tester_ports[inst.id] = tester_port++;
    }

    toolings::DDBConfig ddb_conf{.enable_ddb = false};

    // 2. Spawn kv_node processes
    // Note: Make sure "./kv_node" exists in the same build directory
    auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, "./kv_node", "0.0.0.0:55000", 0, 0, logger, ddb_conf);

    ctrl->register_applier_handler([](testerpb::ApplyResult m) -> void {(void)m; });
    
    std::cout << "Starting cluster..." << std::endl;
    ctrl->run();
    std::this_thread::sleep_for(std::chrono::seconds(2)); // Wait for leader election

    // 3. Create a KvClient
    std::vector<std::string> kv_addrs;
    for (int i = 0; i < num_nodes; i++) {
        kv_addrs.push_back("localhost:" + std::to_string(start_port + 1000 + i));
    }
    kv::KvClient client(kv_addrs);

    // 4. Issue KV operations and measure timing
    const int num_requests = 1000;
    std::vector<double> latencies;
    latencies.reserve(num_requests);

    std::cout << "Running latency benchmark (" << num_requests << " operations)..." << std::endl;

    for (int i = 0; i < num_requests; ++i) {
        std::string key = "lat_key_" + std::to_string(i);
        std::string val = "val_" + std::to_string(i);

        auto start = std::chrono::steady_clock::now();
        auto status = client.put(key, val);
        auto end = std::chrono::steady_clock::now();

        if (status == kvpb::KV_SUCCESS) {
            std::chrono::duration<double, std::milli> diff = end - start;
            latencies.push_back(diff.count());
        } else {
            std::cerr << "Request " << i << " failed!" << std::endl;
        }
    }

    // 5. Compute stats
    std::sort(latencies.begin(), latencies.end());
    double sum = 0;
    for (double lat : latencies) sum += lat;
    
    double avg = latencies.empty() ? 0 : sum / latencies.size();
    double p50 = latencies.empty() ? 0 : latencies[latencies.size() * 0.50];
    double p99 = latencies.empty() ? 0 : latencies[latencies.size() * 0.99];

    // 6. Output format required by spec
    std::cout << "######################################\n";
    std::cout << "#      latAvg       latP50      latP99\n";
    std::cout << "#        (ms)         (ms)        (ms)\n";
    std::cout << "--------------------------------------\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << std::setw(12) << avg 
              << std::setw(13) << p50 
              << std::setw(12) << p99 << "\n";

    // 7. Cleanup
    ctrl->kill();
    return 0;
}