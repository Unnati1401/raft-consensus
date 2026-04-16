#include <iostream>
#include <vector>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <memory>
#include <thread>
#include <mutex>
#include <random>
#include <fstream>

#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

// Helper to run a single client thread
void client_worker(const std::vector<std::string>& kv_addrs, int num_ops, int put_ratio, std::vector<double>& out_latencies) {
    kv::KvClient client(kv_addrs);
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> key_dist(1, 1000);
    std::uniform_int_distribution<> op_dist(1, 100);

    for (int i = 0; i < num_ops; ++i) {
        std::string key = "key_" + std::to_string(key_dist(gen));
        bool is_put = op_dist(gen) <= put_ratio;

        auto start = std::chrono::steady_clock::now();
        if (is_put) {
            client.put(key, "bench_val");
        } else {
            client.get(key);
        }
        auto end = std::chrono::steady_clock::now();

        std::chrono::duration<double, std::milli> diff = end - start;
        out_latencies.push_back(diff.count());
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: ./tput <MaxClientCount> <PutRatio>\n";
        return 1;
    }

    int max_clients = std::stoi(argv[1]);
    int put_ratio = std::stoi(argv[2]);
    const int ops_per_client = 1000;

    rafty::utils::init_logger();
    auto logger = spdlog::default_logger();

    std::ofstream out_file("result.txt");
    out_file << "##################################################################################\n";
    out_file << "# clientCount      latAvg       latP50       latP90       latP99        throughput\n";
    out_file << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
    out_file << "----------------------------------------------------------------------------------\n";

    for (int num_clients = 1; num_clients <= max_clients; num_clients *= 2) {
        // Setup 3-replica cluster
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
        auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
            configs, node_tester_ports, "./kv_node", "0.0.0.0:55000", 0, 0, logger, ddb_conf);
        
        ctrl->register_applier_handler([](testerpb::ApplyResult m) -> void {(void)m; });
        
        ctrl->run();
        std::this_thread::sleep_for(std::chrono::seconds(2)); // Wait for election

        std::vector<std::string> kv_addrs;
        for (int i = 0; i < num_nodes; i++) kv_addrs.push_back("localhost:" + std::to_string(start_port + 1000 + i));

        // Pre-populate keyspace
        std::cout << "Pre-populating keys for " << num_clients << " client(s)..." << std::endl;
        kv::KvClient pop_client(kv_addrs);
        for (int i = 1; i <= 1000; ++i) {
            pop_client.put("key_" + std::to_string(i), "init_val");
        }

        std::cout << "Running benchmark with " << num_clients << " client(s)..." << std::endl;
        
        std::vector<std::vector<double>> all_latencies(num_clients);
        for(auto& lat_vec : all_latencies) lat_vec.reserve(ops_per_client);

        std::vector<std::thread> threads;
        
        auto start_time = std::chrono::steady_clock::now();
        for (int i = 0; i < num_clients; ++i) {
            threads.emplace_back(client_worker, std::cref(kv_addrs), ops_per_client, put_ratio, std::ref(all_latencies[i]));
        }

        for (auto& t : threads) {
            t.join();
        }
        auto end_time = std::chrono::steady_clock::now();

        // Aggregate statistics
        std::vector<double> merged_latencies;
        merged_latencies.reserve(num_clients * ops_per_client);
        for (const auto& lat_vec : all_latencies) {
            merged_latencies.insert(merged_latencies.end(), lat_vec.begin(), lat_vec.end());
        }

        std::sort(merged_latencies.begin(), merged_latencies.end());
        double sum = 0;
        for (double lat : merged_latencies) sum += lat;

        double avg = merged_latencies.empty() ? 0 : sum / merged_latencies.size();
        double p50 = merged_latencies.empty() ? 0 : merged_latencies[merged_latencies.size() * 0.50];
        double p90 = merged_latencies.empty() ? 0 : merged_latencies[merged_latencies.size() * 0.90];
        double p99 = merged_latencies.empty() ? 0 : merged_latencies[merged_latencies.size() * 0.99];

        std::chrono::duration<double> duration_sec = end_time - start_time;
        int throughput = (num_clients * ops_per_client) / duration_sec.count();

        // Console and file output
        std::cout << std::fixed << std::setprecision(2);
        std::cout << std::setw(13) << num_clients << std::setw(11) << avg << std::setw(13) << p50 
                  << std::setw(13) << p90 << std::setw(13) << p99 << std::setw(16) << throughput << "\n";
                  
        out_file << std::fixed << std::setprecision(2);
        out_file << std::setw(13) << num_clients << std::setw(11) << avg << std::setw(13) << p50 
                 << std::setw(13) << p90 << std::setw(13) << p99 << std::setw(16) << throughput << "\n";

        ctrl->kill();
        std::this_thread::sleep_for(std::chrono::seconds(1)); // Cool down before next round
    }

    out_file.close();
    return 0;
}