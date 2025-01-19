#pragma once
#include "schema.hpp"
#include "../../shared-headers/Units.hpp"
#include "leanstore/LeanStore.hpp"
#include <vector>
#include <random>
#include <fstream>
#include <iostream>
#include <chrono>
#include <iomanip>

using namespace leanstore;
using Clock = std::chrono::high_resolution_clock;

struct RecoveryMetrics {
    // Time metrics
    double total_recovery_time_ms = 0;
    double data_verification_time_ms = 0;
    
    // Per-namespace metrics
    struct NamespaceMetrics {
        u64 expected_entries = 0;
        u64 recovered_entries = 0;
        u64 corrupted_entries = 0;
        u64 missing_entries = 0;
    };
    std::vector<NamespaceMetrics> namespace_metrics;
    
    // Overall metrics
    u64 total_entries = 0;
    u64 total_recovered = 0;
    u64 total_corrupted = 0;
    u64 total_missing = 0;
    double entries_per_second = 0;
    double mb_recovered = 0;
    double mb_per_second = 0;

    void initNamespaces(u32 num_namespaces, u64 entries_per_ns) {
        namespace_metrics.resize(num_namespaces);
        for (auto& ns_metric : namespace_metrics) {
            ns_metric.expected_entries = entries_per_ns;
        }
        total_entries = num_namespaces * entries_per_ns;
    }

    void print() const {
        std::cout << "\n=== Recovery Metrics ===\n"
                  << "Total Recovery Time: " << total_recovery_time_ms << " ms\n"
                  << "Verification Time: " << data_verification_time_ms << " ms\n\n";

        // Per-namespace statistics
        for (size_t i = 0; i < namespace_metrics.size(); i++) {
            const auto& ns = namespace_metrics[i];
            std::cout << "Namespace " << i << ":\n"
                     << "  Expected: " << ns.expected_entries << "\n"
                     << "  Recovered: " << ns.recovered_entries << "\n"
                     << "  Corrupted: " << ns.corrupted_entries << "\n"
                     << "  Missing: " << ns.missing_entries << "\n"
                     << "  Success Rate: " 
                     << std::fixed << std::setprecision(2)
                     << (ns.recovered_entries * 100.0 / ns.expected_entries) << "%\n\n";
        }

        // Overall statistics
        std::cout << "Overall Statistics:\n"
                  << "Total Entries: " << total_entries << "\n"
                  << "Successfully Recovered: " << total_recovered << "\n"
                  << "Corrupted: " << total_corrupted << "\n"
                  << "Missing: " << total_missing << "\n"
                  << "Recovery Rate: " << entries_per_second << " entries/s\n"
                  << "Data Recovered: " << mb_recovered << " MB\n"
                  << "Recovery Throughput: " << mb_per_second << " MB/s\n"
                  << "Overall Success Rate: " 
                  << (total_recovered * 100.0 / total_entries) << "%\n"
                  << "=====================\n";
    }

    void saveToCSV(const std::string& engine_name) const {
        std::ofstream csv("recovery_metrics.csv", std::ios::app);
        if (!csv.is_open()) return;

        // Write header if file is empty
        csv.seekp(0, std::ios::end);
        if (csv.tellp() == 0) {
            csv << "Engine,Namespaces,EntriesPerNS,TotalTime_ms,VerifyTime_ms,"
                << "TotalEntries,Recovered,Corrupted,Missing,"
                << "EntriesPerSec,MB_Recovered,MBPerSec,SuccessRate\n";
        }

        csv << engine_name << ","
            << namespace_metrics.size() << ","
            << namespace_metrics[0].expected_entries << ","
            << total_recovery_time_ms << ","
            << data_verification_time_ms << ","
            << total_entries << ","
            << total_recovered << ","
            << total_corrupted << ","
            << total_missing << ","
            << entries_per_second << ","
            << mb_recovered << ","
            << mb_per_second << ","
            << (total_recovered * 100.0 / total_entries) << "\n";
    }
};

DEFINE_uint32(num_namespaces, 1, "Number of namespaces to use");
DEFINE_uint64(entries_per_namespace, 1000000, "Number of entries per namespace");

class RecoveryWorkload {
private:
    std::mt19937_64 rnd;
    std::string verify_file;
    RecoveryMetrics metrics;
    const u64 total_entries;

    // Helper to generate deterministic but unique values
    u64 generateValue(u64 namespace_id, u64 key) {
        // Combine namespace and key to create unique seed
        rnd.seed(namespace_id * 1000000 + key);
        return rnd();
    }

public:
    RecoveryWorkload(const std::string& verification_file) 
        : rnd(std::random_device{}()),
          verify_file(verification_file),
          total_entries(FLAGS_num_namespaces * FLAGS_entries_per_namespace) {
        metrics.initNamespaces(FLAGS_num_namespaces, FLAGS_entries_per_namespace);
    }

    void loadData(LeanStoreAdapter<RecoveryEntry>& adapter) {
        std::ofstream verify_out(verify_file);
        if (!verify_out.is_open()) {
            throw std::runtime_error("Could not open verification file for writing");
        }

        auto start = Clock::now();
        cr::Worker::my().startTX();

        // Write format version and configuration
        verify_out << "v1\n" << FLAGS_num_namespaces << " " 
                  << FLAGS_entries_per_namespace << "\n";

        for (u32 ns = 0; ns < FLAGS_num_namespaces; ns++) {
            for (u64 i = 0; i < FLAGS_entries_per_namespace; i++) {
                RecoveryEntry::Key key{ns, i};
                u64 value = generateValue(ns, i);
                u64 checksum = RecoveryEntry::generateChecksum(ns, i, value);
                
                adapter.insert(key, [&](RecoveryEntry& entry) {
                    entry.value = value;
                    entry.checksum = checksum;
                });

                // Store for verification: namespace_id key value checksum
                verify_out << ns << " " << i << " " << value << " " << checksum << "\n";

                if ((i + 1) % 1000 == 0) {
                    cr::Worker::my().commitTX();
                    cr::Worker::my().startTX();
                }
            }
            
            std::cout << "Loaded namespace " << ns << " with " 
                     << FLAGS_entries_per_namespace << " entries\n";
        }

        cr::Worker::my().commitTX();
        
        auto end = Clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        std::cout << "Loaded " << total_entries << " total entries in " 
                  << duration.count() << "ms" << std::endl;
    }

    bool verifyData(LeanStoreAdapter<RecoveryEntry>& adapter) {
        auto total_start = Clock::now();
        std::ifstream verify_in(verify_file);
        if (!verify_in.is_open()) {
            throw std::runtime_error("Could not open verification file for reading");
        }

        // Read and verify format version and configuration
        std::string version;
        u32 stored_num_namespaces;
        u64 stored_entries_per_ns;
        verify_in >> version >> stored_num_namespaces >> stored_entries_per_ns;

        if (stored_num_namespaces != FLAGS_num_namespaces || 
            stored_entries_per_ns != FLAGS_entries_per_namespace) {
            std::cout << "Configuration mismatch in verification file!\n";
            return false;
        }

        auto verify_start = Clock::now();
        cr::Worker::my().startTX();
        
        u64 ns, key, expected_value, expected_checksum;
        while (verify_in >> ns >> key >> expected_value >> expected_checksum) {
            RecoveryEntry::Key db_key{ns, key};
            bool found = false;
            
            adapter.lookup1(db_key, [&](const RecoveryEntry& entry) {
                found = true;
                u64 actual_checksum = RecoveryEntry::generateChecksum(ns, key, entry.value);
                
                if (entry.value == expected_value && entry.checksum == expected_checksum && 
                    actual_checksum == expected_checksum) {
                    metrics.namespace_metrics[ns].recovered_entries++;
                    metrics.total_recovered++;
                } else {
                    metrics.namespace_metrics[ns].corrupted_entries++;
                    metrics.total_corrupted++;
                    std::cout << "Corruption in namespace " << ns << " key " << key 
                             << ": expected value=" << expected_value 
                             << " got=" << entry.value 
                             << " checksum mismatch=" << (actual_checksum != expected_checksum) 
                             << "\n";
                }
            });

            if (!found) {
                metrics.namespace_metrics[ns].missing_entries++;
                metrics.total_missing++;
                std::cout << "Missing entry in namespace " << ns 
                         << " key " << key << "\n";
            }
        }

        cr::Worker::my().commitTX();
        
        auto end = Clock::now();
        
        // Calculate metrics
        metrics.total_recovery_time_ms = 
            std::chrono::duration_cast<std::chrono::milliseconds>(end - total_start).count();
        metrics.data_verification_time_ms = 
            std::chrono::duration_cast<std::chrono::milliseconds>(end - verify_start).count();
        
        metrics.entries_per_second = 
            (metrics.total_recovered * 1000.0) / metrics.total_recovery_time_ms;
        metrics.mb_recovered = 
            (metrics.total_recovered * sizeof(RecoveryEntry)) / (1024.0 * 1024.0);
        metrics.mb_per_second = 
            (metrics.mb_recovered * 1000.0) / metrics.total_recovery_time_ms;

        metrics.print();
        metrics.saveToCSV("LeanStore");

        return metrics.total_corrupted == 0 && metrics.total_missing == 0;
    }

    const RecoveryMetrics& getMetrics() const { return metrics; }
}; 