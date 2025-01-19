#pragma once
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <chrono>

using namespace std::chrono;

DEFINE_int32(entries_per_namespace, 10000, "Number of entries per namespace");
DEFINE_int32(num_namespaces, 5, "Number of namespaces to create");

u64 total_entries;
std::vector<u64> namespaces;  // Will be initialized with namespace IDs
steady_clock::time_point recovery_start_time;
steady_clock::time_point recovery_end_time;

// -------------------------------------------------------------------------------------
// Helper functions
// -------------------------------------------------------------------------------------
Integer rnd(Integer n)
{
   return leanstore::utils::RandomGenerator::getRand(0, n);
}

// -------------------------------------------------------------------------------------
// Load functions
// -------------------------------------------------------------------------------------
void loadNamespace(u64 ns_id)
{
   u64 successful_inserts = 0;
   u64 checksum = 0;
   
   for (Integer i = 0; i < FLAGS_entries_per_namespace; i++) {
      Integer key = (ns_id * FLAGS_entries_per_namespace) + i;
      Integer value = rnd(1000000);
      checksum += value;
      
      try {
         kv_table.insert({key}, {value, ns_id});
         successful_inserts++;
      } catch (const std::exception& e) {
         std::cout << "Failed to insert key " << key << ": " << e.what() << std::endl;
      }
   }
   
   std::cout << "Namespace " << ns_id << " load complete:" << std::endl;
   std::cout << "  - Successful inserts: " << successful_inserts << "/" << FLAGS_entries_per_namespace << std::endl;
   std::cout << "  - Initial checksum: " << checksum << std::endl;
}

void loadData()
{
   // Initialize namespace IDs
   namespaces.clear();
   for (int i = 0; i < FLAGS_num_namespaces; i++) {
      namespaces.push_back(i);
   }

   for (u64 ns_id : namespaces) {
      cr::Worker::my().startTX();
      loadNamespace(ns_id);
      cr::Worker::my().commitTX();
   }
   total_entries = FLAGS_entries_per_namespace * FLAGS_num_namespaces;
}


// -------------------------------------------------------------------------------------
// Recovery functions
// -------------------------------------------------------------------------------------

bool verifyNamespace(u64 ns_id)
{
   bool success = true;
   u64 count = 0;
   u64 checksum = 0;
   
   cr::Worker::my().startTX();
   std::cout << "Verifying namespace " << ns_id << "..." << std::endl;
   
   kv_table.scan(
      {},
      [&](const kv_t::Key& key, const kv_t& record) {
         if (record.namespace_id == ns_id) {
            count++;
            checksum += record.value;  // Add value to checksum
            
            // Verify key range is correct for this namespace
            if (key.key < (ns_id * FLAGS_entries_per_namespace) || 
                key.key >= ((ns_id + 1) * FLAGS_entries_per_namespace)) {
                std::cout << "Invalid key " << key.key << " found in namespace " << ns_id << std::endl;
                success = false;
            }
            
            // Verify value is in expected range
            if (record.value > 1000000) {
                std::cout << "Invalid value " << record.value << " found for key " << key.key << std::endl;
                success = false;
            }
        }
        return true;
      },
      [&]() {});
   cr::Worker::my().commitTX();

   if (count != FLAGS_entries_per_namespace) {
      std::cout << "Namespace " << ns_id << " verification failed. Expected " << FLAGS_entries_per_namespace 
                << " entries, found " << count << std::endl;
      success = false;
   }
   
   std::cout << "Namespace " << ns_id << " stats:" << std::endl;
   std::cout << "  - Entry count: " << count << std::endl;
   std::cout << "  - Checksum: " << checksum << std::endl;
   
   return success;
}


void performRecovery()
{
   std::cout << "Starting namespace-based recovery..." << std::endl;
   
   // Initialize namespace metadata
   namespaces.clear();
   for (int i = 0; i < FLAGS_num_namespaces; i++) {
      namespaces.push_back(i);
   }
   
   // Scan all records to rebuild namespace metadata
   cr::Worker::my().startTX();
   std::vector<u64> entries_per_ns(FLAGS_num_namespaces, 0);
   
   kv_table.scan(
      {},
      [&](const kv_t::Key& key, const kv_t& record) {
         if (record.namespace_id < FLAGS_num_namespaces) {
            entries_per_ns[record.namespace_id]++;
         }
         return true;
      },
      [&]() {});
   
   cr::Worker::my().commitTX();
   
   // Print recovery statistics
   for (u64 ns_id = 0; ns_id < FLAGS_num_namespaces; ns_id++) {
      std::cout << "Namespace " << ns_id << " recovered with " << entries_per_ns[ns_id] << " entries" << std::endl;
   }
   
   std::cout << "Recovery process completed." << std::endl;
}

void startRecoveryTimer()
{
   recovery_start_time = steady_clock::now();
}

void stopRecoveryTimer()
{
   recovery_end_time = steady_clock::now();
   auto duration = duration_cast<milliseconds>(recovery_end_time - recovery_start_time);
   std::cout << "\nRecovery time: " << duration.count() << " milliseconds" << std::endl;
}

void verifyRecovery()
{
   bool all_success = true;
   for (u64 ns_id : namespaces) {
      if (!verifyNamespace(ns_id)) {
         all_success = false;
      }
   }
   if (all_success) {
      std::cout << "All namespaces recovered successfully!" << std::endl;
   } else {
      std::cout << "Recovery verification failed for some namespaces." << std::endl;
   }
}

void runRecovery()
{
   startRecoveryTimer();
   performRecovery();
   stopRecoveryTimer();
   verifyRecovery();
}




// -------------------------------------------------------------------------------------
// Benchmark functions
// -------------------------------------------------------------------------------------
void runOneQuery()
{
   // Not used in this benchmark
} 