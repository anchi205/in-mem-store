#pragma once
#include <cstdio>
#include <cstring>
#include <ctime>
#include <iostream>
#include <chrono>
#include <algorithm>

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

u64 calculateChecksum(Integer key, Integer value, u64 ns_id) {
    // Convert to unsigned 64-bit to avoid sign issues
    u64 k = static_cast<u64>(key);
    u64 v = static_cast<u64>(value);
    
    // Simple but effective checksum without rotations
    u64 checksum = k;
    checksum = checksum * 31 + v;  // Use prime multiplier
    checksum = checksum * 31 + ns_id;
    return checksum;
}

// -------------------------------------------------------------------------------------
// Load functions
// -------------------------------------------------------------------------------------
void checkForReadsOnNamespace(u64 ns_id)
{
   u64 successful_inserts = 0;
   u64 namespace_checksum = 0;
   
   cr::Worker::my().startTX();
   for (Integer i = 0; i < FLAGS_entries_per_namespace; i++) {
      Integer key = (ns_id * FLAGS_entries_per_namespace) + i;
      
      
      kv_t record;
      bool found = false;
      kv_table.lookup1({key}, [&](const kv_t& rec) {
         record = rec;
         found = true;
      });
      
      if (found) {
         u64 current_checksum = calculateChecksum(key, record.value, record.namespace_id);
         namespace_checksum ^= current_checksum;
      } else {
         std::cout << "Verification failed for key " << key << " - entry not found" << std::endl;
      }
   }
   std::cout << "namespace checksum - " << namespace_checksum << " for namespace - " << ns_id << std::endl;

   cr::Worker::my().commitTX();
  
}

void checkForReads()
{
   // Initialize namespace IDs
   namespaces.clear();
   for (int i = 0; i < FLAGS_num_namespaces; i++) {
      namespaces.push_back(i);
   }

   if (FLAGS_recover_namespaces != "") {
      std::vector<uint64_t> numbers;
      std::string str = FLAGS_recover_namespaces;
      std::stringstream ss(str);
      std::string number;
      
      while (std::getline(ss, number, ',')) {
         numbers.push_back((uint64_t)std::stoi(number));
      }
      for(auto ns_id : numbers) {
         cr::Worker::my().startTX();
         checkForReadsOnNamespace(ns_id);
         cr::Worker::my().commitTX();
      }
      return;
   }
   for (u64 ns_id : namespaces) {
      cr::Worker::my().startTX();
      checkForReadsOnNamespace(ns_id);
      cr::Worker::my().commitTX();
   }
   total_entries = FLAGS_entries_per_namespace * FLAGS_num_namespaces;
}
