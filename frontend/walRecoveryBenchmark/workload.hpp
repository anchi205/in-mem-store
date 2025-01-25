#pragma once
#include "schema.hpp"
#include "../shared/LeanStoreAdapter.hpp"
#include <chrono>
#include <random>
#include <vector>
#include <iostream>

struct WalRecoveryWorkload {
   LeanStoreAdapter& db;
   std::vector<uint64_t> written_keys;
   const uint64_t num_keys = 10000;

   explicit WalRecoveryWorkload(LeanStoreAdapter& db) : db(db) {}

   void testConnection() {
      std::cout << "Testing database connection..." << std::endl;
      WalRecoverySchema::KVTable::Key test_key(1);
      WalRecoverySchema::KVTable::Value test_value(100);
      
      // Write test
      db.insert<WalRecoverySchema::KVTable>(test_key, test_value);
      
      // Read test
      WalRecoverySchema::KVTable::Value read_value;
      bool found = db.lookup<WalRecoverySchema::KVTable>(test_key, read_value);
      
      if (found && read_value.value == test_value.value) {
         std::cout << "Database connection test successful!" << std::endl;
      } else {
         throw std::runtime_error("Database connection test failed!");
      }
   }

   void writeTestData() {
      std::random_device rd;
      std::mt19937_64 gen(rd());
      std::uniform_int_distribution<uint64_t> dis;
      
      std::cout << "Writing " << num_keys << " key-value pairs..." << std::endl;
      auto start = std::chrono::high_resolution_clock::now();

      written_keys.reserve(num_keys);
      for (uint64_t i = 0; i < num_keys; i++) {
         uint64_t key = dis(gen);
         written_keys.push_back(key);
         
         WalRecoverySchema::KVTable::Key k(key);
         WalRecoverySchema::KVTable::Value v(i);
         db.insert<WalRecoverySchema::KVTable>(k, v);
      }

      auto end = std::chrono::high_resolution_clock::now();
      auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
      std::cout << "Write operation completed in " << duration.count() << "ms" << std::endl;
   }

   void verifyRecovery() {
      std::cout << "Verifying recovery by reading back random keys..." << std::endl;
      std::random_device rd;
      std::mt19937 gen(rd());
      std::uniform_int_distribution<> dis(0, written_keys.size() - 1);

      // Verify 100 random keys
      int successful_reads = 0;
      for (int i = 0; i < 100; i++) {
         int idx = dis(gen);
         WalRecoverySchema::KVTable::Key k(written_keys[idx]);
         WalRecoverySchema::KVTable::Value v;
         
         if (db.lookup<WalRecoverySchema::KVTable>(k, v)) {
            successful_reads++;
         }
      }

      std::cout << "Recovery verification: " << successful_reads << "% of sampled keys were recovered successfully" << std::endl;
   }
}; 