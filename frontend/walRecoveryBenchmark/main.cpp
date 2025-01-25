#include "schema.hpp"
#include "workload.hpp"
#include "../shared/LeanStoreAdapter.hpp"
#include <iostream>
#include <chrono>

int main(int argc, char** argv) {
   if (argc < 2) {
      std::cout << "Usage: " << argv[0] << " <db_path>" << std::endl;
      return 1;
   }

   // Step 1: Initialize database and test connection
   std::cout << "\n=== Step 1: Database Initialization and Connection Test ===" << std::endl;
   LeanStoreAdapter db(argv[1]);
   WalRecoveryWorkload workload(db);
   workload.testConnection();

  //  // Step 2: Write test data
  //  std::cout << "\n=== Step 2: Writing Test Data ===" << std::endl;
  //  workload.writeTestData();
   
  //  // Step 3: Ensure WAL is written and close database
  //  std::cout << "\n=== Step 3: Flushing WAL and Closing Database ===" << std::endl;
  //  db.forceCheckpoint();  // Ensure WAL is written
  //  std::cout << "Database state saved and connection closed." << std::endl;

  //  // Step 4: Recover database and verify
  //  std::cout << "\n=== Step 4: Database Recovery ===" << std::endl;
  //  auto recovery_start = std::chrono::high_resolution_clock::now();
   
  //  // Create new connection to trigger recovery
  //  LeanStoreAdapter recovered_db(argv[1]);
   
  //  auto recovery_end = std::chrono::high_resolution_clock::now();
  //  auto recovery_duration = std::chrono::duration_cast<std::chrono::milliseconds>(recovery_end - recovery_start);
   
  //  std::cout << "Recovery completed in " << recovery_duration.count() << "ms" << std::endl;

  //  // Step 5: Verify recovered data
  //  std::cout << "\n=== Step 5: Verifying Recovered Data ===" << std::endl;
  //  WalRecoveryWorkload recovery_workload(recovered_db);
  //  recovery_workload.written_keys = workload.written_keys;  // Pass the written keys for verification
  //  recovery_workload.verifyRecovery();

   return 0;
} 