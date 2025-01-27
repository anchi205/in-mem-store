/**
 * @file main.cpp
 * @brief Recovery benchmark implementation
 */

#include "../shared/LeanStoreAdapterInmem.hpp"
#include "../shared/Types.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "schema.hpp"
// -------------------------------------------------------------------------------------
#include <gflags/gflags.h>
#include "PerfEvent.hpp"
// -------------------------------------------------------------------------------------
#include <unistd.h>
#include <iostream>
#include <string>
#include <vector>

using namespace std;
using namespace leanstore;

// -------------------------------------------------------------------------------------
LeanStoreAdapterInmem<kv_t> kv_table;
// -------------------------------------------------------------------------------------
#include "workload.hpp"
// -------------------------------------------------------------------------------------

void setup(LeanStore& db)
{
   auto& crm = db.getCRManager();
   crm.scheduleJobSync(0, [&]() {
      kv_table = LeanStoreAdapterInmem<kv_t>(db, "kv_store");
   });
}

void loadDB(LeanStore& db, const std::string& program_name)
{
   if (true) {
      auto& crm = db.getCRManager();
      crm.scheduleJobSync(0, loadData);
      crm.joinAll();
      std::cout << "\n=== Initial Data Load Complete ===\n";
      std::cout << "Total entries: " << total_entries << "\n";
      std::cout << "To run recovery benchmark, rerun with --recover flag\n";
      std::cout << "Example: " << program_name << " --recover --ssd_path=./leanstore_ssd_file --worker_threads=6 --entries_per_namespace=10000 --num_namespaces=5\n";
      db.getBufferManager().writeAllBufferFrames();
   }
}


bool verifyDB(LeanStore& db) {
   // Define a unique key and value for verification
   Integer verify_key = 72345;          // Use a unique integer key
   Integer verify_value = 42070;  // Random value for testing
   u64 verify_ns_id = 99;                // Namespace ID

   bool found = false;
   kv_t record;

   // Insert the key-value pair within a scheduled worker thread
   db.getCRManager().scheduleJobSync(0, [&]() {
       cr::Worker::my().startTX(
           leanstore::TX_MODE::OLTP, 
           leanstore::TX_ISOLATION_LEVEL::SNAPSHOT_ISOLATION, 
           false, 
           99  // Specific namespace ID
       );
       kv_table.insert({verify_key}, {verify_value, verify_ns_id});
       cr::Worker::my().commitTX();
   });


   // Lookup the key within a scheduled worker thread
   db.getCRManager().scheduleJobSync(0, [&]() {
       kv_table.lookup1({verify_key}, [&](const kv_t& rec_in) {
           record = rec_in;
           found = true;
       });
   });

   if (!found) {
       std::cerr << "Verification key not found or value mismatch." << std::endl;
       return false;
   }

   if (verify_value == record.value && verify_ns_id == record.namespace_id) {
       std::cout << "Database verification succeeded." << std::endl;
       return true;
   }

   std::cout << "Database verification failed." << std::endl;
   return false;
}

int main(int argc, char** argv)
{
   std::cout << "lola lola lola lola lola lola lola lola lola lola " << std::endl;

   gflags::SetUsageMessage("Leanstore Recovery Benchmark");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   std::string program_name = argv[0];
   
   {
      LeanStore db;
      
      setup(db); 
      if (!verifyDB(db)) {
         std::cerr << "Database connection failed to establish!" << std::endl;
         return 1;
      }
      
      loadDB(db, program_name);
      
   }
   std::cout << "Database and WAL loading completed. Exiting..." << std::endl;
   return 0;
} 