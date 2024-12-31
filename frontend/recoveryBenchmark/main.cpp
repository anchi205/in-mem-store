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

void runRecoveryBenchmark(LeanStore& db)
{
   if (true) {
      auto& crm = db.getCRManager();
      crm.scheduleJobSync(0, runRecovery);
      crm.joinAll();
      db.getBufferManager().writeAllBufferFrames();
   }
}

int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore Recovery Benchmark");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   std::string program_name = argv[0];
   
   {
      LeanStore db;
      setup(db);
      
      if (true) {
         std::cout << "\n=== Starting Recovery Benchmark ===\n";
      } else {
         std::cout << "\n=== Starting Initial Data Load ===\n";
      }
      
      loadDB(db, program_name);
      runRecoveryBenchmark(db);
      std::cout << "Benchmark completed. Exiting..." << std::endl;
   }
   return 0;
} 