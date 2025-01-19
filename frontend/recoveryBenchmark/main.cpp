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
#include "leanstore/LeanStore.hpp"
#include "leanstore/Config.hpp"
#include "../../shared-headers/Units.hpp"
#include <thread>

using namespace std;
using namespace leanstore;

// -------------------------------------------------------------------------------------
LeanStoreAdapterInmem<kv_t> kv_table;
// -------------------------------------------------------------------------------------
#include "workload.hpp"
// -------------------------------------------------------------------------------------

DEFINE_uint32(num_namespaces, 5, "Number of namespaces to use");
DEFINE_uint64(entries_per_namespace, 100000, "Number of entries per namespace");
DEFINE_string(verification_file, "verification.txt", "File to store verification data");
DEFINE_bool(recover, false, "Recover from existing database");

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
   gflags::SetUsageMessage("Recovery Benchmark\nUsage: sudo ./recovery_benchmark --ssd_path=./leanstore_ssd_file --worker_threads=6 --entries_per_namespace=100000 --num_namespaces=5");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   
   LeanStore db;
   
   if (!FLAGS_recover) {
      std::cout << "=== Initial Data Load ===\n";
      std::cout << "Namespaces: " << FLAGS_num_namespaces << "\n";
      std::cout << "Entries per namespace: " << FLAGS_entries_per_namespace << "\n";
      std::cout << "Total entries: " << (FLAGS_num_namespaces * FLAGS_entries_per_namespace) << "\n";
      
      RecoveryWorkload workload(FLAGS_verification_file);
      auto& adapter = db.registerAdapter<RecoveryEntry>("recovery_data");
      
      workload.loadData(adapter);
      
      if (workload.verifyData(adapter)) {
         std::cout << "Initial verification successful\n";
      } else {
         std::cout << "Initial verification failed\n";
         return 1;
      }

      std::cout << "\nData loaded and verified. Run with --recover to test recovery.\n";
      std::cout << "Example: " << argv[0] << " --recover"
                << " --ssd_path=./leanstore_ssd_file"
                << " --worker_threads=6"
                << " --num_namespaces=" << FLAGS_num_namespaces
                << " --entries_per_namespace=" << FLAGS_entries_per_namespace << "\n";
   } else {
      std::cout << "=== Recovery Mode ===\n";
      RecoveryWorkload workload(FLAGS_verification_file);
      auto& adapter = db.registerAdapter<RecoveryEntry>("recovery_data");
      
      if (workload.verifyData(adapter)) {
         std::cout << "Recovery verification successful\n";
      } else {
         std::cout << "Recovery verification failed\n";
         return 1;
      }
   }

   db.startProfilingThread();
   std::this_thread::sleep_for(std::chrono::seconds(2));
   
   return 0;
} 