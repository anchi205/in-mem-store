#include "InmemGeneric.hpp"
#include "leanstore/storage/inmem/Inmem.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/inmem/aof.hpp"

#include <chrono>

// Usage in code
std::vector<uint64_t> parseNamespacesToRecover() {
    std::vector<uint64_t> numbers;
    std::string str = FLAGS_recover_namespaces;
    std::stringstream ss(str);
    std::string number;
    
    while (std::getline(ss, number, ',')) {
        numbers.push_back((uint64_t)std::stoi(number));
    }
    return numbers;
}

using namespace leanstore::storage;
namespace leanstore::storage::inmem
{
   
void InmemGeneric::create(DTID dtid, Config config)
{
   this->dt_id = dtid;
   this->config = config;
   
   if (config.enable_wal) {
      std::string log_dir = "/home/anchita20/Desktop/in-mem-store/walaof/" + std::to_string(dtid);
      try {
         aof = std::make_unique<AOF>(log_dir);
         if (!aof->Init()) {
            throw std::runtime_error("Failed to initialize AOF");
         }
         // Start recovery if WAL is enabled
         bool must_recover = true;
         if(!FLAGS_recover_all && FLAGS_recover_namespaces == "") {
            std::cout << "Skipping recovery...." << std::endl;
            must_recover = false;
         } 
         if (must_recover) {
            std::cout << "Starting recovery.... " << std::endl;
            auto recovery_start = std::chrono::high_resolution_clock::now();
            
            if(FLAGS_recover_all) {
               std::cout << "Recovering all namespaces" << std::endl;
               aof->StartRecovery([this](uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
                  auto& inmem = dynamic_cast<Inmem&>(*this);
                  inmem.replayOperation(namespace_id, type, key, key_length, value, value_length);
               });
            } else {
               auto namespaces = parseNamespacesToRecover();
               for (const auto& ns : namespaces) {
                  std::cout << "Reccovering namespace - " << ns << std::endl;
                  aof->StartNamespaceRecovery([this](uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
                     auto& inmem = dynamic_cast<Inmem&>(*this);
                     inmem.replayOperation(namespace_id, type, key, key_length, value, value_length);
                  }, ns);
               }
            }
            
            auto recovery_end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(recovery_end - recovery_start);
            std::cout << "Recovery completed in " << (duration.count() / 1000.0) << " milliseconds" << std::endl;
         }
      } catch (const std::exception& e) {
         throw std::runtime_error("Failed to initialize AOF for inmem store " + std::to_string(dtid) + ": " + e.what());
      }
   }
}

}  // namespace leanstore::storage::inmem
