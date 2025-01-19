#include "InmemGeneric.hpp"
#include "leanstore/storage/inmem/Inmem.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/inmem/aof.hpp"

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
         if (FLAGS_recover) {
            std::cout << "Starting recovery for dtid " << dtid << std::endl;
            aof->StartRecovery([this](uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length) {
               // Replay operation in the in-memory store
               auto& inmem = dynamic_cast<Inmem&>(*this);
               inmem.replayOperation(namespace_id, type, key, key_length, value, value_length);
            });
            std::cout << "Recovery completed for dtid " << dtid << std::endl;
         }
      } catch (const std::exception& e) {
         throw std::runtime_error("Failed to initialize AOF for inmem store " + std::to_string(dtid) + ": " + e.what());
      }
   }
}

}  // namespace leanstore::storage::inmem
