#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "aof.hpp"
#include <filesystem>
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
namespace fs = std::filesystem;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace inmem
{
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------
class InmemGeneric
{
  public:
   // -------------------------------------------------------------------------------------
   // friend class InmemPessimisticIterator;
   // -------------------------------------------------------------------------------------
   // Swip<BufferFrame> meta_node_bf;  // kept in memory
   // atomic<u64> height = 1;
   DTID dt_id;
   struct Config {
      // bool enable_wal = true;
      bool use_bulk_insert = false;
      bool enable_aof = true;
      std::string aof_directory = "./aof";
   };
   Config config;
   std::unique_ptr<AOF> aof;
   // -------------------------------------------------------------------------------------
   InmemGeneric() = default;
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, Config config);

};
// -------------------------------------------------------------------------------------
}  // namespace inmem
}  // namespace storage
}  // namespace leanstore
