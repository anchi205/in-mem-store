#pragma once
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace inmem
{
// -------------------------------------------------------------------------------------
enum class WAL_LOG_TYPE : u8 {
   WALInsert = 1,
   WALUpdate = 2,
   WALRemove = 3,
   WALAfterBeforeImage = 4,
   WALAfterImage = 5,
   WALLogicalSplit = 10,
   WALInitPage = 11
};
struct WALEntry {
   WAL_LOG_TYPE type;
};
struct WALInitPage : WALEntry {
   DTID dt_id;
};
struct WALLogicalSplit : WALEntry {
   PID parent_pid = -1;
   PID left_pid = -1;
   PID right_pid = -1;
   s32 right_pos = -1;
};
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
      bool enable_wal = true;
      bool use_bulk_insert = false;
   };
   Config config;
   // -------------------------------------------------------------------------------------
   InmemGeneric() = default;
   // -------------------------------------------------------------------------------------
   void create(DTID dtid, Config config);

};
// -------------------------------------------------------------------------------------
}  // namespace inmem
}  // namespace storage
}  // namespace leanstore
