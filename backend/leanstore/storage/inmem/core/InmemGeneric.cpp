#include "InmemGeneric.hpp"

#include "leanstore/Config.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"

using namespace leanstore::storage;
namespace leanstore::storage::inmem
{
   
void InmemGeneric::create(DTID dtid, Config config)
{
   this->dt_id = dtid;
   this->config = config;
   // TODO: write WALs
}

}  // namespace leanstore::storage::inmem
