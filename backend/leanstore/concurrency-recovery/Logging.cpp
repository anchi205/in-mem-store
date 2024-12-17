#include "Worker.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/CRCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace cr
{
// -------------------------------------------------------------------------------------
atomic<u64> Worker::Logging::global_min_gsn_flushed = 0;
atomic<u64> Worker::Logging::global_min_commit_ts_flushed = 0;
atomic<u64> Worker::Logging::global_sync_to_this_gsn = 0;
// -------------------------------------------------------------------------------------
u32 Worker::Logging::walFreeSpace()
{
   // A , B , C : a - b + c % c
   const auto gct_cursor = wal_gct_cursor.load();
   if (gct_cursor == wal_wt_cursor) {
      return FLAGS_wal_buffer_size;
   } else if (gct_cursor < wal_wt_cursor) {
      return gct_cursor + (FLAGS_wal_buffer_size - wal_wt_cursor);
   } else {
      return gct_cursor - wal_wt_cursor;
   }
}
// -------------------------------------------------------------------------------------
u32 Worker::Logging::walContiguousFreeSpace()
{
   const auto gct_cursor = wal_gct_cursor.load();
   return (gct_cursor > wal_wt_cursor) ? gct_cursor - wal_wt_cursor : FLAGS_wal_buffer_size - wal_wt_cursor;
}
// -------------------------------------------------------------------------------------
void Worker::Logging::walEnsureEnoughSpace(u32 requested_size)
{
   if (FLAGS_wal) {
      u32 wait_untill_free_bytes = requested_size + CR_ENTRY_SIZE;
      if ((FLAGS_wal_buffer_size - wal_wt_cursor) < static_cast<u32>(requested_size + CR_ENTRY_SIZE)) {
         wait_untill_free_bytes += FLAGS_wal_buffer_size - wal_wt_cursor;  // we have to skip this round
      }
      // Spin until we have enough space
      if (FLAGS_wal_variant == 2 && walFreeSpace() < wait_untill_free_bytes) {
         wt_to_lw.optimistic_latch.notify_all();
      }
      while (walFreeSpace() < wait_untill_free_bytes) {
      }
      if (walContiguousFreeSpace() < requested_size + CR_ENTRY_SIZE) {  // always keep place for CR entry
         WALMetaEntry& entry = *reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_wt_cursor);
         entry.size = sizeof(WALMetaEntry);
         entry.type = WALEntry::TYPE::CARRIAGE_RETURN;
         entry.size = FLAGS_wal_buffer_size - wal_wt_cursor;
         DEBUG_BLOCK()
         {
            entry.computeCRC();
         }
         // -------------------------------------------------------------------------------------
         wal_wt_cursor = 0;
         publishOffset();
         wal_next_to_clean = 0;
         wal_buffer_round++;  // Carriage Return
      }
      ensure(walContiguousFreeSpace() >= requested_size);
      ensure(wal_wt_cursor + requested_size + CR_ENTRY_SIZE <= FLAGS_wal_buffer_size);
   }
}
// -------------------------------------------------------------------------------------
WALMetaEntry& Worker::Logging::reserveWALMetaEntry()
{
   walEnsureEnoughSpace(sizeof(WALMetaEntry));
   active_mt_entry = reinterpret_cast<WALMetaEntry*>(wal_buffer + wal_wt_cursor);
   active_mt_entry->lsn.store(wal_lsn_counter++, std::memory_order_release);
   active_mt_entry->size = sizeof(WALMetaEntry);
   active_mt_entry->namespace_id = my().active_tx.namespace_id;
   return *active_mt_entry;
}
// -------------------------------------------------------------------------------------
void Worker::Logging::submitWALMetaEntry()
{
   if(!((wal_wt_cursor >= current_tx_wal_start) || (wal_wt_cursor + sizeof(WALMetaEntry) < current_tx_wal_start))) {
      my().active_tx.wal_larger_than_buffer = true;
   }
   DEBUG_BLOCK()
   {
      active_mt_entry->computeCRC();
   }
   wal_wt_cursor += sizeof(WALMetaEntry);
   auto current = wt_to_lw.getNoSync();
   current.wal_written_offset = wal_wt_cursor;
   current.precommitted_tx_commit_ts = my().active_tx.startTS();
   wt_to_lw.pushSync(current);
}
// -------------------------------------------------------------------------------------
void Worker::Logging::submitDTEntry(u64 total_size)
{
   if(!((wal_wt_cursor >= current_tx_wal_start) || (wal_wt_cursor + total_size  < current_tx_wal_start))) {
      my().active_tx.wal_larger_than_buffer = true;
   }
   DEBUG_BLOCK()
   {
      active_dt_entry->computeCRC();
   }
   COUNTERS_BLOCK()
   {
      WorkerCounters::myCounters().wal_write_bytes += total_size;
   }
   wal_wt_cursor += total_size;
   publishMaxGSNOffset();
}
// -------------------------------------------------------------------------------------
// Called by worker, so concurrent writes on the buffer
void Worker::Logging::iterateOverCurrentTXEntries(std::function<void(const WALEntry& entry)> callback)
{
   u64 cursor = current_tx_wal_start;
   while (cursor != wal_wt_cursor) {
      const WALEntry& entry = *reinterpret_cast<WALEntry*>(wal_buffer + cursor);
      ensure(entry.size > 0);
      DEBUG_BLOCK()
      {
         if (entry.type != WALEntry::TYPE::CARRIAGE_RETURN)
            entry.checkCRC();
      }
      if (entry.type == WALEntry::TYPE::CARRIAGE_RETURN) {
         cursor = 0;
      } else {
         callback(entry);
         cursor += entry.size;
      }
   }
}
// -------------------------------------------------------------------------------------
}  // namespace cr
}  // namespace leanstore
