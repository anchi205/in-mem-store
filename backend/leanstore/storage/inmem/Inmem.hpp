#pragma once
// #include "core/BTreeGeneric.hpp"
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

// class KVInterface
// {
//   public:
//    virtual OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback) = 0;
//    virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) = 0;
//    virtual OP_RESULT updateSameSizeInPlace(u8* key,
//                                            u16 key_length,
//                                            std::function<void(u8* value, u16 value_size)>,
//                                            UpdateSameSizeInPlaceDescriptor&) = 0;
//    virtual OP_RESULT remove(u8* key, u16 key_length) = 0;
//    virtual OP_RESULT scanAsc(u8* start_key,
//                              u16 key_length,
//                              std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
//                              std::function<void()>) = 0;
//    virtual OP_RESULT scanDesc(u8* start_key,
//                               u16 key_length,
//                               std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
//                               std::function<void()>) = 0;
//    // -------------------------------------------------------------------------------------
//    virtual u64 countPages() = 0;
//    virtual u64 countEntries() = 0;
//    virtual u64 getHeight() = 0;
//    // -------------------------------------------------------------------------------------
//    virtual OP_RESULT prefixLookup(u8*, u16, std::function<void(const u8*, u16, const u8*, u16)>) { return OP_RESULT::OTHER; }
//    virtual OP_RESULT prefixLookupForPrev(u8*, u16, std::function<void(const u8*, u16, const u8*, u16)>) { return OP_RESULT::OTHER; }
//    virtual OP_RESULT append(std::function<void(u8*)>, u16, std::function<void(u8*)>, u16, std::unique_ptr<u8[]>&) { return OP_RESULT::OTHER; }
//    virtual OP_RESULT rangeRemove(u8*, u16, u8*, u16, [[maybe_unused]] bool page_wise = true) { return OP_RESULT::OTHER; }
// };


class Inmem : public KVInterface
{
  public:
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback) ;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) ;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                           u16 key_length,
                                           std::function<void(u8* value, u16 value_size)>,
                                           UpdateSameSizeInPlaceDescriptor&) ;
   virtual OP_RESULT remove(u8* key, u16 key_length) ;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             std::function<void()>) ;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              std::function<void()>) ;
   // -------------------------------------------------------------------------------------
   virtual OP_RESULT prefixLookup(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) ;
   virtual OP_RESULT prefixLookupForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) ;
   virtual OP_RESULT append(std::function<void(u8*)>, u16, std::function<void(u8*)>, u16, std::unique_ptr<u8[]>&) ;
   virtual OP_RESULT rangeRemove(u8* start_key, u16 start_key_length, u8* end_key, u16 end_key_length, bool page_used) ;
   // -------------------------------------------------------------------------------------
   bool isRangeSurelyEmpty(Slice start_key, Slice end_key);
   // -------------------------------------------------------------------------------------
   virtual u64 countPages() ;
   virtual u64 countEntries() ;
   virtual u64 getHeight() ;
   // -------------------------------------------------------------------------------------
   static SpaceCheckResult checkSpaceUtilization(void* btree_object, BufferFrame& bf);
   static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
   static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
   static void todo(void* btree_object, const u8* entry_ptr, const u64 version_worker_id, const u64 tx_id, const bool called_before);
   static void unlock(void* btree_object, const u8* entry_ptr);
   static void checkpoint(void*, BufferFrame& bf, u8* dest);
   static std::unordered_map<std::string, std::string> serialize(void* btree_object);
   static void deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized);
   static DTRegistry::DTMeta getMeta();
   // -------------------------------------------------------------------------------------
   struct Config {
      bool enable_wal = true;
      bool use_bulk_insert = false;
   };
   // -------------------------------------------------------------------------------------
  protected:
   // WAL / CC
   static void generateDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void applyDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void generateXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
   static void applyXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src);
};
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
