#include "Inmem.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include <gflags/gflags.h>
#include <signal.h>

using namespace std;
using namespace leanstore::storage;

namespace leanstore {
namespace storage {
namespace inmem {


// virtual OP_RESULT lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback) override;
// virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
// virtual OP_RESULT updateSameSizeInPlace(u8* key,
//                                           u16 key_length,
//                                           function<void(u8* value, u16 value_size)>,
//                                           UpdateSameSizeInPlaceDescriptor&) override;
// virtual OP_RESULT remove(u8* key, u16 key_length) override;
// virtual OP_RESULT scanAsc(u8* start_key,
//                            u16 key_length,
//                            function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
//                            function<void()>) override;
// virtual OP_RESULT scanDesc(u8* start_key,
//                            u16 key_length,
//                            function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
//                            function<void()>) override;
// // -------------------------------------------------------------------------------------
// virtual OP_RESULT prefixLookup(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
// virtual OP_RESULT prefixLookupForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
// virtual OP_RESULT append(std::function<void(u8*)>, u16, std::function<void(u8*)>, u16, std::unique_ptr<u8[]>&) override;
// virtual OP_RESULT rangeRemove(u8* start_key, u16 start_key_length, u8* end_key, u16 end_key_length, bool page_used) override;
// // -------------------------------------------------------------------------------------
// // bool isRangeSurelyEmpty(Slice start_key, Slice end_key);
// // -------------------------------------------------------------------------------------
// virtual u64 countPages() override;
// virtual u64 countEntries() override;
// virtual u64 getHeight() override;
// // -------------------------------------------------------------------------------------
// static SpaceCheckResult checkSpaceUtilization(void* btree_object, BufferFrame& bf);
// static ParentSwipHandler findParent(void* btree_object, BufferFrame& to_find);
// static void undo(void* btree_object, const u8* wal_entry_ptr, const u64 tts);
// static void todo(void* btree_object, const u8* entry_ptr, const u64 version_worker_id, const u64 tx_id, const bool called_before);
// static void unlock(void* btree_object, const u8* entry_ptr);
// static void checkpoint(void*, BufferFrame& bf, u8* dest);
// static std::unordered_map<std::string, std::string> serialize(void* btree_object);
// static void deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized);
// static DTRegistry::DTMeta getMeta();


// -------------------------------------------------------------------------------------
OP_RESULT Inmem::lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback)
{
   return OP_RESULT::OK;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::scanAsc(u8* start_key, u16 key_length, 
                        std::function<bool(const u8*, u16, const u8*, u16)> callback,
                        std::function<void()> undo)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length,
                         std::function<bool(const u8*, u16, const u8*, u16)> callback,
                         std::function<void()> undo)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   return OP_RESULT::OK;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::updateSameSizeInPlace(u8* key, u16 key_length,
                                     std::function<void(u8*, u16)> callback,
                                     UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::remove(u8* key, u16 key_length)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::prefixLookup(u8* key, u16 key_length,
                            std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::prefixLookupForPrev(u8* key, u16 key_length,
                                   std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::append(std::function<void(u8*)> o_key, u16 o_key_length,
                      std::function<void(u8*)> o_value, u16 o_value_length,
                      std::unique_ptr<u8[]>& session_ptr)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
OP_RESULT Inmem::rangeRemove(u8* start_key, u16 start_key_length,
                           u8* end_key, u16 end_key_length, bool page_wise)
{
   return OP_RESULT::OTHER;
}

// -------------------------------------------------------------------------------------
u64 Inmem::countEntries()
{
   return 0;
}

// -------------------------------------------------------------------------------------
u64 Inmem::countPages()
{
   return 0;
}

// -------------------------------------------------------------------------------------
u64 Inmem::getHeight()
{
   return 0;
}

} // namespace inmem
} // namespace storage
} // namespace leanstore
