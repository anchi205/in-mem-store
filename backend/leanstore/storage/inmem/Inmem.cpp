#include "Inmem.hpp"

// #include "core/BTreeGenericIterator.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
#include <signal.h>
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore::storage;
// -------------------------------------------------------------------------------------
namespace leanstore
{
namespace storage
{
namespace inmem
{
// -------------------------------------------------------------------------------------
OP_RESULT Inmem::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
{
   
   return OP_RESULT::OTHER;
}
// -------------------------------------------------------------------------------------
bool Inmem::isRangeSurelyEmpty(Slice start_key, Slice end_key)
{
   return false;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::scanAsc(u8* start_key,
                           u16 key_length,
                           std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
                           function<void()>)
{
   
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
{
   
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
{
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::prefixLookup(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::prefixLookupForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback)
{

   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::append(std::function<void(u8*)> o_key,
                          u16 o_key_length,
                          std::function<void(u8*)> o_value,
                          u16 o_value_length,
                          std::unique_ptr<u8[]>& session_ptr)
{
   struct alignas(64) Session {
      BufferFrame* bf;
   };
   // -------------------------------------------------------------------------------------

}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::updateSameSizeInPlace(u8* o_key,
                                         u16 o_key_length,
                                         function<void(u8* payload, u16 payload_size)> callback,
                                         UpdateSameSizeInPlaceDescriptor& update_descriptor)
{
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
OP_RESULT Inmem::remove(u8* o_key, u16 o_key_length)
{
   return OP_RESULT::OTHER;
}
// // -------------------------------------------------------------------------------------
// OP_RESULT Inmem::rangeRemove(u8* start_key, u16 start_key_length, u8* end_key, u16 end_key_length, bool page_wise)
// {
//    const Slice s_key(start_key, start_key_length);
//    const Slice e_key(end_key, end_key_length);
//    jumpmuTry()
//    {
//       BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
//       iterator.exitLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
//          if (leaf->freeSpaceAfterCompaction() >= BTreeNodeHeader::underFullSize) {
//             iterator.cleanUpCallback([&, to_find = leaf.bf] {
//                jumpmuTry()
//                {
//                   this->tryMerge(*to_find);
//                }
//                jumpmuCatch() {}
//             });
//          }
//       });
//       // -------------------------------------------------------------------------------------
//       ensure(config.enable_wal == false);
//       if (!page_wise) {
//          auto ret = iterator.seek(s_key);
//          if (ret != OP_RESULT::OK) {
//             jumpmu_return ret;
//          }
//          while (true) {
//             iterator.assembleKey();
//             auto c_key = iterator.key();
//             if (c_key >= s_key && c_key <= e_key) {
//                COUNTERS_BLOCK()
//                {
//                   WorkerCounters::myCounters().dt_range_removed[dt_id]++;
//                }
//                ret = iterator.removeCurrent();
//                ensure(ret == OP_RESULT::OK);
//                iterator.markAsDirty();
//                if (iterator.cur == iterator.leaf->count) {
//                   ret = iterator.next();
//                }
//             } else {
//                break;
//             }
//          }
//          jumpmu_return OP_RESULT::OK;
//       } else {
//          bool did_purge_full_page = false;
//          iterator.enterLeafCallback([&](HybridPageGuard<BTreeNode>& leaf) {
//             if (leaf->count == 0) {
//                return;
//             }
//             u8 first_key[leaf->getFullKeyLen(0)];
//             leaf->copyFullKey(0, first_key);
//             Slice p_s_key(first_key, leaf->getFullKeyLen(0));
//             // -------------------------------------------------------------------------------------
//             u8 last_key[leaf->getFullKeyLen(leaf->count - 1)];
//             leaf->copyFullKey(leaf->count - 1, last_key);
//             Slice p_e_key(last_key, leaf->getFullKeyLen(leaf->count - 1));
//             if (p_s_key >= s_key && p_e_key <= e_key) {
//                // Purge the whole page
//                COUNTERS_BLOCK()
//                {
//                   WorkerCounters::myCounters().dt_range_removed[dt_id] += leaf->count;
//                }
//                leaf->reset();
//                iterator.markAsDirty();
//                did_purge_full_page = true;
//             }
//          });
//          // -------------------------------------------------------------------------------------
//          while (true) {
//             iterator.seek(s_key);
//             if (did_purge_full_page) {
//                did_purge_full_page = false;
//                continue;
//             } else {
//                break;
//             }
//          }
//       }
//    }
//    jumpmuCatch() {}
//    return OP_RESULT::OK;
// }
// // -------------------------------------------------------------------------------------
// u64 Inmem::countEntries()
// {
//    return BTreeGeneric::countEntries();
// }
// // -------------------------------------------------------------------------------------
// u64 Inmem::countPages()
// {
//    return BTreeGeneric::countPages();
// }
// // -------------------------------------------------------------------------------------
// u64 Inmem::getHeight()
// {
//    return BTreeGeneric::getHeight();
// }
// // -------------------------------------------------------------------------------------
// void Inmem::undo(void*, const u8*, const u64)
// {
//    // TODO: undo for storage
//    TODOException();
// }
// // -------------------------------------------------------------------------------------
// void Inmem::todo(void*, const u8*, const u64, const u64, const bool)
// {
//    UNREACHABLE();
// }
// // -------------------------------------------------------------------------------------
// void Inmem::unlock(void*, const u8*)
// {
//    UNREACHABLE();
// }
// // -------------------------------------------------------------------------------------
// struct DTRegistry::DTMeta Inmem::getMeta()
// {
//    DTRegistry::DTMeta btree_meta = {.iterate_children = iterateChildrenSwips,
//                                     .find_parent = findParent,
//                                     .check_space_utilization = checkSpaceUtilization,
//                                     .checkpoint = checkpoint,
//                                     .undo = undo,
//                                     .todo = todo,
//                                     .unlock = unlock,
//                                     .serialize = serialize,
//                                     .deserialize = deserialize};
//    return btree_meta;
// }
// // -------------------------------------------------------------------------------------
// SpaceCheckResult Inmem::checkSpaceUtilization(void* btree_object, BufferFrame& bf)
// {
//    auto& btree = *reinterpret_cast<BTreeLL*>(btree_object);
//    return BTreeGeneric::checkSpaceUtilization(static_cast<BTreeGeneric*>(&btree), bf);
// }
// // -------------------------------------------------------------------------------------
// struct ParentSwipHandler Inmem::findParent(void* btree_object, BufferFrame& to_find)
// {
//    return BTreeGeneric::findParentJump(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), to_find);
// }
// // -------------------------------------------------------------------------------------
// void Inmem::checkpoint(void* btree_object, BufferFrame& bf, u8* dest)
// {
//    return BTreeGeneric::checkpoint(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), bf, dest);
// }
// // -------------------------------------------------------------------------------------
// std::unordered_map<std::string, std::string> Inmem::serialize(void* btree_object)
// {
//    return BTreeGeneric::serialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)));
// }
// // -------------------------------------------------------------------------------------
// void Inmem::deserialize(void* btree_object, std::unordered_map<std::string, std::string> serialized)
// {
//    BTreeGeneric::deserialize(*static_cast<BTreeGeneric*>(reinterpret_cast<BTreeLL*>(btree_object)), serialized);
// }
// // -------------------------------------------------------------------------------------
// void Inmem::generateDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
// {
//    u64 dst_offset = 0;
//    for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
//       const auto& slot = update_descriptor.slots[a_i];
//       std::memcpy(dst + dst_offset, src + slot.offset, slot.length);
//       dst_offset += slot.length;
//    }
// }
// // -------------------------------------------------------------------------------------
// void Inmem::applyDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
// {
//    u64 src_offset = 0;
//    for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
//       const auto& slot = update_descriptor.slots[a_i];
//       std::memcpy(dst + slot.offset, src + src_offset, slot.length);
//       src_offset += slot.length;
//    }
// }
// // -------------------------------------------------------------------------------------
// void Inmem::generateXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
// {
//    u64 dst_offset = 0;
//    for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
//       const auto& slot = update_descriptor.slots[a_i];
//       for (u64 b_i = 0; b_i < slot.length; b_i++) {
//          *(dst + dst_offset + b_i) ^= *(src + slot.offset + b_i);
//       }
//       dst_offset += slot.length;
//    }
// }
// // -------------------------------------------------------------------------------------
// void Inmem::applyXORDiff(const UpdateSameSizeInPlaceDescriptor& update_descriptor, u8* dst, const u8* src)
// {
//    u64 src_offset = 0;
//    for (u64 a_i = 0; a_i < update_descriptor.count; a_i++) {
//       const auto& slot = update_descriptor.slots[a_i];
//       for (u64 b_i = 0; b_i < slot.length; b_i++) {
//          *(dst + slot.offset + b_i) ^= *(src + src_offset + b_i);
//       }
//       src_offset += slot.length;
//    }
// }
// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
