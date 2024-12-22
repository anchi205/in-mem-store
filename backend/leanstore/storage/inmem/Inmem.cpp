#include "Inmem.hpp"

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

OP_RESULT Inmem::lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback)
{
   std::vector<u8> search_key(key, key + key_length);
   std::lock_guard<std::mutex> lock(mutex);
   
   auto it = store.lower_bound(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end() && std::equal(it->first.key.begin(), it->first.key.end(), search_key.begin(), search_key.end())) {
      payload_callback(it->first.value.data(), it->first.value.size());
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

// OP_RESULT Inmem::lookup(u8* key, u16 key_length, function<void(const u8*, u16)> payload_callback)
// {
//    while (true) {
//       jumpmuTry()
//       {
//          HybridPageGuard<BTreeNode> leaf;
//          findLeafCanJump(leaf, key, key_length);
//          // -------------------------------------------------------------------------------------
//          DEBUG_BLOCK()
//          {
//             s16 sanity_check_result = leaf->compareKeyWithBoundaries(key, key_length);
//             leaf.recheck();
//             if (sanity_check_result != 0) {
//                cout << leaf->count << endl;
//             }
//             ensure(sanity_check_result == 0);
//          }
//          // -------------------------------------------------------------------------------------
//          s16 pos = leaf->lowerBound<true>(key, key_length);
//          if (pos != -1) {
//             payload_callback(leaf->getPayload(pos), leaf->getPayloadLength(pos));
//             leaf.recheck();
//             jumpmu_return OP_RESULT::OK;
//          } else {
//             leaf.recheck();
//             raise(SIGTRAP);
//             jumpmu_return OP_RESULT::NOT_FOUND;
//          }
//       }
//       jumpmuCatch()
//       {
//          WorkerCounters::myCounters().dt_restarts_read[dt_id]++;
//       }
//    }
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }

// -------------------------------------------------------------------------------------

// OP_RESULT Inmem::scanAsc(u8* start_key,
//                            u16 key_length,
//                            std::function<bool(const u8* key, u16 key_length, const u8* payload, u16 payload_length)> callback,
//                            function<void()>)
// {
//    COUNTERS_BLOCK()
//    {
//       WorkerCounters::myCounters().dt_scan_asc[dt_id]++;
//    }
//    Slice key(start_key, key_length);
//    jumpmuTry()
//    {
//       BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
//       OP_RESULT ret = iterator.seek(key);
//       while (ret == OP_RESULT::OK) {
//          iterator.assembleKey();
//          auto key = iterator.key();
//          auto value = iterator.value();
//          if (!callback(key.data(), key.length(), value.data(), value.length())) {
//             break;
//          }
//          ret = iterator.next();
//       }
//       jumpmu_return OP_RESULT::OK;
//    }
//    jumpmuCatch() {}
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }

OP_RESULT Inmem::scanAsc(u8* start_key, u16 key_length, 
                        std::function<bool(const u8*, u16, const u8*, u16)> callback,
                        std::function<void()>)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.lower_bound(KeyValue(start_key, key_length, nullptr, 0));
   
   while (it != store.end()) {
      if (!callback(it->first.key.data(), it->first.key.size(),
                   it->first.value.data(), it->first.value.size())) {
         break;
      }
      ++it;
   }
   return OP_RESULT::OK;
}

// -------------------------------------------------------------------------------------

// OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length, std::function<bool(const u8*, u16, const u8*, u16)> callback, function<void()>)
// {
//    COUNTERS_BLOCK()
//    {
//       WorkerCounters::myCounters().dt_scan_desc[dt_id]++;
//    }
//    const Slice key(start_key, key_length);
//    jumpmuTry()
//    {
//       BTreeSharedIterator iterator(*static_cast<BTreeGeneric*>(this));
//       auto ret = iterator.seekForPrev(key);
//       if (ret != OP_RESULT::OK) {
//          jumpmu_return ret;
//       }
//       while (true) {
//          iterator.assembleKey();
//          auto key = iterator.key();
//          auto value = iterator.value();
//          if (!callback(key.data(), key.length(), value.data(), value.length())) {
//             jumpmu_return OP_RESULT::OK;
//          } else {
//             if (iterator.prev() != OP_RESULT::OK) {
//                jumpmu_return OP_RESULT::NOT_FOUND;
//             }
//          }
//       }
//    }
//    jumpmuCatch() {}
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }

OP_RESULT Inmem::scanDesc(u8* start_key, u16 key_length,
                         std::function<bool(const u8*, u16, const u8*, u16)> callback,
                         std::function<void()>)
{
   std::lock_guard<std::mutex> lock(mutex);
   auto it = store.lower_bound(KeyValue(start_key, key_length, nullptr, 0));
   
   while (it != store.begin()) {
      --it;
      if (!callback(it->first.key.data(), it->first.key.size(),
                   it->first.value.data(), it->first.value.size())) {
         break;
      }
   }
   return OP_RESULT::OK;
}
// -------------------------------------------------------------------------------------
OP_RESULT Inmem::insert(u8* key, u16 key_length, u8* value, u16 value_length)
{
   std::vector<u8> key_vec(key, key + key_length);

   // Check if the key already exists
   auto it = store.find(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end()) {
      // Move the key to the front of the LRU list
      std::string key_str(key_vec.begin(), key_vec.end());
      auto lru_it = lru_map.find(key_str);
      if (lru_it != lru_map.end()) {
         lru_list.erase(lru_it->second);
      }
      lru_list.push_front(key_vec);
      lru_map[key_str] = lru_list.begin();
      return OP_RESULT::DUPLICATE;
   }

   // Check if the store is full
   if (store.size() >= MAX_STORE_SIZE) {
      // Evict the least recently used item
      auto lru_key = lru_list.back();
      std::string lru_key_str(lru_key.begin(), lru_key.end());
      store.erase(KeyValue(lru_key.data(), lru_key.size(), nullptr, 0));
      lru_map.erase(lru_key_str);
      lru_list.pop_back();
   }

   // Insert the new key-value pair
   store.insert({KeyValue(key, key_length, value, value_length), nullptr});
   std::string key_str(key_vec.begin(), key_vec.end());
   lru_list.push_front(key_vec);
   lru_map[key_str] = lru_list.begin();
   return OP_RESULT::OK;
}


// OP_RESULT Inmem::insert(u8* o_key, u16 o_key_length, u8* o_value, u16 o_value_length)
// {
//    cr::activeTX().markAsWrite();
//    if (config.enable_wal) {
//       cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
//    }
//    const Slice key(o_key, o_key_length);
//    const Slice value(o_value, o_value_length);
//    jumpmuTry()
//    {
//       BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
//       OP_RESULT ret = iterator.insertKV(key, value);
//       ensure(ret == OP_RESULT::OK);
//       if (config.enable_wal) {
//          auto wal_entry = iterator.leaf.reserveWALEntry<WALInsert>(key.length() + value.length());
//          wal_entry->type = WAL_LOG_TYPE::WALInsert;
//          wal_entry->key_length = key.length();
//          wal_entry->value_length = value.length();
//          std::memcpy(wal_entry->payload, key.data(), key.length());
//          std::memcpy(wal_entry->payload + key.length(), value.data(), value.length());
//          wal_entry.submit();
//       } else {
//          iterator.markAsDirty();
//       }
//       jumpmu_return OP_RESULT::OK;
//    }
//    jumpmuCatch() {}
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }

// -------------------------------------------------------------------------------------


// OP_RESULT Inmem::updateSameSizeInPlace(u8* o_key,
//                                          u16 o_key_length,
//                                          function<void(u8* payload, u16 payload_size)> callback,
//                                          UpdateSameSizeInPlaceDescriptor& update_descriptor)
// {
//    cr::activeTX().markAsWrite();
//    if (config.enable_wal) {
//       cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
//    }
//    Slice key(o_key, o_key_length);
//    jumpmuTry()
//    {
//       BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
//       auto ret = iterator.seekExact(key);
//       if (ret != OP_RESULT::OK) {
//          jumpmu_return ret;
//       }
//       auto current_value = iterator.mutableValue();
//       if (config.enable_wal) {
//          assert(update_descriptor.count > 0);  // if it is a secondary index, then we can not use updateSameSize
//          // -------------------------------------------------------------------------------------
//          const u16 delta_length = update_descriptor.size() + update_descriptor.diffLength();
//          auto wal_entry = iterator.leaf.reserveWALEntry<WALUpdate>(key.length() + delta_length);
//          wal_entry->type = WAL_LOG_TYPE::WALUpdate;
//          wal_entry->key_length = key.length();
//          wal_entry->delta_length = delta_length;
//          u8* wal_ptr = wal_entry->payload;
//          std::memcpy(wal_ptr, key.data(), key.length());
//          wal_ptr += key.length();
//          std::memcpy(wal_ptr, &update_descriptor, update_descriptor.size());
//          wal_ptr += update_descriptor.size();
//          generateDiff(update_descriptor, wal_ptr, current_value.data());
//          // The actual update by the client
//          callback(current_value.data(), current_value.length());
//          generateXORDiff(update_descriptor, wal_ptr, current_value.data());
//          wal_entry.submit();
//       } else {
//          callback(current_value.data(), current_value.length());
//          iterator.markAsDirty();
//       }
//       iterator.contentionSplit();
//       jumpmu_return OP_RESULT::OK;
//    }
//    jumpmuCatch() {}
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }

OP_RESULT Inmem::updateSameSizeInPlace(u8* key, u16 key_length,
                                     std::function<void(u8*, u16)> callback,
                                     UpdateSameSizeInPlaceDescriptor&)
{
   auto it = store.find(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end()) {
      // Create a copy of the value, update it, then replace
      std::vector<u8> new_value = it->first.value;
      callback(new_value.data(), new_value.size());
      
      // Remove old and insert new
      store.erase(it);
      store.insert({KeyValue(key, key_length, new_value.data(), new_value.size()), nullptr});
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}
// -------------------------------------------------------------------------------------
OP_RESULT Inmem::remove(u8* key, u16 key_length)
{
   auto it = store.find(KeyValue(key, key_length, nullptr, 0));
   if (it != store.end()) {
      store.erase(it);
      return OP_RESULT::OK;
   }
   return OP_RESULT::NOT_FOUND;
}

// OP_RESULT Inmem::remove(u8* o_key, u16 o_key_length)
// {
//    cr::activeTX().markAsWrite();
//    if (config.enable_wal) {
//       cr::Worker::my().logging.walEnsureEnoughSpace(PAGE_SIZE * 1);
//    }
//    const Slice key(o_key, o_key_length);
//    jumpmuTry()
//    {
//       BTreeExclusiveIterator iterator(*static_cast<BTreeGeneric*>(this));
//       auto ret = iterator.seekExact(key);
//       if (ret != OP_RESULT::OK) {
//          jumpmu_return ret;
//       }
//       Slice value = iterator.value();
//       if (config.enable_wal) {
//          auto wal_entry = iterator.leaf.reserveWALEntry<WALRemove>(o_key_length + value.length());
//          wal_entry->type = WAL_LOG_TYPE::WALRemove;
//          wal_entry->key_length = o_key_length;
//          wal_entry->value_length = value.length();
//          std::memcpy(wal_entry->payload, key.data(), key.length());
//          std::memcpy(wal_entry->payload + o_key_length, value.data(), value.length());
//          wal_entry.submit();
//          iterator.markAsDirty();
//       } else {
//          iterator.markAsDirty();
//       }
//       ret = iterator.removeCurrent();
//       ensure(ret == OP_RESULT::OK);
//       iterator.mergeIfNeeded();
//       jumpmu_return OP_RESULT::OK;
//    }
//    jumpmuCatch() {}
//    UNREACHABLE();
//    return OP_RESULT::OTHER;
// }
// -------------------------------------------------------------------------------------

// -------------------------------------------------------------------------------------

u64 Inmem::countPages() {
   return 1; // Or implement actual page counting logic
}

u64 Inmem::countEntries() {
   return 0; // Or implement actual entry counting logic
}

u64 Inmem::getHeight() {
   return 0; // Or implement actual height calculation logic
}


// -------------------------------------------------------------------------------------
}  // namespace btree
}  // namespace storage
}  // namespace leanstore
