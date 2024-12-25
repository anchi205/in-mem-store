#pragma once
#include "leanstore/storage/inmem/core/InmemGeneric.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include <fstream>
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
class Inmem : public KVInterface, public InmemGeneric
{
  // ************************* NEEDS TO BE PORTED *************************
  public:
   struct WALBeforeAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALAfterImage : WALEntry {
      u16 image_size;
      u8 payload[];
   };
   struct WALInsert : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   struct WALUpdate : WALEntry {
      u16 key_length;
      u16 delta_length;
      u8 payload[];
   };
   struct WALRemove : WALEntry {
      u16 key_length;
      u16 value_length;
      u8 payload[];
   };
   // -------------------------------------------------------------------------------------
   Inmem() = default;
   // -------------------------------------------------------------------------------------
   // ************************* NEEDS TO BE PORTED *************************
   virtual OP_RESULT lookup(u8* key, u16 key_length, std::function<void(const u8*, u16)> payload_callback) override;
   virtual OP_RESULT insert(u8* key, u16 key_length, u8* value, u16 value_length) override;
   virtual OP_RESULT updateSameSizeInPlace(u8* key,
                                           u16 key_length,
                                           std::function<void(u8* value, u16 value_size)>,
                                           UpdateSameSizeInPlaceDescriptor&) override;
   virtual OP_RESULT remove(u8* key, u16 key_length) override;
   virtual OP_RESULT scanAsc(u8* start_key,
                             u16 key_length,
                             std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                             std::function<void()>) override;
   virtual OP_RESULT scanDesc(u8* start_key,
                              u16 key_length,
                              std::function<bool(const u8* key, u16 key_length, const u8* value, u16 value_length)>,
                              std::function<void()>) override;
   // -------------------------------------------------------------------------------------
   void getMeta(){};
// -------------------------------------------------------------------------------------
   virtual u64 countPages() override;
   virtual u64 countEntries() override;
   virtual u64 getHeight() override;


   private:
      struct KeyValue {
         std::vector<u8> key;
         std::vector<u8> value;
         
         KeyValue(const u8* k, u16 klen, const u8* v, u16 vlen) 
            : key(k, k + klen), value(v, v + vlen) {}
         
         bool operator<(const KeyValue& other) const {
            return std::lexicographical_compare(
               key.begin(), key.end(),
               other.key.begin(), other.key.end()
            );
         }
      };

      struct KeyCompare {
         bool operator()(const KeyValue& a, const KeyValue& b) const {
            return a < b;
         }
         
         bool operator()(const KeyValue& a, const std::vector<u8>& b) const {
            return std::lexicographical_compare(
               a.key.begin(), a.key.end(),
               b.begin(), b.end()
            );
         }
         
         bool operator()(const std::vector<u8>& a, const KeyValue& b) const {
            return std::lexicographical_compare(
               a.begin(), a.end(),
               b.key.begin(), b.key.end()
            );
         }
      };

      std::map<KeyValue, std::nullptr_t, KeyCompare> store;
      // mutable std::mutex mutex;
      std::list<std::vector<u8>> lru_list;
      std::unordered_map<std::string, std::list<std::vector<u8>>::iterator> lru_map;
      const size_t MAX_STORE_SIZE = 1000000;  // Default max size


   // virtual OP_RESULT prefixLookup(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
   // virtual OP_RESULT prefixLookupForPrev(u8* key, u16 key_length, std::function<void(const u8*, u16, const u8*, u16)> payload_callback) override;
   // virtual OP_RESULT append(std::function<void(u8*)>, u16, std::function<void(u8*)>, u16, std::unique_ptr<u8[]>&) override;
   // virtual OP_RESULT rangeRemove(u8* start_key, u16 start_key_length, u8* end_key, u16 end_key_length, bool page_used) override;
   // bool isRangeSurelyEmpty(Slice start_key, Slice end_key);
};
// -------------------------------------------------------------------------------------
}  // namespace inmem
}  // namespace storage
}  // namespace leanstore
