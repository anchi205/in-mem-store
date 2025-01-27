#pragma once
#include "leanstore/storage/inmem/core/InmemGeneric.hpp"
#include "leanstore/Config.hpp"
#include "leanstore/KVInterface.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/storage/buffer-manager/BufferManager.hpp"
#include "leanstore/sync-primitives/PageGuard.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/storage/inmem/aof.hpp"
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
  public:
   // -------------------------------------------------------------------------------------
   Inmem() = default;
   ~Inmem() {
      if (aof) {
         aof->setStop(true);
      }
   }
   // -------------------------------------------------------------------------------------
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

   // Recovery methods
   bool StartRecovery();
   bool RecoverNamespace(uint64_t namespace_id);
   void replayOperation(uint64_t namespace_id, WALRecordType type, const u8* key, u16 key_length, const u8* value, u16 value_length);

   std::vector<u8> serialize_for_wal(const u8* key, u16 key_length, const u8* value, u16 value_length) {
      std::vector<u8> log_data;
      log_data.reserve(sizeof(u16) + key_length + sizeof(u16) + value_length);
      // Add key length and key
      log_data.insert(log_data.end(), reinterpret_cast<const u8*>(&key_length), reinterpret_cast<const u8*>(&key_length) + sizeof(u16));
      log_data.insert(log_data.end(), key, key + key_length);
      // Add value length and value
      log_data.insert(log_data.end(), reinterpret_cast<const u8*>(&value_length), reinterpret_cast<const u8*>(&value_length) + sizeof(u16));
      log_data.insert(log_data.end(), value, value + value_length);
      return log_data;
   }

   std::tuple<std::vector<u8>, u16, std::vector<u8>, u16> deserialize_from_wal(const std::vector<u8>& log_data) {
      size_t pos = 0;
      // Read key length
      u16 key_length = *reinterpret_cast<const u16*>(log_data.data() + pos);
      pos += sizeof(u16);
      // Read key
      std::vector<u8> key(log_data.begin() + pos, log_data.begin() + pos + key_length);
      pos += key_length;
      // Read value length
      u16 value_length = *reinterpret_cast<const u16*>(log_data.data() + pos);
      pos += sizeof(u16);
      // Read value
      std::vector<u8> value(log_data.begin() + pos, log_data.begin() + pos + value_length);
      return {key, key_length, value, value_length};
   }
   std::vector<u8> serialize_for_log(WALRecordType type, const std::vector<u8>& data) {
      std::vector<u8> wal_data;
      wal_data.push_back(static_cast<u8>(type));  // First byte is the record type
      wal_data.insert(wal_data.end(), data.begin(), data.end());
      return wal_data;
   }
   std::pair<WALRecordType, std::vector<u8>> deserialize_from_log(const std::vector<u8>& wal_data) {
      if (wal_data.empty()) {
         throw std::invalid_argument("WAL data is empty.");
      }

      WALRecordType type = static_cast<WALRecordType>(wal_data[0]);
      std::vector<u8> data(wal_data.begin() + 1, wal_data.end());
      return {type, data};
   }

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
      std::list<std::vector<u8>> lru_list;
      std::unordered_map<std::string, std::list<std::vector<u8>>::iterator> lru_map;
      const size_t MAX_STORE_SIZE = 1000000;  // Default max size

      void logOperation(uint64_t namespace_id, WALRecordType type, const std::vector<u8>& data);
};
// -------------------------------------------------------------------------------------
}  // namespace inmem
}  // namespace storage
}  // namespace leanstore
