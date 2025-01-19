/**
 * @file schema.hpp
 * @brief Schema for recovery benchmark with namespace support
 */
#pragma once
#include "../shared/Types.hpp"

struct RecoveryEntry {
   static constexpr int id = 0;
   struct Key {
      u64 namespace_id;  // Namespace identifier
      u64 key;          // Key within namespace
      
      bool operator==(const Key& other) const {
         return namespace_id == other.namespace_id && key == other.key;
      }
   };
   u64 value;           // Value stored (will be unique per namespace/key pair)
   u64 checksum;        // Checksum to verify data integrity

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& key) {
      unsigned pos = 0;
      pos += fold(out + pos, key.namespace_id);
      pos += fold(out + pos, key.key);
      return pos;
   }
   
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& key) {
      unsigned pos = 0;
      pos += unfold(in + pos, key.namespace_id);
      pos += unfold(in + pos, key.key);
      return pos;
   }
   
   static constexpr unsigned maxFoldLength() { 
      return sizeof(Key::namespace_id) + sizeof(Key::key); 
   }

   // Generate checksum for value verification
   static u64 generateChecksum(u64 namespace_id, u64 key, u64 value) {
      return namespace_id ^ (key << 16) ^ (value >> 8);
   }
}; 