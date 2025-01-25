#pragma once
#include "leanstore/LeanStore.hpp"
#include "../shared/Types.hpp"
#include <string>

using namespace std;

struct WalRecoverySchema {
   struct KVTable {
      static constexpr uint64_t key_size = 8;  // 8 bytes for uint64_t
      static constexpr uint64_t value_size = 8;  // 8 bytes for uint64_t

      struct Key {
         uint64_t key;
         Key() = default;
         explicit Key(uint64_t k) : key(k) {}
         bool operator==(const Key& other) const { return key == other.key; }
         bool operator!=(const Key& other) const { return key != other.key; }
      };

      struct Value {
         uint64_t value;
         Value() = default;
         explicit Value(uint64_t v) : value(v) {}
      };

      static uint64_t getKey(Key& key) { return key.key; }
      static uint64_t getValue(Value& value) { return value.value; }
   };
}; 