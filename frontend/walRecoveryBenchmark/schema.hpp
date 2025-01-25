/**
 * @file schema.hpp
 * @brief defines Schema for recovery benchmark.
 */
#include "../shared/Types.hpp"

struct kv_t {
   static constexpr int id = 0;
   struct Key {
      static constexpr int id = 0;
      Integer key;
   };
   Integer value;
   u64 namespace_id;  // namespace ID as u64

   template <class T>
   static unsigned foldKey(uint8_t* out, const T& key)
   {
      unsigned pos = 0;
      pos += fold(out + pos, key.key);
      return pos;
   }
   template <class T>
   static unsigned unfoldKey(const uint8_t* in, T& key)
   {
      unsigned pos = 0;
      pos += unfold(in + pos, key.key);
      return pos;
   }
   static constexpr unsigned maxFoldLength() { return 0 + sizeof(Key::key); };
}; 