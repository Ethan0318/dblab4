#include "storage/lru_buffer_strategy.h"

namespace huadb {

LRUBufferStrategy::LRUBufferStrategy() {
  for (size_t i = 0; i < BUFFER_SIZE; i++) {
    time[i] = (size_t)-1;
  }
}

void LRUBufferStrategy::Access(size_t frame_no) {
  for (size_t i = 0; i < BUFFER_SIZE; i++) {
    if (time[i] != (size_t)-1) {
      time[i]++;
    }
  }
  time[frame_no] = 0;
};

size_t LRUBufferStrategy::Evict() {
  size_t max_i = 0;
  for (size_t i = 1; i < BUFFER_SIZE; i++) {
    if (time[i] > time[max_i]) {
      max_i = i;
    }
  }
  return max_i;
}

}  // namespace huadb
