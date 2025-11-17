#pragma once

#include "storage/buffer_strategy.h"
#include "common/constants.h"

namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  LRUBufferStrategy();
  void Access(size_t frame_no) override;
  size_t Evict() override;

 private:
  size_t time[BUFFER_SIZE];
};

}  // namespace huadb
