#include "area.h"
#include <iostream>

namespace corekv {
static const int kBlockSize = 4096;
SimpleVectorAlloc::SimpleVectorAlloc()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {
      std::cout<<"构造成功"<<std::endl;
    }

SimpleVectorAlloc::~SimpleVectorAlloc() {
  for (uint32_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}
void SimpleVectorAlloc::Deallocate(void*, int32_t n) {
  //暂时不支持这个操作
  
}
char* SimpleVectorAlloc::AllocateFallback(uint32_t bytes) {
  if(bytes > kBlockSize / 4){
    char* result = AllocateNewBlock(bytes);
    return result;
  }
  // 这个地方可能会导致部分内存的浪费。举个例子，假设之前的内存分配使得alloc_bytes_remaining_
  // 为256B，但是本次上层调用者需要申请的内存大小为512B，那么alloc_ptr_指向的内存不够本次
  // 分配，那么这个时候leveldb就会重新申请一块大小为kBlockSize的内存，并让alloc_ptr_指向这块
  // 新内存，那alloc_ptr_原来指向的那块大小为256B的内存就没有被使用了，由于alloc_ptr_被重新
  // 赋值，所以原有那块256B内存就找不到了。所以本项目实现了stl的内存池防止这一现象的发生。
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

void* SimpleVectorAlloc::Allocate(uint32_t bytes) {
  
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  //保证是8的倍
  uint32_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  uint32_t slop = (current_mod == 0 ? 0 : align - current_mod);
 // std::cout<<slop<<std::endl;
  uint32_t needed = bytes + slop;
  char* result = nullptr;
  //如果是当前剩余的内存足够，我们直接使用即可
  if (needed <= alloc_bytes_remaining_) {
    
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
    return (char *)result;
  }else {
    //如果不够我们开辟新内存
    return AllocateFallback(bytes);    
  }
}

char* SimpleVectorAlloc::AllocateNewBlock(uint32_t block_bytes) {
  char* result = (char*)malloc(block_bytes);
  blocks_.push_back(result);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}
}  // namespace corekv
