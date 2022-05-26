#ifndef DB_SKIPLIST_H_
#define DB_SKIPLIST_H_

#include <assert.h>
#include <stdint.h>

#include <atomic>
#include <new>
#include <iostream>
#include <cstdio>


#include "logger/log.h"
#include "logger/log_level.h"
#include "utils/random_util.h"
/*
 * SkipList 属于 leveldb 中的核心数据结构，也是 memory table 的具体实现
 *
 * SkipList 的实现挺有意思的，leveldb 是一个 key-value DB，但是 SkipList 类中只定义了 Key，
 * 而没有定义 value。这是为什么?
 *
 * 因为 leveldb 直接将 User Key 和 User Value 打包成了一个更大的 Key，塞到了 Skip List 中。
 *
 * ┌───────────────┬─────────────────┬────────────────────────────┬───────────────┬───────────────┐
 * │ size(varint32)│ User Key(string)│Sequence Number | kValueType│ size(varint32)│  User Value   │
 * └───────────────┴─────────────────┴────────────────────────────┴───────────────┴───────────────┘
 *  
*/
namespace corekv {
struct SkipListOption {
  static constexpr int32_t kMaxHeight = 20;
  //有多少概率被选中, 空间和时间的折中
  static constexpr unsigned int kBranching = 4;
};

template <typename _KeyType, typename _KeyComparator, typename _Allocator>
class SkipList final {
  struct Node;

 public:
  SkipList(_KeyComparator comparator  );

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;
  void Insert(const _KeyType& key) {
    // 该对象记录的是要节点要插入位置的前一个对象，本质是链表的插入
    Node* prev[SkipListOption::kMaxHeight] = {nullptr};
    //在key的构造过程中，有一个持续递增的序号，因此理论上不会有重复的key
    Node* node = FindGreaterOrEqual(key, prev);
    if (nullptr != node) {
      if (Equal(key, node->key)) {
        LOG(WARN, "key:%s has existed", key);
        return;
      }
    }

    int32_t new_level = RandomHeight();
    int32_t cur_max_level = GetMaxHeight();
    if (new_level > cur_max_level) {
      //因为skiplist存在多层，而刚开始的时候只是分配kMaxHeight个空间，每一层的next并没有真正使用
      for (int32_t index = cur_max_level; index < new_level; ++index) {
        prev[index] = head_;
      }
      // 更新当前的最大值
      cur_height_.store(new_level, std::memory_order_relaxed);
    }
    Node* new_node = NewNode(key, new_level);
    for (int32_t index = 0; index < new_level; ++index) {
      new_node->NoBarrier_SetNext(index, prev[index]->NoBarrier_Next(index));
      // ：假设 Thread-1 store()的那个值，成功被 Thread-2 load()到了，
      //那么 Thread-1 在store()之前对内存的所有写入操作，此时对 Thread-2 来说，都是可见的。
      prev[index]->SetNext(index, new_node);
    }
  }
  bool Contains(const _KeyType& key) {
    Node* node = FindGreaterOrEqual(key, nullptr);
      return nullptr != node && Equal(key, node->key);
  }
  bool Equal(const _KeyType& a, const _KeyType& b) {
    return comparator_.Compare(a, b) == 0;
  }

 private:
  Node* NewNode(const _KeyType& key, int32_t height);
  int32_t RandomHeight();
  int32_t GetMaxHeight() {
    return cur_height_.load(std::memory_order_relaxed);
  }
  bool KeyIsAfterNode(const _KeyType& key, Node* n) {
    return (nullptr != n && comparator_.Compare(key, n->key) < 0);
  }
  //找到一个大于等于key的node
  Node* FindGreaterOrEqual(const _KeyType& key, Node** prev) {
    Node* cur = head_;
    //当前有效的最高层
    int32_t level = GetMaxHeight() - 1;
    Node* near_bigger_node = nullptr;
    while (true) {
      // 根据跳表原理，他是从最上层开始，向左或者向下遍历
      Node* next = cur->Next(level);
      // 说明key比next要大，直接往后next即可
      if (KeyIsAfterNode(key, next)) {
        cur = next;
      } else {
        if (prev != NULL) {
          prev[level] = cur;
        }
        if (level == 0) {
          return next;
        }
        //进入下一层
        level--;
      }
    }
  }
  // 找到小于key中最大的key
  Node* FindLessThan(const _KeyType& key) {
    Node* cur = head_;
    int32_t level = GetMaxHeight() - 1;
    while (true) {
      Node* next = cur->Next(level);
      int32_t cmp = (next == nullptr) ? 1 : comparator_.Compare(next->key, key);
      //刚好next大于等于0
      if (cmp >= 0) {
        // 因为高度是随机生成的，在这里只有level=0才能确定到底是哪个node
        if (level == 0) {
          return cur;
        } else {
          level--;
        }
      } else {
        cur = next;
      }
    }
  }
  //查找最后一个节点的数据
  Node* FindLast() {
    Node* cur = head_;
    static constexpr uint32_t kBaseLevel = 0;
    while (true) {
      Node* next = cur->Next(kBaseLevel);
      if (nullptr == next) {
        return cur;
      }
      cur = next;
    }
  }

 private:
 _KeyComparator comparator_;            //比较器
 Node* head_ = nullptr;
  std::atomic<int32_t> cur_height_;  //当前有效的层数
  _Allocator arena_;  //内存管理对象
  RandomUtil rnd_;
};

/*
 * Node 中使用了比较多的关于指令重排的内容。
 * http://senlinzhan.github.io/2017/12/04/cpp-memory-order/
 *
 * 需要注意的是，memory ordering 是针对于单线程而来的，也就是同一个线程内的指令重排情况，比如
 * 现在有 2 条语句:
 *
 *  x = 100;
 *  y.store();
 *
 * 其中 x 的写入是非原子性的，而 y 的写入是原子性的，不管我们使用何种 memory ordering，y 的原子
 * 写入永远是满足的，也就是说，y.store() 必然是多个线程的一个同步点。但是，由于指令重排的原因，x = 100;
 * 可能会在 y.store(); 之后执行，也可能会在其之前执行。memory ordering 限制的就是这个。
 *
 * 1. Relaxed ordering
 *
 * Relaxed ordering，也就是 std::memory_order_relaxed，不对重排进行任何限制，只保证相关内存操作的原子性。
 * 原子操作之前或者是之后的指令怎么被重排，我们并不关心，反正保证对内存的操作是原子性的就行了。通常用于计数器等场景中
 * Relaxed ordering 仅仅保证load()和store()是原子操作，除此之外，不提供任何跨线程的同步。
 *
 * 2. Release-Acquire ordering
 *
 * Release-Acquire ordering 由两个参数所指定，一个是 std::memory_order_acquire，用于 load() 方法，
 * 一个则是 std::memory_order_release， 用于 store() 方法。
 *
 * std::memory_order_acquire 表示在 load() 之后的所有读写操作，不允许被重排到这个 load() 的前面。
 * std::memory_order_release 表示在 store() 之前的所有读写操作，不允许被重排到这个 store() 的后面
 */
template <typename _KeyType, class _KeyComparator, typename _Allocator>
struct SkipList<_KeyType, _KeyComparator, _Allocator>::Node {
  explicit Node(const _KeyType& k) : key(k) {}

  const _KeyType key;

  Node* Next(int32_t n) {
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }


  Node* NoBarrier_Next(int n) {
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // 这里提前声明并申请了一个内存，用于存储第 0 层的数据，因为第 0 层必然存在数据。
  std::atomic<Node*> next_[1];
};

template <typename _KeyType, class _Comparator, typename _Allocator>
SkipList<_KeyType, _Comparator, _Allocator>::SkipList(_Comparator cmp) {
  cur_height_ = 1;
  comparator_ =cmp;
  head_ = NewNode(0, SkipListOption::kMaxHeight);
  for (int i = 0; i < cur_height_; i++) {
    head_->SetNext(i, nullptr);
  }
}



template <typename _KeyType, typename _Comparator, typename _Allocator>
typename SkipList<_KeyType, _Comparator, _Allocator>::Node*
SkipList<_KeyType, _Comparator, _Allocator>::NewNode(const _KeyType& key,
                                                     int32_t height) {
  char* node_memory = (char*)arena_.Allocate(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  //定位new写法
  return new (node_memory)Node(key);
}

template <typename _KeyType, typename _Comparator, typename _Allocator>
int32_t SkipList<_KeyType, _Comparator, _Allocator>::RandomHeight() {
  int32_t height = 1;
  while (height < SkipListOption::kMaxHeight &&
         ((rnd_.GetSimpleRandomNum() % SkipListOption::kBranching) == 0)) {
    height++;
  }
  return height;
}

}  // namespace corekv
#endif
