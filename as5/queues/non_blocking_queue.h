#include <cstdint>

#include "../common/allocator.h"

#define LFENCE asm volatile("lfence" : : : "memory")
#define SFENCE asm volatile("sfence" : : : "memory")

#define ADDRESS_MASK 0xFFFFFFFFFFFF
#define COUNT_MASK 0xFFFF000000000000
#define SHIFT 48

template <class P>
struct pointer_t {
  P* ptr;

  P* address() {
    return reinterpret_cast<P*>(reinterpret_cast<uintptr_t>(ptr) &
                                ADDRESS_MASK);
  }

  uint count() {
    return (reinterpret_cast<uintptr_t>(ptr) & COUNT_MASK) >> SHIFT;
  }

  void combine(P* newPtr, uint newCount) {
    ptr = reinterpret_cast<P*>(
        (reinterpret_cast<uintptr_t>(newPtr) & ADDRESS_MASK) |
        (static_cast<uintptr_t>(newCount) << SHIFT));
  }
};

template <class T>
class Node {
 public:
  T value;
  pointer_t<Node<T>> next;
};

template <class T>
class NonBlockingQueue {
  CustomAllocator my_allocator_;
  pointer_t<Node<T>> q_head;
  pointer_t<Node<T>> q_tail;

 public:
  NonBlockingQueue() : my_allocator_() {
    std::cout << "Using NonBlockingQueue\n";
  }

  void initQueue(long t_my_allocator_size) {
    std::cout << "Using Allocator\n";
    my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
    Node<T>* sentinel = (Node<T>*)my_allocator_.newNode();
    sentinel->next.ptr = nullptr;
    q_head.ptr = sentinel;
    q_tail.ptr = sentinel;
  }

  void enqueue(T value) {
    Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
    newNode->value = value;
    newNode->next.ptr = nullptr;
    pointer_t<Node<T>> tail;
    pointer_t<Node<T>> node;
    SFENCE;
    while (true) {
      tail = q_tail;
      LFENCE;
      pointer_t<Node<T>> next = tail.address()->next;
      LFENCE;
      if (tail.ptr == q_tail.ptr) {
        if (next.address() == nullptr) {
          node.combine(newNode, next.count() + 1);
          if (CAS(&tail.address()->next, next, node)) break;
        } else {
          node.combine(next.address(), tail.count() + 1);
          CAS(&q_tail, tail, node);
        }
      }
    }
    SFENCE;
    node.combine(newNode, tail.count() + 1);
    CAS(&q_tail, tail, node);
  }

  bool dequeue(T* value) {
    pointer_t<Node<T>> head;
    pointer_t<Node<T>> tail;
    pointer_t<Node<T>> node;
    while (true) {
      head = q_head;
      LFENCE;
      tail = q_tail;
      LFENCE;
      pointer_t<Node<T>> next = head.address()->next;
      LFENCE;
      if (head.ptr == q_head.ptr) {
        if (head.address() == tail.address()) {
          if (next.address() == nullptr) return false;
          node.combine(next.address(), tail.count() + 1);
          CAS(&q_tail, tail, node);
        } else {
          *value = next.address()->value;
          node.combine(next.address(), head.count() + 1);
          if (CAS(&q_head, head, node)) break;
        }
      }
    }
    my_allocator_.freeNode(head.address());
    return true;
  }

  void cleanup() { my_allocator_.cleanup(); }
};
