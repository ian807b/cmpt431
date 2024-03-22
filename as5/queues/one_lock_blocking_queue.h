#include <atomic>
#include <mutex>

#include "../common/allocator.h"

extern std::atomic<bool> no_more_enqueues;

template <class T>
class Node {
 public:
  T value;
  Node<T>* next;
};

template <class T>
class OneLockBlockingQueue {
  Node<T>* q_head;
  Node<T>* q_tail;
  std::atomic<bool> wakeup_dq;
  CustomAllocator my_allocator_;
  std::mutex q_mutex;

 public:
  OneLockBlockingQueue() : my_allocator_() {
    std::cout << "Using OneLockBlockingQueue\n";
  }

  void initQueue(long t_my_allocator_size) {
    std::cout << "Using Allocator\n";
    my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
    Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
    newNode->next = nullptr;
    q_head = newNode;
    q_tail = newNode;
    wakeup_dq.store(false);
  }

  void enqueue(T value) {
    Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
    newNode->value = value;
    newNode->next = nullptr;
    q_mutex.lock();
    q_tail->next = newNode;
    q_tail = newNode;
    q_mutex.unlock();
    wakeup_dq.store(true);
  }

  bool dequeue(T* value) {
    q_mutex.lock();
    Node<T>* oldHead = q_head;
    Node<T>* newHead = oldHead->next;

    while (newHead == nullptr) {
      q_mutex.unlock();

      while (!wakeup_dq.load() && !no_more_enqueues.load()) {
      }

      q_mutex.lock();
      oldHead = q_head;
      newHead = oldHead->next;
      if (newHead == nullptr && no_more_enqueues.load()) {
        q_mutex.unlock();
        return false;
      }
      wakeup_dq.store(true);
    }

    *value = newHead->value;
    q_head = newHead;
    q_mutex.unlock();

    my_allocator_.freeNode(oldHead);

    return true;
  }

  void cleanup() { my_allocator_.cleanup(); }
};