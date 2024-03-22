#include <mutex>

#include "../common/allocator.h"

template <class T>
class Node {
 public:
  T value;
  Node<T>* next;
};

template <class T>
class OneLockQueue {
  Node<T>* q_head;
  Node<T>* q_tail;
  std::mutex q_mutex;
  CustomAllocator my_allocator_;

 public:
  OneLockQueue() : my_allocator_(), q_head(nullptr), q_tail(nullptr) {
    std::cout << "Using OneLockQueue\n";
  }

  void initQueue(long t_my_allocator_size) {
    std::cout << "Using Allocator\n";
    my_allocator_.initialize(t_my_allocator_size, sizeof(Node<T>));
    Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
    newNode->next = nullptr;
    q_head = newNode;
    q_tail = newNode;
  }

  void enqueue(T value) {
    Node<T>* newNode = (Node<T>*)my_allocator_.newNode();
    newNode->value = value;
    newNode->next = nullptr;
    q_mutex.lock();
    q_tail->next = newNode;
    q_tail = newNode;
    q_mutex.unlock();
  }

  bool dequeue(T* value) {
    std::lock_guard<std::mutex> lock(q_mutex);
    Node<T>* oldHead = q_head;
    Node<T>* newHead = oldHead->next;

    if (newHead == NULL) {
      return false;
    }

    *value = newHead->value;
    q_head = newHead;

    my_allocator_.freeNode(oldHead);

    return true;
  }

  void cleanup() { my_allocator_.cleanup(); }
};