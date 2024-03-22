// Created by: Ian Hwang on Jan. 17, 2024
// Assignment 1 CMPT 431
// Student ID: 301461304

#include <pthread.h>
#include <stdlib.h>

#include <iomanip>

#include "core/circular_queue.h"
#include "core/utils.h"

class Producer {
 public:
  long n_items_to_produce;         // Total number of items to be produced
  long producer_id;                // Producer ID
  long n_items_per_type[3] = {0};  // Number of items to be produced per type
  long n_items_produced;           // Total number of items produced
  long n_items_produced_per_type[3] = {0};  // Number of items produced per type
  long value_produced_per_type[3] = {0};    // Value of items produced per type
  CircularQueue *production_buffer;         // Pointer to the production buffer
  pthread_mutex_t *mutex;                   // Pointer to the mutex
  pthread_cond_t *cond_full;   // Pointer to the condition for full buffer
  pthread_cond_t *cond_empty;  // Pointer to the condition for empty buffer
  int *active_producer_count;  // Pointer to the active producer count
  int *active_consumer_count;  // Pointer to the active consumer count
  int n_producers;             // Total number of producers
  double *producer_times;      // Pointer to the array of producer times
};

class Consumer {
 public:
  long n_items_to_consume;  // Total number of items to be consumed
  long consumer_id;         // Consumer ID
  long n_items_consumed;    // Total number of items consumed
  long n_items_consumed_per_type[3] = {0};  // Number of items consumed per type
  long value_consumed_per_type[3] = {0};    // Value of items consumed per type
  CircularQueue *production_buffer;         // Pointer to the production buffer
  pthread_mutex_t *mutex;                   // Pointer to the mutex
  pthread_cond_t *cond_full;   // Pointer to the condition for full buffer
  pthread_cond_t *cond_empty;  // Pointer to the condition for empty buffer
  int *active_producer_count;  // Pointer to the active producer count
  int *active_consumer_count;  // Pointer to the active consumer count
  double *consumer_times;      // Pointer to the array of consumer times
};

class ProducerConsumerProblem {
  long n_items;
  int n_producers;
  int n_consumers;
  CircularQueue production_buffer;

  // Dynamic array of thread identifiers for producer and consumer threads.
  // Use these identifiers while creating the threads and joining the threads.
  pthread_t *producer_threads;
  pthread_t *consumer_threads;

  Producer *producers;
  Consumer *consumers;

  int active_producer_count;
  int active_consumer_count;

  // define any other members, mutexes, condition variables here
  pthread_mutex_t mutex;
  pthread_cond_t cond_full;
  pthread_cond_t cond_empty;

  // Dynamic array of doubles to store the time taken by each producer and
  // consumer thread.
  double *producer_times;
  double *consumer_times;

 public:
  // The following 6 methods should be defined in the implementation file
  // (solution.cpp)
  ProducerConsumerProblem(long _n_items, int _n_producers, int _n_consumers,
                          long _queue_size);
  ~ProducerConsumerProblem();
  void startProducers();
  void startConsumers();
  void joinProducers();
  void joinConsumers();
  void printStats();
};
