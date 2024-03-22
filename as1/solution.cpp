// Created by: Ian Hwang on Jan. 17, 2024
// Assignment 1 CMPT 431
// Student ID: 301461304

#include "solution.h"

void *producerFunction(void *_arg) {
  Producer *producer = static_cast<Producer *>(_arg);
  timer t1;
  t1.start();  // Timer starts
  long item = 0;

  while (producer->n_items_produced < producer->n_items_to_produce) {
    pthread_mutex_lock(producer->mutex);  // Lock mutex

    // Calculate item value
    long item_value = producer->producer_id + (item * producer->n_producers);
    int item_type;

    // Determine the item type
    if (producer->n_items_produced < producer->n_items_per_type[0]) {
      item_type = 0;
    } else if (producer->n_items_produced < (producer->n_items_per_type[0] +
                                             producer->n_items_per_type[1])) {
      item_type = 1;
    } else {
      item_type = 2;
    }

    bool ret = producer->production_buffer->enqueue(
        item_value, producer->producer_id, item_type);
    if (ret == true) {
      if (producer->production_buffer->itemCount() == 1) {
        // The queue is no longer empty
        // Signal all consumers indicating queue is not empty
        pthread_cond_broadcast(producer->cond_empty);
      }
      // Update item count and value for item type produced
      producer->n_items_produced++;
      producer->n_items_produced_per_type[item_type]++;
      producer->value_produced_per_type[item_type] += item_value;
      item++;
    } else {
      // production_buffer is full, so block on conditional variable waiting for
      // consumer to signal.
      if (producer->production_buffer->isFull()) {
        pthread_cond_wait(producer->cond_full, producer->mutex);
      }
    }
    pthread_mutex_unlock(producer->mutex);  // Unlock mutex
  }

  // After production is completed:
  // Update the number of producers that are currently active.
  pthread_mutex_lock(producer->mutex);
  --(*producer->active_producer_count);
  pthread_mutex_unlock(producer->mutex);
  // The producer that was last active (can be determined using
  // `active_producer_count`) will keep signalling the consumers until all
  // consumers have finished (can be determined using `active_consumer_count`).
  if (*producer->active_producer_count == 0) {
    while ((*producer->active_consumer_count) > 0) {
      pthread_cond_broadcast(producer->cond_empty);
    }
  }

  double time_taken = t1.stop();                                 // Timer stops
  producer->producer_times[producer->producer_id] = time_taken;  // Store time

  return NULL;
}

void *consumerFunction(void *_arg) {
  Consumer *consumer = static_cast<Consumer *>(_arg);
  timer t1;
  t1.start();  // Timer starts
  int source;
  int item_type;
  long item_value;

  while (true) {
    pthread_mutex_lock(consumer->mutex);  // Lock mutex
    bool ret =
        consumer->production_buffer->dequeue(&item_value, &source, &item_type);
    if (ret == true) {
      if (consumer->production_buffer->itemCount() ==
          consumer->production_buffer->getCapacity() - 1) {
        // The queue is no longer full
        // Signal all producers indicating queue is not full
        pthread_cond_broadcast(consumer->cond_full);
      }
      // Update value and count for consumed item type
      consumer->n_items_consumed++;
      consumer->n_items_consumed_per_type[item_type]++;
      consumer->value_consumed_per_type[item_type] += item_value;
      pthread_mutex_unlock(consumer->mutex);  // Unlock mutex
    } else {
      // Scenario 1: No more active producers and the queue is empty
      if (*consumer->active_producer_count == 0) {
        pthread_mutex_unlock(consumer->mutex);
        break;
      }
      // Scenario 2: The queue is empty, but producers are still active
      pthread_cond_wait(consumer->cond_empty, consumer->mutex);
      pthread_mutex_unlock(consumer->mutex);
    }
  }

  // Decrement active consumer count
  pthread_mutex_lock(consumer->mutex);
  --(*consumer->active_consumer_count);
  pthread_mutex_unlock(consumer->mutex);
  double time_taken = t1.stop();                                 // Timer stops
  consumer->consumer_times[consumer->consumer_id] = time_taken;  // Store time

  return NULL;
}

ProducerConsumerProblem::ProducerConsumerProblem(long _n_items,
                                                 int _n_producers,
                                                 int _n_consumers,
                                                 long _queue_size)
    : n_items(_n_items),
      n_producers(_n_producers),
      n_consumers(_n_consumers),
      production_buffer(_queue_size) {
  std::cout << "Constructor\n";
  std::cout << "Number of producers: " << n_producers << "\n";
  std::cout << "Number of consumers: " << n_consumers << "\n";

  if (n_consumers) {
    consumers = new Consumer[n_consumers];
    consumer_threads = new pthread_t[n_consumers];
  }
  if (n_producers) {
    producers = new Producer[n_producers];
    producer_threads = new pthread_t[n_producers];
  }

  // Initialize all mutex and conditional variables here.
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond_full, NULL);
  pthread_cond_init(&cond_empty, NULL);

  // Time arrays
  producer_times = new double[n_producers];
  consumer_times = new double[n_consumers];
}

ProducerConsumerProblem::~ProducerConsumerProblem() {
  std::cout << "Destructor\n";
  if (n_producers) {
    delete[] producers;
    delete[] producer_threads;
    delete[] producer_times;  // Free time array
  }
  if (n_consumers) {
    delete[] consumers;
    delete[] consumer_threads;
    delete[] consumer_times;  // Free time array
  }
  // Destroy all mutex and conditional variables here.
  pthread_mutex_destroy(&mutex);
  pthread_cond_destroy(&cond_full);
  pthread_cond_destroy(&cond_empty);
}

void ProducerConsumerProblem::startProducers() {
  std::cout << "Starting Producers\n";
  active_producer_count = n_producers;
  // Compute number of items for each thread, and number of items per type per
  // thread
  long n_items_per_thread = n_items / n_producers;
  long n_items_for_first_producer =
      n_items_per_thread + (n_items % n_producers);

  // Create threads P1, P2, P3,.. using pthread_create.
  for (int i = 0; i < n_producers; i++) {
    long n_items_this_producer =
        (i == 0) ? n_items_for_first_producer : n_items_per_thread;
    long type0_this_producer = n_items_this_producer / 2;
    long type1_this_producer = n_items_this_producer / 3;
    long type2_this_producer =
        n_items_this_producer - (type0_this_producer + type1_this_producer);
    producers[i].n_items_to_produce = n_items_this_producer;
    producers[i].producer_id = i;
    producers[i].n_items_per_type[0] = type0_this_producer;
    producers[i].n_items_per_type[1] = type1_this_producer;
    producers[i].n_items_per_type[2] = type2_this_producer;
    producers[i].n_items_produced = 0;
    producers[i].production_buffer = &production_buffer;
    producers[i].mutex = &mutex;
    producers[i].cond_full = &cond_full;
    producers[i].cond_empty = &cond_empty;
    producers[i].active_producer_count = &active_producer_count;
    producers[i].active_consumer_count = &active_consumer_count;
    producers[i].n_producers = n_producers;
    producers[i].producer_times = producer_times;
    pthread_create(&producer_threads[i], NULL, producerFunction, &producers[i]);
  }
}

void ProducerConsumerProblem::startConsumers() {
  std::cout << "Starting Consumers\n";
  active_consumer_count = n_consumers;

  // Create consumer threads C1, C2, C3,.. using pthread_create.
  for (int i = 0; i < n_consumers; i++) {
    consumers[i].consumer_id = i;
    consumers[i].production_buffer = &production_buffer;
    consumers[i].mutex = &mutex;
    consumers[i].cond_full = &cond_full;
    consumers[i].cond_empty = &cond_empty;
    consumers[i].active_producer_count = &active_producer_count;
    consumers[i].active_consumer_count = &active_consumer_count;
    consumers[i].consumer_times = consumer_times;
    pthread_create(&consumer_threads[i], NULL, consumerFunction, &consumers[i]);
  }
}

void ProducerConsumerProblem::joinProducers() {
  std::cout << "Joining Producers\n";
  // Join the producer threads with the main thread using pthread_join
  for (int i = 0; i < n_producers; i++) {
    pthread_join(producer_threads[i], NULL);
  }
}

void ProducerConsumerProblem::joinConsumers() {
  std::cout << "Joining Consumers\n";
  // Join the consumer threads with the main thread using pthread_join
  for (int i = 0; i < n_consumers; i++) {
    pthread_join(consumer_threads[i], NULL);
  }
}

void ProducerConsumerProblem::printStats() {
  std::cout << "Producer stats\n";
  std::cout
      << "producer_id, items_produced_type0:value_type0, "
         "items_produced_type1:value_type1, items_produced_type2:value_type2, "
         "total_value_produced, time_taken\n";

  long total_produced[3] = {0};        // total produced per type
  long total_value_produced[3] = {0};  // total value produced per type

  for (int i = 0; i < n_producers; i++) {
    for (int j = 0; j < 3; j++) {
      total_produced[j] += producers[i].n_items_produced_per_type[j];
      total_value_produced[j] += producers[i].value_produced_per_type[j];
    }
  }

  // Make sure you print the producer stats in the following manner
  //  0, 125000:31249750000, 83333:55555111112, 41667:38194638888, 124999500000,
  //  0.973188 1, 125000:31249875000, 83333:55555194445, 41667:38194680555,
  //  124999750000, 1.0039 2, 125000:31250000000, 83333:55555277778,
  //  41667:38194722222, 125000000000, 1.02925 3, 125000:31250125000,
  //  83333:55555361111, 41667:38194763889, 125000250000, 0.999188
  for (int i = 0; i < n_producers; i++) {
    std::cout << producers[i].producer_id << ", "
              << producers[i].n_items_produced_per_type[0] << ":"
              << producers[i].value_produced_per_type[0] << ", "
              << producers[i].n_items_produced_per_type[1] << ":"
              << producers[i].value_produced_per_type[1] << ", "
              << producers[i].n_items_produced_per_type[2] << ":"
              << producers[i].value_produced_per_type[2] << ", "
              << producers[i].value_produced_per_type[0] +
                     producers[i].value_produced_per_type[1] +
                     producers[i].value_produced_per_type[2]
              << ", " << producer_times[i] << std::endl;
  }

  std::cout << "Total produced = "
            << total_produced[0] + total_produced[1] + total_produced[2]
            << "\n";
  std::cout << "Total value produced = "
            << total_value_produced[0] + total_value_produced[1] +
                   total_value_produced[2]
            << "\n";
  std::cout << "Consumer stats\n";
  std::cout << "consumer_id, items_consumed_type0:value_type0, "
               "items_consumed_type1:value_type1, "
               "items_consumed_type2:value_type2, time_taken\n";

  long total_consumed[3] = {0};        // total consumed per type
  long total_value_consumed[3] = {0};  // total value consumed per type = 0;

  for (int i = 0; i < n_consumers; i++) {
    for (int j = 0; j < 3; j++) {
      total_consumed[j] += consumers[i].n_items_consumed_per_type[j];
      total_value_consumed[j] += consumers[i].value_consumed_per_type[j];
    }
  }

  // Make sure you print the consumer stats in the following manner
  // 0, 256488:63656791749, 163534:109699063438, 87398:79885550318, 1.02899
  // 1, 243512:61342958251, 169798:112521881008, 79270:72893255236, 1.02891
  for (int i = 0; i < n_consumers; i++) {
    // Print per consumer statistcs with above format
    std::cout << consumers[i].consumer_id << ", "
              << consumers[i].n_items_consumed_per_type[0] << ":"
              << consumers[i].value_consumed_per_type[0] << ", "
              << consumers[i].n_items_consumed_per_type[1] << ":"
              << consumers[i].value_consumed_per_type[1] << ", "
              << consumers[i].n_items_consumed_per_type[2] << ":"
              << consumers[i].value_consumed_per_type[2] << ", "
              << consumer_times[i] << std::endl;
  }

  std::cout << "Total consumed = "
            << total_consumed[0] + total_consumed[1] + total_consumed[2]
            << "\n";
  std::cout << "Total value consumed = "
            << total_value_consumed[0] + total_value_consumed[1] +
                   total_value_consumed[2]
            << "\n";
}
