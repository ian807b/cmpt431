#include <stdlib.h>

#include <iomanip>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "core/graph.h"
#include "core/utils.h"

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
typedef double PageRankType;
#endif

void compute_page_rank(uintV start, uintV end, PageRankType pr_curr[], PageRankType pr_next[],
                       CustomBarrier &barrier, uint max_iters, Graph &g,
                       std::vector<double> &time_per_thread, uintV thread_id,
                       std::vector<std::mutex> &mutex) {
  timer timer;
  timer.start();
  for (int iter = 0; iter < max_iters; iter++) {
    for (uintV u = start; u < end; u++) {
      uintE out_degree = g.vertices_[u].getOutDegree();
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        std::lock_guard<std::mutex> guard(mutex[v]);  // Mutex
        pr_next[v] += (pr_curr[u] / (PageRankType)out_degree);
      }
    }
    barrier.wait();
    for (uintV v = start; v < end; v++) {
      pr_next[v] = PAGE_RANK(pr_next[v]);
      pr_curr[v] = pr_next[v];
      pr_next[v] = 0.0;
    }
    barrier.wait();
  }
  time_per_thread[thread_id] = timer.stop();
}

void pageRankParallel(Graph &g, int max_iters, uint n_threads) {
  uintV n = g.n_;

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  std::vector<std::thread> threads;
  std::vector<double> time_per_thread(n_threads, 0.0);
  CustomBarrier barrier(n_threads);
  std::vector<std::mutex> mutex(n);

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  t1.start();

  // Variables for parallel execution
  uintV start, end;
  uintV base_num_of_vertices = n / n_threads;
  uintV remaining_vertices = n % n_threads;

  for (uintV i = 0; i < n_threads; i++) {
    start = i * base_num_of_vertices;
    if (i == n_threads - 1) {
      end = start + base_num_of_vertices + remaining_vertices;
    } else {
      end = start + base_num_of_vertices;
    }
    threads.emplace_back(compute_page_rank, start, end, pr_curr, pr_next, std::ref(barrier),
                         max_iters, std::ref(g), std::ref(time_per_thread), i, std::ref(mutex));
  }

  for (auto &t : threads) {
    t.join();
  }

  time_taken = t1.stop();
  // -------------------------------------------------------------------
  std::cout << "thread_id, time_taken" << std::endl;
  for (uintV i = 0; i < n_threads; i++) {
    std::cout << i << ", " << time_per_thread[i] << std::endl;
  }

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page ranks : " << sum_of_page_ranks << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] pr_curr;
  delete[] pr_next;
}

int main(int argc, char *argv[]) {
  cxxopts::Options options("page_rank_push",
                           "Calculate page_rank using serial and parallel execution");
  options.add_options(
      "", {
              {"nThreads", "Number of Threads",
               cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
              {"nIterations", "Maximum number of iterations",
               cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
              {"inputFile", "Input graph file path",
               cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
          });

  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::cout << "Using INT" << std::endl;
#else
  std::cout << "Using DOUBLE" << std::endl;
#endif
  std::cout << std::fixed;
  std::cout << "Number of Threads : " << n_threads << std::endl;
  std::cout << "Number of Iterations: " << max_iterations << std::endl;

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";
  pageRankParallel(g, max_iterations, n_threads);

  return 0;
}
