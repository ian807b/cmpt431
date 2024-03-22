#include <stdlib.h>

#include <atomic>
#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "core/graph.h"
#include "core/utils.h"

#define DEFAULT_STRATEGY_CHOICE "1"
#define DEFAULT_GRANULARITY "1"

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

struct ThreadStats {
  uintV vertices_processed = 0;
  uintE edges_processed = 0;
  double time_taken_barrier1 = 0.0;
  double time_taken_barrier2 = 0.0;
  double time_in_getNextVertex = 0.0;
};

std::atomic<uintV> next_vertex(0);
std::atomic<double> total_time_in_getNextVertex(0.0);

int getNextVertexToBeProcessed(uintV n, int granularity = 1) {
  uintV vertex = next_vertex.fetch_add(granularity, std::memory_order_relaxed);
  if (vertex < n) {
    return vertex;
  } else {
    return -1;
  }
}

void dynamic_page_rank_granularity(PageRankType pr_curr[], PageRankType pr_next[],
                                   CustomBarrier &barrier1, CustomBarrier &barrier2, uint max_iters,
                                   Graph &g, std::vector<double> &time_per_thread, uint thread_id,
                                   std::vector<ThreadStats> &thread_stats, int granularity) {
  timer timer, barrier_timer, getNextVertex_timer;
  timer.start();
  for (uint iter = 0; iter < max_iters; iter++) {
    while (true) {
      getNextVertex_timer.start();
      int start_vertex = getNextVertexToBeProcessed(g.n_, granularity);
      thread_stats[thread_id].time_in_getNextVertex += getNextVertex_timer.stop();
      if (start_vertex == -1) break;

      for (int i = 0; i < granularity && start_vertex + i < g.n_; ++i) {
        int u = start_vertex + i;
        thread_stats[thread_id].vertices_processed++;
        uintE in_degree = g.vertices_[u].getInDegree();
        thread_stats[thread_id].edges_processed += in_degree;
        for (uintE j = 0; j < in_degree; j++) {
          uintV v = g.vertices_[u].getInNeighbor(j);
          uintE v_out_degree = g.vertices_[v].getOutDegree();
          if (v_out_degree > 0) {
            pr_next[u] += (pr_curr[v] / (PageRankType)v_out_degree);
          }
        }
      }
    }
    barrier_timer.start();
    barrier1.wait();
    thread_stats[thread_id].time_taken_barrier1 += barrier_timer.stop();

    if (thread_id == 0) {
      next_vertex.store(0);
    }
    barrier_timer.start();
    barrier1.wait();
    thread_stats[thread_id].time_taken_barrier1 += barrier_timer.stop();

    while (true) {
      getNextVertex_timer.start();
      int start_vertex = getNextVertexToBeProcessed(g.n_, granularity);
      thread_stats[thread_id].time_in_getNextVertex += getNextVertex_timer.stop();
      if (start_vertex == -1) break;

      for (int i = 0; i < granularity && start_vertex + i < g.n_; ++i) {
        int v = start_vertex + i;
        pr_next[v] = PAGE_RANK(pr_next[v]);
        pr_curr[v] = pr_next[v];
        pr_next[v] = 0.0;
      }
    }
    barrier_timer.start();
    barrier2.wait();
    thread_stats[thread_id].time_taken_barrier2 += barrier_timer.stop();

    if (thread_id == 0) {
      next_vertex.store(0);
    }
    barrier_timer.start();
    barrier2.wait();
    thread_stats[thread_id].time_taken_barrier2 += barrier_timer.stop();
  }
  time_per_thread[thread_id] = timer.stop();
}

void dynamic_page_rank(PageRankType pr_curr[], PageRankType pr_next[], CustomBarrier &barrier1,
                       CustomBarrier &barrier2, uint max_iters, Graph &g,
                       std::vector<double> &time_per_thread, uint thread_id,
                       std::vector<ThreadStats> &thread_stats) {
  timer timer, barrier_timer, getNextVertex_timer;
  timer.start();
  for (uint iter = 0; iter < max_iters; iter++) {
    while (true) {
      getNextVertex_timer.start();
      uintV u = getNextVertexToBeProcessed(g.n_);
      thread_stats[thread_id].time_in_getNextVertex += getNextVertex_timer.stop();
      if (u == -1) break;
      thread_stats[thread_id].vertices_processed++;
      uintE in_degree = g.vertices_[u].getInDegree();
      thread_stats[thread_id].edges_processed += in_degree;
      for (uintE i = 0; i < in_degree; i++) {
        uintV v = g.vertices_[u].getInNeighbor(i);
        uintE v_out_degree = g.vertices_[v].getOutDegree();
        if (v_out_degree > 0) {
          pr_next[u] += (pr_curr[v] / (PageRankType)v_out_degree);
        }
      }
    }
    barrier_timer.start();
    barrier1.wait();
    thread_stats[thread_id].time_taken_barrier1 += barrier_timer.stop();

    if (thread_id == 0) {
      next_vertex.store(0);
    }
    barrier_timer.start();
    barrier1.wait();
    thread_stats[thread_id].time_taken_barrier1 += barrier_timer.stop();

    while (true) {
      getNextVertex_timer.start();
      uintV v = getNextVertexToBeProcessed(g.n_);
      thread_stats[thread_id].time_in_getNextVertex += getNextVertex_timer.stop();
      if (v == -1) break;
      pr_next[v] = PAGE_RANK(pr_next[v]);
      pr_curr[v] = pr_next[v];
      pr_next[v] = 0.0;
    }
    barrier_timer.start();
    barrier2.wait();
    thread_stats[thread_id].time_taken_barrier2 += barrier_timer.stop();

    if (thread_id == 0) {
      next_vertex.store(0);
    }
    barrier_timer.start();
    barrier2.wait();
    thread_stats[thread_id].time_taken_barrier2 += barrier_timer.stop();
  }
  time_per_thread[thread_id] = timer.stop();
}

void compute_page_rank(uintV start, uintV end, PageRankType pr_curr[], PageRankType pr_next[],
                       CustomBarrier &barrier1, CustomBarrier &barrier2, uint max_iters, Graph &g,
                       std::vector<double> &time_per_thread, uint thread_id,
                       std::vector<ThreadStats> &thread_stats) {
  timer timer, barrier_timer;
  timer.start();
  for (int iter = 0; iter < max_iters; iter++) {
    for (uintV v = start; v < end; v++) {
      thread_stats[thread_id].vertices_processed++;
      uintE in_degree = g.vertices_[v].getInDegree();
      thread_stats[thread_id].edges_processed += in_degree;
      for (uintE i = 0; i < in_degree; i++) {
        uintV u = g.vertices_[v].getInNeighbor(i);
        uintE u_out_degree = g.vertices_[u].getOutDegree();
        if (u_out_degree > 0) pr_next[v] += (pr_curr[u] / (PageRankType)u_out_degree);
      }
    }
    barrier_timer.start();
    barrier1.wait();
    thread_stats[thread_id].time_taken_barrier1 += barrier_timer.stop();

    for (uintV v = start; v < end; v++) {
      pr_next[v] = PAGE_RANK(pr_next[v]);
      pr_curr[v] = pr_next[v];
      pr_next[v] = 0.0;
    }

    barrier_timer.start();
    barrier2.wait();
    thread_stats[thread_id].time_taken_barrier2 += barrier_timer.stop();
  }

  time_per_thread[thread_id] = timer.stop();
}

void case_four(PageRankType pr_curr[], PageRankType pr_next[], uint max_iters, Graph &g,
               std::vector<double> &time_per_thread, uint n_threads,
               std::vector<std::thread> &threads, std::vector<ThreadStats> &thread_stats,
               int granularity) {
  CustomBarrier barrier1(n_threads);
  CustomBarrier barrier2(n_threads);

  for (uint thread_id = 0; thread_id < n_threads; thread_id++) {
    threads.emplace_back(dynamic_page_rank_granularity, pr_curr, pr_next, std::ref(barrier1),
                         std::ref(barrier2), max_iters, std::ref(g), std::ref(time_per_thread),
                         thread_id, std::ref(thread_stats), granularity);
  }

  for (auto &t : threads) {
    t.join();
  }
}

void case_three(PageRankType pr_curr[], PageRankType pr_next[], uint max_iters, Graph &g,
                std::vector<double> &time_per_thread, uint n_threads,
                std::vector<std::thread> &threads, std::vector<ThreadStats> &thread_stats) {
  CustomBarrier barrier1(n_threads);
  CustomBarrier barrier2(n_threads);

  for (uint thread_id = 0; thread_id < n_threads; thread_id++) {
    threads.emplace_back(dynamic_page_rank, pr_curr, pr_next, std::ref(barrier1),
                         std::ref(barrier2), max_iters, std::ref(g), std::ref(time_per_thread),
                         thread_id, std::ref(thread_stats));
  }

  for (auto &t : threads) {
    t.join();
  }
}

void case_two(PageRankType pr_curr[], PageRankType pr_next[], uint max_iters, Graph &g,
              std::vector<double> &time_per_thread, uint n_threads,
              std::vector<std::thread> &threads, uintV n, uintE m,
              std::vector<ThreadStats> &thread_stats) {
  uintE m_div_t = m / n_threads;
  uintV start_vertex = 0;
  uintE total_assigned_edges = 0;
  CustomBarrier barrier1(n_threads);
  CustomBarrier barrier2(n_threads);

  for (uint thread_id = 0; thread_id < n_threads; thread_id++) {
    uintE min_num_edges = (thread_id + 1) * m_div_t;
    uintV end_vertex = start_vertex;

    if (thread_id == n_threads - 1) {
      end_vertex = n;
    } else {
      while (end_vertex < n && total_assigned_edges < min_num_edges) {
        total_assigned_edges += g.vertices_[end_vertex].getInDegree();
        end_vertex++;
      }
    }

    threads.emplace_back(compute_page_rank, start_vertex, end_vertex, pr_curr, pr_next,
                         std::ref(barrier1), std::ref(barrier2), max_iters, std::ref(g),
                         std::ref(time_per_thread), thread_id, std::ref(thread_stats));

    start_vertex = end_vertex;
  }

  for (auto &t : threads) {
    t.join();
  }
}

void case_one(PageRankType pr_curr[], PageRankType pr_next[], uint max_iters, Graph &g,
              std::vector<double> &time_per_thread, uint n_threads,
              std::vector<std::thread> &threads, uintV n, std::vector<ThreadStats> &thread_stats) {
  uintV start, end;
  uintV base_num_of_vertices = n / n_threads;
  uintV remaining_vertices = n % n_threads;
  CustomBarrier barrier1(n_threads);
  CustomBarrier barrier2(n_threads);

  for (uint thread_id = 0; thread_id < n_threads; thread_id++) {
    start = thread_id * base_num_of_vertices;
    if (thread_id == n_threads - 1) {
      end = start + base_num_of_vertices + remaining_vertices;
    } else {
      end = start + base_num_of_vertices;
    }
    threads.emplace_back(compute_page_rank, start, end, pr_curr, pr_next, std::ref(barrier1),
                         std::ref(barrier2), max_iters, std::ref(g), std::ref(time_per_thread),
                         thread_id, std::ref(thread_stats));
  }

  for (auto &t : threads) {
    t.join();
  }
}

void pageRankParallel(Graph &g, uint max_iters, uint n_threads, int strategy_choice,
                      int granularity) {
  uintV n = g.n_;
  uintE m = g.m_;
  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];
  std::vector<std::thread> threads;
  std::vector<double> time_per_thread(n_threads, 0.0);
  std::vector<ThreadStats> thread_stats(n_threads);

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  timer t1;
  double time_taken = 0.0;
  t1.start();

  switch (strategy_choice) {
    case 1:
      case_one(pr_curr, pr_next, max_iters, std::ref(g), std::ref(time_per_thread), n_threads,
               std::ref(threads), n, std::ref(thread_stats));
      break;
    case 2:
      case_two(pr_curr, pr_next, max_iters, std::ref(g), std::ref(time_per_thread), n_threads,
               std::ref(threads), n, m, std::ref(thread_stats));
      break;
    case 3:
      case_three(pr_curr, pr_next, max_iters, std::ref(g), std::ref(time_per_thread), n_threads,
                 std::ref(threads), std::ref(thread_stats));
      break;
    case 4:
      case_four(pr_curr, pr_next, max_iters, std::ref(g), std::ref(time_per_thread), n_threads,
                std::ref(threads), std::ref(thread_stats), granularity);
      break;
    default:
      case_one(pr_curr, pr_next, max_iters, std::ref(g), std::ref(time_per_thread), n_threads,
               std::ref(threads), n, std::ref(thread_stats));
  }

  time_taken = t1.stop();
  // -------------------------------------------------------------------
  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, "
               "getNextVertex_time, total_time"
            << std::endl;
  for (uintV i = 0; i < n_threads; i++) {
    std::cout << i << ", " << thread_stats[i].vertices_processed << ", "
              << thread_stats[i].edges_processed << ", " << thread_stats[i].time_taken_barrier1
              << ", " << thread_stats[i].time_taken_barrier2 << ", "
              << thread_stats[i].time_in_getNextVertex << ", " << time_per_thread[i] << std::endl;
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
  cxxopts::Options options("page_rank_pull",
                           "Calculate page_rank using serial and parallel execution");
  options.add_options(
      "", {
              {"nThreads", "Number of Threads",
               cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
              {"nIterations", "Maximum number of iterations",
               cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
              {"inputFile", "Input graph file path",
               cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
              {"strategy", "Choice of Strategies",
               cxxopts::value<int>()->default_value(DEFAULT_STRATEGY_CHOICE)},
              {"granularity", "Choice of Granularity",
               cxxopts::value<int>()->default_value(DEFAULT_GRANULARITY)},
          });

  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  int strategy_choice = cl_options["strategy"].as<int>();
  int granularity = cl_options["granularity"].as<int>();

  if (strategy_choice != 1 && strategy_choice != 2 && strategy_choice != 3 &&
      strategy_choice != 4) {
    std::cout << "Available strategy choices: 1, 2, 3, and 4.\n";
    return 1;
  }

  if (granularity <= 0) {
    std::cout << "Granularity can only be a positive integer.\n";
    return 1;
  }

  if (strategy_choice != 4 && granularity != 1) {
    std::cout << "Granularity can specified for strategy 4 only.\n";
    return 1;
  }

#ifdef USE_INT
  std::cout << "Using INT\n";
#else
  std::cout << "Using DOUBLE\n";
#endif
  std::cout << std::fixed;
  std::cout << "Number of Threads : " << n_threads << std::endl;
  std::cout << "Strategy : " << strategy_choice << std::endl;
  std::cout << "Granularity : " << granularity << std::endl;
  std::cout << "Iterations: " << max_iterations << std::endl;

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  pageRankParallel(g, max_iterations, n_threads, strategy_choice, granularity);

  return 0;
}
