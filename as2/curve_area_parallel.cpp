#include <stdlib.h>

#include <iomanip>
#include <iostream>
#include <thread>
#include <vector>

#include "core/utils.h"

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "1000000000"
#define DEFAULT_NUMBER_OF_THREADS "1"
#define DEFAULT_A "2"
#define DEFAULT_B "1"
#define DEFAULT_RANDOM_SEED "1"

struct ThreadData {
  unsigned long curve_points;
  double time_taken;
};

uint c_const = (uint)RAND_MAX + (uint)1;
inline double get_random_coordinate(uint *random_seed) {
  return ((double)rand_r(random_seed)) /
         c_const;  // thread-safe random number generator
}

ThreadData get_points_in_curve(unsigned long n_points_for_thread,
                               uint random_seed, float a, float b) {
  unsigned long local_curve_count = 0;
  double x_coord, y_coord;
  timer thread_timer;
  ThreadData thread_data;
  thread_timer.start();

  for (unsigned long i = 0; i < n_points_for_thread; i++) {
    x_coord = ((2.0 * get_random_coordinate(&random_seed)) - 1.0);
    y_coord = ((2.0 * get_random_coordinate(&random_seed)) - 1.0);
    if ((a * sqr(x_coord) + b * sqr(sqr(y_coord))) <= 1.0) local_curve_count++;
  }

  thread_data.curve_points = local_curve_count;
  thread_data.time_taken = thread_timer.stop();

  return thread_data;
}

void thread_function(int index, unsigned long points_per_thread, uint r_seed,
                     float a, float b, std::vector<ThreadData> *thread_data) {
  (*thread_data)[index] = get_points_in_curve(points_per_thread, r_seed, a, b);
}

void curve_area_calculation_parallel(unsigned long n_points, float a, float b,
                                     uint r_seed, uint n_threads) {
  uint random_seed = r_seed;
  std::vector<std::thread> threads;
  std::vector<ThreadData> thread_data(n_threads);
  unsigned long base_points_per_thread = n_points / n_threads;
  unsigned long remaining_points = n_points % n_threads;
  unsigned long points_per_thread;
  unsigned long total_curve_points = 0;
  double total_time_taken = 0.0;
  timer total_timer;

  total_timer.start();

  for (uint i = 0; i < n_threads; i++) {
    points_per_thread = base_points_per_thread + (i < remaining_points ? 1 : 0);
    random_seed = r_seed + i;
    threads.push_back(std::thread(thread_function, i, points_per_thread,
                                  random_seed, a, b, &thread_data));
  }

  for (auto &t : threads) {
    t.join();
  }

  for (auto data : thread_data) {
    total_curve_points += data.curve_points;
  }

  double area_value = 4.0 * (double)total_curve_points / (double)n_points;

  total_time_taken = total_timer.stop();

  //-------------------------- Print results --------------------------
  std::cout << "thread_id, points_generated, curve_points, time_taken\n";
  for (uint i = 0; i < n_threads; ++i) {
    points_per_thread = base_points_per_thread + (i < remaining_points ? 1 : 0);
    std::cout << i << ", " << points_per_thread << ", "
              << thread_data[i].curve_points << ", "
              << std::setprecision(TIME_PRECISION) << thread_data[i].time_taken
              << "\n";
  }
  std::cout << "Total points generated : " << n_points << "\n";
  std::cout << "Total points in curve : " << total_curve_points << "\n";
  std::cout << "Area : " << std::setprecision(VAL_PRECISION) << area_value
            << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << total_time_taken << "\n";
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("Curve_area_calculation",
                           "Calculate area inside curve a x^2 + b y ^4 = 1 "
                           "using serial and parallel execution");
  options.add_options(
      "custom",
      {{"nThreads", "Number of threads",
        cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
       {"nPoints", "Number of points",
        cxxopts::value<unsigned long>()->default_value(
            DEFAULT_NUMBER_OF_POINTS)},
       {"coeffA", "Coefficient a",
        cxxopts::value<float>()->default_value(DEFAULT_A)},
       {"coeffB", "Coefficient b",
        cxxopts::value<float>()->default_value(DEFAULT_B)},
       {"rSeed", "Random Seed",
        cxxopts::value<uint>()->default_value(DEFAULT_RANDOM_SEED)}});
  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  unsigned long n_points = cl_options["nPoints"].as<unsigned long>();
  float a = cl_options["coeffA"].as<float>();
  float b = cl_options["coeffB"].as<float>();
  uint r_seed = cl_options["rSeed"].as<uint>();
  std::cout << "Number of points : " << n_points << "\n";
  std::cout << "Number of threads : " << n_threads << "\n";
  std::cout << "A : " << a << "\n"
            << "B : " << b << "\n";
  std::cout << "Random Seed : " << r_seed << "\n";

  curve_area_calculation_parallel(n_points, a, b, r_seed, n_threads);
  return 0;
}