#include <stdlib.h>

#include <iomanip>
#include <iostream>
#include <thread>

#include "core/utils.h"

#define DEFAULT_GRID_SIZE "1000"
#define DEFAULT_NUMBER_OF_THREADS "1"
#define DEFAULT_CX "1"
#define DEFAULT_CY "1"
#define DEFAULT_TIME_STEPS "1000"
#define DEFAULT_MIDDLE_TEMP "600"

class TemperatureArray {
 private:
  uint size;
  uint step;
  double Cx;
  double Cy;
  double *CurrArray;
  double *PrevArray;
  void assign(double *A, uint x, uint y, double newvalue) {
    A[x * size + y] = newvalue;
  };
  double read(double *A, uint x, uint y) { return A[x * size + y]; };

 public:
  TemperatureArray(uint input_size, double iCx, double iCy,
                   double init_temp) {  // create array of dimension sizexsize
    size = input_size;
    Cx = iCx;
    Cy = iCy;
    step = 0;
    CurrArray = (double *)malloc(size * size * sizeof(double));
    PrevArray = (double *)malloc(size * size * sizeof(double));
    for (uint i = 0; i < size; i++)
      for (uint j = 0; j < size; j++) {
        if ((i > size / 3) && (i < 2 * size / 3) && (j > size / 3) &&
            (j < 2 * size / 3)) {
          assign(PrevArray, i, j, init_temp);
          assign(CurrArray, i, j, init_temp);
        } else {
          assign(PrevArray, i, j, 0);
          assign(CurrArray, i, j, 0);
        }
      }
  };

  ~TemperatureArray() {
    free(PrevArray);
    free(CurrArray);
  };

  void IncrementStepCount() { step++; };

  uint ReadStepCount() { return (step); };

  void ComputeNewTemp(uint x, uint y) {
    if ((x > 0) && (x < size - 1) && (y > 0) && (y < size - 1))
      assign(CurrArray, x, y,
             read(PrevArray, x, y) +
                 Cx * (read(PrevArray, x - 1, y) + read(PrevArray, x + 1, y) -
                       2 * read(PrevArray, x, y)) +
                 Cy * (read(PrevArray, x, y - 1) + read(PrevArray, x, y + 1) -
                       2 * read(PrevArray, x, y)));
  };

  void SwapArrays() {
    double *temp = PrevArray;
    PrevArray = CurrArray;
    CurrArray = temp;
  };

  double temp(uint x, uint y) { return read(CurrArray, x, y); };
};

inline void heat_transfer_calculation(uint tid, uint size, uint start, uint end,
                                      double *time_taken, TemperatureArray *T,
                                      uint steps, CustomBarrier &barrier) {
  timer t1;
  t1.start();
  uint stepcount;
  for (stepcount = 1; stepcount <= steps; stepcount++) {
    for (uint x = start; x <= end; x++) {
      for (uint y = 0; y < size; y++) {
        T->ComputeNewTemp(x, y);
      }
    }
    barrier.wait();
    if (tid == 0) {
      T->SwapArrays();
      T->IncrementStepCount();
    }
    barrier.wait();
  }  // end of current step
  *time_taken = t1.stop();
}

void heat_transfer_calculation_parallel(uint size, uint number_of_threads,
                                        TemperatureArray *T, uint steps) {
  std::vector<std::thread> threads;
  std::vector<double> time_taken(number_of_threads, 0.0);
  std::vector<uint> startx(number_of_threads);
  std::vector<uint> endx(number_of_threads);
  CustomBarrier barrier(number_of_threads);
  timer timer;
  double total_time_taken = 0.0;
  timer.start();

  // The following code is used to determine start and end of each thread's
  // share of the grid Also used to determine which points to print out at the
  // end of this function
  uint min_columns_for_each_thread = size / number_of_threads;
  uint excess_columns = size % number_of_threads;
  uint curr_column = 0;

  for (uint i = 0; i < number_of_threads; i++) {
    startx[i] = curr_column;
    if (excess_columns > 0) {
      endx[i] = curr_column + min_columns_for_each_thread;
      excess_columns--;
    } else {
      endx[i] = curr_column + min_columns_for_each_thread - 1;
    }
    curr_column = endx[i] + 1;
  }
  for (uint i = 0; i < number_of_threads; i++) {
    threads.emplace_back(heat_transfer_calculation, i, size, startx[i], endx[i],
                         &time_taken[i], T, steps, std::ref(barrier));
  }

  for (auto &t : threads) {
    t.join();
  }
  total_time_taken = timer.stop();
  //*------------------------------------------------------------------------

  // Print these statistics for each thread
  std::cout << "thread_id, start_column, end_column, time_taken\n";
  for (uint i = 0; i < number_of_threads; ++i) {
    std::cout << i << ", " << startx[i] << ", " << endx[i] << ", "
              << std::setprecision(TIME_PRECISION) << time_taken[i] << "\n";
  }

  uint step = size / 6;
  uint position = 0;
  for (uint x = 0; x < 6; x++) {
    std::cout << "Temp[" << position << "," << position
              << "]=" << T->temp(position, position) << std::endl;
    position += step;
  }

  // Print temparature at select boundary points;
  for (uint i = 0; i < number_of_threads; i++) {
    std::cout << "Temp[" << endx[i] << "," << endx[i]
              << "]=" << T->temp(endx[i], endx[i]) << std::endl;
  }

  //*------------------------------------------------------------------------

  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << total_time_taken << "\n";
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options(
      "Heat_transfer_calculation",
      "Model heat transfer in a grid using serial and parallel execution");
  options.add_options(
      "custom",
      {{"nThreads", "Number of threads",
        cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_THREADS)},
       {"gSize", "Grid Size",
        cxxopts::value<uint>()->default_value(DEFAULT_GRID_SIZE)},
       {"iCX", "Coefficient of horizontal heat transfer",
        cxxopts::value<double>()->default_value(DEFAULT_CX)},
       {"iCY", "Coefficient of vertical heat transfer",
        cxxopts::value<double>()->default_value(DEFAULT_CY)},
       {"mTemp", "Temperature in middle of array",
        cxxopts::value<double>()->default_value(DEFAULT_MIDDLE_TEMP)},
       {"tSteps", "Time Steps",
        cxxopts::value<uint>()->default_value(DEFAULT_TIME_STEPS)}});
  auto cl_options = options.parse(argc, argv);
  uint n_threads = cl_options["nThreads"].as<uint>();
  uint grid_size = cl_options["gSize"].as<uint>();
  double Cx = cl_options["iCX"].as<double>();
  double Cy = cl_options["iCY"].as<double>();
  double init_temp = cl_options["mTemp"].as<double>();
  uint steps = cl_options["tSteps"].as<uint>();
  std::cout << "Grid Size : " << grid_size << "x" << grid_size << std::endl;
  std::cout << "Number of threads : " << n_threads << std::endl;
  std::cout << "Cx : " << Cx << std::endl << "Cy : " << Cy << std::endl;
  std::cout << "Temperature in the middle of grid : " << init_temp << std::endl;
  std::cout << "Time Steps : " << steps << std::endl;

  std::cout << "Initializing Temperature Array..." << std::endl;
  TemperatureArray *T = new TemperatureArray(grid_size, Cx, Cy, init_temp);
  if (!T) {
    std::cout << "Cannot Initialize Temperature Array...Terminating"
              << std::endl;
    return 2;
  }
  heat_transfer_calculation_parallel(grid_size, n_threads, T, steps);

  delete T;
  return 0;
}
