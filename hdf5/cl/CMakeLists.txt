# Minimum version of CMake required
cmake_minimum_required(VERSION 3.15)

# Define the project name and language
project(MySingleIntProject CXX)

# Use C++17 standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# --- Find the HDF5 Library ---
# Point CMake to the HDF5 installation directory.
set(HDF5_DIR "C:/Program Files/HDF_Group/HDF5/1.14.6/cmake")

# Use find_package to locate HDF5 and its components.
find_package(HDF5 REQUIRED)

# --- Create Your Executable ---
# This line tells CMake to build an executable named "my_hdf5_app"
# from your source file.
add_executable(my_hdf5_app create_single_int.cpp)

# --- Link Your Executable to HDF5 ---
# This line does all the magic: adds include paths, library paths,
# link libraries, and necessary definitions.
target_link_libraries(my_hdf5_app PRIVATE HDF5::HDF5)

message(STATUS "SUCCESS: Building your custom application 'my_hdf5_app'.")