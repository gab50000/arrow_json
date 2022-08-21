 cmake_minimum_required(VERSION 3.22.2)
 project(ArrowJSON)

 add_definitions("-std=c++11")

 include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
 conan_basic_setup()

 add_executable(main main.cpp)
 target_link_libraries(main ${CONAN_LIBS})