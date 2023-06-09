
set(TEST_BASIC_SRC test.cpp)
add_executable(test_basic ${TEST_BASIC_SRC})
add_dependencies(test_basic ${PROJECT_NAME})
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/data)
file(MAKE_DIRECTORY ${CMAKE_SOURCE_DIR}/log)
add_test(test ${CMAKE_BINARY_DIR}/bin/test_basic ${CMAKE_BINARY_DIR}/data/file.dat)
set_property(TEST test APPEND PROPERTY ENVIRONMENT LD_PRELOAD=${CMAKE_BINARY_DIR}/lib/libdlio_profiler.so)
set_property(TEST test APPEND PROPERTY ENVIRONMENT LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib)
set_property(TEST test APPEND PROPERTY ENVIRONMENT DLIO_PROFILER_DIR=${CMAKE_BINARY_DIR}/data)
set_property(TEST test APPEND PROPERTY ENVIRONMENT DLIO_PROFILER_LOG_DIR=${CMAKE_SOURCE_DIR}/log)
set_property(TEST test APPEND PROPERTY ENVIRONMENT DLIO_PROFILER_LOG_LEVEL=INFO)

find_program(PYTHON_EXE python)
message("-- Found python at location " ${PYTHON_EXE})
add_test(test_py python3 ${CMAKE_CURRENT_SOURCE_DIR}/test.py)
file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/test/data)
set_property(TEST test_py APPEND PROPERTY ENVIRONMENT PYTHONPATH=$ENV{PYTHONPATH}:${CMAKE_SOURCE_DIR}/venv/lib)
set_property(TEST test_py APPEND PROPERTY ENVIRONMENT LD_LIBRARY_PATH=${CMAKE_BINARY_DIR}/lib:${CMAKE_SOURCE_DIR}/dependency/.spack-env/view/lib64)
set_property(TEST test_py APPEND PROPERTY ENVIRONMENT DLIO_PROFILER_LOG_LEVEL=INFO)
