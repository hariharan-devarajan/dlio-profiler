cmake_minimum_required(VERSION 3.4...3.18)
project(dlio_profiler)
#------------------------------------------------------------------------------
# Version information
#------------------------------------------------------------------------------
set(DLIO_PROFILER_VERSION_MAJOR "0")
set(DLIO_PROFILER_VERSION_MINOR "0")
set(DLIO_PROFILER_VERSION_PATCH "1")
set(DLIO_PROFILER_PACKAGE ${PROJECT_NAME})
set(DLIO_PROFILER_PACKAGE_NAME ${PROJECT_NAME})
set(DLIO_PROFILER_PACKAGE_VERSION "${DLIO_PROFILER_VERSION_MAJOR}.${DLIO_PROFILER_VERSION_MINOR}.${DLIO_PROFILER_VERSION_PATCH}")
set(DLIO_PROFILER_PACKAGE_VERSION_MAJOR "${DLIO_PROFILER_VERSION_MAJOR}.${DLIO_PROFILER_VERSION_MINOR}")
set(DLIO_PROFILER_PACKAGE_VERSION_MINOR "${DLIO_PROFILER_VERSION_PATCH}")
set(DLIO_PROFILER_PACKAGE_STRING "${DLIO_PROFILER_PACKAGE_NAME} ${DLIO_PROFILER_PACKAGE_VERSION}")
set(DLIO_PROFILER_PACKAGE_TARNAME "${DLIO_PROFILER_PACKAGE}")
cmake_minimum_required(VERSION 3.0)
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_BINARY_DIR}/lib/cmake)
link_directories(${CMAKE_BINARY_DIR}/lib)
set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_CXX_STANDARD 17)
set(DLIO_PROFILER_CMAKE_DIR ${CMAKE_SOURCE_DIR}/CMake ${CMAKE_BINARY_DIR}/lib/cmake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${DLIO_PROFILER_CMAKE_DIR})
set(CMAKE_PREFIX_PATH ${CMAKE_PREFIX_PATH} ${CMAKE_BINARY_DIR})

#------------------------------------------------------------------------------
# Build options
#------------------------------------------------------------------------------
option(BUILD_PYTHON_BINDINGS "Build python bindings." ON)

if (BUILD_PYTHON_BINDINGS)
    add_subdirectory(external/pybind11)
endif()
add_subdirectory(external/cpp-logger)
find_package(cpp-logger REQUIRED)
add_subdirectory(external/GOTCHA)
# Code needed for using gotcha as a submodule
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/external/GOTCHA/cmake/gotcha_testing.cmake
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/gotcha/gotcha-config.cmake @ONLY
)
export(EXPORT gotcha-targets
        FILE "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/gotcha/gotchaTargets.cmake")
include_directories(external/GOTCHA/include)
add_subdirectory(external/brahma)
add_dependencies(brahma gotcha)
link_directories(${CMAKE_BINARY_DIR}/lib)
link_directories(${CMAKE_BINARY_DIR})
set(DEPENDENCY_LIB stdc++fs -lhwloc)
if (BUILD_PYTHON_BINDINGS)
    execute_process (COMMAND python3 -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())" OUTPUT_VARIABLE PYTHON_SITE_PACKAGES OUTPUT_STRIP_TRAILING_WHITESPACE)
    message("-- " "[Python] ${PYTHON_SITE_PACKAGES} found for installing python binding")
endif()

if (${CPP_LOGGER_FOUND})
    include_directories(${CPP_LOGGER_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${CPP_LOGGER_LIBRARIES})
    message(STATUS "[DLIO_PROFILER] found cpp-logger at ${CPP_LOGGER_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "-- [DLIO_PROFILER] cpp-logger is needed for ${PROJECT_NAME} build")
endif ()
find_package(brahma REQUIRED)
if (${BRAHMA_FOUND})
    message(STATUS "[DLIO_PROFILER] found brahma at ${BRAHMA_INCLUDE_DIRS}")
    include_directories(${BRAHMA_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${BRAHMA_LIBRARIES})
else ()
    message(FATAL_ERROR "-- [DLIO_PROFILER] brahma is needed for ${PROJECT_NAME} build")
endif ()
# find Gotcha
find_package(gotcha REQUIRED)
if (${gotcha_FOUND})
    message(STATUS "[Brahma] found gotcha at ${gotcha_INCLUDE_DIRS}")
    include_directories(${gotcha_INCLUDE_DIRS})
    set(DEPENDENCY_LIB ${DEPENDENCY_LIB} ${gotcha_LIBRARIES})
else ()
    message(FATAL_ERROR "-- [Brahma] gotcha is needed for ${PROJECT_NAME} build")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
set(DLIO_PROFILER_SRC ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/brahma/posix.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/brahma/stdio.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/dlio_profiler.cpp
        ${CMAKE_CURRENT_SOURCE_DIR}/include/dlio_profiler/dlio_profiler.h
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/writer/chrome_writer.cpp)
set(DLIO_PROFILER_PUBLIC_INCLUDE
        ${CMAKE_CURRENT_SOURCE_DIR}/include/dlio_profiler/macro.h
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/brahma/posix.h
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/brahma/stdio.h
        ${CMAKE_CURRENT_SOURCE_DIR}/include/dlio_profiler/dlio_profiler.h)
set(DLIO_PROFILER_PRIVATE_INCLUDE
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/dlio_logger.h
        ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/core/constants.h)
add_library(${PROJECT_NAME} SHARED)
target_link_libraries(${PROJECT_NAME} ${DEPENDENCY_LIB})
add_dependencies(${PROJECT_NAME} cpp-logger)
add_dependencies(${PROJECT_NAME} brahma)
add_dependencies(${PROJECT_NAME} gotcha)
message(STATUS ${CMAKE_PREFIX_PATH})
if (BUILD_PYTHON_BINDINGS)
    pybind11_add_module(${PROJECT_NAME}_py ${DLIO_PROFILER_SRC} ${CMAKE_CURRENT_SOURCE_DIR}/src/dlio_profiler/dlio_profiler_py.cpp)
    target_compile_definitions(${PROJECT_NAME}_py PRIVATE VERSION_INFO=${EXAMPLE_VERSION_INFO})
    target_link_libraries(${PROJECT_NAME}_py PRIVATE ${DEPENDENCY_LIB})
endif()
target_sources(${PROJECT_NAME}
        PRIVATE
        ${DLIO_PROFILER_SRC}
        )
target_include_directories(${PROJECT_NAME}
        PRIVATE
        # where the library itself will look for its internal headers
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        PUBLIC
        # where top-level project will look for the library's public headers
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        # where external projects will look for the library's public headers
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )

#------------------------------------------------------------------------------
# Setup install and output Directories
#------------------------------------------------------------------------------
if (NOT DLIO_PROFILER_INSTALL_BIN_DIR)
    set(DLIO_PROFILER_INSTALL_BIN_DIR ${CMAKE_INSTALL_PREFIX}/bin)
    message(STATUS ${DLIO_PROFILER_INSTALL_BIN_DIR})
endif ()
if (NOT DLIO_PROFILER_INSTALL_LIB_DIR)
    set(DLIO_PROFILER_INSTALL_LIB_DIR ${CMAKE_INSTALL_PREFIX}/lib)
endif ()
if (NOT DLIO_PROFILER_INSTALL_INCLUDE_DIR)
    set(DLIO_PROFILER_INSTALL_INCLUDE_DIR ${CMAKE_INSTALL_PREFIX}/include)
endif ()
if (NOT DLIO_PROFILER_INSTALL_DATA_DIR)
    set(DLIO_PROFILER_INSTALL_DATA_DIR ${CMAKE_INSTALL_PREFIX}/share)
endif ()

if (NOT CMAKE_INSTALL_RPATH)
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
    set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif ()

#------------------------------------------------------------------------------
if (NOT DLIO_PROFILER_EXTERNALLY_CONFIGURED)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY
            ${CMAKE_BINARY_DIR}/bin CACHE PATH "Single Directory for all Executables."
            )

    set(CMAKE_INCLUDE_OUTPUT_DIRECTORY
            ${CMAKE_BINARY_DIR}/include CACHE PATH "Store the headers."
            )
    set(EXECUTABLE_OUTPUT_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY
            ${CMAKE_BINARY_DIR}/lib CACHE PATH "Single Directory for all Libraries"
            )
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY
            ${CMAKE_BINARY_DIR}/lib CACHE PATH "Single Directory for all static libraries."
            )
endif ()

#------------------------------------------------------------------------------
# Disallow in-source build
#------------------------------------------------------------------------------
if ("${CMAKE_CURRENT_SOURCE_DIR}" STREQUAL "${CMAKE_BINARY_DIR}")
    message(FATAL_ERROR
            "${PROJECT_NAME} requires an out of source Build. "
            "Please create a separate binary directory and run CMake there.")
endif ()

#------------------------------------------------------------------------------
# Set a default build type if none was specified
#------------------------------------------------------------------------------
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'RelWithDebInfo' as none was specified.")
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
            "MinSizeRel" "RelWithDebInfo")
endif ()

if (NOT "${CMAKE_CXX_STANDARD}")
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
endif ()

#-----------------------------------------------------------------------------
# Targets built within this project are exported at Install time for use
# by other projects.
#-----------------------------------------------------------------------------
if (NOT DLIO_PROFILER_EXPORTED_TARGETS)
    set(DLIO_PROFILER_EXPORTED_TARGETS "dlio-profiler-targets")
endif ()

set(DLIO_PROFILER_LIBTYPE SHARED)
set_target_properties(${libtarget}
        PROPERTIES
        DEBUG_OUTPUT_NAME ${LIB_DEBUG_NAME}
        RELEASE_OUTPUT_NAME ${LIB_RELEASE_NAME}
        MINSIZEREL_OUTPUT_NAME ${LIB_RELEASE_NAME}
        RELWITHDEBINFO_OUTPUT_NAME ${LIB_RELEASE_NAME}
        VERSION ${LIB_VERSION}
        SOVERSION ${LIB_VERSION}
        )

include(GNUInstallDirs)
# the variant with PUBLIC_HEADER property unfortunately does not preserve the folder structure
#set_target_properties(${PROJECT_NAME} PROPERTIES PUBLIC_HEADER "${public_headers}")
# so instead we iterate through public headers and install them "manually"
foreach (header ${DLIO_PROFILER_PUBLIC_INCLUDE})
    file(RELATIVE_PATH header_file_path "${CMAKE_CURRENT_SOURCE_DIR}/${PROJECT_NAME}" "${header}")
    get_filename_component(header_directory_path "${header_file_path}" DIRECTORY)
    install(
            FILES ${header}
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${header_directory_path}"
    )
    file(COPY ${header}
            DESTINATION "${CMAKE_INCLUDE_OUTPUT_DIRECTORY}/${header_directory_path}")
endforeach ()
#-----------------------------------------------------------------------------
# Configure the config.cmake file for the build directory
#-----------------------------------------------------------------------------
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/CMake/${PROJECT_NAME}-config.cmake.build.in
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config.cmake @ONLY
)

install(
        FILES
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config.cmake
        DESTINATION
        ${DLIO_PROFILER_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
)

install(TARGETS ${PROJECT_NAME}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(TARGETS ${PROJECT_NAME}_py
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS ${PROJECT_NAME}_py
        LIBRARY DESTINATION ${PYTHON_SITE_PACKAGES}
        PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
#-----------------------------------------------------------------------------
# Configure the ${PROJECT_NAME}-config-version .cmake file for the install directory
#-----------------------------------------------------------------------------
configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/CMake/${PROJECT_NAME}-config-version.cmake.in
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config-version.cmake @ONLY
)

install(
        FILES
        ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}-config-version.cmake
        DESTINATION
        ${DLIO_PROFILER_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
)

install(
        FILES
        ${CMAKE_BINARY_DIR}/external/GOTCHA/gotcha-config.cmake
        DESTINATION
        ${DLIO_PROFILER_INSTALL_LIB_DIR}/cmake/gotcha
)
install(
        TARGETS
        ${PROJECT_NAME}
        EXPORT
        ${DLIO_PROFILER_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${DLIO_PROFILER_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${DLIO_PROFILER_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${DLIO_PROFILER_INSTALL_BIN_DIR}
)
export(EXPORT ${DLIO_PROFILER_EXPORTED_TARGETS}
        FILE "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/cmake/${PROJECT_NAME}/${PROJECT_NAME}Targets.cmake")

install(EXPORT
        ${DLIO_PROFILER_EXPORTED_TARGETS}
        DESTINATION
        ${DLIO_PROFILER_INSTALL_LIB_DIR}/cmake/${PROJECT_NAME}
        FILE
        ${DLIO_PROFILER_EXPORTED_TARGETS}.cmake
        )
option(DLIO_PROFILER_BUILD_TEST "Build the unit tests" ON)
if (DLIO_PROFILER_BUILD_TEST)
    enable_testing()
    add_subdirectory(test)
endif ()
