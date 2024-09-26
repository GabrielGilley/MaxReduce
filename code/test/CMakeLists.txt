include(${CMAKE_CURRENT_SOURCE_DIR}/../compile_filters.cmake)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/tests-bin)
set(CTEST_BINARY_DIRECTORY ${PROJECT_BINARY_DIR}/tests-bin)

file(GLOB test_sources "test_*.cpp")
foreach(src ${test_sources})
    get_filename_component(test_name ${src} NAME_WE)
    add_executable(${test_name} ${src})

    target_link_libraries(${test_name} pando)
    target_link_libraries(${test_name} elga)

    set_property(TARGET ${test_name} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${test_name} PROPERTY CXX_STANDARD_REQUIRED on)
    target_compile_options(${test_name} PRIVATE -Werror -Wall -Wextra)

    add_test(NAME ${test_name} COMMAND ${CTEST_BINARY_DIRECTORY}/${test_name} "${PROJECT_BINARY_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 180)
endforeach()

file(GLOB test_sources_python "test_*.py")
foreach(src ${test_sources_python})
    get_filename_component(test_name ${src} NAME_WE)
    get_filename_component(file_name ${src} NAME)

    configure_file(${src} ${EXECUTABLE_OUTPUT_PATH}/${file_name} COPYONLY)

    add_test(NAME ${test_name} COMMAND python3 -m pytest ${CTEST_BINARY_DIRECTORY})
    set_tests_properties(${test_name} PROPERTIES TIMEOUT 180)
endforeach()

file(GLOB testlib_sources "testlib_*.cpp")
foreach(src ${testlib_sources})
    get_filename_component(filter_name ${src} NAME_WE)
    add_library(${filter_name} SHARED ${src})

    target_link_libraries(${filter_name} pando)
    target_link_libraries(${filter_name} elga)

    set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD_REQUIRED on)
    target_compile_options(${filter_name} PRIVATE -Werror -Wall -Wextra)
endforeach()

compile_filters(${CMAKE_CURRENT_SOURCE_DIR}/filters filters/)
