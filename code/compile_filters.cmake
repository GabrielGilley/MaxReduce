function(compile_filters SRC_DIR DST_DIR)

    # First, create all python filters and their cpp wrappers so they all get compiled together later
    file(GLOB_RECURSE filter_py "${SRC_DIR}/*.pyx")
    foreach (src ${filter_py})
        get_filename_component(filter_name ${src} NAME_WE)
        configure_file("${PROJECT_SOURCE_DIR}/python_filter_template/python_template.cpp" "${filter_name}_cpp_interface.cpp")
        set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${src})
        execute_process(
            COMMAND cython -+ -o "${CMAKE_CURRENT_BINARY_DIR}/${filter_name}.cpp" ${src} -I "${PROJECT_SOURCE_DIR}/python_filter_template"
            RESULT_VARIABLE ret
        )
        if (NOT ret EQUAL "0")
            message(FATAL_ERROR "Cython compilation of ${src} failed")
        endif()

        set(src "${CMAKE_CURRENT_BINARY_DIR}/${filter_name}.cpp" "${CMAKE_CURRENT_BINARY_DIR}/${filter_name}_cpp_interface.cpp")

        add_library(${filter_name} SHARED ${src})
        target_link_libraries(${filter_name} pando elga)

        # Allow cython to use "single-phase" initialization so that filters can be imported after Py_Initialize()
        target_compile_options(${filter_name} PRIVATE -DCYTHON_PEP489_MULTI_PHASE_INIT=0)

        include_directories(${PYTHON_INCLUDE_DIRS})
        target_link_libraries(${filter_name} ${PYTHON_LIBRARIES})

        set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD 17)
        set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD_REQUIRED on)

        if (NOT ${DST_DIR} STREQUAL "")
            set_target_properties(${filter_name} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${DST_DIR})
        endif()
        target_compile_options(${filter_name} PRIVATE -Werror -Wall -Wextra)
    endforeach()


    file(GLOB_RECURSE filter_sources "${SRC_DIR}/*.cpp")
    foreach(src ${filter_sources})
        get_filename_component(filter_name ${src} NAME_WE)
        get_filename_component(filter_dir ${src} DIRECTORY)

        add_library(${filter_name} SHARED ${src})
        target_link_libraries(${filter_name} pando rapidjson elga)

        #FIXME generalize this somehow
        if (${filter_name} MATCHES ".*smart_contract_decode.*")
            target_link_libraries(${filter_name} pybind11::embed)
        endif()

        set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD 17)
        set_property(TARGET ${filter_name} PROPERTY CXX_STANDARD_REQUIRED on)

        if (NOT ${DST_DIR} STREQUAL "")
            set_target_properties(${filter_name} PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${DST_DIR})
        endif()
        target_compile_options(${filter_name} PRIVATE -Werror -Wall -Wextra)
    endforeach()

    file(GLOB_RECURSE filter_deps "${SRC_DIR}/*.py" "${SRC_DIR}/*.json")
    foreach (dep ${filter_deps})
        configure_file(${dep} "." COPYONLY)
    endforeach()

    file(GLOB_RECURSE filter_rust "${SRC_DIR}/*.toml")
    foreach (rs ${filter_rust})
        corrosion_import_crate(MANIFEST_PATH ${rs})
    endforeach()

endfunction()
