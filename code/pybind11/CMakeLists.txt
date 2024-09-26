# Add pybind11
find_package(pybind11 REQUIRED)

set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ version selection")

set(pybind_module_name "pando_api")     ## variable in cmake called ${pybind_module_name}

#TODO the NO_EXTRAS flag, according to documentation, can potentially increase
#binary size. Without it, we were getting seg faults on cout. Should figure out
#the best way to handle this
pybind11_add_module(${pybind_module_name} NO_EXTRAS ./python_api.cpp)     ## creates a target


#pybind11_add_module(${pybind_module_name} ./python_api.cpp)     ## creates a target
                    ## normally this would be an executable or a shared objected with the
                    ## output name "${pybind_module_name}", but due to the special pybind11
                    ## stuff, it is actually called ${pybind_module_name}_something_something.so
                    ## this is a /target/ in cmake

set_target_properties(${pybind_module_name} PROPERTIES COMPILE_FLAGS "-DPYBIND_MODULE_NAME=${pybind_module_name}")
    ## now set a "property" on the target, saying add to the command line that runs
    ## the compiler to set -DX=Y, meaning inside of the code "X" is a macro that is
    ## set as "#define X Y" in the beginning of compilation

target_link_libraries(${pybind_module_name} PRIVATE elga pando stdc++fs boost_context boost_fiber)
    ## make the new target "linked" to pando, that is, it will have pando's code and includes
    ## properly connected. In this case pando has no code so just includes

set_property(TARGET ${pybind_module_name} PROPERTY CXX_STANDARD 17)
set_property(TARGET ${pybind_module_name} PROPERTY CXX_STANDARD_REQUIRED on)
    ## make the new target a c++17 

target_compile_options(${pybind_module_name} PRIVATE -Werror -Wall -Wextra)
    ## make the new target complain loudly with errors so we program better

configure_file("${PROJECT_SOURCE_DIR}/pybind11/pando.py" "pando.py")
