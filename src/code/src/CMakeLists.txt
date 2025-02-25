file(GLOB exe_sources "*.cpp")

foreach(src ${exe_sources})
    get_filename_component(exe_name ${src} NAME_WE)
    add_executable(${exe_name} ${src})

    target_link_libraries (${exe_name} LINK_PRIVATE
        pando
        elga
        )

    set_property(TARGET ${exe_name} PROPERTY CXX_STANDARD 17)
    set_property(TARGET ${exe_name} PROPERTY CXX_STANDARD_REQUIRED on)
    target_compile_options(${exe_name} PRIVATE -Werror -Wall -Wextra)

    install (TARGETS ${exe_name} DESTINATION ${CMAKE_INSTALL_BINDIR})
endforeach()

target_link_libraries(pando_top LINK_PRIVATE ncursesw)
