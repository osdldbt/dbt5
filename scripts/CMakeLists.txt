add_subdirectory (pgsql)

set (PROGRAMFILES "")
foreach (FILE dbt5
              dbt5-build
              dbt5-build-egen
              dbt5-get-os-info
              dbt5-post-process
              dbt5-report
              dbt5-run
        )
    configure_file(${FILE}.in ${CMAKE_BINARY_DIR}/${FILE} @ONLY)
    set (PROGRAMFILES ${PROGRAMFILES} ${CMAKE_BINARY_DIR}/${FILE})
endforeach()

install (
    PROGRAMS ${PROGRAMFILES}
    DESTINATION "bin"
)
