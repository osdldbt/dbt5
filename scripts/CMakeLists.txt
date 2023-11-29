add_subdirectory (pgsql)

configure_file(
    dbt5.in
    ${CMAKE_BINARY_DIR}/dbt5
    @ONLY
)

configure_file(
    dbt5-build-egen.in
    ${CMAKE_BINARY_DIR}/dbt5-build-egen
    @ONLY
)

configure_file(
    dbt5-post-process.in
    ${CMAKE_BINARY_DIR}/dbt5-post-process
    @ONLY
)

install (
    PROGRAMS ${CMAKE_BINARY_DIR}/dbt5
             ${CMAKE_BINARY_DIR}/dbt5-build-egen
             ${CMAKE_BINARY_DIR}/dbt5-post-process
    DESTINATION "bin"
)
