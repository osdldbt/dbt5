add_subdirectory (pgsql)

configure_file(
    dbt5-build-egen.in
    ${CMAKE_BINARY_DIR}/dbt5-build-egen
    @ONLY
)

install (
    PROGRAMS ${CMAKE_BINARY_DIR}/dbt5-build-egen
    DESTINATION "bin"
)
