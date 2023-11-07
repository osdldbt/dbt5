configure_file(
    dbt5-pgsql-load-stored-procs.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-load-stored-procs
    @ONLY
)

install (
    PROGRAMS ${CMAKE_BINARY_DIR}/dbt5-pgsql-load-stored-procs
    DESTINATION "bin"
)
