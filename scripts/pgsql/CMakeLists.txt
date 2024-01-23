configure_file(
    dbt5-pgsql-build-db.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-build-db
    @ONLY
)
configure_file(
    dbt5-pgsql-check-db.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-check-db
    @ONLY
)
configure_file(
    dbt5-pgsql-create-db.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-db
    @ONLY
)
configure_file(
    dbt5-pgsql-create-indexes.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-indexes
    @ONLY
)
configure_file(
    dbt5-pgsql-create-tables.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-tables
    @ONLY
)
configure_file(
    dbt5-pgsql-load-stored-procs.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-load-stored-procs
    @ONLY
)
configure_file(
    dbt5-pgsql-plans.in
    ${CMAKE_BINARY_DIR}/dbt5-pgsql-plans
    @ONLY
)

install (
    PROGRAMS ${CMAKE_BINARY_DIR}/dbt5-pgsql-build-db
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-check-db
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-db
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-indexes
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-create-tables
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-load-stored-procs
             ${CMAKE_BINARY_DIR}/dbt5-pgsql-plans
    DESTINATION "bin"
)
