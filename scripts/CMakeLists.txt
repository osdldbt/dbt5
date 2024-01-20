add_subdirectory (pgsql)

configure_file(
    dbt5.in
    ${CMAKE_BINARY_DIR}/dbt5
    @ONLY
)

configure_file(
    dbt5-build.in
    ${CMAKE_BINARY_DIR}/dbt5-build
    @ONLY
)

configure_file(
    dbt5-build-egen.in
    ${CMAKE_BINARY_DIR}/dbt5-build-egen
    @ONLY
)

configure_file(
    dbt5-get-os-info.in
    ${CMAKE_BINARY_DIR}/dbt5-get-os-info
    @ONLY
)

configure_file(
    dbt5-post-process.in
    ${CMAKE_BINARY_DIR}/dbt5-post-process
    @ONLY
)

configure_file(
    dbt5-report.in
    ${CMAKE_BINARY_DIR}/dbt5-report
    @ONLY
)

configure_file(
    dbt5-run.in
    ${CMAKE_BINARY_DIR}/dbt5-run
    @ONLY
)

install (
    PROGRAMS ${CMAKE_BINARY_DIR}/dbt5
             ${CMAKE_BINARY_DIR}/dbt5-build
             ${CMAKE_BINARY_DIR}/dbt5-build-egen
             ${CMAKE_BINARY_DIR}/dbt5-get-os-info
             ${CMAKE_BINARY_DIR}/dbt5-post-process
             ${CMAKE_BINARY_DIR}/dbt5-report
             ${CMAKE_BINARY_DIR}/dbt5-run
    DESTINATION "bin"
)
