add_test (
    NAME egen_chunking
    COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/test_egen_chunking
)
set_tests_properties (
    egen_chunking
    PROPERTIES
    ENVIRONMENT
    "TOPDIR=${CMAKE_SOURCE_DIR};DBT5_BUILD_EGEN=${CMAKE_BINARY_DIR}/dbt5-build-egen"
    LABELS "slow"
    RUN_SERIAL TRUE
    SKIP_RETURN_CODE 77
    TIMEOUT 86400
)

add_test (
    NAME pgsql_transactions
    COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/test_pgsql_transactions
)
set_tests_properties (
    pgsql_transactions
    PROPERTIES
    ENVIRONMENT "TOPDIR=${CMAKE_SOURCE_DIR}"
    LABELS "integration;pgsql"
    RUN_SERIAL TRUE
    SKIP_RETURN_CODE 77
    TIMEOUT 86400
)

add_test (
    NAME pgsql_storedprocs_plpgsql
    COMMAND /bin/sh
            ${CMAKE_CURRENT_SOURCE_DIR}/test_pgsql_storedprocs_plpgsql
)
set_tests_properties (
    pgsql_storedprocs_plpgsql
    PROPERTIES
    ENVIRONMENT "TOPDIR=${CMAKE_SOURCE_DIR}"
    LABELS "integration;pgsql"
    RUN_SERIAL TRUE
    SKIP_RETURN_CODE 77
    TIMEOUT 86400
)

add_test (
    NAME pgsql_storedprocs_c
    COMMAND /bin/sh ${CMAKE_CURRENT_SOURCE_DIR}/test_pgsql_storedprocs_c
)
set_tests_properties (
    pgsql_storedprocs_c
    PROPERTIES
    ENVIRONMENT "TOPDIR=${CMAKE_SOURCE_DIR}"
    LABELS "integration;pgsql"
    RUN_SERIAL TRUE
    SKIP_RETURN_CODE 77
    TIMEOUT 86400
)
