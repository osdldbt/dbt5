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
