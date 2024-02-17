CMAKE_MINIMUM_REQUIRED(VERSION 2.8.12)
PROJECT(dbt5 NONE)

set (PROJECT_VERSION_MAJOR 0)
set (PROJECT_VERSION_MINOR 6)
set (PROJECT_VERSION_PATCH 1)

set (PROJECT_VERSION
     ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set (SHELLOPTIONS "set -x")
endif ()

set (
    HOMEPAGE
    "Database Test 5 (DBT-5) project page: https://github.com/osdldbt/dbt5"
)

add_subdirectory (appimage)
add_subdirectory (patches)
add_subdirectory (man)
add_subdirectory (scripts)
add_subdirectory (src)
add_subdirectory (storedproc)

set (PGSQL_SCRIPTS scripts/pgsql/dbt5-pgsql-start-db
                   scripts/pgsql/dbt5-pgsql-drop-tables
                   scripts/pgsql/dbt5-pgsql-restore-db
                   scripts/pgsql/dbt5-pgsql-backup-db
                   scripts/pgsql/dbt5-pgsql-db-plans
                   scripts/pgsql/dbt5-pgsql-drop-db
                   scripts/pgsql/dbt5-pgsql-db-stat
                   scripts/pgsql/dbt5-pgsql-stop-db)

#
# Install rules.
#

INSTALL(PROGRAMS scripts/dbt5-plot-transaction-rate
                 ${PGSQL_SCRIPTS}
        DESTINATION "bin")

SET(CPACK_CMAKE_GENERATOR "Unix Makefiles")
SET(CPACK_GENERATOR "TXZ;TZIP")

SET(CPACK_PACKAGE_NAME "dbt5")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Database Test 5")
SET(CPACK_PACKAGE_VENDOR "osdldbt")

SET(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
SET(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")

SET(CPACK_INSTALL_CMAKE_PROJECTS "")

SET(CPACK_PACKAGE_DESCRIPTION_FILE "README")
SET(CPACK_RESOURCE_FILE_LICENSE "LICENSE")

set (CPACK_SOURCE_IGNORE_FILES
     "\\\\.bz2\\$;\\\\.gz\\$;\\\\.xz\\$;\\\\.Z\\$;\\\\.zip\\$"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/CMakeCache\\\\.txt\\$"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/CMakeFiles/"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/cmake_install\\\\.cmake\\$"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/CPackConfig\\\\.cmake\\$"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/\\\\.gitignore\\$"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/\\\\_CPack\\\\_Packages/"
)
set (CPACK_SOURCE_IGNORE_FILES
     "${CPACK_SOURCE_IGNORE_FILES};/CPackSourceConfig\\\\.cmake\\$"
)

include (CPack)

set (APPIMAGE_SOURCE_DIR ${CMAKE_SOURCE_DIR}/appimage)

add_custom_target(appimage-podman
    COMMAND cp -aLr /usr/local/AppDir .
    COMMAND VERSION=${PROJECT_VERSION} /usr/local/squashfs-root/AppRun
            --appdir AppDir
            --custom-apprun=${CMAKE_BINARY_DIR}/AppRun
            --desktop-file=${APPIMAGE_SOURCE_DIR}/dbt5.desktop
            --icon-file=${APPIMAGE_SOURCE_DIR}/dbt5.svg
            --output appimage
)
