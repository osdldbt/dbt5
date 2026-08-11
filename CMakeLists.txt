CMAKE_MINIMUM_REQUIRED(VERSION 3.14...4.0)
PROJECT(dbt5 NONE)

if (CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message (FATAL_ERROR
        "In-tree builds are not supported.  Use a separate build directory, "
        "e.g. `make release`, or `cmake -H. -Bbuild/release`.  Remove the "
        "`CMakeCache.txt` file and `CMakeFiles/` directory created in the "
        "source directory by this attempt before trying again."
    )
endif ()

set (PROJECT_VERSION_MAJOR 0)
set (PROJECT_VERSION_MINOR 10)
set (PROJECT_VERSION_PATCH 13)

# Mark the patch level of a build that is not a release: with the commit it
# came from when that commit is not tagged, and as dirty when the tree has
# uncommitted changes.  A source tree with no git metadata, such as an
# unpacked release tarball, keeps the version set above.
find_package (Git QUIET)
if (GIT_FOUND AND EXISTS ${CMAKE_SOURCE_DIR}/.git)
    execute_process (
        COMMAND ${GIT_EXECUTABLE} tag --points-at HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_TAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    if (NOT GIT_TAGS)
        execute_process (
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_SHA1
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if (GIT_SHA1)
            set (PROJECT_VERSION_PATCH "${PROJECT_VERSION_PATCH}-${GIT_SHA1}")
        endif ()
    endif ()

    # Refresh the stat information recorded in the index first: diff-index
    # trusts it, so a tree whose files were merely touched or copied anew
    # would otherwise read as dirty.  git describe --dirty does the same
    # refresh before its own diff-index.
    execute_process (
        COMMAND ${GIT_EXECUTABLE} update-index -q --refresh
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        ERROR_QUIET
    )

    # diff-index reports 1 when tracked files differ from HEAD, which is the
    # test that git describe --dirty makes.  Anything else is a failure to
    # read the repository at all, such as a checkout whose git directory is
    # not reachable, and says nothing about the state of the tree.
    execute_process (
        COMMAND ${GIT_EXECUTABLE} diff-index --quiet HEAD --
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        RESULT_VARIABLE GIT_DIFF_STATUS
        ERROR_QUIET
    )
    if (GIT_DIFF_STATUS EQUAL 1)
        set (PROJECT_VERSION_PATCH "${PROJECT_VERSION_PATCH}-dirty")
    endif ()
endif ()

set (PROJECT_VERSION
     ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH})

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set (SHELLOPTIONS "set -x")
endif ()

set (
    HOMEPAGE
    "Database Test 5 (DBT-5) project page: https://github.com/osdldbt/dbt5"
)

if (EXISTS ${CMAKE_SOURCE_DIR}/egen/prj)
    set (EGEN_SOURCE_DIR ${CMAKE_SOURCE_DIR}/egen)
else ()
    include (FetchContent)
    FetchContent_Declare(
        egen
        GIT_REPOSITORY https://github.com/osdldbt/egen.git
        GIT_TAG        1.14.0
    )
    FetchContent_MakeAvailable (egen)
    set (EGEN_SOURCE_DIR ${egen_SOURCE_DIR})
endif ()

# Included rather than added as a subdirectory so the appimage target
# runs in, and appimagetool writes the AppImage to, the top of the
# build directory.
include (appimage/CMakeLists.txt)
add_subdirectory (patches)
add_subdirectory (man)
add_subdirectory (scripts)
add_subdirectory (src)
add_subdirectory (storedproc)

include (CTest)
if (BUILD_TESTING)
    add_subdirectory (tests)
endif ()

set (PGSQL_SCRIPTS scripts/pgsql/dbt5-pgsql-start-db
                   scripts/pgsql/dbt5-pgsql-drop-tables
                   scripts/pgsql/dbt5-pgsql-db-plans
                   scripts/pgsql/dbt5-pgsql-drop-db
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

SET(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_SOURCE_DIR}/README")
SET(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_SOURCE_DIR}/LICENSE")

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
