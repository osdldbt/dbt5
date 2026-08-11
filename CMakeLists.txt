cmake_minimum_required(VERSION 3.14...4.0)
project(dbt5
    VERSION 0.10.13
    DESCRIPTION "Database Test 5"
    HOMEPAGE_URL "https://github.com/osdldbt/dbt5"
    LANGUAGES NONE
)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_BINARY_DIR)
    message(FATAL_ERROR
        "In-tree builds are not supported.  Use a separate build directory, "
        "e.g. `make release`, or `cmake -S . -B build/release`.  Remove the "
        "`CMakeCache.txt` file and `CMakeFiles/` directory created in the "
        "source directory by this attempt before trying again."
    )
endif()

# Mark the version of a build that is not a release: with the commit it
# came from when that commit is not tagged, and as dirty when the tree has
# uncommitted changes.  A source tree with no git metadata, such as an
# unpacked release tarball, keeps the version from project() above.
find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
    execute_process(
        COMMAND ${GIT_EXECUTABLE} tag --points-at HEAD
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_TAGS
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )

    if(NOT GIT_TAGS)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
            WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
            OUTPUT_VARIABLE GIT_SHA1
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET
        )
        if(GIT_SHA1)
            set(PROJECT_VERSION "${PROJECT_VERSION}-${GIT_SHA1}")
        endif()
    endif()

    # Refresh the stat information recorded in the index first: diff-index
    # trusts it, so a tree whose files were merely touched or copied anew
    # would otherwise read as dirty.  git describe --dirty does the same
    # refresh before its own diff-index.
    execute_process(
        COMMAND ${GIT_EXECUTABLE} update-index -q --refresh
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        ERROR_QUIET
    )

    # diff-index reports 1 when tracked files differ from HEAD, which is the
    # test that git describe --dirty makes.  Anything else is a failure to
    # read the repository at all, such as a checkout whose git directory is
    # not reachable, and says nothing about the state of the tree.
    execute_process(
        COMMAND ${GIT_EXECUTABLE} diff-index --quiet HEAD --
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        RESULT_VARIABLE GIT_DIFF_STATUS
        ERROR_QUIET
    )
    if(GIT_DIFF_STATUS EQUAL 1)
        set(PROJECT_VERSION "${PROJECT_VERSION}-dirty")
    endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(SHELLOPTIONS "set -x")
endif()

set(HOMEPAGE
    "Database Test 5 (DBT-5) project page: ${PROJECT_HOMEPAGE_URL}"
)

if(EXISTS "${PROJECT_SOURCE_DIR}/egen/prj")
    set(EGEN_SOURCE_DIR ${PROJECT_SOURCE_DIR}/egen)
else()
    include(FetchContent)
    FetchContent_Declare(
        egen
        GIT_REPOSITORY https://github.com/osdldbt/egen.git
        GIT_TAG        1.14.0
    )
    FetchContent_MakeAvailable(egen)
    set(EGEN_SOURCE_DIR ${egen_SOURCE_DIR})
endif()

# Included rather than added as a subdirectory so the appimage target
# runs in, and appimagetool writes the AppImage to, the top of the
# build directory.
include(appimage/CMakeLists.txt)
add_subdirectory(patches)
add_subdirectory(man)
add_subdirectory(scripts)
add_subdirectory(src)
add_subdirectory(storedproc)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

install(
    PROGRAMS
        scripts/dbt5-plot-transaction-rate
        scripts/pgsql/dbt5-pgsql-start-db
        scripts/pgsql/dbt5-pgsql-drop-tables
        scripts/pgsql/dbt5-pgsql-db-plans
        scripts/pgsql/dbt5-pgsql-drop-db
        scripts/pgsql/dbt5-pgsql-stop-db
    TYPE BIN
)

set(CPACK_PACKAGE_VENDOR "osdldbt")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_FILE "${PROJECT_SOURCE_DIR}/README")
set(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE")

# Only source packages are produced (see the package target in the top
# Makefile).  Keep the binary package empty should one ever be built.
set(CPACK_INSTALL_CMAKE_PROJECTS "")

set(CPACK_SOURCE_GENERATOR "TXZ;ZIP")
set(CPACK_SOURCE_IGNORE_FILES
    "[.]bz2$" "[.]gz$" "[.]xz$" "[.]Z$" "[.]zip$"
    "/CMakeCache[.]txt$"
    "/CMakeFiles/"
    "/cmake_install[.]cmake$"
    "/CPackConfig[.]cmake$"
    "/[.]gitignore$"
    "/_CPack_Packages/"
    "/CPackSourceConfig[.]cmake$"
)

include(CPack)
