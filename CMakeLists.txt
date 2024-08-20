CMAKE_MINIMUM_REQUIRED(VERSION 3.5.0)
PROJECT(dbt5 NONE)

set (PROJECT_VERSION_MAJOR 0)
set (PROJECT_VERSION_MINOR 10)
set (PROJECT_VERSION_PATCH 7)

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

find_program(APPIMAGE NAMES appimagetool)

if (APPIMAGE)
    set (APPIMAGETOOL ${APPIMAGE})
else()
    set (APPIMAGETOOL /usr/local/squashfs-root/AppRun)
endif(APPIMAGE)

set (GNUPLOT ../AppDir/usr/bin/gnuplot)
set (PSQL ../AppDir/usr/bin/psql)

add_custom_target(appimage
    COMMAND [ -d "/usr/local/share/licenses" ] &&
            cp -pr /usr/local/share/licenses ../AppDir/usr/share || exit 0
    COMMAND [ -d "/usr/local/lib/python3.11" ] &&
            mkdir -p ../AppDir/usr/lib/ &&
            cp -pr /usr/local/lib/python3.11 ../AppDir/usr/lib/ || exit 0
    COMMAND mkdir -p ../AppDir/usr/share/licenses/dbt5-${PROJECT_VERSION}
    COMMAND cp -p ../../LICENSE
            ../AppDir/usr/share/licenses/dbt5-${PROJECT_VERSION}/
    COMMAND mkdir -p ../AppDir/usr/bin ../AppDir/usr/share/metainfo
    COMMAND cp -p ${APPIMAGE_SOURCE_DIR}/org.osdl.dbt5.appdata.xml
            ../AppDir/usr/share/metainfo
    COMMAND cp -p ${APPIMAGE_SOURCE_DIR}/dbt5.svg
                  ${APPIMAGE_SOURCE_DIR}/org.osdl.dbt5.desktop AppRun ../AppDir
    COMMAND cp -p `command -v gnuplot`
                  `command -v jq`
                  `command -v perl`
                  `command -v psql`
                  `command -v python3`
                  `command -v rst2html5`
                  `command -v spar`
                  `command -v sqlite3`
                  `command -v toml`
                  ../AppDir/usr/bin/
    COMMAND mkdir -p ../AppDir/etc/perl
                     ../AppDir/etc/perl/Carp
                     ../AppDir/etc/perl/Exporter
                     ../AppDir/etc/perl/Getopt
                     ../AppDir/etc/perl/warnings
    COMMAND cp -p `find /usr -type f -name Carp.pm | head -n 1`
                  `find /usr -type f -name Exporter.pm | head -n 1`
                  `find /usr -type f -name constant.pm | head -n 1`
                  `find /usr -type f -name open.pm | head -n 1`
                  `find /usr -type f -name overload.pm | head -n 1`
                  `find /usr -type f -name overloading.pm | head -n 1`
                  `find /usr -type f -name strict.pm | head -n 1`
                  `find /usr -type f -name vars.pm | head -n 1`
                  `find /usr -type f -name warnings.pm | head -n 1`
               ../AppDir/etc/perl/
    COMMAND cp -p `find /usr -type f -name Heavy.pm | grep Carp | head -n 1`
               ../AppDir/etc/perl/Carp/
    COMMAND cp -p `find /usr -type f -name Heavy.pm | grep Exporter | head -n 1`
               ../AppDir/etc/perl/Exporter/
    COMMAND cp -p `find /usr -type f -name Long.pm | grep Getopt | head -n 1`
               ../AppDir/etc/perl/Getopt/
    COMMAND cp -p `find /usr -type f -name register.pm | grep warnings | head -n 1`
               ../AppDir/etc/perl/warnings/
    COMMAND sed -i -e "s#/usr#././#g"
                ../AppDir/usr/bin/gnuplot
                ../AppDir/usr/bin/jq
                ../AppDir/usr/bin/perl
                ../AppDir/usr/bin/psql
                ../AppDir/usr/bin/python3
                ../AppDir/usr/bin/spar
                ../AppDir/usr/bin/sqlite3
                ../AppDir/usr/bin/toml
    COMMAND sed -i "1s/.*/#!\\\\/usr\\\\/bin\\\\/env python3/"
            ../AppDir/usr/bin/rst2html5
    COMMAND cp -p `command -v dbt-plot`
                  `command -v dbt-plot-transaction-distribution.gnuplot`
                  `command -v dbt-plot-transaction-rate.gnuplot`
                  `command -v dbt-pgsql-generate-db-report`
                  `command -v dbt-pgsql-generate-index-report`
                  `command -v dbt-pgsql-generate-table-report`
                  ../AppDir/usr/bin/
    COMMAND cp -p `command -v ts`
                  `command -v ts-pgsql-stat`
                  `command -v ts-plot-collectd`
                  `command -v ts-plot-pgsql`
                  `command -v ts-plot-pidstat`
                  `command -v ts-plot-queries`
                  `command -v ts-plot-sar`
                  `command -v ts-plot-spar`
                  `command -v ts-process-pidstat`
                  `command -v ts-profile`
                  `command -v ts-report-pid`
                  `command -v ts-report-sar`
                  `command -v ts-sysstat`
                  ../AppDir/usr/bin/
    COMMAND curl -o ../AppDir/usr/bin/stackcollapse-perf.pl -SsOL
            https://raw.githubusercontent.com/brendangregg/FlameGraph/master/stackcollapse-perf.pl
    COMMAND curl -o ../AppDir/usr/bin/flamegraph.pl -SsOL
            https://raw.githubusercontent.com/brendangregg/FlameGraph/master/flamegraph.pl
    COMMAND chmod +x ../AppDir/usr/bin/stackcollapse-perf.pl
            ../AppDir/usr/bin/flamegraph.pl
    COMMAND mkdir -p ../AppDir/usr/lib/
    COMMAND cp -paL
               `ldd ${GNUPLOT} | grep libcairo\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libfreetype\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libfribidi\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libgraphite2\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep harfbuzz\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libbsd\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libpango-1\\\\\\\\.0\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libpangoft2-1\\\\\\\\.0\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libpangocairo-1\\\\\\\\.0\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libpixman-1\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libpng16\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libX11\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libXdmcp\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libXext\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libXrender\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libXau\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libxcb\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libxcb-shm\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${GNUPLOT} | grep libxcb-render\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${PSQL} | grep libcrypto\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${PSQL} | grep libpq\\\\\\\\. | cut -d \" \" -f 3`
               `ldd ${PSQL} | grep libssl\\\\\\\\. | cut -d \" \" -f 3`
               ../AppDir/usr/lib/
    COMMAND VERSION=${PROJECT_VERSION} ARCH=`uname -m` ${APPIMAGETOOL}
            --no-appstream ../AppDir
)
