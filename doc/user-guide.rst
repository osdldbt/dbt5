------------
User's Guide
------------

Building TPC-E Tools
====================

The kit requires the TPC-E Tools to be built for the specific database
management system to be tested.  The TPC-E Tools is developed in such a way
that it needs to rebuilt or another copy needs to be built in order to test
different database management systems at the same time.

DBT-5 provides a script to apply patches and provide code to compile the TPC-E
Tools. The patches that are applied are minor code changes and code is supplied
to build sponsor supplied code.

By default, CMake's FetchContent feature automatically fetches the TPC-E Tools
source from https://github.com/osdldbt/egen.git during the configure step.  The
source is placed in the build directory under ``_deps/egen-src``.

For example, to build the TPC-E Tools for PostgreSQL (pgsql) using the
automatically fetched source::

    cmake -H. -Bbuilds/release -DCMAKE_INSTALL_PREFIX=/usr/local
    cp -a builds/release/_deps/egen-src
    dbt5-build-egen --include-dir=src/include --patch-dir=patches \
            --source-dir=src builds/release/_deps/egen-src

Alternatively, the TPC-E Tools can be downloaded directly from the TPC:
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

To build from the TPC zip distribution, unzip the TPC-E Tools zip file into an
empty directory and run `dbt5-build-egen` against the resulting directory::

    mkdir /tmp/egen
    cd /tmp/egen
    unzip /path/to/*-tpc-e-tool.zip
    dbt5-build-egen --include-dir=src/include --patch-dir=patches \
            --source-dir=src /tmp/egen
