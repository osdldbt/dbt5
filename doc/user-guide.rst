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

The TPC-E Tools source is provided by the ``egen`` git submodule, which
tracks https://github.com/osdldbt/egen.git.  Initialize it when cloning the
kit::

    git clone --recurse-submodules https://github.com/osdldbt/dbt5.git

Or in an existing clone::

    git submodule update --init

When the submodule is initialized, CMake uses it directly.  Otherwise
CMake's FetchContent feature automatically fetches the TPC-E Tools source
from the same repository during the configure step and places it in the
build directory under ``_deps/egen-src``.

For example, to build the TPC-E Tools for PostgreSQL (pgsql), copy the
source from the submodule to a working directory and run `dbt5-build-egen`
against it (`dbt5-build-egen` patches and builds the directory in place, so
building a copy keeps the submodule checkout clean)::

    cp -a egen /tmp/egen
    dbt5-build-egen --include-dir=src/include --patch-dir=patches \
            --source-dir=src /tmp/egen

Alternatively, the TPC-E Tools can be downloaded directly from the TPC:
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

To build from the TPC zip distribution, unzip the TPC-E Tools zip file into an
empty directory and run `dbt5-build-egen` against the resulting directory::

    mkdir /tmp/egen
    cd /tmp/egen
    unzip /path/to/*-tpc-e-tool.zip
    dbt5-build-egen --include-dir=src/include --patch-dir=patches \
            --source-dir=src /tmp/egen
