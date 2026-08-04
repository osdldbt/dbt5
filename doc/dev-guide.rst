---------------
Developer Guide
---------------

This document is for detailing anything related to the development of this test
kit.

Building the Kit
================

CMake is build system used for this kit.  A `Makefile` is provided to
automate some of the tasks.  In-tree builds are not supported; the provided
targets configure and build under the `build/` directory.

The TPC-E Tools source is provided by the ``egen`` git submodule.  Clone
with ``--recurse-submodules``, or run ``git submodule update --init`` in an
existing clone, to use the local copy.  When the submodule is not
initialized, CMake downloads the source at configure time via FetchContent.

Building for debugging::

    make debug

Building for release::

    make release

Building source packages::

    make package

See the **AppImage** section for details on building an AppImage.  There are
additional requirements for the `appimage` target in the `Makefile`.
Alternatively, the kit provides scripts in the *tools* diretory to create a
container that can create an AppImage.

Sandbox
=======

A `compose.yml` and additional files in `.compose` are provided to help create
a test environment to test and debug the binaries by running a minimal
workload, as oppose to the entire kit with statistics collection.  It currently
works with **docker** and will hopefully work with **podman** at some point.

This environment should also be suitable to test database loading (e.g.
`EGenLoader`) and the transaction testing tool (`TestTxn`).

Restarting a container rebuilds the kit for the container to help assist
testing the current state of the code in the repository.

Here are a brief list of commands with **docker** to help get started.  Please
refer to the respective Compose documentation for more information.  These
commands must be executed from the top of the source directory.

The compose environment will use the smallest, but invalid, parameters possible
in the interest of being to test things quickly.  Please refer to the benchmark
specification for further explanation of these parameters.  These can be
adjusted with environment variables:

* `CUSTOMERS` - The total number of customer, must be a multiple of 1000.
* `DAYS` - Initial trade days.
* `SCALEFACTOR` - The number of customers per trade results transactions per
  second.
* `USERS` - The number of users to emulate.

Provision and deploy the environment::

    docker compose up -d

Stop the containers::

    docker compose stop

Remove the containers::

    docker compose down

Follow the container system logs::

    docker compose logs -f database

Build the database (only needs to be once, or each time the database container
is recreated)::

    docker compose run load

Run the workload and specify the test duration in seconds (results are saved in
the `results` subdirectory)::

    docker compose run driver -d 120

Database Container
------------------

PostgreSQL
..........

Connect to the database::

    docker compose exec -u postgres database psql dbt5

Regression Tests
================

The `tests/` directory contains regression tests that are run by CTest and
written with shunit2, so shunit2 must be installed to run them.  Configuring a
build tree registers the tests with CTest, and the `test` target in the
`Makefile` configures the release build tree and runs them::

    make test

The tests can also be run from an already configured build tree::

    cd build/release && ctest --output-on-failure

CTest only shows a test's output when the test fails.  Use `ctest -V` to
always show the output, including shunit2 reporting each test function as it
runs.  A test script can also be run directly, which shows its output as it
happens; when the kit is not installed, the test must be told where to find
`dbt5-build-egen`::

    DBT5_BUILD_EGEN=build/release/dbt5-build-egen \
            sh tests/test_egen_chunking

The *egen_chunking* test builds the TPC-E Tools in a temporary
directory, then generates flat data files once with a single `EGenLoader`
instance covering every customer and again with multiple instances each
covering a subrange of customers, and verifies that both sets of data files
contain the same data.  This is the property that makes parallel data
generation and loading safe.  The test requires the `egen` submodule to be
initialized and is skipped otherwise.

The test parameters default to the minimum valid values defined by the
benchmark specification: 5000 customers, a scale factor of 500, and 300
initial trade days.  These defaults generate two spec-minimum data sets of
tens of gigabytes each and may take over an hour.  The parameters can be
adjusted with environment variables for smaller, non-conformant runs while
developing::

    ITD=10 make test

* `CHUNK` - The number of customers generated per `EGenLoader` instance, must
  be a multiple of 1000.
* `ITD` - Initial trade days.
* `SF` - The number of customers per trade result transactions per second.
* `TOTAL` - The total number of customers, must be a multiple of 1000.

AppImage
========

AppImages are only for Linux based systems: https://appimage.org/

The AppImageKit AppImage can be downloaded from:
https://github.com/AppImage/AppImageKit/releases

It is recommended to build AppImages on older distributions:
https://docs.appimage.org/introduction/concepts.html#build-on-old-systems-run-on-newer-systems

The logo used is the number "5" from the Freeware Metal On Metal Font.

See the `README.rst` in the `tools/` directory for an example of creating
an AppImage with a Podman container.

Building the AppImage
---------------------
The AppImages builds a custom minimally configured PostgreSQL build to reduce
library dependency requirements.  Part of this reason is to make it easier to
include libraries with compatible licences.  At least version PostgreSQL 11
should be used for the `pg_type_d.h` header file.

At the time of this document, PostgreSQL 11 was configured with the following
options::

    ./configure --without-ldap --without-readline --without-zlib \
          --without-gssapi --with-openssl

Don't forget that both `PATH` and `LD_LIBRARY_PATH` may need to be set
appropriately depending on where the custom build of PostgreSQL is installed.

Including TPC-E Tools in the AppImage
-------------------------------------

Review the TPC EULA for redistribution of TPC provided code and binaries before
redistributing any AppImages that include any TPC provided code or binaries:
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

Two scripts are provided, one to create a container for building the AppImage
and one to building the AppImage::

    tools/build-appimage-container
    EGEN=tpc-e-tool.zip tools/build-appimage


The environment variable `EGEN` can be set to location and name of the TPC-E
Tools zip file otherwise the script will try to detect whether the TPC-E Tools
zip exists in the top level directory.  Otherwise the AppImage will be created
without the TPC-E tools included.

When the TPC-E Tools are included in the AppImage, the *build* and *run*
commands do not need the use of the `--tpcetools` flag and will automatically
use the included TPC-E Tools binaries.
