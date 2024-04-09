---------------
Developer Guide
---------------

This document is for detailing anything related to the development of this test
kit.

Building the Kit
================

CMake is build system used for this kit.  A `Makefile.cmake` is provided to
automate some of the tasks.

Building for debugging::

    make -f Makefile.cmake debug

Building for release::

    make -f Makefile.cmake release

Building source packages::

    make -f Makefile.cmake package

See the **AppImage** section for details on building an AppImage.  There are
additional requirements for the `appimage` target in the `Makefile.cmake`.
Alternatively, the kit provides scripts in the *tools* diretory to create a
container that can create an AppImage.

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

In the source directory, create the subdirectory *egen* and unzip the TPC-E
tools into it::

    mkdir egen
    cd egen
    unzip /tmp/*-tpc-e-tool.zip

Two scripts are provided, one to create a container for building the AppImage
and one to building the AppImage::

    EGEN=1 tools/build-appimage-container
    EGEN=1 tools/build-appimage


The environment variable `EGEN` must be set to `1` otherwise the AppImage will
be created without the TPC-E tools included.

When the TPC-E Tools are included in the AppImage, the *build* and *run*
commands do not need the use of the `--tpcetools` flag and will automatically
use the included TPC-E Tools binaries.
