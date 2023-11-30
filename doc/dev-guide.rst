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
