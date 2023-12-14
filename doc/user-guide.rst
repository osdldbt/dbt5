------------
User's Guide
------------

Building TPC-E Tools
====================

Download the TPC-E Tools directly from the TPC:
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

The kit requires the TPC-E Tools to be built for the specific database
management system to be tested.  The TPC-E Tools is developed in such a way
that it needs to rebuilt or another copy needs to be built if a different
database management system is to be tested.

DBT-5 provides a script to apply patches and code to compile the TPC-E Tools.
The patches that are applied are minor code changes and code is supplied to
build sponsor supplied code.

For example, to build the TPC-E Tools for PostgreSQL (pgsql), unzip the TPC-E
Tools zip file into an empty directory and run `dbt5-build-egen` against the
resulting directory::

    mkdir /tmp/egen
    cd /tmp/egen
    unzip /path/to/*-tpc-e-tool.zip
    dbt5-build-egen /tmp/egen
