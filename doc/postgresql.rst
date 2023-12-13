PostgreSQL
==========

Building the Kit
----------------

Developed against Postgresql 8.4 and newer.  May work with older versions but
not quite tested.

By default, the kit will use PL/pgsql stored functions, but you may use C
stored functions instead.  The C stored functions need to be separately built
and installed on the database system before they can be used by the database::

    cd storedproc/pgsql/c
    make
    make install
    dbt5 pgsql-load-stored-procs -t c

Build the Database
------------------

To build the minimum sized database in a database `dbt5`::

    dbt5 build --tpcetools=/tmp/egen pgsql

Run a Test
----------

Run a quick 120 second (2 minute) test with 1 user::

    dbt5 run -d 120 pgsql /tmp/results
