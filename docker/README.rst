These scripts and Docker files are for build testing, and evaluating DBT-5 at
small scale factors.  There is room for improvement but this should be enough to
get a complete 2-tier environment up and running.

The quickest way to try out the kit is to run::

    # Prepare containers.
    docker/start-database
    docker/build-driver

Description of all the helper scripts:

* `build-database` - Build a Docker image, by default with the minimal valid
                     scale factor.
* `build-driver` - Build a Docker image that can be used to test and use the
                     driver components.
* `compile-dbt5` - Simply attempt to compile the local source code in a known
                   valid C++ environment.
* `compile-dbt5-pgsql-c` - Simply attempt to compile PostgreSQL C functions in
                           a known valid C environment.
* `prepare-image` - Build a Docker image to be further expanded by the other
                    components in the kit.
