These scripts and Docker files are for build testing, and evaluating DBT-5 at
small scale factors.  There is room for improvement but this should be enough to
get a complete 2-tier environment up and running.

The quickest way to try out the kit is to run::

    # Prepare containers.
    docker/start-database
    docker/build-driver

    # Start a container so that we can start individual processes.
    docker run --rm -it --name dbt5-driver-0 dbt5-driver /bin/bash
    
    # Open a new terminal to start the brokerage house, double check what the IP
    # address of the database containers is.
    docker/start-brokerage-house 172.17.0.2

    # Open a new terminal to start the market exchange
    docker/start-market-exchange

    # Open a new terminal to start the driver for a 2 minute (120 seconds) test
    # and save the results in /tmp/results.  Note /tmp not needed.
    docker/run-test results 120

    # In the previous terminal where we ran "docker run", process results.
    dbt5-post-process /tmp/results/ce_mix.log

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
* `run-test` - Script to start the driver.
* `start-database` - Script to start the database, and create the Docker image
                     if not done already.
* `start-brokerage-house` - Script to start the brokerage house.
* `start-market-exchange` - Script to start the market exchange.
