CMAKE_MINIMUM_REQUIRED(VERSION 2.4)
PROJECT(dbt5 CXX)

#
# Defining binaries so to minimized typoes.
#

SET(DBT5_BROKERAGE_HOUSE bin/BrokerageHouseMain)
SET(DBT5_DRIVER bin/DriverMain)
SET(DBT5_MARKET_EXCHANGE bin/MarketExchangeMain)
SET(DBT5_TEST_TXN bin/TestTxn)

#
# Check for large file support by using 'getconf'.
#

FIND_PROGRAM(GETCONF getconf)
IF (GETCONF)
    SET(GETCONF_FOUND TRUE)
ENDIF (GETCONF)

IF (GETCONF_FOUND)
    EXECUTE_PROCESS(COMMAND ${GETCONF} LFS_CFLAGS COMMAND ${GETCONF}
            LFS64_CFLAGS OUTPUT_VARIABLE LFS_CFLAGS
            OUTPUT_STRIP_TRAILING_WHITESPACE)
ENDIF (GETCONF_FOUND)

IF (DBMS STREQUAL "pgsql")
    #
    # Create PostgreSQL specific configuration.
    #

    FIND_PROGRAM(PG_CONFIG pg_config)

    IF (PG_CONFIG)
       SET(PG_CONFIG_FOUND TRUE)
    ENDIF (PG_CONFIG)

    IF (PG_CONFIG_FOUND)
        MESSAGE(STATUS "Found pg_config for PostgreSQL: ${PG_CONFIG}")

        EXECUTE_PROCESS(COMMAND ${PG_CONFIG} --includedir OUTPUT_VARIABLE
                PG_INCLUDEDIR OUTPUT_STRIP_TRAILING_WHITESPACE)
        EXECUTE_PROCESS(COMMAND ${PG_CONFIG} --libdir OUTPUT_VARIABLE
                PG_LIBDIR OUTPUT_STRIP_TRAILING_WHITESPACE)

        SET(DBCFLAGS "-DLIBPQ -I${PG_INCLUDEDIR}")

        SET(DB_LIBS "-L${PG_LIBDIR} -lpqxx -lpq")

        SET(DB_SCRIPTS scripts/pgsql/dbt5-pgsql-start-db
                       scripts/pgsql/dbt5-pgsql-create-indexes
                       scripts/pgsql/dbt5-pgsql-create-tables
                       scripts/pgsql/dbt5-pgsql-drop-tables
                       scripts/pgsql/dbt5-pgsql-restore-db
                       scripts/pgsql/dbt5-pgsql-load-stored-procs
                       scripts/pgsql/dbt5-pgsql-backup-db
                       scripts/pgsql/dbt5-pgsql-db-plans
                       scripts/pgsql/dbt5-pgsql-drop-db
                       scripts/pgsql/dbt5-pgsql-check-db
                       scripts/pgsql/dbt5-pgsql-db-stat
                       scripts/pgsql/dbt5-pgsql-stop-db
                       scripts/pgsql/dbt5-pgsql-create-db
                       scripts/pgsql/dbt5-pgsql-build-db)
    ENDIF (PG_CONFIG_FOUND)
ELSE (NOT DEFINED DBMS)
    MESSAGE(FATAL_ERROR "Need to specify -DDBMS=pgsql")

ENDIF (DBMS STREQUAL "pgsql")

#
# Defining commonly used paths.
#

SET(EGENINC egen/inc)
SET(EGENSRC egen/src)

SET(TXN src/transactions)
SET(INTF src/interfaces)

SET(BH src/BrokerageHouse)
SET(CE src/Driver)
SET(ME src/MarketExchange)

#
# Defining common groups of source files.
#

SET(TXN_SOURCE ${TXN}/${DBMS}/DBConnection.cpp
               ${TXN}/TxnBaseDB.cpp
               ${TXN}/MarketFeedDB.cpp
               ${TXN}/TradeCleanupDB.cpp
               ${TXN}/DataMaintenanceDB.cpp
               ${TXN}/MarketWatchDB.cpp
               ${TXN}/SecurityDetailDB.cpp
               ${TXN}/BrokerVolumeDB.cpp
               ${TXN}/CustomerPositionDB.cpp
               ${TXN}/TradeUpdateDB.cpp
               ${TXN}/TradeLookupDB.cpp
               ${TXN}/TradeResultDB.cpp
               ${TXN}/TradeOrderDB.cpp
               ${TXN}/TradeStatusDB.cpp)

#
# Files to build.
#

ADD_EXECUTABLE(${DBT5_BROKERAGE_HOUSE}
               ${EGENSRC}/DateTime
               ${EGENSRC}/locking.cpp
               ${INTF}/CSocket.cpp
               ${INTF}/TxnHarnessSendToMarket.cpp
               ${TXN_SOURCE}
               ${BH}/BrokerageHouseMain.cpp
               ${BH}/BrokerageHouse.cpp)

ADD_EXECUTABLE(${DBT5_DRIVER}
               ${CE}/Driver.cpp
               ${CE}/DriverMain.cpp
               src/Customer/Customer.cpp
               ${EGENSRC}/BaseLogger.cpp
               ${EGENSRC}/EGenLogFormatterTab.cpp
               ${EGENSRC}/CETxnMixGenerator.cpp
               ${EGENSRC}/CETxnInputGenerator.cpp
               ${EGENSRC}/ReadRowFunctions.cpp
               ${EGENSRC}/InputFlatFilesStructure.cpp
               ${EGENSRC}/error.cpp
               ${EGENSRC}/AddressTable.cpp
               ${EGENSRC}/EGenVersion.cpp
               ${EGENSRC}/CustomerTable.cpp
               ${EGENSRC}/CustomerSelection.cpp
               ${EGENSRC}/Random.cpp
               ${EGENSRC}/Person.cpp
               ${EGENSRC}/DateTime.cpp
               ${EGENSRC}/CE.cpp
               ${EGENSRC}/DM.cpp
               ${EGENSRC}/locking.cpp
               ${EGENSRC}/MEE.cpp
               ${EGENSRC}/MEETradingFloor.cpp
               ${EGENSRC}/MEETickerTape.cpp
               ${EGENSRC}/MEEPriceBoard.cpp
               ${EGENSRC}/WheelTime.cpp
               ${EGENSRC}/MEESecurity.cpp
               ${INTF}/CESUT.cpp
               ${INTF}/CSocket.cpp
               ${INTF}/DMSUT.cpp
               ${INTF}/BaseInterface.cpp)

ADD_EXECUTABLE(${DBT5_MARKET_EXCHANGE}
               ${EGENSRC}/ReadRowFunctions.cpp
               ${EGENSRC}/BaseLogger.cpp
               ${EGENSRC}/EGenLogFormatterTab.cpp
               ${EGENSRC}/EGenVersion.cpp
               ${EGENSRC}/error.cpp
               ${INTF}/CSocket.cpp
               ${EGENSRC}/Random.cpp
               ${EGENSRC}/DateTime.cpp
               ${EGENSRC}/WheelTime.cpp
               ${EGENSRC}/MEESecurity.cpp
               ${EGENSRC}/MEEPriceBoard.cpp
               ${EGENSRC}/MEETickerTape.cpp
               ${EGENSRC}/MEETradingFloor.cpp
               ${EGENSRC}/MEE.cpp
               ${EGENSRC}/locking.cpp
               ${EGENSRC}/InputFlatFilesStructure.cpp
               ${INTF}/MEESUT.cpp
               ${INTF}/BaseInterface.cpp
               ${ME}/MarketExchange.cpp
               ${ME}/MarketExchangeMain.cpp)

ADD_EXECUTABLE(${DBT5_TEST_TXN}
 ${EGENSRC}/ReadRowFunctions.cpp
                  ${EGENSRC}/BaseLogger.cpp
                  ${EGENSRC}/EGenLogFormatterTab.cpp
                  ${EGENSRC}/InputFlatFilesStructure.cpp
                  ${EGENSRC}/error.cpp
                  ${EGENSRC}/EGenVersion.cpp
                  ${EGENSRC}/AddressTable.cpp
                  ${EGENSRC}/CustomerTable.cpp
                  ${EGENSRC}/CustomerSelection.cpp
                  ${EGENSRC}/Random.cpp
                  ${EGENSRC}/Person.cpp
                  ${EGENSRC}/DateTime.cpp
                  ${EGENSRC}/CETxnInputGenerator.cpp
                  ${EGENSRC}/DM.cpp
                  ${EGENSRC}/WheelTime.cpp
                  ${EGENSRC}/MEESecurity.cpp
                  ${EGENSRC}/MEEPriceBoard.cpp
                  ${EGENSRC}/MEETickerTape.cpp
                  ${EGENSRC}/MEETradingFloor.cpp
                  ${EGENSRC}/MEE.cpp
                  ${EGENSRC}/locking.cpp
                  ${INTF}/MEESUTtest.cpp
                  ${INTF}/DMSUTtest.cpp
                  ${TXN_SOURCE}
                  ${INTF}/BaseInterface.cpp
                  ${INTF}/CESUT.cpp
                  ${INTF}/CSocket.cpp
                  ${INTF}/TxnHarnessSendToMarketTest.cpp
                  ${INTF}/TxnHarnessSendToMarket.cpp
                  ${BH}/BrokerageHouse.cpp
                  src/TestTransactions/TestTxn.cpp)

#
# Set compiler flags.
#

SET(CMAKE_CXX_FLAGS "-g -Wall -I${EGENINC} -Isrc/include ${LFS_CFLAGS}")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__STDC_FORMAT_MACROS")
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__STDC_CONSTANT_MACROS")

SET_SOURCE_FILES_PROPERTIES(${TXN}/${DBMS}/DBConnection.cpp COMPILE_FLAGS
        "${DBCFLAGS}")

#
# Set linker flags.
#

SET_TARGET_PROPERTIES(${DBT5_BROKERAGE_HOUSE} PROPERTIES LINK_FLAGS
        "${DB_LIBS} -lpthread")
SET_TARGET_PROPERTIES(${DBT5_DRIVER} PROPERTIES LINK_FLAGS "-lpthread")
SET_TARGET_PROPERTIES(${DBT5_MARKET_EXCHANGE} PROPERTIES LINK_FLAGS "-lpthread")
SET_TARGET_PROPERTIES(${DBT5_TEST_TXN} PROPERTIES LINK_FLAGS
        "${DB_LIBS} -lpthread")

#
# Install rules.
#

INSTALL(PROGRAMS ${DBT5_BROKERAGE_HOUSE}
                 ${DBT5_DRIVER}
                 ${DBT5_MARKET_EXCHANGE}
                 ${DBT5_TEST_TXN}
                 scripts/dbt5-post-process
                 scripts/dbt5-sysstats
                 scripts/dbt5-get-os-info
                 scripts/dbt5-run-workload
                 scripts/dbt5-plot-transaction-rate
                 scripts/dbt5-generate-report
                 ${DB_SCRIPTS}
        DESTINATION "/bin")
