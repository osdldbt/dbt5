#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
#
# 2006 Rilson Nascimento

DIR=`dirname $0`
. ${DIR}/pgsql_profile || exit 1

# Broker Volume transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/BrokerVolume.sql || exit 1

# Customer Position transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/CustomerPosition.sql || exit 1

# Data Maintenance transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/DataMaintenance.sql || exit 1

# Market Feed transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/MarketFeed.sql || exit 1

# Market Watch transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/MarketWatch.sql || exit 1

# Security Detail transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/SecurityDetail.sql || exit 1

# Trade Cleanup transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeCleanup.sql || exit 1

# Trade Lookup transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeLookup.sql || exit 1

# Trade Order transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeOrder.sql || exit 1

# Trade Result transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeResult.sql || exit 1

# Trade Status transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeStatus.sql || exit 1

# Trade Update transaction
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeUpdate.sql || exit 1
