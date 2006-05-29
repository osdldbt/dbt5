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

# Load tables
echo account_permission
${PSQL} -d ${DBNAME} -c "select count(*) from ACCOUNT_PERMISSION" 
echo customer
${PSQL} -d ${DBNAME} -c "select count(*) from CUSTOMER" 
echo customer_account
${PSQL} -d ${DBNAME} -c "select count(*) from CUSTOMER_ACCOUNT" 
echo customer_taxrate
${PSQL} -d ${DBNAME} -c "select count(*) from CUSTOMER_TAXRATE" 
echo holding
${PSQL} -d ${DBNAME} -c "select count(*) from HOLDING" 
echo holding_history
${PSQL} -d ${DBNAME} -c "select count(*) from HOLDING_HISTORY" 
echo holding_summary
${PSQL} -d ${DBNAME} -c "select count(*) from HOLDING_SUMMARY" 
echo watch_item
${PSQL} -d ${DBNAME} -c "select count(*) from WATCH_ITEM" 
echo watch_list
${PSQL} -d ${DBNAME} -c "select count(*) from WATCH_LIST" 
echo broker
${PSQL} -d ${DBNAME} -c "select count(*) from BROKER" 
echo cash_transaction
${PSQL} -d ${DBNAME} -c "select count(*) from CASH_TRANSACTION" 
echo charge
${PSQL} -d ${DBNAME} -c "select count(*) from CHARGE" 
echo commission_rate
${PSQL} -d ${DBNAME} -c "select count(*) from COMMISSION_RATE" 
echo settlement
${PSQL} -d ${DBNAME} -c "select count(*) from SETTLEMENT" 
echo trade
${PSQL} -d ${DBNAME} -c "select count(*) from TRADE" 
echo trade_history
${PSQL} -d ${DBNAME} -c "select count(*) from TRADE_HISTORY" 
echo trade_request
${PSQL} -d ${DBNAME} -c "select count(*) from TRADE_REQUEST" 
echo trade_type
${PSQL} -d ${DBNAME} -c "select count(*) from TRADE_TYPE" 
echo company
${PSQL} -d ${DBNAME} -c "select count(*) from COMPANY" 
echo company_competitor
${PSQL} -d ${DBNAME} -c "select count(*) from COMPANY_COMPETITOR" 
echo daily_market
${PSQL} -d ${DBNAME} -c "select count(*) from DAILY_MARKET" 
echo exchange
${PSQL} -d ${DBNAME} -c "select count(*) from EXCHANGE" 
echo financial
${PSQL} -d ${DBNAME} -c "select count(*) from FINANCIAL" 
echo industry
${PSQL} -d ${DBNAME} -c "select count(*) from INDUSTRY" 
echo last_trade
${PSQL} -d ${DBNAME} -c "select count(*) from LAST_TRADE" 
echo news_item
${PSQL} -d ${DBNAME} -c "select count(*) from NEWS_ITEM" 
echo news_xref
${PSQL} -d ${DBNAME} -c "select count(*) from NEWS_XREF" 
echo sector
${PSQL} -d ${DBNAME} -c "select count(*) from SECTOR" 
echo security
${PSQL} -d ${DBNAME} -c "select count(*) from SECURITY" 
echo address
${PSQL} -d ${DBNAME} -c "select count(*) from ADDRESS" 
echo status_type
${PSQL} -d ${DBNAME} -c "select count(*) from STATUS_TYPE" 
echo taxrate
${PSQL} -d ${DBNAME} -c "select count(*) from TAXRATE" 
echo zip_code
${PSQL} -d ${DBNAME} -c "select count(*) from ZIP_CODE" 
