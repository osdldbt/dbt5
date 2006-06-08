/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * 2006 Rilson Nascimento
 *
 * 28 may 2006
 */

#include "../../inc/pgsql/PGSQLLoad.h"

using namespace TPCE;

//Explicit Instantiation to generate code
template class CPGSQLLoader<ACCOUNT_PERMISSION_ROW>;
template class CPGSQLLoader<ADDRESS_ROW>;
template class CPGSQLLoader<BROKER_ROW>;
template class CPGSQLLoader<CASH_TRANSACTION_ROW>;
template class CPGSQLLoader<CHARGE_ROW>;
template class CPGSQLLoader<COMMISSION_RATE_ROW>;
template class CPGSQLLoader<COMPANY_ROW>;
template class CPGSQLLoader<COMPANY_COMPETITOR_ROW>;
template class CPGSQLLoader<CUSTOMER_ROW>;
template class CPGSQLLoader<CUSTOMER_ACCOUNT_ROW>;
template class CPGSQLLoader<CUSTOMER_TAXRATE_ROW>;
template class CPGSQLLoader<DAILY_MARKET_ROW>;
template class CPGSQLLoader<EXCHANGE_ROW>;
template class CPGSQLLoader<FINANCIAL_ROW>;
template class CPGSQLLoader<HOLDING_ROW>;
template class CPGSQLLoader<HOLDING_HISTORY_ROW>;
template class CPGSQLLoader<HOLDING_SUMMARY_ROW>;
template class CPGSQLLoader<INDUSTRY_ROW>;
template class CPGSQLLoader<LAST_TRADE_ROW>;
template class CPGSQLLoader<NEWS_ITEM_ROW>;
template class CPGSQLLoader<NEWS_XREF_ROW>;
template class CPGSQLLoader<SECTOR_ROW>;
template class CPGSQLLoader<SECURITY_ROW>;
template class CPGSQLLoader<SETTLEMENT_ROW>;
template class CPGSQLLoader<STATUS_TYPE_ROW>;
template class CPGSQLLoader<TAXRATE_ROW>;
template class CPGSQLLoader<TRADE_ROW>;
template class CPGSQLLoader<TRADE_HISTORY_ROW>;
template class CPGSQLLoader<TRADE_REQUEST_ROW>;
template class CPGSQLLoader<TRADE_TYPE_ROW>;
template class CPGSQLLoader<WATCH_ITEM_ROW>;
template class CPGSQLLoader<WATCH_LIST_ROW>;
template class CPGSQLLoader<ZIP_CODE_ROW>;
