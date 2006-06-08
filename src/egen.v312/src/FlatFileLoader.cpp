/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

#include "../inc/FlatFileLoad_stdafx.h"

namespace TPCE
{
//Explicit Instantiation to generate code
template class CFlatFileLoader<ACCOUNT_PERMISSION_ROW>;
template class CFlatFileLoader<ADDRESS_ROW>;
template class CFlatFileLoader<BROKER_ROW>;
template class CFlatFileLoader<CASH_TRANSACTION_ROW>;
template class CFlatFileLoader<CHARGE_ROW>;
template class CFlatFileLoader<COMMISSION_RATE_ROW>;
template class CFlatFileLoader<COMPANY_ROW>;
template class CFlatFileLoader<COMPANY_COMPETITOR_ROW>;
template class CFlatFileLoader<CUSTOMER_ROW>;
template class CFlatFileLoader<CUSTOMER_ACCOUNT_ROW>;
template class CFlatFileLoader<CUSTOMER_TAXRATE_ROW>;
template class CFlatFileLoader<DAILY_MARKET_ROW>;
template class CFlatFileLoader<EXCHANGE_ROW>;
template class CFlatFileLoader<FINANCIAL_ROW>;
template class CFlatFileLoader<HOLDING_ROW>;
template class CFlatFileLoader<HOLDING_HISTORY_ROW>;
template class CFlatFileLoader<HOLDING_SUMMARY_ROW>;
template class CFlatFileLoader<INDUSTRY_ROW>;
template class CFlatFileLoader<LAST_TRADE_ROW>;
template class CFlatFileLoader<NEWS_ITEM_ROW>;
template class CFlatFileLoader<NEWS_XREF_ROW>;
template class CFlatFileLoader<SECTOR_ROW>;
template class CFlatFileLoader<SECURITY_ROW>;
template class CFlatFileLoader<SETTLEMENT_ROW>;
template class CFlatFileLoader<STATUS_TYPE_ROW>;
template class CFlatFileLoader<TAXRATE_ROW>;
template class CFlatFileLoader<TRADE_ROW>;
template class CFlatFileLoader<TRADE_HISTORY_ROW>;
template class CFlatFileLoader<TRADE_REQUEST_ROW>;
template class CFlatFileLoader<TRADE_TYPE_ROW>;
template class CFlatFileLoader<WATCH_ITEM_ROW>;
template class CFlatFileLoader<WATCH_LIST_ROW>;
template class CFlatFileLoader<ZIP_CODE_ROW>;
} // namespace TPCE

