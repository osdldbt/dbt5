/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a preliminary
 * version of a benchmark specification being developed by the TPC. The
 * Work is being made available to the public for review and comment only.
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors
 * - Sergey Vasilevskiy
 */

// 2006 Rilson Nascimento

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
