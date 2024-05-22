/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright The DBT-5 Authors
 */

#include "DBConnection.h"
#include "DBConnectionClientSide.h"

CDBConnectionClientSide::CDBConnectionClientSide(const char *szHost,
		const char *szDBName, const char *szDBPort, bool bVerbose)
: CDBConnection(szHost, szDBName, szDBPort, bVerbose)
{
}

CDBConnectionClientSide::~CDBConnectionClientSide() {}

void
CDBConnectionClientSide::execute(
		const TBrokerVolumeFrame1Input *pIn, TBrokerVolumeFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TCustomerPositionFrame1Input *pIn,
		TCustomerPositionFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TCustomerPositionFrame2Input *pIn,
		TCustomerPositionFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TDataMaintenanceFrame1Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TMarketWatchFrame1Input *pIn, TMarketWatchFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TSecurityDetailFrame1Input *pIn,
		TSecurityDetailFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TTradeCleanupFrame1Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame1Input *pIn, TTradeLookupFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame2Input *pIn, TTradeLookupFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame3Input *pIn, TTradeLookupFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeLookupFrame4Input *pIn, TTradeLookupFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame1Input *pIn, TTradeOrderFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame2Input *pIn, TTradeOrderFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame3Input *pIn, TTradeOrderFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeOrderFrame4Input *pIn, TTradeOrderFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame1Input *pIn, TTradeResultFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame2Input *pIn, TTradeResultFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame3Input *pIn, TTradeResultFrame3Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame4Input *pIn, TTradeResultFrame4Output *pOut)
{
}

void
CDBConnectionClientSide::execute(const TTradeResultFrame5Input *pIn)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeResultFrame6Input *pIn, TTradeResultFrame6Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeStatusFrame1Input *pIn, TTradeStatusFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame1Input *pIn, TTradeUpdateFrame1Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame2Input *pIn, TTradeUpdateFrame2Output *pOut)
{
}

void
CDBConnectionClientSide::execute(
		const TTradeUpdateFrame3Input *pIn, TTradeUpdateFrame3Output *pOut)
{
}
