/*
 *  header files used throughout the project
 *  Copyright (C) 2006-2007 Rilson Nascimento
 */

#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include <iostream>
#include <fstream>
#include <assert.h>
#include <pthread.h>
using namespace std;

// libpqxx's header and namespace
#include <pqxx/pqxx>
using namespace pqxx;

#include <CThreadErr.h>
#include <CSocket.h>
#include <SocketPorts.h>
#include <EGenStandardTypes.h>
#include <MiscConsts.h>
#include <TxnHarnessStructs.h>
#include <CommonStructs.h>
#include <TxnHarnessSendToMarketInterface.h>
#include <TxnHarnessSendToMarket.h>

#include <DBConnection.h>
#include <TxnBaseDB.h>
#include <TradeStatusDB.h>
#include <TradeOrderDB.h>
#include <TradeResultDB.h>
#include <TradeLookupDB.h>
#include <TradeUpdateDB.h>
#include <CustomerPositionDB.h>
#include <BrokerVolumeDB.h>
#include <SecurityDetailDB.h>
#include <MarketWatchDB.h>
#include <TradeCleanupDB.h>

#include <BaseInterface.h>
#include <DM.h>
#include <DMSUT.h>
#include <DataMaintenanceDB.h>
#include <TradeCleanupDB.h>

#include <MEE.h>
#include <MEESUT.h>
#include <MarketFeedDB.h>

#include <CE.h>
#include <CESUT.h>

#include <harness.h>

#include <BrokerageHouse.h>
#include <DriverMarket.h>
#include <Driver.h>

// Array Tokenizer
void inline TokenizeArray(const string& str2, vector<string>& tokens)
{
	// This is essentially an empty array. i.e. '{}'
	if (str2.size() < 3)
		return;

	// We only call this function because we need to chop up arrays that
	// are in the format '{{1,2,3},{a,b,c}}', so trim off the braces.
	string str = str2.substr(1, str2.size() - 2);

	// Skip delimiters at beginning.
	string::size_type lastPos = str.find_first_of("{", 0);
	// Find first "non-delimiter".
	string::size_type pos = str.find_first_of("}", lastPos);

	while (string::npos != pos || string::npos != lastPos)
	{
		// Found a token, add it to the vector.
		tokens.push_back(str.substr(lastPos, pos - lastPos + 1));

		lastPos = str.find_first_of("{", pos);
		pos = str.find_first_of("}", lastPos);
	}
}

// String Tokenizer
// FIXME: This token doesn't handle strings with escaped characters.
void inline TokenizeSmart(const string& str, vector<string>& tokens)
{
	// This is essentially an empty array. i.e. '{}'
	if (str.size() < 3)
		return;

	string::size_type lastPos = 1;
	string::size_type pos = 1;
	bool end = false;
	while (end == false)
	{
		if (str[lastPos] == '"') {
			pos = str.find_first_of("\"", lastPos + 1);
			if (pos == string::npos) {
				pos = str.find_first_of("}", lastPos);
				end = true;
			}
			tokens.push_back(str.substr(lastPos + 1, pos - lastPos - 1));
			lastPos = pos + 2;
		} else if (str[lastPos] == '\0') {
			return;
		} else {
			pos = str.find_first_of(",", lastPos);
			if (pos == string::npos) {
				pos = str.find_first_of("}", lastPos);
				end = true;
			}
			tokens.push_back(str.substr(lastPos, pos - lastPos));
			lastPos = pos + 1;
		}
	}
}

bool inline check_count(int should, int is, char *file, int line) {
	if (should != is) {
		cout << "*** array length (" << is <<
				") does not match expections (" << should << "): " << file <<
				":" << line << endl;
		return false;
	}
	return true;
}

#endif	// TRANSACTIONS_H
