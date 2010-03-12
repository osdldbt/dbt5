/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * executable that opens the Market Exchange to business
 * 30 July 2006
 */

#include "transactions.h"

// Establish defaults for command line options
char szBHaddr[iMaxHostname + 1] = "localhost"; // Brokerage House address
int iListenPort = MarketExchangePort; // socket port to listen
int iBHlistenPort = BrokerageHousePort;
// # of customers for this instance
TIdent iConfiguredCustomerCount = iDefaultLoadUnitSize;
// total number of customers in the database
TIdent iActiveCustomerCount = iDefaultLoadUnitSize;

// EGen flat_ing directory location
char szFileLoc[iMaxPath + 1];
// path to output files
char outputDirectory[iMaxPath + 1] = ".";

// shows program usage
void Usage()
{
	cout << "Usage: MarketExchangeMain [options]" << endl << endl;
	cout << "   Option      Default     Description" << endl;
	cout << "   ==========  ==========  =============================" << endl;
	printf("   -a integer  %-10d  Active customer count\n",
			iActiveCustomerCount);
	printf("   -c integer  %-10d  Configured customer count\n",
			iConfiguredCustomerCount);
	cout << "   -e string               Location of EGen flat_in directory" <<
			endl;
	printf("   -l integer  %-10d  Socket listen port\n", iListenPort);
	printf("   -h string   %-10s  Brokerage House address\n", szBHaddr);
	printf("   -o string   %-10s  directory for output files\n",
			outputDirectory);
	printf("   -p integer  %-10d  Brokerage House listen port\n",
			iBHlistenPort);
}

// Parse command line
void ParseCommandLine(int argc, char *argv[])
{
	int arg;
	char *sp;
	char *vp;

	// Scan the command line arguments
	for (arg = 1; arg < argc; ++arg) {

		// Look for a switch 
		sp = argv[arg];
		if (*sp == '-') {
			++sp;
		}
		*sp = (char)tolower(*sp);

		/*
		 *  Find the switch's argument.  It is either immediately after the
		 *  switch or in the next argv
		 */
		vp = sp + 1;
		// Allow for switched that don't have any parameters.
		// Need to check that the next argument is in fact a parameter
		// and not the next switch that starts with '-'.
		//
		if ((*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-')) {
			vp = argv[++arg];
		}

		// Parse the switch
		switch (*sp) {
		case 'a':
			sscanf(vp, "%"PRId64, &iActiveCustomerCount);
			break;
		case 'c':
			sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
			break;
		case 'e':
			strncpy(szFileLoc, vp, iMaxPath);
			break;
		case 'h':
			strncpy(szBHaddr, vp, iMaxHostname);
			break;
		case 'l':
			sscanf(vp, "%d", &iListenPort);
			break;
		case 'o':
			strncpy(outputDirectory, vp, iMaxPath);
			break;
		case 'p':
			sscanf(vp, "%d", &iBHlistenPort);
			break;
		default:
			Usage();
			cout << "Error: Unrecognized option: " << sp << endl;
			exit(ERROR_BAD_OPTION);
		}
	}
}

int main(int argc, char* argv[])
{
	// Establish defaults for command line options
	strncpy(szFileLoc, "flat_in", iMaxPath);

	cout << endl << "dbt5 - Market Exchange Main" << endl;

	// Parse command line
	ParseCommandLine(argc, argv);

	// Let the user know what settings will be used.
	cout << endl << "Using the following settings:" << endl << endl;
	cout << "\tSecurity.txt file location:\t" << szFileLoc << endl;
	cout << "\tConfigured customer count:\t" <<
			iConfiguredCustomerCount << endl;
	cout << "\tActive customer count:\t\t" << iActiveCustomerCount << endl;
	cout << "\tListen port:\t\t\t" << iListenPort << endl;
	cout << "\tBrokerage House address:\t\t" << szBHaddr << endl;
	cout << "\tBrokerage House port:\t\t" << iBHlistenPort << endl;

	try {
		CMarketExchange	MarketExchange(szFileLoc, iConfiguredCustomerCount,
				iActiveCustomerCount, iListenPort, szBHaddr, iBHlistenPort,
				outputDirectory);
		cout << "Market Exchange started, waiting for trade requests..." <<
				endl;

		MarketExchange.Listener();
	} catch (CBaseErr *pErr) {
		cout << endl << "Error " << pErr->ErrorNum() << ": " <<
				pErr->ErrorText(); if (pErr->ErrorLoc()) {
			cout << " at " << pErr->ErrorLoc() << endl;
		} else {
			cout << endl;
		}
		return 1;
	} catch (std::bad_alloc err) {
		// operator new will throw std::bad_alloc exception if there is no
		// sufficient memory for the request.
		cout << endl << endl << "*** Out of memory ***" << endl;
		return 2;
	}

	pthread_exit(NULL);

	cout << "Market Exchange closed to business" << endl;
	return(0);
}
