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

#include "MarketExchange.h"
#include "DBT5Consts.h"

// Establish defaults for command line options
char szBHaddr[iMaxHostname + 1] = "localhost"; // Brokerage House address
int iListenPort = iMarketExchangePort; // socket port to listen
int iBHlistenPort = iBrokerageHousePort;
// # of customers for this instance
TIdent iConfiguredCustomerCount = iDefaultCustomerCount;
// total number of customers in the database
TIdent iActiveCustomerCount = iDefaultCustomerCount;

// EGen flat_ing directory location
char szFileLoc[iMaxPath + 1];
// path to output files
char outputDirectory[iMaxPath + 1] = ".";

// shows program usage
void usage()
{
	cout << "Usage: MarketExchangeMain [options]" << endl << endl;
	cout << "   Option      Default     Description" << endl;
	cout << "   ==========  ==========  =============================" << endl;
	printf("   -c integer  %-10ld  Configured customer count\n",
			iConfiguredCustomerCount);
	cout << "   -i string               Location of EGen flat_in directory" <<
			endl;
	printf("   -l integer  %-10d  Socket listen port\n", iListenPort);
	printf("   -h string   %-10s  Brokerage House address\n", szBHaddr);
	printf("   -o string   %-10s  directory for output files\n",
			outputDirectory);
	printf("   -t integer  %-10ld  Active customer count\n",
			iActiveCustomerCount);
	printf("   -p integer  %-10d  Brokerage House listen port\n",
			iBHlistenPort);
}

// Parse command line
void parse_command_line(int argc, char *argv[])
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
		case 'c':
			iActiveCustomerCount = atol(vp);
			break;
		case 'h':
			strncpy(szBHaddr, vp, iMaxHostname);
			break;
		case 'i':
			strncpy(szFileLoc, vp, iMaxPath);
			break;
		case 'l':
			iListenPort = atoi(vp);
			break;
		case 'o':
			strncpy(outputDirectory, vp, iMaxPath);
			break;
		case 'p':
			sscanf(vp, "%d", &iBHlistenPort);
			break;
		case 't':
			iConfiguredCustomerCount = atol(vp);
			break;
		default:
			usage();
			cout << endl << "Error: Unrecognized option: " << sp << endl;
			exit(ERROR_BAD_OPTION);
		}
	}
}

int main(int argc, char *argv[])
{
	// Establish defaults for command line options
	strncpy(szFileLoc, "flat_in", iMaxPath);

	cout << "dbt5 - Market Exchange Main" << endl;
	cout << "Listener port: " << iListenPort << endl << endl;

	// Parse command line
	parse_command_line(argc, argv);

	// Let the user know what settings will be used.
	cout << "Using the following settings:" << endl << endl;
	cout << "EGen flat_in directory location: " << szFileLoc << endl;
	cout << "Configured customer count: " << iConfiguredCustomerCount << endl;
	cout << "Active customer count: " << iActiveCustomerCount << endl;
	cout << "Brokerage House address: " << szBHaddr << endl;
	cout << "Brokerage House port: " << iBHlistenPort << endl;

	try {
		CMarketExchange MarketExchange(szFileLoc, iConfiguredCustomerCount,
				iActiveCustomerCount, iListenPort, szBHaddr, iBHlistenPort,
				outputDirectory);
		cout << "Market Exchange started, waiting for trade requests..." <<
				endl;

		MarketExchange.startListener();
	} catch (CBaseErr *pErr) {
		cout << "Error " << pErr->ErrorNum() << ": " <<
				pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout << " at " << pErr->ErrorLoc();
		}
		cout << endl;
		return 1;
	} catch (std::bad_alloc err) {
		// operator new will throw std::bad_alloc exception if there is no
		// sufficient memory for the request.
		cout << "*** Out of memory ***" << endl;
		return 2;
	}

	pthread_exit(NULL);

	cout << "Market Exchange closed for business." << endl;
	return(0);
}
