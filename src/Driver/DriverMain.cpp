/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 *
 * Copyright (C) 2006 Rilson Nascimento
 *               2010 Mark Wong
 *
 * 12 August 2006
 */

// TODO Partitioning by C_ID: This should be done by using the apropriate CCE's
// constructor.

#include "Driver.h"
#include "DBT5Consts.h"

// Establish defaults for command line options
char szBHaddr[iMaxHostname + 1] = "localhost"; // Brokerage House address
int iBHListenerPort = iBrokerageHousePort;
// # of customers for this instance
TIdent iConfiguredCustomerCount = iDefaultCustomerCount;
// total number of customers in the database
TIdent iActiveCustomerCount = iDefaultCustomerCount;
int iScaleFactor = 500; // # of customers for 1 TRTPS
int iDaysOfInitialTrades = 300;
int iTestDuration = 0;
int iSleep = 1000; // msec between thread creation
int iUsers = 0; // # users
int iPacingDelay = 0;

char szInDir[iMaxPath + 1]; // path to EGen input files
char outputDirectory[iMaxPath + 1] = "."; // path to output files
// automatic RNG seed generation requires unique input
UINT32 iSeed = 0;

// shows program usage
void usage()
{
	cout << "Usage: DriverMain {options}" << endl << endl <<
			"   Option      Default    Description" << endl <<
			"   ==========  =========  ===============================" << endl;
	printf("   -c integer  %-9ld  Configured customer count\n",
			iActiveCustomerCount);
	printf("   -d integer             Duration of the test (seconds)\n");
	printf("   -f integer  %-9d  # of customers per 1 TRTPS\n",
			iScaleFactor);
	printf("   -h string   %-9s  Brokerage House address\n", szBHaddr);
	printf("   -i string   %-9s  Path to EGen flat_in directory\n", szInDir);
	printf("   -n integer  %-9d  millisecond delay between transactions\n",
			iPacingDelay);
	printf("   -o string   %-9s  # directory for output files\n",
			outputDirectory);
	printf("   -p integer  %-9d  Brokerage House listener port\n",
			iBHListenerPort);
	printf("   -r integer             Random number generator seed\n");
	printf("                          Invalidates run if used\n");
	printf("   -t integer  %-9ld  Active customer count\n",
			iConfiguredCustomerCount);
	printf("   -u integer             # of Users\n");
	printf("   -w integer  %-9d  # of Days of Initial Trades\n",
			iDaysOfInitialTrades);
	printf("   -y integer  %-9d  millisecond delay between thread creation\n",
			iSleep);
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
		*sp = (char) tolower(*sp);

		/*
		*  Find the switch's argument.  It is either immediately after the
		*  switch or in the next argv
		*/
		vp = sp + 1;
			// Allow for switched that don't have any parameters.
			// Need to check that the next argument is in fact a parameter
			// and not the next switch that starts with '-'.
			//
		if  ((*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-')) {
			vp = argv[++arg];
		}

		// Parse the switch
		switch (*sp) {
		case 'c':
			iActiveCustomerCount = atol(vp);
			break;
		case 'd':
			iTestDuration = atoi(vp);
			break;
		case 'f':
			iScaleFactor = atoi(vp);
			break;
		case 'h':
			strncpy(szBHaddr, vp, iMaxHostname);
			break;
		case 'i':	// input files path
			strncpy(szInDir, vp, iMaxPath);
			break;
		case 'n':
			iPacingDelay = atoi(vp);
			break;
		case 'o':
			strncpy(outputDirectory, vp, iMaxPath);
			break;
		case 'p':
			iBHListenerPort = atoi(vp);
			break;
		case 'r':
			iSeed = atoi(vp);
			break;
		case 't':
			iConfiguredCustomerCount = atol(vp);
			break;
		case 'w':
			iDaysOfInitialTrades = atoi(vp);
			break;
		case 'u':
			iUsers = atoi(vp);
			break;
		case 'y':
			iSleep = atoi(vp);
			break;
		default:
			usage();
			cout << endl << "Error: Unrecognized option: " << sp << endl;
			exit (ERROR_BAD_OPTION);
		}
	}

}

// Validate Parameters
bool ValidateParameters()
{
	bool bRet = true;

	// Configured Customer count must be a non-zero integral multiple of load
	// unit size.
	if ((iDefaultLoadUnitSize > iConfiguredCustomerCount) ||
			(0 != iConfiguredCustomerCount % iDefaultLoadUnitSize)) {
		cerr << "The specified customer count (-c " <<
				iConfiguredCustomerCount <<
				") must be a non-zero integral multiple of the load unit size (" <<
				iDefaultLoadUnitSize << ")." << endl;

		bRet = false;
	}

	// Active customer count must be a non-zero integral multiple of load unit
	// size.
	if ((iDefaultLoadUnitSize > iActiveCustomerCount) ||
			(0 != iActiveCustomerCount % iDefaultLoadUnitSize)) {
		cerr << "The total customer count (-a " << iActiveCustomerCount <<
				") must be a non-zero integral multiple of the load unit size (" <<
				iDefaultLoadUnitSize << ")." << endl;

		bRet = false;
	}

	// Completed trades in 8 hours must be a non-zero integral multiple of 100
	// so that exactly 1% extra trade ids can be assigned to simulate aborts.
	//
	if ((INT64) (HoursPerWorkDay * SecondsPerHour * iDefaultLoadUnitSize /
			iScaleFactor) % 100 != 0) {
		cerr << "Incompatible value for Scale Factor (-f) specified." << endl;
		cerr << HoursPerWorkDay << " * " << SecondsPerHour <<
				" * Load Unit Size (" << iDefaultLoadUnitSize <<
				") / Scale Factor (" << iScaleFactor <<
				") must be integral multiple of 100." << endl;

		bRet = false;
	}

	if (iDaysOfInitialTrades <= 0) {
		cerr << "The specified number of 8-Hour Workdays (-i " <<
				(iDaysOfInitialTrades) << ") must be non-zero." << endl;

		bRet = false;
	}

	// iUsers must be assigned
	if (iUsers == 0) {
		cerr << "The number of users threads must be specified." << endl;
		bRet = false;
	}

	// iTestDuration must be assigned
	if (iTestDuration == 0) {
		cerr << "The duration of the test must be specified." << endl;
		bRet = false;
	}

	return bRet;
}

int main(int argc, char *argv[])
{
	// Establish defaults for command line options
	strncpy(szInDir, "flat_in", iMaxPath);

	cout << "dbt5 - Driver Customer Emulator Main" << endl;

	// Parse command line
	parse_command_line(argc, argv);

	// Validate parameters
	if (!ValidateParameters()) {
		return ERROR_INVALID_OPTION_VALUE; // exit returning a non-zero code
	}

	// Let the user know what settings will be used.
	cout << "Using the following settings:" << endl << endl;

	cout << "Input files location: " << szInDir << endl << endl;

	cout << "Brokerage House address: " << szBHaddr << endl;
	cout << "Brokerage House port: " << iBHListenerPort << endl << endl;;

	cout << "Configured customer count: " << iConfiguredCustomerCount << endl;
	cout << "Active customer count: " << iActiveCustomerCount << endl;
	cout << "Days of initial trades: " << iDaysOfInitialTrades << endl;
	cout << "Scale Factor: " << iScaleFactor << endl << endl;

	cout << "User Threads: " << iUsers << endl;
	cout << "Sleep between creating users: " << iSleep << endl << endl;

	cout << "Test duration (sec): " << iTestDuration << endl;
	cout << "Pacing Delay (msec): " << iPacingDelay << endl << endl;
	cout << "Unique ID (seed): " << iSeed << endl;

	try {
		CDriver Driver(szInDir, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades,
				iSeed, szBHaddr, iBHListenerPort, iUsers, iPacingDelay,
				outputDirectory);
		Driver.runTest(iSleep, iTestDuration);

	} catch (CBaseErr *pErr) {
		cout << endl << "Error " << pErr->ErrorNum() << ": " <<
				pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout << " at " << pErr->ErrorLoc() << endl;
		} else {
			cout << endl;
		}
		return(1);
	}

	return(0);
}
