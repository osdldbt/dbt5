/*
 * DriverCustomerMain.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 12 August 2006
 */

// TODO Partitioning by C_ID: This should be done by using the apropriate CCE's
// constructor.

#include <transactions.h>

using namespace TPCE;

// Establish defaults for command line options
char	szBHaddr[1024] = "localhost";					// Brokerage House address
int		iBHlistenPort = BrokerageHousePort;
// # of customers in one load unit
int		iLoadUnitSize = iDefaultLoadUnitSize;
// # of customers for this instance
TIdent		iConfiguredCustomerCount = iDefaultLoadUnitSize;
// total number of customers in the database
TIdent		iActiveCustomerCount = iDefaultLoadUnitSize;
int		iScaleFactor = 500;					// # of customers for 1 TRTPS
int		iDaysOfInitialTrades = 300;
int		iTestDuration = 0;
int		iSleep = 1000;						// msec between thread creation
int		iUsers = 0;						// # users
int		iPacingDelay = 0;

char		szInDir[iMaxPath];		// path to EGen input files
char		outputDirectory[iMaxPath] = ".";			// path to output files
// automatic RNG seed generation requires unique input
UINT32 		UniqueId = 0;

// shows program usage
void Usage()
{
	cerr <<
			"\nUsage: DriverCustomerMain {options}" << endl << endl <<
			"  where" << endl <<
			"   Option	Default		      Description" << endl <<
			"   =========	==================    =============================================" <<
					endl <<
			"   -e string	" << szInDir << "    Path to EGen input files" <<
					endl <<
			"   -c number	" << iConfiguredCustomerCount <<
					"\t\t      Configured customer count" << endl <<
			"   -a number	" << iActiveCustomerCount <<
					"\t\t      Active customer count" << endl <<
			"   -h string	" << szBHaddr <<
					"\t\t      Brokerage House address" << endl <<
			"   -b number	" << iBHlistenPort <<
					"\t\t      Brokerage House listen port" << endl <<
			"   -f number	" << iScaleFactor <<
					"\t\t      # of customers for 1 TRTPS" << endl <<
			"   -d number	" << iDaysOfInitialTrades <<
					"\t\t      # of Days of Initial Trades" << endl <<
			"   -l number	" << iLoadUnitSize <<
					"\t\t      # of customers in one load unit" << endl <<
			"   -t number	                      Duration of the test (seconds)" <<
					endl <<
			"   -s number	" << iSleep <<
					"\t\t      # of msec between thread creation" << endl <<
			"   -u number	                      # of Users" << endl <<
			"   -o string	" << outputDirectory <<
					"\t\t      # directory for output files" << endl <<
			"   -p number	" << iPacingDelay <<
					"\t\t      # of msec to wait after the current txn and" <<
					endl <<
			"\t\t\t\t      before the next txn" << endl <<
			"   -g number			      Unique input for automatic seed generation" <<
					endl;
}

// Parse command line
void ParseCommandLine( int argc, char *argv[] )
{
	int   arg;
	char  *sp;
	char  *vp;

	// Scan the command line arguments
	for ( arg = 1; arg < argc; ++arg ) {

		// Look for a switch 
		sp = argv[arg];
		if ( *sp == '-' )
		{
			++sp;
		}
		*sp = (char)tolower( *sp );
		
		/*
		*  Find the switch's argument.  It is either immediately after the
		*  switch or in the next argv
		*/
		vp = sp + 1;
			// Allow for switched that don't have any parameters.
			// Need to check that the next argument is in fact a parameter
			// and not the next switch that starts with '-'.
			//
		if ( (*vp == 0) && ((arg + 1) < argc) && (argv[arg + 1][0] != '-') )
			{        
			vp = argv[++arg];
		}
		
		// Parse the switch
		switch ( *sp )
		{
			case 'e':	// input files path
				strncpy(szInDir, vp, sizeof(szInDir));
				break;
			case 'h':
				strncpy(szBHaddr, vp, sizeof(szBHaddr));
				break;
			case 'c':
				sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
				break;
			case 'a':
				sscanf(vp, "%"PRId64, &iActiveCustomerCount);
        			break;
			case 'b':
				sscanf(vp, "%d", &iBHlistenPort);
				break;
			case 'f':
				sscanf(vp, "%d", &iScaleFactor);
				break;
			case 'd':
				sscanf(vp, "%d", &iDaysOfInitialTrades);
				break;
			case 'l':
				sscanf(vp, "%d", &iLoadUnitSize);
				break;
			case 'g':
				sscanf(vp, "%d", &UniqueId);
				break;
			case 's':
				sscanf(vp, "%d", &iSleep);
				break;
			case 'u':
				sscanf(vp, "%d", &iUsers);
				break;
			case 'o':
				strncpy(outputDirectory, vp,
						sizeof(outputDirectory));
				break;
			case 'p':
				sscanf(vp, "%d", &iPacingDelay);
				break;
			case 't':
				sscanf(vp, "%d", &iTestDuration);
				break;
			default:
				Usage();
				cout<<"Error: Unrecognized option: "<<sp<<endl;
				exit( ERROR_BAD_OPTION );
		}
	}

}

// Validate Parameters
bool ValidateParameters()
{
	bool bRet = true;

	// Configured Customer count must be a non-zero integral multiple of load
	// unit size.
	if ((iLoadUnitSize > iConfiguredCustomerCount) ||
			(0 != iConfiguredCustomerCount % iLoadUnitSize))
	{
		cerr << "The specified customer count (-c " << iConfiguredCustomerCount 
			<< ") must be a non-zero integral multiple of the load unit size (" 
			<< iLoadUnitSize << ")." << endl;

		bRet = false;
	}

	// Active customer count must be a non-zero integral multiple of load unit
	// size.
	if ((iLoadUnitSize > iActiveCustomerCount) ||
			(0 != iActiveCustomerCount % iLoadUnitSize))
	{
		cerr << "The total customer count (-t " << iActiveCustomerCount 
			<< ") must be a non-zero integral multiple of the load unit size (" 
			<< iLoadUnitSize << ")." << endl;

		bRet = false;
	}	

	// Completed trades in 8 hours must be a non-zero integral multiple of 100
	// so that exactly 1% extra trade ids can be assigned to simulate aborts.
	//
	if ((INT64)(HoursPerWorkDay * SecondsPerHour * iLoadUnitSize /
			iScaleFactor) % 100 != 0)
	{
		cerr << "Incompatible value for Scale Factor (-f) specified." << endl;
		cerr << HoursPerWorkDay << " * " << SecondsPerHour <<
				" * Load Unit Size (" << iLoadUnitSize <<
				") / Scale Factor (" << iScaleFactor <<
				") must be integral multiple of 100." << endl;

		bRet = false;
	}

	if (iDaysOfInitialTrades <= 0) 
	{
		cerr << "The specified number of 8-Hour Workdays (-w " 
			<< (iDaysOfInitialTrades) << ") must be non-zero." << endl;

		bRet = false;
	}

	// UniqueId must be assigned
	if (UniqueId == 0)
	{
		cerr << "non-zero unique id number must be specified."<<endl;
		bRet = false;
	}

	// iUsers must be assigned
	if (iUsers == 0)
	{
		cerr << "number of users must be specified."<<endl;
		bRet = false;
	}

	// iTestDuration must be assigned
	if (iTestDuration == 0)
	{
		cerr << "the duration of the test must be specified."<<endl;
		bRet = false;
	}

	return bRet;
}

// main
int main(int argc, char* argv[])
{
	// Establish defaults for command line options
	strncpy(szInDir, "EGen_v3.14/flat_in", sizeof(szInDir)-1);

	cout<<endl<<"dbt5 - Driver Customer Emulator Main"<<endl;

	// Parse command line
	ParseCommandLine(argc, argv);

	// Validate parameters
	if (!ValidateParameters())
	{
		return ERROR_INVALID_OPTION_VALUE;	// exit returning a non-zero code
	}	

	// Let the user know what settings will be used.
	cout<<endl<<"Using the following settings:"<<endl<<endl;
	cout<<"\tInput files location:\t\t"<<szInDir<<endl;
	cout<<"\tConfigured customer count:\t"<<iConfiguredCustomerCount<<endl;
	cout<<"\tActive customer count:\t\t"<<iActiveCustomerCount<<endl;
	cout<<"\tBrokerage House address:\t\t"<<szBHaddr<<endl;
	cout<<"\tBrokerage House port:\t\t"<<iBHlistenPort<<endl;
	cout<<"\tScale Factor:\t\t\t"<<iScaleFactor<<endl;
	cout<<"\t#Days of initial trades:\t"<<iDaysOfInitialTrades<<endl;
	cout<<"\tLoad unit size:\t\t\t"<<iLoadUnitSize<<endl;
	cout<<"\tSleep between Users:\t\t"<<iSleep<<endl;
	cout<<"\tTest duration (sec):\t\t"<<iTestDuration<<endl;
	cout<<"\tUnique ID:\t\t\t"<<UniqueId<<endl;
	cout<<"\t# of Users:\t\t\t"<<iUsers<<endl;
	cout<<"\tPacing Delay (msec):\t\t"<<iPacingDelay<<endl<<endl;

	try
	{
		CDriverCustomer    DriverCustomer(szInDir, iConfiguredCustomerCount,
				iActiveCustomerCount, iScaleFactor, iDaysOfInitialTrades,
				UniqueId, szBHaddr, iBHlistenPort, iUsers, iPacingDelay,
				outputDirectory);
		DriverCustomer.RunTest(iSleep, iTestDuration);
		
	}
	catch (CBaseErr *pErr)
	{
		cout<<endl<<"Error "<<pErr->ErrorNum()<<": "<<pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout<<" at "<<pErr->ErrorLoc()<<endl;
		} else {
			cout<<endl;
		}
		return(1);
	}

	return(0);
}
