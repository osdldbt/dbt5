/*
 * DriverMarketMain.cpp
 * executable that opens the Market Exchange to business
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;

// Establish defaults for command line options
int		iListenPort = DriverMarketPort;				// socket port to listen
TIdent		iConfiguredCustomerCount = iDefaultLoadUnitSize;	// # of customers for this instance
TIdent		iActiveCustomerCount = iDefaultLoadUnitSize;		// total number of customers in the database

char		szFileLoc[iMaxPath];					// Security.txt file location

// shows program usage
void Usage()
{
	cout<<"\nUsage: DriverMarketMain [options]"<<endl<<endl;
	cout<<"  where"<<endl;
	cout<<"   Option	Default				   Description"<<endl;
	cout<<"   =========	===============================    ============================="<<endl;
	cout<<"   -s string	"<<szFileLoc<<"   Location of Security.txt file"<<endl;
	cout<<"   -c number	"<<iConfiguredCustomerCount<<"\t\t\t\t   Configured customer count"<<endl;
	cout<<"   -a number	"<<iActiveCustomerCount<<"\t\t\t\t   Active customer count"<<endl;
	cout<<"   -l number	"<<iListenPort<<"\t\t\t\t   Socket listen port"<<endl;
	cout<<endl;
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
			case 's':	// Security file location
				strncpy(szFileLoc, vp, sizeof(szFileLoc));
				break;
			case 'c':
				sscanf(vp, "%"PRId64, &iConfiguredCustomerCount);
				break;
			case 'a':
				sscanf(vp, "%"PRId64, &iActiveCustomerCount);
        			break;
			case 'l':
				sscanf(vp, "%"PRId64, &iListenPort);
				break;
			default:
				Usage();
				cout<<"Error: Unrecognized option: "<<sp<<endl;
				exit( ERROR_BAD_OPTION );
		}
	}

}

// main
int main(int argc, char* argv[])
{
	// Establish defaults for command line options
	strncpy(szFileLoc, "EGen_v3.14/flat_in/Security.txt", sizeof(szFileLoc)-1);

	cout<<endl<<"dbt5 - Driver Market Main"<<endl;

	// Parse command line
	ParseCommandLine(argc, argv);

	// Let the user know what settings will be used.
	cout<<endl<<"Using the following settings:"<<endl<<endl;
	cout<<"\tSecurity.txt file location:\t"<<szFileLoc<<endl;
	cout<<"\tConfigured customer count:\t"<<iConfiguredCustomerCount<<endl;
	cout<<"\tActive customer count:\t\t"<<iActiveCustomerCount<<endl;
	cout<<"\tListen port:\t\t\t"<<iListenPort<<endl;

	try
	{
		CDriverMarket	DriverMarket(szFileLoc, iConfiguredCustomerCount, iActiveCustomerCount, iListenPort);
		cout<<endl<<"Market Exchange opened to business, waiting trade requests..."<<endl;
	
		DriverMarket.Listener();
	}
	catch (CSocketErr *pErr)
	{
		cout<<endl<<"Error: "<<pErr->ErrorText();
		cout<<" at "<<pErr->GetLocation()<<endl;
		cout<<endl;
		return(1);
	}
	catch (CBaseErr *pErr)
	{
		cout<<endl<<"Error "<<pErr->ErrorNum()<<": "<<pErr->ErrorText();
		if (pErr->ErrorLoc()) {
			cout<<" at "<<pErr->ErrorLoc()<<endl;
		} else {
			cout<<endl;
		}
		return 1;
	}
	// operator new will throw std::bad_alloc exception if there is no sufficient memory for the request.
	catch (std::bad_alloc err)
	{
		cout<<endl<<endl<<"*** Out of memory ***"<<endl;
		return 2;
	}

	pthread_exit(NULL);

	cout<<"Market Exchange closed to business"<<endl;
	return(0);
}
