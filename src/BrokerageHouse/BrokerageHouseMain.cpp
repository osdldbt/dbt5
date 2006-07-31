/*
 * BrokerageHouseMain.cpp
 * executable that opens the Brokerage House to business
 *
 * 2006 Rilson Nascimento
 *
 * 25 July 2006
 */

#include <transactions.h>

using namespace TPCE;

char		szHost[iMaxPGHost];
char		szDBName[iMaxPGDBName];
char 		szPostmasterPort[iMaxPGPort];

// shows program usage
void Usage()
{
	cout<<"\nUsage: BrokerageHouseMain [options]"<<endl<<endl;
	cout<<"  where"<<endl;
	cout<<"   Option		Default    Description"<<endl;
	cout<<"   =========		=========  ==============="<<endl;
	cout<<"   -s string		localhost  Database server"<<endl;
	cout<<"   -d string		dbt5       Database name"<<endl;
	cout<<"   -p string		5432       Postmaster port"<<endl;
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
			case 's':	// Database host name.
				strncpy(szHost, vp, sizeof(szHost));
				break;
		
			case 'd':	// Database name.
				strncpy(szDBName, vp, sizeof(szDBName));
				break;
		
			case 'p':     // Postmaster port
				strncpy(szPostmasterPort, vp, sizeof(szPostmasterPort));
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
	// Establish defaults for command line options.
	strncpy(szHost, "localhost", sizeof(szHost)-1);
	strncpy(szDBName, "dbt5", sizeof(szDBName)-1);
	strncpy(szPostmasterPort, "5432", sizeof(szPostmasterPort)-1);

	cout<<endl<<"dbt5 - Brokerage House Main"<<endl;

	// Parse command line
	ParseCommandLine(argc, argv);

	// Let the user know what settings will be used.
	cout<<endl<<"Using the following settings:"<<endl<<endl;
	cout<<"\tHost:\t\t\t"<<szHost<<endl;
	cout<<"\tDatabase:\t\t"<<szDBName<<endl;
	cout<<"\tPostmaster port:\t"<<szPostmasterPort<<endl;

	try
	{
		CBrokerageHouse	BrokerageHouse( szHost, szDBName, szPostmasterPort );
		cout<<endl<<"Brokerage House opened to business, waiting traders..."<<endl;
	
		BrokerageHouse.Listener();
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
	//
	catch (std::bad_alloc err)
	{
		cout<<endl<<endl<<"*** Out of memory ***"<<endl;
		return 2;
	}
	// exceptions thrown by pqxx
	//
	catch (const pqxx::sql_error &e)
	{
		cout<<"SQL error: "<<e.what()<<endl
		    <<"Query was: '"<<e.query()<<"'"<<endl;
		return 3;
	}
	catch (const pqxx::broken_connection &e) // broken connection
	{
		cout<<"libpxx: "<<e.what()<<endl;
		return 3;
	}
	catch (const exception &e)
	{
		cout<<e.what()<<endl;
		return 3;
	}

	pthread_exit(NULL);

	cout<<"Brokerage House closed to business"<<endl;
	return(0);
}
