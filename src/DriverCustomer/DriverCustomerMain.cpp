/*
 * DriverMain.cpp
 *
 * 2006 Rilson Nascimento
 *
 * 30 July 2006
 */

#include <transactions.h>

using namespace TPCE;
 
int main()
{
	try
	{
		// initialize Input Generator
		//
		CLogFormatTab fmt;
		CEGenLogger log(eDriverEGenLoader, 0, "Driver.log", &fmt);
	
		char*	szInDir = "EGen_v3.14/flat_in";
	
		CInputFiles	inputFiles;
		inputFiles.Initialize(eDriverEGenLoader, iDefaultLoadUnitSize, iDefaultLoadUnitSize, szInDir);
	
		TDriverCETxnSettings	m_DriverCETxnSettings;
	
		CCETxnInputGenerator	m_TxnInputGenerator(inputFiles, iDefaultLoadUnitSize, iDefaultLoadUnitSize, 
								500, 10*HoursPerWorkDay, &log, &m_DriverCETxnSettings);
	
		// initialize CE - Customer Emulator
		//
		CCESUT	m_CESUT;
		CCE	m_CE( &m_CESUT, &log, inputFiles, iDefaultLoadUnitSize,
						iDefaultLoadUnitSize, 500, 10, 1, &m_DriverCETxnSettings );

		while(true)
		{
			m_CE.DoTxn();
			sleep(1);
		}
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
	catch (CSocketErr *pErr)
	{
		cout<<endl<<"Error: "<<pErr->ErrorText();
		cout<<" at "<<pErr->GetLocation()<<endl;
		cout<<endl;
		return(1);
	}

	pthread_exit(NULL);

	return(0);
}
