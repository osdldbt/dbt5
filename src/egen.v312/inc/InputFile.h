/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	A template that represents one input file in memory.
*
*/
#ifndef INPUT_FILE_H
#define INPUT_FILE_H

#include <sstream>
#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T, typename TKeyAndElementsLimits> class CInputFile
{
	//Type of in-memory representation of input files
	typedef CFixedMap<T, TKeyAndElementsLimits>	CFileInMemoryList;	//(key, data) pairs container

	CFileInMemoryList		m_list;

	void ReadList(const char *szListFile)
	{
		ifstream	tmpFile;

		if (szListFile)
		{
			tmpFile.open(szListFile, ios_base::in);
			if (tmpFile)
			{
			    ReadList(tmpFile);
				tmpFile.close();
			}
			else
			{	//Open failed
				tmpFile.close();
				throw new CSystemErr(CSystemErr::eCreateFile, "CInputFile::ReadList");	
			}
		}
		else
		{
			throw new CSystemErr(CSystemErr::eCreateFile, "CInputFile::ReadList");
		}
	}	

	void ReadList(const string &str)
	{
	    istringstream tmpFile(str);
	    ReadList(tmpFile);
	}

	void ReadList(istream &tmpFile) {
	    T	row;
	    memset(&row, 0, sizeof(row));
	    int	iThreshold = 0, iWeight;
	    while(tmpFile.good())
	    {				
		tmpFile>>iWeight;	//first  the weight
		// We don't know if we've hit the end of the file
		// until after trying the first read.
		if( ! tmpFile.eof() )
		{
		    row.Load(tmpFile);	//then the rest of the row
		    iThreshold += iWeight;	//used as the key
		    m_list.Add(iThreshold-1/*because weights start from 1*/, &row, iWeight);//add to the container
		}
	    }
	}

public:	

	//Constructor.

	//iTotalDataElements is the number of data elements in the file.
	//Right now it's the same as the number of lines since
	//there is only one entry per line.
	CInputFile(const char *szListFile)
	{
		ReadList(szListFile);	
	}

	CInputFile(const string &str)
	{
		ReadList(str);	
	}

	//return the whole record for a given key
	//returns the second member of the element pair
	T*	GetRecord(int key) { return m_list.GetElement(key); }

	//Get the next unique record in the set.
	T* GetRecordByPassKey( int iElementID )
	{
		return( m_list.GetElementByPassKey( iElementID ));
	}

	// Return current record count
	int RecordCount( )
	{
		return m_list.ElementCount();
	}

	//return the highest key number that exists in the list
	int GetGreatestKey() { return m_list.GetHighestKey(); }
};

}	// namespace TPCE

#endif //INPUT_FILE_H
