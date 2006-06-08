/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class representing just straight flat input file.
*/
#ifndef FLAT_FILE_H
#define FLAT_FILE_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T, typename TKeyAndElementsLimits> class CFlatFile
{
protected:
	//Type of in-memory representation of input files
	typedef CFixedArray<T, TKeyAndElementsLimits>	CFileInMemoryList;	//array of arrays

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
				throw new CSystemErr(CSystemErr::eCreateFile, "CFlatFile::ReadList");	
			}
		}
		else
		{
			throw new CSystemErr(CSystemErr::eCreateFile, "CFlatFile::ReadList");
		}
	}

	void ReadList(const string &str)
	{
	    istringstream tmpFile(str);
	    ReadList(tmpFile);
	}

	void ReadList(istream &tmpFile)
	{
	    T	row;
	    memset(&row, 0, sizeof(row));

	    while(tmpFile.good())
	    {				
		row.Load(tmpFile);	//read the row
		// We don't know if we've hit the end of the file
		// until after trying the read.
		if( ! tmpFile.eof() )
		{
		    m_list.Add(&row);	//insert into the container
		}

	    }
	}


public:	

	//Constructor.
	CFlatFile(const char *szListFile)
	{
		ReadList(szListFile);	
	}
	
	CFlatFile(const string &str)
	{
		ReadList(str);	
	}
	
	//Returns the element at a specific index
	T*	GetRecord(int index) { return &m_list[index]; };

	//Returns the size of the file (number of rows)
	UINT	GetSize()	{return (UINT)m_list.size();}	
};

}	// namespace TPCE

#endif //FLAT_FILE_H
