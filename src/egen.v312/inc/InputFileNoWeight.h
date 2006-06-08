/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	A template that represents one input file without the weight column in memory.
*
*/
#ifndef INPUT_FILE_NO_WEIGHT_H
#define INPUT_FILE_NO_WEIGHT_H

#include "EGenTables_stdafx.h"

namespace TPCE
{

template <typename T> class CInputFileNoWeight
{
	typedef vector<T>*	PVectorT;
	//Type of in-memory representation of input files
	typedef vector<PVectorT>	CFileInMemoryList;	//array of arrays
	
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
				throw new CSystemErr(CSystemErr::eCreateFile, "CInputFileNoWeight::ReadList");	
			}
		}
		else
		{
			throw new CSystemErr(CSystemErr::eCreateFile, "CInputFileNoWeight::ReadList");
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
	    int	iThreshold = 0, iIndex;
	    int iLastIndex = -1; /* must be different from the 1st index in the input file */

				while(tmpFile.good())
				{				
					tmpFile>>iIndex;	//read the first column, which is the index
					if( ! tmpFile.eof() )
					{
						row.Load(tmpFile);	//read the row
						if (iIndex!=iLastIndex)
						{
							PVectorT parray_row = new vector<T>;
							if (parray_row!=NULL)
								m_list.push_back(parray_row);	//new array
							else
								throw new CMemoryErr("CInputFileNoWeight::ReadFile");
							iLastIndex = iIndex;
						}
						//Indices in the file start with 1 => substract 1.
						m_list[iIndex-1]->push_back(row);	//insert into the container
					}					
				}
	}	

public:	

	//Constructor.
	CInputFileNoWeight(const char *szListFile)
	{
		ReadList(szListFile);	
	}

	CInputFileNoWeight(const string &str)
	{
		ReadList(str);	
	}

	//Destructor
	~CInputFileNoWeight()
	{
		for(size_t i=0; i<m_list.size(); ++i)
			delete m_list[i];
	}

	//Returns the element at a specific index
	PVectorT	GetRecord(int index) { return m_list[index]; };

	//Returns the number of records in the file (needed for TaxRates table
	UINT		GetSize() { return (UINT)m_list.size(); }
};

}	// namespace TPCE

#endif //INPUT_FILE_NO_WEIGHT_H
