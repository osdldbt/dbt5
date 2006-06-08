/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	A Map container that stores (key, value) pairs and provides random access by key.
*	The container size (number of elements) is fixed.
*	The key type is always int.
*
*/
#ifndef FIXED_MAP_H
#define FIXED_MAP_H

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

class CFixedMapErr : public CBaseErr
{
public:
	enum eFixedMapErrs
	{ 
		eNotEnoughMemory,
		eKeyOutOfRange,
		eOverflow
	};

	CFixedMapErr( eFixedMapErrs iErr, char *szLoc) : CBaseErr(iErr, szLoc) {};
	int ErrorType() {return ERR_TYPE_FIXED_MAP;};
	
	char *ErrorText() 
	{
		int i;
		static char * szErrs[] = { 
			(char*)"Not enough memory",
			(char*)"Key value out of range.",
			(char*)"Cannot insert element - container is full.",
			(char*)""
		};
		
		for(i = 0; szErrs[i][0]; i++)
		{
			// Confirm that an error message has been defined for the error code
			if ( i == m_idMsg )
				break;
		}
		
		return(szErrs[i][0] ? szErrs[i] : (char*)ERR_UNKNOWN);
	}
};

/*
*	Fixed-size map container
*
*	The first template parameter specifies data element type.
*	The second template parameter specifies a struct type that
*	contains highest possible key value and the total number of
*	data elements possible to insert 
*	(should be less or equal to the highest key value).
*	The struct in the second parameter must define HighestKey()
*	and TotalElements() public member functions.
*/
template <typename TData, typename TKeyAndElementsLimits>
class CFixedMap
{
	//Highest key value. Key values range is [0, m_iHighestKey)
	//and
	//the size of the container (total number of data elements)
	TKeyAndElementsLimits	m_sLimits;
	int		m_iHighestKey;		//Highest key value from limits; taken once in constructor for performance
	int		m_iCurrentElements;	//current number of elements (cannot be greater than m_iTotalElements)
	TData	*m_pData;			//array of data elements
	int		*m_keys;			//An array that maps key values to indices of the corresponding 
								//data values in m_pData array.
								//Key value is an index in m_keys array and data value index is the
								//value in m_keys array.	

public:
	typedef TData*	PData;		//pointer to a data element

	//Constructor
	CFixedMap()
		: m_iCurrentElements(0)	//no elements in the beginning
	{
		m_iHighestKey = m_sLimits.HighestKey();

		m_keys = new int[m_iHighestKey];

		m_pData = new TData[m_sLimits.TotalElements()];
	}
	//Destructor
	~CFixedMap()
	{
		if (m_keys != NULL)
			delete [] m_keys;

		if (m_pData != NULL)
			delete [] m_pData;
	}

	//Add a (key, data) pair to the container.
	//Operation is performed in constant time for any (key, data) pair.
	//iPrevKeysToFill specifies how many previous (to iKey) m_keys entries
	//to fill with the identical (iKey, *pData) association
	void Add(int iKey, TData *pData, int iPrevKeysToFill = 1)
	{
		if (m_iCurrentElements < m_sLimits.TotalElements())
		{	//have place to insert new association
			if (iKey>=0 && iKey < m_iHighestKey)
			{	//correct key value

				for (int j = 0; j < iPrevKeysToFill && (iKey - j)>=0; ++j)
					m_keys[iKey - j] = m_iCurrentElements;	//set the (key -> data) association

				m_pData[m_iCurrentElements] = *pData;	//copy the data value

				++m_iCurrentElements;				//because just added one element
			}
			else
			{	//incorrect key value (won't be a place for it in the m_keys array)
				throw new CFixedMapErr(CFixedMapErr::eKeyOutOfRange, (char*)"CFixedMap::Add");
			}
		}
		else
		{
			//container is full
			throw new CFixedMapErr(CFixedMapErr::eOverflow, (char*)"CFixedMap::Add");
		}
	}

	//Return reference to the data element associated with iKey.
	//Operation is performed in constant time for any key value.
	TData* GetElement(int iKey)
	{
		assert(iKey>=0 && iKey < m_iHighestKey);
		//correct key value
		return &m_pData[m_keys[iKey]];	//return reference to the data value				
	}

	// Provide reference to the next unique element.
	TData* GetElementByPassKey( int ElementID )
	{
		assert( ElementID >=0 && ElementID < m_iCurrentElements );
		return( &m_pData[ElementID] );
	}

	// Return current element count.
	int ElementCount( )
	{
		return m_iCurrentElements;
	}

	//Return the highest possible key number + 1
	int GetHighestKey() {return m_iHighestKey;}
};

}	// namespace TPCE

#endif //FIXED_MAP_H
