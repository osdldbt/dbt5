/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	A fixed size array that provides random access by index.
*
*/
#ifndef FIXED_ARRAY_H
#define FIXED_ARRAY_H

#include "EGenUtilities_stdafx.h"

namespace TPCE
{

class CFixedArrayErr : public CBaseErr
{
public:
	enum eFixedArrayErrs
	{ 
		eNotEnoughMemory,
		eIndexOutOfRange,
		eOverflow
	};

	CFixedArrayErr( eFixedArrayErrs iErr, char *szLoc) : CBaseErr(iErr, szLoc) {};
	int ErrorType() {return ERR_TYPE_FIXED_ARRAY;};
	
	char *ErrorText() 
	{
		int i;
		static char * szErrs[] = { 
			(char*)"Not enough memory",
			(char*)"Index out of range.",
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
*	Fixed-size array container
*
*	The first template parameter specifies data element type.
*	The second template parameter specifies a struct type that
*	contains the total number of data elements possible to store. 
*	The struct in the second parameter must define TotalElements() public member function.
*/
template <typename TData, typename TElementsLimits>
class CFixedArray
{	
	//Really using only the TotalElements() member function on this type
	TElementsLimits	m_sLimits;
	int				m_iTotalElements;	// total elements from limits; taken once in the constructor for performance
	int				m_iCurrentElements;	//current number of elements (cannot be greater than m_iTotalElements)
	TData			*m_pData;			//array of data elements	

public:
	typedef TData*	PData;		//pointer to a data element

	//Constructor
	CFixedArray()
		: m_iCurrentElements(0)	//no elements in the beginning
	{	
		m_iTotalElements = m_sLimits.TotalElements();

		m_pData = new TData[m_iTotalElements];
	}
	//Destructor
	~CFixedArray()
	{
		if (m_pData != NULL)
			delete [] m_pData;
	}

	//Add a (key, data) pair to the container.
	//Operation is performed in constant time for any (key, data) pair.
	void Add(TData *pData)
	{
		if (m_iCurrentElements < m_iTotalElements)
		{	//have place to insert new association

			m_pData[m_iCurrentElements] = *pData;	//copy the data value

			++m_iCurrentElements;				//because just added one element		
		}
		else
		{
			//container is full
			throw new CFixedArrayErr(CFixedArrayErr::eOverflow, (char*)"CFixedArray::Add");
		}
	}

	//Element access by index
	TData& operator[](int iIndex)
	{
		assert(iIndex>=0 && iIndex < m_iTotalElements);
		//correct index value
		return m_pData[iIndex];	//return reference to the data value		
	}

	//Return the total number of elements
	int size() {return m_iTotalElements;}
};

}	// namespace TPCE

#endif //FIXED_ARRAY_H
