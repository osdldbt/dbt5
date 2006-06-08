/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Base interface for all loader classes.
*/

#ifndef BASE_LOADER_H
#define BASE_LOADER_H

namespace TPCE
{

template <typename T> class CBaseLoader
{

public:
	typedef const T*	PT;		//pointer to the table row

	virtual void Init() {};		//resets to clean state; needed after FinishLoad to continue loading

	virtual void WriteNextRecord(PT next_record) = 0;	//must be defined in subclasses	

	virtual void Commit() {};	// commit all records written up to this point; default implementation is empty

	virtual void FinishLoad() = 0;	//finish load 	
};

}	// namespace TPCE

#endif	// #ifndef BASE_LOADER_H
