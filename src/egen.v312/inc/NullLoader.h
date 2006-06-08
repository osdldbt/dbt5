/*
*	(c) Copyright 2002-2005, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/


/*
*	Null loader e.g. it throws away the passed in generated data.
*	Useful for testing.
*/

#ifndef NULL_LOADER_H
#define NULL_LOADER_H

namespace TPCE
{

template <typename T> class CNullLoader : public CBaseLoader<T>
{

public:
	typedef const T*	PT;		//pointer to the table row

	virtual void WriteNextRecord(PT next_record) {};	//do not load	

	virtual void FinishLoad() {};	//do nothing
};

}	// namespace TPCE

#endif	// #ifndef NULL_LOADER_H
