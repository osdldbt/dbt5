/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Base interface used to output generation and load progress
*	and any other supporting information.
*/

#ifndef EGEN_GENERATE_AND_LOAD_BASE_OUTPUT_H
#define EGEN_GENERATE_AND_LOAD_BASE_OUTPUT_H


namespace TPCE
{

class CGenerateAndLoadBaseOutput
{
public:

	virtual void OutputStart(string szMsg) = 0;		// output beginning of table generation	

	virtual void OutputProgress(string szMsg) = 0;	// output progress of table generation

	virtual void OutputComplete(string szMsg) = 0;	// output completion of table generation

	virtual void OutputNewline() = 0;				// output end-of-line
};

}	// namespace TPCE

#endif //EGEN_GENERATE_AND_LOAD_BASE_OUTPUT_H
