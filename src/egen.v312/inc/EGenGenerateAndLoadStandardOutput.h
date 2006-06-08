/*
*	(c) Copyright 2002-2004, Microsoft Corporation
*	Provided to the TPC under license.
*	Written by Sergey Vasilevskiy.
*/

/*
*	Class for generation and load output to stdout.
*/

#ifndef EGEN_GENERATE_AND_LOAD_STANDARD_OUTPUT_H
#define EGEN_GENERATE_AND_LOAD_STANDARD_OUTPUT_H


namespace TPCE
{

class CGenerateAndLoadStandardOutput : public CGenerateAndLoadBaseOutput
{
public:

	void OutputStart(string szMsg)		// output beginning of table generation
	{
		printf(szMsg.c_str());
		fflush(stdout);	// in case there is no newline character in szMsg
	}

	void OutputProgress(string szMsg)	// output progress of table generation
	{
		printf(szMsg.c_str());
		fflush(stdout);	// in case there is no newline character in szMsg
	}

	void OutputComplete(string szMsg)	// output completion of table generation
	{
		printf(szMsg.c_str());
		fflush(stdout);	// in case there is no newline character in szMsg
	}

	void OutputNewline()				// output end-of-line
	{
		printf("\n");
	}
};

}	// namespace TPCE

#endif //EGEN_GENERATE_AND_LOAD_STANDARD_OUTPUT_H
