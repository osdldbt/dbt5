/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Class representing a flat file loader.
*/
#ifndef FLAT_FILE_LOADER_H
#define FLAT_FILE_LOADER_H

#include "../inc/FlatFileLoad_stdafx.h"

namespace TPCE
{
#ifdef DATETIME_FORMAT
const int	FlatFileDateTimeFormat = DATETIME_FORMAT;	// user-defined
#else
const int	FlatFileDateTimeFormat = 11;		// YYYY-MM-DD hh:mm:ss
#endif
#ifdef TIME_FORMAT
const int	FlatFileTimeFormat = TIME_FORMAT;	// user-defined
#else
const int	FlatFileTimeFormat = 01;		// hh:mm:ss
#endif
#ifdef DATE_FORMAT
const int	FlatFileDateFormat = DATE_FORMAT;	// user-defined
#else
const int	FlatFileDateFormat = 10;		// YYYY-MM-DD
#endif

// Overwrite vs. append functionality for output flat files.
enum FlatFileOutputModes {
	FLAT_FILE_OUTPUT_APPEND = 0,
	FLAT_FILE_OUTPUT_OVERWRITE
};

/*
*	FlatLoader class.
*/
template <typename T> class CFlatFileLoader : public CBaseLoader<T>
{		
protected:
	FILE			*hOutFile;

public:	

	CFlatFileLoader(char *szFileName, FlatFileOutputModes FlatFileOutputMode);
	~CFlatFileLoader(void);

	virtual void WriteNextRecord(const T* next_record) {};
	void FinishLoad();	//finish load 

};

}	// namespace TPCE


using namespace TPCE;

/*
*	The constructor.
*/
template <typename T>
CFlatFileLoader<T>::CFlatFileLoader(char *szFileName, FlatFileOutputModes FlatFileOutputMode)
{
	if( FLAT_FILE_OUTPUT_APPEND == FlatFileOutputMode )
	{
		hOutFile = fopen( szFileName, "a" );
	}
	else if( FLAT_FILE_OUTPUT_OVERWRITE == FlatFileOutputMode )
	{
		hOutFile = fopen( szFileName, "w" );
	}

	if (!hOutFile)
	{
		throw new CSystemErr(CSystemErr::eCreateFile, "CFlatFileLoader<T>::CFlatFileLoader");
	}
}

/*
*	Destructor.
*/
template <typename T>
CFlatFileLoader<T>::~CFlatFileLoader()
{
	fclose(hOutFile);
}

/*
*	Commit sent rows. This needs to be called after the last row has been sent
*	and before the object is destructed. Otherwise all rows will be discarded.
*/
template <typename T>
void CFlatFileLoader<T>::FinishLoad()
{
	fflush(hOutFile);
}

#endif //FLAT_FILE_LOADER_H
