/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for NEWS_XREF.
*/
#ifndef FLAT_NEWS_XREF_LOAD_H
#define FLAT_NEWS_XREF_LOAD_H

namespace TPCE
{

class CFlatNewsXRefLoad : public CFlatFileLoader <NEWS_XREF_ROW>
{
public:
	CFlatNewsXRefLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<NEWS_XREF_ROW>(szFileName, FlatFileOutputMode) {};

	/*
	*	Writes a record to the file.
	*/
	virtual void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, NewsXRefRowFmt,
				  next_record->NX_NI_ID,
				  next_record->NX_CO_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatNewsXRefLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_NEWS_XREF_LOAD_H
