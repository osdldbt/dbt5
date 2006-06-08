/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for WATCH_LIST.
*/
#ifndef FLAT_WATCH_LIST_LOAD_H
#define FLAT_WATCH_LIST_LOAD_H

namespace TPCE
{

class CFlatWatchListLoad : public CFlatFileLoader <WATCH_LIST_ROW>
{
public:
	CFlatWatchListLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<WATCH_LIST_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, WatchListRowFmt,
				  next_record->WL_ID,
				  next_record->WL_C_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatWatchListLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_WATCH_LIST_LOAD_H
