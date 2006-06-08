/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for WATCH_ITEM.
*/
#ifndef FLAT_WATCH_ITEM_LOAD_H
#define FLAT_WATCH_ITEM_LOAD_H

namespace TPCE
{

class CFlatWatchItemLoad : public CFlatFileLoader <WATCH_ITEM_ROW>
{
public:
	CFlatWatchItemLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<WATCH_ITEM_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, WatchItemRowFmt,
				  next_record->WI_WL_ID,
				  next_record->WI_S_SYMB
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatWatchItemLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_WATCH_ITEM_LOAD_H
