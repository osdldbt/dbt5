/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for HOLDING_HISTORY.
*/
#ifndef FLAT_HOLDING_HISTORY_LOAD_H
#define FLAT_HOLDING_HISTORY_LOAD_H

namespace TPCE
{

class CFlatHoldingHistoryLoad : public CFlatFileLoader <HOLDING_HISTORY_ROW>
{
private:
public:
	CFlatHoldingHistoryLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<HOLDING_HISTORY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, HoldingHistoryRowFmt,
				  next_record->HH_H_T_ID,
				  next_record->HH_T_ID,
				  next_record->HH_BEFORE_QTY,
				  next_record->HH_AFTER_QTY
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatHoldingHistoryLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_HOLDING_HISTORY_LOAD_H
