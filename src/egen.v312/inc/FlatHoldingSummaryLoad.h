/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for HOLDING_SUMMARY.
*/
#ifndef FLAT_HOLDING_SUMMARY_LOAD_H
#define FLAT_HOLDING_SUMMARY_LOAD_H

namespace TPCE
{

class CFlatHoldingSummaryLoad : public CFlatFileLoader <HOLDING_SUMMARY_ROW>
{
private:
public:
	CFlatHoldingSummaryLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<HOLDING_SUMMARY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, HoldingSummaryRowFmt,
				  next_record->HS_CA_ID,
				  next_record->HS_S_SYMB,
				  next_record->HS_QTY
			);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatHoldingSummaryLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_HOLDING_SUMMARY_LOAD_H
