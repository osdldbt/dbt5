/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for TRADE_HISTORY.
*/
#ifndef FLAT_TRADE_HISTORY_H
#define FLAT_TRADE_HISTORY_H

namespace TPCE
{

class CFlatTradeHistoryLoad : public CFlatFileLoader <TRADE_HISTORY_ROW>
{
private:
	CDateTime	Flat_TH_DTS;
public:
	CFlatTradeHistoryLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<TRADE_HISTORY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_TH_DTS = next_record->TH_DTS;
		int rc = fprintf( hOutFile, TradeHistoryRowFmt,
				  next_record->TH_T_ID,
				  Flat_TH_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->TH_ST_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatTradeHistory::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_TRADE_HISTORY_H
