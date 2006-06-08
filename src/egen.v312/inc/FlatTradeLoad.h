/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for TRADE.
*/
#ifndef FLAT_TRADE_LOAD_H
#define FLAT_TRADE_LOAD_H

namespace TPCE
{

class CFlatTradeLoad : public CFlatFileLoader <TRADE_ROW>
{
private:
	CDateTime	Flat_T_DTS;
public:
	CFlatTradeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<TRADE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_T_DTS = next_record->T_DTS;
		int rc = fprintf( hOutFile, TradeRowFmt,
				  next_record->T_ID,
				  Flat_T_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->T_ST_ID,
				  next_record->T_TT_ID,
				  next_record->T_IS_CASH,
				  next_record->T_S_SYMB,
				  next_record->T_QTY,
				  next_record->T_BID_PRICE,
				  next_record->T_CA_ID,
				  next_record->T_EXEC_NAME,
				  next_record->T_TRADE_PRICE,
				  next_record->T_CHRG,
				  next_record->T_COMM,
				  next_record->T_TAX,
				  next_record->T_LIFO
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatTradeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_TRADE_LOAD_H
