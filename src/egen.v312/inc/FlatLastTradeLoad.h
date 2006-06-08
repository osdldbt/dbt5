/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for LAST_TRADE.
*/
#ifndef FLAT_LAST_TRADE_LOAD_H
#define FLAT_LAST_TRADE_LOAD_H

namespace TPCE
{

class CFlatLastTradeLoad : public CFlatFileLoader <LAST_TRADE_ROW>
{
private:
	CDateTime	Flat_LT_DTS;
public:
	CFlatLastTradeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<LAST_TRADE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_LT_DTS = next_record->LT_DTS;
		int rc = fprintf( hOutFile, LastTradeRowFmt,
				  next_record->LT_S_SYMB,
				  Flat_LT_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->LT_PRICE,
				  next_record->LT_OPEN_PRICE,
				  next_record->LT_VOL
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatLastTradeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_LAST_TRADE_LOAD_H
