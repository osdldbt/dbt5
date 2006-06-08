/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for TRADE_TYPE.
*/
#ifndef FLAT_TRADE_TYPE_LOAD_H
#define FLAT_TRADE_TYPE_LOAD_H

namespace TPCE
{

class CFlatTradeTypeLoad : public CFlatFileLoader <TRADE_TYPE_ROW>
{
public:
	CFlatTradeTypeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<TRADE_TYPE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, TradeTypeRowFmt,
				  next_record->TT_ID,
				  next_record->TT_NAME,
				  next_record->TT_IS_SELL,
				  next_record->TT_IS_MRKT
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatTradeTypeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_TRADE_TYPE_LOAD_H
