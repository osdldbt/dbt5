/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for TRADE_REQUEST.
*/
#ifndef FLAT_TRADE_REQUEST_LOAD_H
#define FLAT_TRADE_REQUEST_LOAD_H

namespace TPCE
{

class CFlatTradeRequestLoad : public CFlatFileLoader <TRADE_REQUEST_ROW>
{	
public:
	CFlatTradeRequestLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<TRADE_REQUEST_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{		
		int rc = fprintf( hOutFile, TradeRequestRowFmt,
				  next_record->TR_T_ID,			
				  next_record->TR_TT_ID,
				  next_record->TR_S_SYMB,
				  next_record->TR_QTY,
				  next_record->TR_BID_PRICE,
				  next_record->TR_CA_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatTradeRequestLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_TRADE_REQUEST_LOAD_H
