/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for EXCHANGE.
*/
#ifndef FLAT_EXCHANGE_LOAD_H
#define FLAT_EXCHANGE_LOAD_H

namespace TPCE
{

class CFlatExchangeLoad : public CFlatFileLoader <EXCHANGE_ROW>
{
public:
	CFlatExchangeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<EXCHANGE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, ExchangeRowFmt,
				  next_record->EX_ID,
				  next_record->EX_NAME,
				  next_record->EX_NUM_SYMB,
				  next_record->EX_OPEN,
				  next_record->EX_CLOSE,
				  next_record->EX_DESC,
				  next_record->EX_AD_ID
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatExchangeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_EXCHANGE_LOAD_H
