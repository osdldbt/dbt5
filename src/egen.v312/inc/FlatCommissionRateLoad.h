/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for COMMISSIOIN_RATE.
*/
#ifndef FLAT_COMMISSION_RATE_LOAD_H
#define FLAT_COMMISSION_RATE_LOAD_H

namespace TPCE
{

class CFlatCommissionRateLoad : public CFlatFileLoader <COMMISSION_RATE_ROW>
{
public:
	CFlatCommissionRateLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<COMMISSION_RATE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, CommissionRateRowFmt,
				  next_record->CR_C_TIER,
				  next_record->CR_TT_ID,
				  next_record->CR_EX_ID,
				  next_record->CR_FROM_QTY,
				  next_record->CR_TO_QTY,
				  next_record->CR_RATE
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCommissionRateLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_COMMISSION_RATE_LOAD_H
