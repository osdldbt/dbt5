/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for SETTLEMENT.
*/
#ifndef FLAT_SETTLEMENT_LOAD_H
#define FLAT_SETTLEMENT_LOAD_H

namespace TPCE
{

class CFlatSettlementLoad : public CFlatFileLoader <SETTLEMENT_ROW>
{
private:
	CDateTime	Flat_SE_CASH_DUE_DATE;
public:
	CFlatSettlementLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<SETTLEMENT_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_SE_CASH_DUE_DATE = next_record->SE_CASH_DUE_DATE;
		int rc = fprintf( hOutFile, SettlementRowFmt,
				  next_record->SE_T_ID,
				  next_record->SE_CASH_TYPE,
				  Flat_SE_CASH_DUE_DATE.ToStr(FlatFileDateFormat),
				  next_record->SE_AMT
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatSettlementLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_SETTLEMENT_LOAD_H
