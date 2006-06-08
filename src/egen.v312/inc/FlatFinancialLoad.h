/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for FINANCIAL.
*/
#ifndef FLAT_FINANCIAL_LOAD_H
#define FLAT_FINANCIAL_LOAD_H

namespace TPCE
{

class CFlatFinancialLoad : public CFlatFileLoader <FINANCIAL_ROW>
{
private:
	CDateTime	Flat_FI_QTR_START_DATE;
public:
	CFlatFinancialLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<FINANCIAL_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_FI_QTR_START_DATE = next_record->FI_QTR_START_DATE;
		int rc = fprintf( hOutFile, FinancialRowFmt,
				  next_record->FI_CO_ID,
				  next_record->FI_YEAR,
				  next_record->FI_QTR,
				  Flat_FI_QTR_START_DATE.ToStr(FlatFileDateFormat),
				  next_record->FI_REVENUE,
				  next_record->FI_NET_EARN,
				  next_record->FI_BASIC_EPS,
				  next_record->FI_DILUT_EPS,
				  next_record->FI_MARGIN,
				  next_record->FI_INVENTORY,
				  next_record->FI_ASSETS,
				  next_record->FI_LIABILITY,
				  next_record->FI_OUT_BASIC,
				  next_record->FI_OUT_DILUT
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatFinancialLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_FINANCIAL_LOAD_H
