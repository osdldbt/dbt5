/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for CASH_TRANSACTION.
*/
#ifndef FLAT_CASH_TRANSACTION_LOAD_H
#define FLAT_CASH_TRANSACTION_LOAD_H

namespace TPCE
{

class CFlatCashTransactionLoad : public CFlatFileLoader <CASH_TRANSACTION_ROW>
{
private:
	CDateTime	Flat_CT_DTS;
public:
	CFlatCashTransactionLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<CASH_TRANSACTION_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_CT_DTS = next_record->CT_DTS;
		int rc = fprintf( hOutFile, CashTransactionRowFmt,
				  next_record->CT_T_ID,
				  Flat_CT_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->CT_AMT,
				  next_record->CT_NAME
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCashTransactionLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_CASH_TRANSACTION_LOAD_H
