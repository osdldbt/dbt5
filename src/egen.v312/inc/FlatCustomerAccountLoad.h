/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for CUSTOMER_ACCOUNT.
*/
#ifndef FLAT_CUSTOMER_ACCOUNT_LOAD_H
#define FLAT_CUSTOMER_ACCOUNT_LOAD_H

namespace TPCE
{

class CFlatCustomerAccountLoad : public CFlatFileLoader <CUSTOMER_ACCOUNT_ROW>
{
public:
	CFlatCustomerAccountLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<CUSTOMER_ACCOUNT_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, CustomerAccountRowFmt,
				  next_record->CA_ID,
				  next_record->CA_B_ID,
				  next_record->CA_C_ID,
				  next_record->CA_NAME,
				  (int)next_record->CA_TAX_ST,
				  next_record->CA_BAL
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCustomerAccountLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_CUSTOMER_ACCOUNT_LOAD_H
