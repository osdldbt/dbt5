/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for CUSTOMER_TAXRATE.
*/
#ifndef FLAT_CUSTOMER_TAXRATE_LOAD_H
#define FLAT_CUSTOMER_TAXRATE_LOAD_H

namespace TPCE
{

class CFlatCustomerTaxrateLoad : public CFlatFileLoader <CUSTOMER_TAXRATE_ROW>
{
public:
	CFlatCustomerTaxrateLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<CUSTOMER_TAXRATE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, CustomerTaxrateRowFmt,
				  next_record->CX_TX_ID,
				  next_record->CX_C_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCustomerTaxrateLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_CUSTOMER_TAXRATE_LOAD_H
