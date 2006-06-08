/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for TAXRATE.
*/
#ifndef FLAT_TAXRATE_LOAD_H
#define FLAT_TAXRATE_LOAD_H

namespace TPCE
{

class CFlatTaxrateLoad : public CFlatFileLoader <TAXRATE_ROW>
{
public:
	CFlatTaxrateLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<TAXRATE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, TaxrateRowFmt,
				  next_record->TX_ID,
				  next_record->TX_NAME,
				  next_record->TX_RATE
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatTaxrateLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_TAXRATE_LOAD_H
