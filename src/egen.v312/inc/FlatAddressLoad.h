/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for Address.
*/

#ifndef FLAT_ADDRESS_LOAD_H
#define FLAT_ADDRESS_LOAD_H

namespace TPCE
{

class CFlatAddressLoad : public CFlatFileLoader <ADDRESS_ROW>
{
public:
	CFlatAddressLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<ADDRESS_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, AddressRowFmt,
				  next_record->AD_ID,
				  next_record->AD_LINE1,
				  next_record->AD_LINE2,
				  next_record->AD_ZC_CODE,
				  next_record->AD_CTRY
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatAddressLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_ADDRESS_LOAD_H
