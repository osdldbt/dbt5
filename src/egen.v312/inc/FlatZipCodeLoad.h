/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for ZIP_CODE.
*/
#ifndef FLAT_ZIP_CODE_LOAD_H
#define FLAT_ZIP_CODE_LOAD_H

namespace TPCE
{

class CFlatZipCodeLoad : public CFlatFileLoader <ZIP_CODE_ROW>
{
public:
	CFlatZipCodeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<ZIP_CODE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, ZipCodeRowFmt,
				  next_record->ZC_CODE,
				  next_record->ZC_TOWN,
				  next_record->ZC_DIV
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatZipCodeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_ZIP_CODE_LOAD_H
