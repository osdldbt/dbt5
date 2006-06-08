/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for COMPANY.
*/
#ifndef FLAT_COMPANY_LOAD_H
#define FLAT_COMPANY_LOAD_H

namespace TPCE
{

class CFlatCompanyLoad : public CFlatFileLoader <COMPANY_ROW>
{
private:
	CDateTime	Flat_CO_OPEN_DATE;
public:
	CFlatCompanyLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<COMPANY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_CO_OPEN_DATE = next_record->CO_OPEN_DATE;
		int rc = fprintf( hOutFile, CompanyRowFmt,
				  next_record->CO_ID,
				  next_record->CO_ST_ID,
				  next_record->CO_NAME,
				  next_record->CO_IN_ID,
				  next_record->CO_SP_RATE,
				  next_record->CO_CEO,
				  next_record->CO_AD_ID,
				  next_record->CO_DESC,
				  Flat_CO_OPEN_DATE.ToStr(FlatFileDateFormat)
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCompanyLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_COMPANY_LOAD_H
