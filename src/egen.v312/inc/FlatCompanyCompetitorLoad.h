/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for COMPANY_COMPETITOR.
*/
#ifndef FLAT_COMPANY_COMPETITOR_LOAD_H
#define FLAT_COMPANY_COMPETITOR_LOAD_H

namespace TPCE
{

class CFlatCompanyCompetitorLoad : public CFlatFileLoader <COMPANY_COMPETITOR_ROW>
{
public:
	CFlatCompanyCompetitorLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<COMPANY_COMPETITOR_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, CompanyCompetitorRowFmt,
				  next_record->CP_CO_ID,
				  next_record->CP_COMP_CO_ID,
				  next_record->CP_IN_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCompanyCompetitorLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_COMPANY_COMPETITOR_LOAD_H
