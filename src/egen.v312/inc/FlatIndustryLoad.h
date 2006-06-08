/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for INDUSTRY.
*/
#ifndef FLAT_INDUSTRY_LOAD_H
#define FLAT_INDUSTRY_LOAD_H

namespace TPCE
{

class CFlatIndustryLoad : public CFlatFileLoader <INDUSTRY_ROW>
{
public:
	CFlatIndustryLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<INDUSTRY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, IndustryRowFmt,
				  next_record->IN_ID,
				  next_record->IN_NAME,
				  next_record->IN_SC_ID
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatIndustryLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_INDUSTRY_LOAD_H
