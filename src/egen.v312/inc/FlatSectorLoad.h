/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for SECTOR.
*/
#ifndef FLAT_SECTOR_LOAD_H
#define FLAT_SECTOR_LOAD_H

namespace TPCE
{

class CFlatSectorLoad : public CFlatFileLoader <SECTOR_ROW>
{
public:
	CFlatSectorLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<SECTOR_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, SectorRowFmt,
				  next_record->SC_ID,
				  next_record->SC_NAME
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatSectorLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_SECTOR_LOAD_H
