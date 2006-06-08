/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for STATUS_TYPE.
*/
#ifndef FLAT_STATUS_TYPE_LOAD_H
#define FLAT_STATUS_TYPE_LOAD_H

namespace TPCE
{

class CFlatStatusTypeLoad : public CFlatFileLoader <STATUS_TYPE_ROW>
{
public:
	CFlatStatusTypeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<STATUS_TYPE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, StatusTypeRowFmt,
				  next_record->ST_ID,
				  next_record->ST_NAME
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatStatusType::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_STATUS_TYPE_LOAD_H
