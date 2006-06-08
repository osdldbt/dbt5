/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for Account Permission.
*/

#ifndef FLAT_ACCOUNT_PERMISSION_LOAD_H
#define FLAT_ACCOUNT_PERMISSION_LOAD_H

namespace TPCE
{

class CFlatAccountPermissionLoad : public CFlatFileLoader <ACCOUNT_PERMISSION_ROW>
{
public:
	CFlatAccountPermissionLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode) : CFlatFileLoader<ACCOUNT_PERMISSION_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, AccountPermissionRowFmt,
				  next_record->AP_CA_ID,
				  next_record->AP_ACL,
				  next_record->AP_TAX_ID,
				  next_record->AP_L_NAME,
				  next_record->AP_F_NAME
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatAccountPermissionLoad::WriteNextRecord");
		} 
	}
};

}	// namespace TPCE

#endif //FLAT_ACCOUNT_PERMISSION_LOAD_H
