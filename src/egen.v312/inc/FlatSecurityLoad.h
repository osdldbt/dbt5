/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for SECURITY.
*/
#ifndef FLAT_SECURITY_LOAD_H
#define FLAT_SECURITY_LOAD_H

namespace TPCE
{

class CFlatSecurityLoad : public CFlatFileLoader <SECURITY_ROW>
{
private:
	CDateTime	Flat_S_START_DATE;
	CDateTime	Flat_S_EXCH_DATE;
	CDateTime	Flat_S_52WK_HIGH_DATE;
	CDateTime	Flat_S_52WK_LOW_DATE;
public:
	CFlatSecurityLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<SECURITY_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_S_START_DATE = next_record->S_START_DATE;
		Flat_S_EXCH_DATE = next_record->S_EXCH_DATE;
		int rc = fprintf( hOutFile, SecurityRowFmt_1,
				  next_record->S_SYMB,
				  next_record->S_ISSUE,
				  next_record->S_ST_ID,
				  next_record->S_NAME,
				  next_record->S_EX_ID,
				  next_record->S_CO_ID,
				  next_record->S_NUM_OUT,
				  Flat_S_START_DATE.ToStr(FlatFileDateFormat),
				  Flat_S_EXCH_DATE.ToStr(FlatFileDateFormat),
				  next_record->S_PE
				);

		if( OLTP_VALUE_IS_NULL == next_record->S_52WK.iIndicator )
		{
			rc = fprintf( hOutFile, "||||" );
		}
		else
		{
			Flat_S_52WK_HIGH_DATE = next_record->S_52WK.HIGH_DATE;
			Flat_S_52WK_LOW_DATE = next_record->S_52WK.LOW_DATE;
			rc = fprintf( hOutFile, SecurityRowFmt_2,
				      next_record->S_52WK.HIGH,
				      Flat_S_52WK_HIGH_DATE.ToStr(FlatFileDateFormat),
				      next_record->S_52WK.LOW,
				      Flat_S_52WK_LOW_DATE.ToStr(FlatFileDateFormat)
				    );
		}

		rc = fprintf( hOutFile, SecurityRowFmt_3,
			      next_record->S_DIVIDEND,
			      next_record->S_YIELD
			    );

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatSecurityLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_SECURITY_LOAD_H
