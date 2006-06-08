/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for CUSTOMER.
*/

#ifndef FLAT_CUSTOMER_LOAD_H
#define FLAT_CUSTOMER_LOAD_H

namespace TPCE
{

class CFlatCustomerLoad : public CFlatFileLoader <CUSTOMER_ROW>
{
protected:
	CDateTime	Flat_C_DOB;
public:
	CFlatCustomerLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<CUSTOMER_ROW>(szFileName, FlatFileOutputMode) {};

	/*
	*	Writes a record to the file.
	*/
	virtual void WriteNextRecord(PT next_record)
	{
		Flat_C_DOB = next_record->C_DOB;
		int rc = fprintf( hOutFile, CustomerRowFmt,
				  next_record->C_ID,
				  next_record->C_TAX_ID,
				  next_record->C_ST_ID,
				  next_record->C_L_NAME,
				  next_record->C_F_NAME,
				  next_record->C_M_NAME,
				  next_record->C_GNDR,
				  (int)next_record->C_TIER,
				  Flat_C_DOB.ToStr(FlatFileDateFormat),
				  next_record->C_AD_ID,
				  next_record->C_CTRY_1,
				  next_record->C_AREA_1,
				  next_record->C_LOCAL_1,
				  next_record->C_EXT_1,
				  next_record->C_CTRY_2,
				  next_record->C_AREA_2,
				  next_record->C_LOCAL_2,
				  next_record->C_EXT_2,
				  next_record->C_CTRY_3,
				  next_record->C_AREA_3,
				  next_record->C_LOCAL_3,
				  next_record->C_EXT_3,
				  next_record->C_EMAIL_1,
				  next_record->C_EMAIL_2
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatCustomerLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_CUSTOMER_LOAD_H
