/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for CHARGE.
*/
#ifndef FLAT_CHARGE_LOAD_H
#define FLAT_CHARGE_LOAD_H

namespace TPCE
{

class CFlatChargeLoad : public CFlatFileLoader <CHARGE_ROW>
{
public:
	CFlatChargeLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<CHARGE_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, ChargeRowFmt,
				  next_record->CH_TT_ID,
				  next_record->CH_C_TIER,
				  next_record->CH_CHRG
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatChargeLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_CHARGE_LOAD_H
