/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for HOLDING.
*/
#ifndef FLAT_HOLDING_LOAD_H
#define FLAT_HOLDING_LOAD_H

namespace TPCE
{

class CFlatHoldingLoad : public CFlatFileLoader <HOLDING_ROW>
{
private:
	CDateTime	Flat_H_DTS;
public:
	CFlatHoldingLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<HOLDING_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_H_DTS = next_record->H_DTS;
		int rc = fprintf( hOutFile, HoldingRowFmt,
				  next_record->H_T_ID,
				  next_record->H_CA_ID,
				  next_record->H_S_SYMB,
				  Flat_H_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->H_PRICE,
				  next_record->H_QTY
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatHoldingLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_HOLDING_LOAD_H
