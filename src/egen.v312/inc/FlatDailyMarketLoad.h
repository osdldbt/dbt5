/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for DAILY_MARKET.
*/
#ifndef FLAT_DAILY_MARKET_LOAD_H
#define FLAT_DAILY_MARKET_LOAD_H

namespace TPCE
{

class CFlatDailyMarketLoad : public CFlatFileLoader <DAILY_MARKET_ROW>
{
private:
	CDateTime	Flat_DM_DATE;
public:
	CFlatDailyMarketLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<DAILY_MARKET_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		Flat_DM_DATE = next_record->DM_DATE;
		int rc = fprintf( hOutFile, DailyMarketRowFmt,
				  Flat_DM_DATE.ToStr(FlatFileDateFormat),
				  next_record->DM_S_SYMB,
				  next_record->DM_CLOSE,
				  next_record->DM_HIGH,
				  next_record->DM_LOW,
				  next_record->DM_VOL
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatDailyMarketLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_DAILY_MARKET_LOAD_H
