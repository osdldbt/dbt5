/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for BROKER.
*/

#ifndef FLAT_BROKER_LOAD_H
#define FLAT_BROKER_LOAD_H

namespace TPCE
{

class CFlatBrokerLoad : public CFlatFileLoader <BROKER_ROW>
{
public:
	CFlatBrokerLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<BROKER_ROW>(szFileName, FlatFileOutputMode){};

	/*
	*	Writes a record to the file.
	*/
	void WriteNextRecord(PT next_record)
	{
		int rc = fprintf( hOutFile, BrokerRowFmt,
				  next_record->B_ID,
				  next_record->B_ST_ID,
				  next_record->B_NAME,
				  next_record->B_NUM_TRADES,
				  next_record->B_COMM_TOTAL
				);

		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatBrokerLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_BROKER_LOAD_H
