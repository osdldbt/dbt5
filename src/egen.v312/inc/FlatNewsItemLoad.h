/*
*	Written by Doug Johnson, Hewlett-Packard Corporation, for the TPC.
*/

/*
*	Flat file loader for NEWS_ITEM.
*/
#ifndef FLAT_NEWS_ITEM_LOAD_H
#define FLAT_NEWS_ITEM_LOAD_H

namespace TPCE
{

class CFlatNewsItemLoad : public CFlatFileLoader <NEWS_ITEM_ROW>
{
protected:
	CDateTime	Flat_NI_DTS;
public:
	CFlatNewsItemLoad( char *szFileName, FlatFileOutputModes FlatFileOutputMode ) : CFlatFileLoader<NEWS_ITEM_ROW>(szFileName, FlatFileOutputMode) {};

	/*
	*	Writes a record to the file.
	*/
	virtual void WriteNextRecord(PT next_record)
	{
		Flat_NI_DTS = next_record->NI_DTS;
		int rc = fprintf( hOutFile, NewsItemRowFmt,
				  next_record->NI_ID,
				  next_record->NI_HEADLINE,
				  next_record->NI_SUMMARY,
				  next_record->NI_ITEM,
				  Flat_NI_DTS.ToStr(FlatFileDateTimeFormat),
				  next_record->NI_SOURCE,
				  next_record->NI_AUTHOR
				);
		if (rc < 0) {
			throw new CSystemErr(CSystemErr::eWriteFile, "CFlatNewsItemLoad::WriteNextRecord");
		}
	}
};

}	// namespace TPCE

#endif //FLAT_NEWS_ITEM_LOAD_H
