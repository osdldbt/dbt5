/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*/
#ifndef DATE_TIME_H
#define DATE_TIME_H

#ifdef COMPILE_ODBC_LOAD
// ODBC headers
#include <sql.h>
#include <sqlext.h>
#include <odbcss.h>
#endif //COMPILE_ODBC_LOAD

namespace TPCE
{

// Common datetime structure.
// Identical to ODBC's TIMESTAMP_STRUCT
//
typedef struct tagTIMESTAMP_STRUCT
{
        INT16    year;
        UINT16   month;
        UINT16   day;
        UINT16   hour;
        UINT16   minute;
        UINT16   second;
        UINT32   fraction;
} TIMESTAMP_STRUCT;

class CDateTime
{
private:
	INT32		m_dayno;	// absolute day number since 1-Jan-0001, starting from zero
	INT32		m_msec;		// milliseconds from the beginning of the day
	char	*m_szText;	// text representation; only allocated if needed

	friend bool operator >(const CDateTime& l_dt, const CDateTime& r_dt);

public:
	CDateTime(void);		// current local date/time
	CDateTime(INT32 dayno);	// date as specified; time set to 0:00:00 (midnight)
	CDateTime(INT32 year, INT32 month, INT32 day);	// date as specified; time set to 0:00:00 (midnight)
	CDateTime(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);

	CDateTime(TIMESTAMP_STRUCT *ts); //date specified in the TIMESTAMP struct

	CDateTime(const CDateTime& dt);	//proper copy constructor - does not copy m_szText
	~CDateTime(void);

	void SetToCurrent(void);	// set to current local date/time
	void Set(INT32 dayno);
	void Set(INT32 year, INT32 month, INT32 day);	// date as specified; time set to 0:00:00 (midnight)
	void Set(INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec);
	void SetHMS(INT32 hour, INT32 minute, INT32 second, INT32 msec);

	inline INT32 DayNo(void) { return m_dayno; };
	inline INT32 MSec(void) { return m_msec; };
	void GetYMD(INT32* year, INT32* month, INT32* day);
	void GetYMDHMS(INT32* year, INT32* month, INT32* day, INT32* hour, INT32* minute, INT32* second, INT32* msec);
	void GetHMS(INT32* hour, INT32* minute, INT32* second, INT32* msec);

	void GetTimeStamp(TIMESTAMP_STRUCT* ts);

#ifdef COMPILE_ODBC_LOAD
	void GetDBDATETIME(DBDATETIME* dt);
#endif //COMPILE_ODBC_LOAD

	static INT32 YMDtoDayno( INT32 yr, INT32 mm, INT32 dd );
	char* ToStr( INT32 style );

	void Add(INT32 days, INT32 msec);
	void AddMinutes(INT32 Minutes);

	bool operator <(const CDateTime&);
	bool operator <=(const CDateTime&);
	// operator > is defined as an external (not in-class) operator in CDateTime.cpp
	bool operator >=(const CDateTime&);
	bool operator ==(const CDateTime&);

	// compute the difference between two DateTimes; 
	// result in seconds
	double operator -(const CDateTime& dt);
	INT32 DiffInMilliSeconds( const CDateTime& BaseTime );
	INT32 DiffInMilliSeconds( CDateTime* pBaseTime );
	// add another DateTime to this one
	CDateTime& operator += (const CDateTime& dt);
	//Proper assignment operator - does not copy szText
	CDateTime& operator = (const CDateTime& dt);

	bool IsValid( INT32 year, INT32 month, INT32 day, INT32 hour, INT32 minute, INT32 second, INT32 msec );
};

}	// namespace TPCE

#endif //DATE_TIME_H
