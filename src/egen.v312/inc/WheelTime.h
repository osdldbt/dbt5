/******************************************************************************
*	(c) Copyright 2004, Hewlett Packard
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Doug Johnson
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		This class provides functionality for managing the 
*						relationship between time and locations in a wheel.
*
******************************************************************************/

#ifndef WHEEL_TIME_H
#define WHEEL_TIME_H

#include "EGenUtilities_stdafx.h"
#include "Wheel.h"

namespace TPCE
{

class CWheelTime
{

private:
	PWheelConfig	m_pWheelConfig;	//Pointer to configuration info for the wheel
	INT32			m_Cycles;		//Number of completed cycles so far
	INT32			m_Index;		//Index into the current cycle

public:
	CWheelTime( PWheelConfig pWheelConfig );
	CWheelTime( PWheelConfig pWheelConfig, INT32 Cycles, INT32 Index );
	CWheelTime( PWheelConfig pWheelConfig, CDateTime& Base, CDateTime& Now, INT32 Offset );
	~CWheelTime( void );

	inline INT32 Cycles( void ) { return m_Cycles; };
	inline INT32 Index( void ) { return m_Index; };

	void Add( INT32 Interval );

	INT32 Offset( const CWheelTime& Time );

	void Set( INT32 Cycles, INT32 Index );
	void Set( CDateTime& Base, CDateTime& Now );
	void Set( CDateTime* pBase, CDateTime* pNow );

	CWheelTime& operator = (const CWheelTime& Time);
	bool operator <(const CWheelTime& Time);
	CWheelTime& operator += ( const INT32& Interval );
	CWheelTime operator ++ ( INT32 );

};

}	// namespace TPCE

#endif //WHEEL_TIME_H
