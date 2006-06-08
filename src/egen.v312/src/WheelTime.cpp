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
*	Description:		Implementation of the CWheelTime class.
*						See WheelTime.h for a high level description.
*
******************************************************************************/

#include "../inc/WheelTime.h"

using namespace TPCE;

CWheelTime::CWheelTime( PWheelConfig pWheelConfig )
	: m_pWheelConfig( pWheelConfig )
	, m_Cycles( 0 )
	, m_Index( 0 )
{
}

CWheelTime::CWheelTime( PWheelConfig pWheelConfig, INT32 Cycles, INT32 Index )
	: m_pWheelConfig( pWheelConfig )
	, m_Cycles( Cycles )
	, m_Index( Index )
{
}

CWheelTime::CWheelTime( PWheelConfig pWheelConfig, CDateTime& Base, CDateTime& Now, INT32 Offset )
	: m_pWheelConfig( pWheelConfig )
{
	Set( Base , Now );
	Add( Offset );
}

CWheelTime::~CWheelTime( void )
{
}

void CWheelTime::Add( INT32 Interval )
{
	//DJ - should throw error if Interval >= m_pWheelConfig->WheelSize?
	m_Cycles += Interval / m_pWheelConfig->WheelSize;
	m_Index += Interval % m_pWheelConfig->WheelSize;
	if( m_Index >= m_pWheelConfig->WheelSize )
	{
		//Handle wrapping in the wheel - assume we don't allow multi-cycle intervals
		m_Cycles++;
		m_Index -= m_pWheelConfig->WheelSize;
	}
}

INT32 CWheelTime::Offset( const CWheelTime& Time )
{
	INT32	Interval;

	Interval = ( m_Cycles - Time.m_Cycles ) * m_pWheelConfig->WheelSize;
	Interval += ( m_Index - Time.m_Index );
	return( Interval );
}

void CWheelTime::Set( INT32 Cycles, INT32 Index )
{
	m_Cycles = Cycles;
	m_Index = Index;	//DJ - should throw error if Index >= m_pWheelConfig->WheelSize
}

// Set is overloaded. This version is used by the timer wheel.
void CWheelTime::Set( CDateTime& Base, CDateTime& Now )
{
	INT32		Offset;	//offset from BaseTime in milliseconds

	//DJ - If Now < Base, then we should probably throw an exception

	Offset = Now.DiffInMilliSeconds( Base ) / m_pWheelConfig->WheelResolution; // convert based on wheel resolution
	m_Cycles = Offset / m_pWheelConfig->WheelSize;
	m_Index = Offset % m_pWheelConfig->WheelSize;
}

// Set is overloaded. This version is used by the event wheel.
void CWheelTime::Set( CDateTime* pBase, CDateTime* pNow )
{
	INT32		Offset;	//offset from BaseTime in milliseconds

	//DJ - If Now < Base, then we should probably throw an exception

	Offset = pNow->DiffInMilliSeconds( pBase ) / m_pWheelConfig->WheelResolution; // convert based on wheel resolution
	m_Cycles = Offset / m_pWheelConfig->WheelSize;
	m_Index = Offset % m_pWheelConfig->WheelSize;
}

bool CWheelTime::operator <(const CWheelTime& Time)
{
	return ( m_Cycles == Time.m_Cycles ) ? ( m_Index < Time.m_Index ) : ( m_Cycles < Time.m_Cycles );
}

CWheelTime& CWheelTime::operator = (const CWheelTime& Time)
{
	m_pWheelConfig = Time.m_pWheelConfig;
	m_Cycles = Time.m_Cycles;
	m_Index = Time.m_Index;

	return *this;
}

CWheelTime& CWheelTime::operator += ( const INT32& Interval )
{
	Add( Interval );
	return *this;
}

CWheelTime CWheelTime::operator ++ ( INT32 )
{
	Add( 1 );
	return *this;
}
