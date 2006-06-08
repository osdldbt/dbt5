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
*	Description:		Template for a Timer Wheel. This allows for efficient
*						handling of timers that have a known maximum period.
*
******************************************************************************/

#ifndef TIMER_WHEEL_H
#define TIMER_WHEEL_H

#include "EGenUtilities_stdafx.h"
#include "MiscConsts.h"
#include "Wheel.h"
#include "WheelTime.h"
#include "TimerWheelTimer.h"

namespace TPCE
{

template < class T, class T2, INT32 Period, INT32 Resolution >
class CTimerWheel
{

private:
	CDateTime							m_BaseTime;

	CWheelTime							m_LastTime;
	CWheelTime							m_CurrentTime;
	CWheelTime							m_NextTime;

	TWheelConfig						m_WheelConfig;

	list< CTimerWheelTimer< T, T2 >* >	m_TimerWheel[ ( Period * ( MsPerSecond / Resolution )) ];

	INT32								m_NumberOfTimers;

	INT32	ExpiryProcessing( void );
	void	ProcessTimerList( list< CTimerWheelTimer< T, T2 >* >* pList );
	INT32	SetNextTime( void );

public:
	static const INT32	NO_OUTSTANDING_TIMERS = -1;

	CTimerWheel( );
	~CTimerWheel( void );

	bool	Empty( void );
	INT32	ProcessExpiredTimers( void );
	INT32	StartTimer( double Offset, T2* pExpiryObject, void (T2::* pExpiryFunction) ( T* ), T* pExpiryData );

};	// class CTimerWheel

template < class T, class T2, INT32 Period, INT32 Resolution >
CTimerWheel< T,  T2, Period, Resolution >::CTimerWheel( )
	: m_WheelConfig(( Period * ( MsPerSecond / Resolution )), Resolution )
	, m_BaseTime()
	, m_LastTime( &m_WheelConfig, 0, 0 )
	, m_CurrentTime( &m_WheelConfig, 0, 0 )
	, m_NextTime( &m_WheelConfig, MaxWheelCycles, ( Period * ( MsPerSecond / Resolution )) - 1 )
	, m_NumberOfTimers( 0 )
{
	m_BaseTime.SetToCurrent();
}

template < class T, class T2, INT32 Period, INT32 Resolution >
CTimerWheel< T,  T2, Period, Resolution >::~CTimerWheel( void )
{
	typename list< CTimerWheelTimer<T, T2>* >::iterator	ExpiredTimer;

	for( INT32 ii=0; ii < ( Period * ( MsPerSecond / Resolution )); ii++ )
	{
		if( ! m_TimerWheel[ii].empty() )
		{
			ExpiredTimer = m_TimerWheel[ii].begin();
			while( ExpiredTimer != m_TimerWheel[ii].end() )
			{
				delete *ExpiredTimer;
				m_NumberOfTimers--;
				ExpiredTimer++;
			}
			m_TimerWheel[ii].clear();
		}
	}
}

template < class T, class T2, INT32 Period, INT32 Resolution >
bool CTimerWheel< T,  T2, Period, Resolution >::Empty( void )
{
	return( m_NumberOfTimers == 0 ? true : false );
}

template < class T, class T2, INT32 Period, INT32 Resolution >
INT32 CTimerWheel< T,  T2, Period, Resolution >::StartTimer( double Offset, T2* pExpiryObject, void (T2::* pExpiryFunction) ( T* ), T* pExpiryData )
{
	CDateTime					Now;
	CWheelTime					RequestedTime( &m_WheelConfig , m_BaseTime, Now, (INT32) (Offset * ( MsPerSecond / Resolution )));
	CTimerWheelTimer<T, T2>*	pNewTimer = new CTimerWheelTimer<T, T2>( pExpiryObject, pExpiryFunction, pExpiryData );
	
	//Update current wheel position
	m_CurrentTime.Set( m_BaseTime, Now );

	// Since the current time has been updated, we should make sure
	// any outstanding timers have been processed before proceeding.
	ExpiryProcessing();

	m_TimerWheel[ RequestedTime.Index() ].push_back( pNewTimer );
	m_NumberOfTimers++;

	if( RequestedTime < m_NextTime )
	{
		m_NextTime = RequestedTime;
	}

	return( m_NextTime.Offset( m_CurrentTime ));
}

template < class T, class T2, INT32 Period, INT32 Resolution >
INT32 CTimerWheel< T,  T2, Period, Resolution >::ProcessExpiredTimers( void )
{
	CDateTime	Now;

	// Update current wheel position
	m_CurrentTime.Set( m_BaseTime, Now );

	return( ExpiryProcessing() );
}

template < class T, class T2, INT32 Period, INT32 Resolution >
INT32 CTimerWheel< T,  T2, Period, Resolution >::ExpiryProcessing( void )
{
	while( m_LastTime < m_CurrentTime )
	{
		m_LastTime++;
		if( ! m_TimerWheel[ m_LastTime.Index() ].empty() )
		{
			ProcessTimerList( &m_TimerWheel[ m_LastTime.Index() ] );
		}
	}
	return( SetNextTime() );
}

template < class T, class T2, INT32 Period, INT32 Resolution >
void CTimerWheel< T,  T2, Period, Resolution >::ProcessTimerList( list< CTimerWheelTimer<T,T2>* >* pList )
{
	typename list< CTimerWheelTimer<T,T2>* >::iterator	ExpiredTimer;


	ExpiredTimer = pList->begin();
	while( ExpiredTimer != pList->end() )
	{
		(((*ExpiredTimer)->m_pExpiryObject)->*((*ExpiredTimer)->m_pExpiryFunction)) ( (*ExpiredTimer)->m_pExpiryData );
		delete *ExpiredTimer;
		m_NumberOfTimers--;
		ExpiredTimer++;
	}
	pList->clear();
}

template < class T, class T2, INT32 Period, INT32 Resolution >
INT32 CTimerWheel< T,  T2, Period, Resolution >::SetNextTime( void )
{
	if( 0 == m_NumberOfTimers )
	{
		m_NextTime.Set( MaxWheelCycles, ( Period * ( MsPerSecond / Resolution )) - 1 );
		return( NO_OUTSTANDING_TIMERS );
	}
	else
	{
		m_NextTime = m_CurrentTime;
		while( m_TimerWheel[ m_NextTime.Index() ].empty() )
		{
			m_NextTime++;
		}
		return( m_NextTime.Offset( m_CurrentTime ));
	}
}

}	// namespace TPCE

#endif //TIMER_WHEEL_H
