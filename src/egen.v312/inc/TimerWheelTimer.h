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
*	Description:		Template for timers stored in the timer wheel.
*
******************************************************************************/

#ifndef TIMER_WHEEL_TIMER_H
#define TIMER_WHEEL_TIMER_H

namespace TPCE
{

template <class T, class T2>
class CTimerWheelTimer
{

private:

public:

	T*	m_pExpiryData;						//The data to be passed back
	T2*	m_pExpiryObject;					//The object on which to call the function
	void (T2::* m_pExpiryFunction) ( T* );	//The function to call at expiration

	CTimerWheelTimer( T2* pExpiryObject, void (T2::* pExpiryFunction) ( T* ), T* pExpiryData );

	~CTimerWheelTimer(void);

};	// class CTimerWheelTimer

template <class T, class T2>
CTimerWheelTimer<T,T2>::CTimerWheelTimer( T2* pExpiryObject, void (T2::* pExpiryFunction) ( T* ), T* pExpiryData )
{
	m_pExpiryData = pExpiryData;
	m_pExpiryObject = pExpiryObject;
	m_pExpiryFunction = pExpiryFunction;
}

template <class T, class T2>
CTimerWheelTimer<T,T2>::~CTimerWheelTimer(void)
{
}

}	// namespace TPCE

#endif //TIMER_WHEEL_TIMER_H
