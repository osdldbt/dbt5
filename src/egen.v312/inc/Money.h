/******************************************************************************
*	(c) Copyright 2005, Microsoft Corporation
*	Provided to the TPC under license.
******************************************************************************/

/******************************************************************************
*
*	Original Author:	Sergey Vasilevskiy
*
*	Contributors:		{Add your name here if you make modifications}
*
*	Description:		Money type that keeps all calculations in integer
*						number of cents. Needed for consistency of initial
*						database population.
*
******************************************************************************/

#ifndef MONEY_H
#define MONEY_H

namespace TPCE
{

class CMoney
{
	int		m_iAmountInCents;		// dollar amount * 100

	// Define binary operators when CMoney is the right operand
	//
	friend CMoney operator *(int l_i, CMoney r_m);
	friend CMoney operator *(double l_f, CMoney r_m);
	friend double operator /(double l_f, CMoney r_m);

public:
	// Default constructor - initialize to $0
	//
	CMoney()
	: m_iAmountInCents(0)
	{
	}

	// Initialize from another CMoney
	//
	CMoney(CMoney* m)
	: m_iAmountInCents(m->m_iAmountInCents)
	{
	}

	// Initialize CMoney from double
	//
	CMoney(double fAmount)
	: m_iAmountInCents((int)(100.0 * fAmount + 0.5))	// round floating-point number correctly
	{
	}

	// Return amount in integer dollars and fractional cents e.g. $123.99
	//
	double DollarAmount()
	{
		return m_iAmountInCents / 100.0;
	}

	// Define arithmetic operations on CMoney and CMoney
	//
	CMoney operator +(const CMoney& m)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents += m.m_iAmountInCents;

		return ret;
	}

	CMoney& operator +=(const CMoney& m)
	{
		m_iAmountInCents += m.m_iAmountInCents;

		return *this;
	}

	CMoney operator -(const CMoney& m)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents -= m.m_iAmountInCents;

		return ret;
	}

	CMoney& operator -=(const CMoney& m)
	{
		m_iAmountInCents -= m.m_iAmountInCents;

		return *this;
	}

	CMoney operator *(const CMoney& m)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents *= m.m_iAmountInCents;

		return ret;
	}

	CMoney operator /(const CMoney& m)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents /= m.m_iAmountInCents;

		return ret;
	}

	// Define arithmetic operations on CMoney and int
	//
	CMoney operator *(int i)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents *= i;

		return ret;
	}
	
	// Define arithmetic operations on CMoney and double
	//
	CMoney operator +(double f)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents += (int)(100.0 * f + 0.5);

		return ret;
	}

	CMoney operator -(double f)
	{
		CMoney	ret(this);

		ret.m_iAmountInCents -= (int)(100.0 * f + 0.5);

		return ret;
	}
	
	CMoney& operator -=(double f)
	{
		m_iAmountInCents -= (int)(100.0 * f + 0.5);

		return *this;
	}

	CMoney operator *(double f)
	{
		CMoney	ret(this);

		// Do a trick for correct rounding. Can't use ceil or floor functions
		// because they do not round properly (e.g. down when < 0.5, up when >= 0.5).
		//
		if (ret.m_iAmountInCents > 0)
		{
			ret.m_iAmountInCents = (int)(ret.m_iAmountInCents * f + 0.5);
		}
		else
		{
			ret.m_iAmountInCents = (int)(ret.m_iAmountInCents * f - 0.5);
		}		

		return ret;
	}

	CMoney operator /(double f)
	{
		CMoney	ret(this);
		
		if (ret.m_iAmountInCents > 0)
		{
			ret.m_iAmountInCents = (int)(ret.m_iAmountInCents / f + 0.5);
		}
		else
		{
			ret.m_iAmountInCents = (int)(ret.m_iAmountInCents / f - 0.5);
		}

		return ret;
	}

	// Assignment of a double (presumed fractional dollar amount e.g. in the form $123.89)
	//
	CMoney& operator=(double f)
	{
		m_iAmountInCents = (int)(100.0 * f + 0.5);

		return *this;
	}

	// Comparison operators
	//
	bool operator==(const CMoney& m)
	{
		return m_iAmountInCents == m.m_iAmountInCents;
	}

	bool operator>(const CMoney& m)
	{
		return m_iAmountInCents > m.m_iAmountInCents;
	}
	bool operator>=(const CMoney& m)
	{
		return m_iAmountInCents >= m.m_iAmountInCents;
	}


	bool operator<(const CMoney& m)
	{
		return m_iAmountInCents < m.m_iAmountInCents;
	}
	bool operator<=(const CMoney& m)
	{
		return m_iAmountInCents <= m.m_iAmountInCents;
	}
};

}

#endif	// #ifndef MONEY_H
