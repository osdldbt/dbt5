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

#include "../inc/EGenUtilities_stdafx.h"

namespace TPCE
{

// Define * operator to make possible integer operand on the left
//
CMoney operator *(int l_i, CMoney r_m)
{
	CMoney ret;

	ret = r_m * l_i;

	return ret;
}

// Define * operator to make possible double operand on the left
//
CMoney operator *(double l_f, CMoney r_m)
{
	CMoney ret;

	ret = r_m * l_f;

	return ret;
}

// Define / operator to make possible double operand on the left
//
double operator /(double l_f, CMoney r_m)
{
	return l_f / r_m.DollarAmount();
}

}
