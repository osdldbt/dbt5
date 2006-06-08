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
*	Description:		This file contains items that are common across timer
*						wheel, event wheel, and wheel time code.
*
******************************************************************************/

#ifndef WHEEL_H
#define WHEEL_H

//Use EGen standard types.
#include "EGenStandardTypes.h"

namespace TPCE
{


// Used to help define "infinitely far into the future"
const INT32	MaxWheelCycles = 999999999;

typedef struct TWheelConfig
{
	INT32	WheelSize;			// Total size of the wheel (based on the period and resolution)
	INT32	WheelResolution;	// Expressed in milliseconds

	TWheelConfig( INT32 Size, INT32 Resolution )
		: WheelSize( Size )
		, WheelResolution( Resolution )
	{
	};
} *PWheelConfig;


}	// namespace TPCE

#endif //WHEEL_H
