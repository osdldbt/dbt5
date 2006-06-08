/*	FILE: RANDOM.CPP 
 *
 *	(c) Copyright 2002-2003, Microsoft Corporation
 *	Provided to the TPC under license.
 *
 * Authors: Charles Levine, Philip Durr
 *			Microsoft Corp.
 *
 * NOTE: This class has been "significantly" updated from its original version.
 *       Initially this was a 32-bit RNG, it has been modified to now be based
 *       on a 64-bit RNG. (3/2/04 -dj.)
 */

#include "../inc/EGenUtilities_stdafx.h"

using namespace TPCE;

inline RNGSEED CRandom::UInt64Rand(void){

    UINT64 a = (UINT64) UInt64Rand_A_MULTIPLIER;
    UINT64 c = (UINT64) UInt64Rand_C_INCREMENT;
    m_seed = (m_seed * a + c); // implicitly truncated to 64bits
	
    return (m_seed);
}

RNGSEED CRandom::RndNthElement( RNGSEED nSeed, RNGSEED nCount) {
  UINT64	a = UInt64Rand_A_MULTIPLIER;
  UINT64	c = UInt64Rand_C_INCREMENT;
  int		nBit;
  UINT64	Apow = a;
  UINT64	Dsum = UInt64Rand_ONE;

  // if nothing to do, do nothing !
  if( nCount == 0 ) {
	  return nSeed;
  }

  // Recursively compute X(n) = A * X(n-1) + C
  //
  // explicitly:
  // X(n) = A^n * X(0) + { A^(n-1) + A^(n-2) + ... A + 1 } * C
  //
  // we write this as:
  // X(n) = Apow(n) * X(0) + Dsum(n) * C
  //
  // we use the following relations:
  // Apow(n) = A^(n%2)*Apow(n/2)*Apow(n/2)
  // Dsum(n) =   (n%2)*Apow(n/2)*Apow(n/2) + (Apow(n/2) + 1) * Dsum(n/2)
  //

  // first get the highest non-zero bit
  for( nBit = 0; (nCount >> nBit) != UInt64Rand_ONE ; nBit ++){}
  
  // go 1 bit at the time
  while( --nBit >= 0 ) {
    Dsum *= (Apow + 1);
    Apow = Apow * Apow;
    if( ((nCount >> nBit) % 2) == 1 ) { // odd value
      Dsum += Apow;
      Apow *= a;
    }
  }
  nSeed = nSeed * Apow + Dsum * c;
  return nSeed;
}

CRandom::CRandom(void)
{
	do
		//use portable way to get the seed
		m_seed = (RNGSEED) time(NULL);
	while (m_seed == 0);	
}

CRandom::CRandom(RNGSEED seed)
{
	m_seed = seed;
}

void CRandom::SetSeed(RNGSEED seed)
{
	m_seed = seed; 
}

// returns a random value in the range [0 .. 0.99999999999999999994578989137572]
// care should be taken in casting the result as a float because of the 
// potential loss of precision.
double CRandom::RndDouble(void)
{
	return ((double)UInt64Rand()) * (double) UInt64Rand_RECIPROCAL_2_POWER_64;
}

//return Nth element in the sequence converted to double
double CRandom::RndNthDouble(RNGSEED Seed, RNGSEED N)
{
	return ((double)RndNthElement(Seed, N)) * (double) UInt64Rand_RECIPROCAL_2_POWER_64;
}

int CRandom::RndIntRange(int min, int max)
{
	if ( min == max ) {
		return min;
	}
	// Check on system symbol for MAXINT
	// assert( max < MAXINT );
	// This assert would detect when the next line would 
	// cause an overflow.
	max++;
	if ( max <= min ) {
		return max;
	}
	
	return min + (int)(RndDouble() * (double)(max - min));
}
INT64 CRandom::RndInt64Range( INT64 min, INT64 max)
{
	if ( min == max )
		return min;
	// Check on system symbol for 64-bit MAXINT
	//assert( max < MAXINT );
	// This assert would detect when the next line would 
	// cause an overflow.
	max++;
	if ( max <= min )
		return max;
	
	return min + (INT64)(RndDouble() * (double)(max - min));
}

//return Nth element in the sequence over the integer range
int CRandom::RndNthIntRange(RNGSEED Seed, RNGSEED N, int min, int max)
{
	if ( min == max )
		return min;
	max++;
	if ( max <= min )
		return max;

	return min + (int)(RndNthDouble(Seed, N) * (double)(max - min));
}

//return Nth element in the sequence over the integer range
INT64 CRandom::RndNthInt64Range(RNGSEED Seed, RNGSEED N, INT64 min, INT64 max)
{
	if ( min == max )
		return min;
	max++;
	if ( max <= min )
		return max;

	return min + (INT64)(RndNthDouble(Seed, N) * (double)(max - min));
}

int CRandom::RndIntRangeExclude(int low, int high, int exclude)
{
	int		temp;

	temp = RndIntRange( low, high-1 );
	if (temp >= exclude)
		temp += 1;

	return temp;
}

INT64 CRandom::RndInt64RangeExclude(INT64 low, INT64 high, INT64 exclude)
{
	INT64		temp;

	temp = RndInt64Range( low, high-1 );
	if (temp >= exclude)
		temp += 1;

	return temp;
}

float CRandom::RndFloatRange(float min, float max)
{
	return min + (float) RndDouble() * (max - min);
}

double CRandom::RndDoubleRange(double min, double max)
{
	return min + RndDouble() * (max - min);
}

double CRandom::RndDoubleIncrRange(double min, double max, double incr)
{
	INT64 width = (INT64)((max - min) / incr);  // need [0..width], so no +1
	return min + ((double)RndInt64Range(0, width) * incr);
}

INT64 CRandom::NURnd( INT64 T, INT64 A, INT32 s )
{
	return ((( RndInt64Range( 1, T ) | (RndInt64Range( 0, A ) << s )) % T ) + 1 );
}


/*
*	Returns an alphanumeric string in a specified format;
*/

void CRandom::RndAlphaNumFormatted(char *szReturnString, char *szFormat)
{
	while (szFormat && *szFormat)
	{
		switch (*szFormat) 
		{
		case 'a': *szReturnString = UpperCaseLetters[ RndIntRange( 0, 25 ) ];	//only uppercase
			break;
		case 'n': *szReturnString = Numerals[ RndIntRange( 0, 9 ) ];
			break;
		default:
			*szReturnString = *szFormat;
		}

		++szFormat;
		++szReturnString;
	}
	*szReturnString = '\0';
}
