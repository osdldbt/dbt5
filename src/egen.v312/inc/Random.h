/*
*	(c) Copyright 2002-2003, Microsoft Corporation
*	Provided to the TPC under license.
*/

#ifndef RANDOM_H
#define RANDOM_H

namespace TPCE
{

// Define platform specific syntax for constants.
#ifdef WIN32
	#define UInt64Rand_A_MULTIPLIER (6364136223846793005uI64)
	#define UInt64Rand_C_INCREMENT  (1uI64)
	#define UInt64Rand_ONE (1uI64)
#endif // WIN32

// Define platform specific syntax for constants.
#if defined (__unix) || (_AIX)
	#define UInt64Rand_A_MULTIPLIER (6364136223846793005ull)
	#define UInt64Rand_C_INCREMENT  (1ull)
	#define UInt64Rand_ONE (1ull)
#endif // (__unix) || (_AIX)

// Independent RNG seed type.
typedef UINT64	RNGSEED;

// For efficiency, use a constant for 1/2^64.
#define UInt64Rand_RECIPROCAL_2_POWER_64 (5.421010862427522e-20)



class CRandom
{
private:	
	RNGSEED m_seed;
	inline RNGSEED UInt64Rand(void);

public:
	CRandom(void);
	CRandom(RNGSEED seed);
	~CRandom(void) { return; };

	void SetSeed(RNGSEED seed);
	inline RNGSEED GetSeed(void) { return m_seed; };
	RNGSEED RndNthElement( RNGSEED nSeed, RNGSEED nCount);

	// returns a random value in the range [0 .. 0.99999999999999999994578989137572]
	// care should be taken in casting the result as a float because of the 
	// potential loss of precision.
	double RndDouble(void);

	//return Nth element in the sequence converted to double
	double RndNthDouble(RNGSEED Seed, RNGSEED N);

	// returns a random integer value in the range [min .. max]
	int RndIntRange(int min, int max);

	// returns a random 64-bit integer value in the range [min .. max]
	INT64 RndInt64Range(INT64 min, INT64 max);

	// returns a random integer value in the range [low .. high] excluding the value (exclude)
	INT64 RndInt64RangeExclude(INT64 low, INT64 high, INT64 exclude);

	//return Nth element in the sequence over the integer range
	int RndNthIntRange(RNGSEED Seed, RNGSEED N, int min, int max);

	//return Nth element in the sequence over the integer range
	INT64 RndNthInt64Range(RNGSEED Seed, RNGSEED N, INT64 min, INT64 max);
	
	// returns a random integer value in the range [low .. high] excluding the value (exclude)
	int RndIntRangeExclude(int low, int high, int exclude);

	// returns a random float value in the range of [min .. max]
	float RndFloatRange(float min, float max);

	// returns a random double value in the range of [min .. max]
	double RndDoubleRange(double min, double max);

	// returns a random double value in the range of [min .. max] with incr precision
	double RndDoubleIncrRange(double min, double max, double incr);

	// returns TRUE or FALSE, with the chance of TRUE being as specified by (percent)
	inline bool RndPercent(int percent)
		{ return (RndIntRange(1, 100) <= percent); };

	// Returns a random integer percentage (i.e. whole number between 1 and 100, inclusive)
	inline int RndGenerateIntegerPercentage( )
	{
		return( RndIntRange( 1, 100 ));
	}

	// Returns a non-uniform random 64-bit integer in range of [1 .. T]
	INT64 NURnd( INT64 T, INT64 A, INT32 s );

	//Returns random alphanumeric string obeying a specific format.
	//For the format: n - given character must be numeric
	//				  a - given character must be alphabetical
	//Example: "nnnaannnnaannn"
	void RndAlphaNumFormatted(char *szReturnString, char *szFormat);

};

}	// namespace TPCE

#endif // RANDOM_H
