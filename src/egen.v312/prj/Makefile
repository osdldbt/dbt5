#
# Makefile to build EGenLoader.
#
#
######## Platform specific defines
########
########include $(TPCE_ROOT)/Makefile.config
########
######### define CXX to be your c++ compiler if your system does 
######### not already define it  (you need it for AIX)
########CXX=xlC
########CCFLAGS=$(CFLAGS_OS) $(CFLAGS_EGEN) $(CFLAGS_DEBUG)
########LDFLAGS=$(LDFLAGS_EGEN)

#
# NOTE: EGen make use of PRId64 as a printf format specification for
# printing out 64-bit integers (see inc/EGenStandardTypes.h). In some
# situations (e.g. when using g++) it may be necessary to set the following
# -D__STDC_FORMAT_MACROS to get the right result from <inttypes.h>.
#

# Application specific defines
APPDEFINES=-D__STDC_FORMAT_MACROS -DCOMPILE_PGSQL_LOAD

# [Rilson] Flags for libpq and libpqxx: PostgreSQL API's
#CCFLAGS = -I/usr/local/pgsql/include -I/usr/local/pgsql/include/server -I/usr/local/pgsql/include/postgresql/server -I/usr/local/pgsql/include/pgsql/server 
PGFLAGS=-L /usr/postgresql-8.1.3/src/interfaces/libpq -lpq -lpqxx

# Directory for source files
SRC = ../src
# Directory for source files
PRJ = ../prj
# Directory for intermediate files
OBJ = ../obj
# Directory for include files
INC = ../inc
# Directory for library files
LIB = ../lib
# Directory for executable files
EXE = ../bin

VPATH=$(SRC):$(OBJ)
.SUFFIXES: .cpp
.cpp.o:
	$(CXX) $(CCFLAGS) $(APPDEFINES) -c $< -o $(OBJ)/$@

# Specify each library as a single target
# with source and include files separates into different variables.

EGenDriverCELib_src =		CE.cpp \
				CETxnMixGenerator.cpp \
				CETxnInputGenerator.cpp

EGenDriverCELib_obj =		$(EGenDriverCELib_src:.cpp=.o)


EGenDriverDMLib_src =		DM.cpp

EGenDriverDMLib_obj =		$(EGenDriverDMLib_src:.cpp=.o)


EGenDriverMEELib_src =		MEE.cpp \
				MEEPriceBoard.cpp \
				MEESecurity.cpp \
				MEETickerTape.cpp \
				MEETradingFloor.cpp \
				WheelTime.cpp

EGenDriverMEELib_obj =		$(EGenDriverMEELib_src:.cpp=.o)


EGenTables_src =		AddressTable.cpp \
				CustomerSelection.cpp \
				CustomerTable.cpp \
				InputFlatFilesStructure.cpp \
				Person.cpp \
				ReadRowFunctions.cpp \
				TradeGen.cpp

EGenTables_obj =		$(EGenTables_src:.cpp=.o)


EGenUtilities_src =		DateTime.cpp \
				error.cpp \
				Random.cpp \
				Money.cpp \
				EGenVersion.cpp


EGenUtilities_obj =		$(EGenUtilities_src:.cpp=.o)

EGenUtilities_inc =		EGenUtilities_stdafx.h \
				DateTime.h \
				FixedArray.h \
				FixedMap.h \
				Random.h \
				TableConsts.h \
				error.h


FlatFileLoader_src =		FlatFileLoader.cpp

FlatFileLoader_obj =		$(FlatFileLoader_src:.cpp=.o)


EGenGenerateAndLoad_src =	EGenGenerateAndLoad.cpp

EGenGenerateAndLoad_obj =	$(EGenGenerateAndLoad_src:.cpp=.o)


EGenLoader_src =		EGenLoader.cpp

EGenLoader_obj =		$(EGenLoader_src:.cpp=.o)


# Using pattern rules that were defined earlier. 
# All options are specified through the variables.

EGenLoader:			EGenUtilities \
				EGenDriverMEELib \
				EGenTables \
				FlatFileLoaderObj \
				EGenGenerateAndLoadObj \
				$(EGenLoader_obj)
	cd $(OBJ); \
	$(CXX) 	$(LDFLAGS) \
				$(EGenUtilities_obj) \
				$(EGenDriverMEELib_obj) \
				$(EGenTables_obj) \
				$(FlatFileLoader_obj) \
				$(EGenGenerateAndLoad_obj) \
				$(EGenLoader_obj) $(PGFLAGS)\
				-o $(EXE)/$@; \
	cd $(PRJ); \
	ls -l $(EXE)


EGenDriverLib:			EGenDriverCELib \
				EGenDriverDMLib \
				EGenDriverMEELib

EGenDriverCELib:		$(EGenDriverCELib_obj)

EGenDriverDMLib:		$(EGenDriverDMLib_obj)

EGenDriverMEELib:		$(EGenDriverMEELib_obj)

EGenUtilities:			$(EGenUtilities_obj)

EGenTables:			$(EGenTables_obj) 

FlatFileLoaderObj:		$(FlatFileLoader_obj)

EGenGenerateAndLoadObj:		$(EGenGenerateAndLoad_obj)

clean:
	cd $(OBJ); \
	rm -f			$(EGenUtilities_obj) \
				$(EGenDriverCELib_obj) \
				$(EGenDriverDMLib_obj) \
				$(EGenDriverMEELib_obj) \
				$(EGenTables_obj) \
				$(FlatFileLoader_obj) \
				$(EGenGenerateAndLoad_obj) \
				$(EGenLoader_obj); \
	cd $(EXE); \
	rm -f			EGenLoader; \
	cd $(PRJ)
