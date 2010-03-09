# Makefile
# 2006-2010 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

INTF=../interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = MarketExchangeMain

MarketExchangeMain_CPPFLAGS=-I@egen_path@/inc \
	-I@egen_path@/TestHarness/Reference/inc -I../include @DB_CFLAGS@
MarketExchangeMain_LDFLAGS=$(DB_LDFLAGS)
MarketExchangeMain_SOURCES = $(EGEN)/ReadRowFunctions.cpp \
                           $(EGEN)/BaseLogger.cpp \
                           $(EGEN)/EGenLogFormatterTab.cpp \
                           $(EGEN)/EGenVersion.cpp \
                           $(EGEN)/error.cpp \
                           $(INTF)/CSocket.cpp \
                           $(EGEN)/Random.cpp \
                           $(EGEN)/DateTime.cpp \
                           $(EGEN)/WheelTime.cpp \
                           $(EGEN)/MEESecurity.cpp \
                           $(EGEN)/MEEPriceBoard.cpp \
                           $(EGEN)/MEETickerTape.cpp \
                           $(EGEN)/MEETradingFloor.cpp \
                           $(EGEN)/MEE.cpp \
                           $(EGEN)/locking.cpp \
                           $(EGEN)/InputFlatFilesStructure.cpp \
                           $(INTF)/MEESUT.cpp \
                           $(INTF)/BaseInterface.cpp \
                           MarketExchange.cpp \
                           MarketExchangeMain.cpp
