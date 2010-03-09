# Makefile
# 2006-2010 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

INTF=../interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = DriverMain

DriverMain_CPPFLAGS=-I@egen_path@/inc -I../include @DB_CFLAGS@
DriverMain_LDFLAGS=$(DB_LDFLAGS)
DriverMain_SOURCES = Driver.cpp \
                     DriverMain.cpp \
                     ../Customer/Customer.cpp \
                     $(EGEN)/BaseLogger.cpp \
                     $(EGEN)/EGenLogFormatterTab.cpp \
                     $(EGEN)/CETxnMixGenerator.cpp \
                     $(EGEN)/CETxnInputGenerator.cpp \
                     $(EGEN)/ReadRowFunctions.cpp \
                     $(EGEN)/InputFlatFilesStructure.cpp \
                     $(EGEN)/error.cpp \
                     $(EGEN)/AddressTable.cpp \
                     $(EGEN)/EGenVersion.cpp \
                     $(EGEN)/CustomerTable.cpp \
                     $(EGEN)/CustomerSelection.cpp \
                     $(EGEN)/Random.cpp \
                     $(EGEN)/Person.cpp \
                     $(EGEN)/DateTime.cpp \
                     $(EGEN)/CE.cpp \
                     $(EGEN)/DM.cpp \
                     $(EGEN)/locking.cpp \
                     $(EGEN)/MEE.cpp \
                     $(EGEN)/MEETradingFloor.cpp \
                     $(EGEN)/MEETickerTape.cpp \
                     $(EGEN)/MEEPriceBoard.cpp \
                     $(EGEN)/WheelTime.cpp \
                     $(EGEN)/MEESecurity.cpp \
                     $(INTF)/CESUT.cpp \
                     $(INTF)/CSocket.cpp \
                     $(INTF)/DMSUT.cpp \
                     $(INTF)/BaseInterface.cpp
