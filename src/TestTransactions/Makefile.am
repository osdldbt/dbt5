# Makefile
# 2006 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

TXN=../transactions
INTF=../interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = TestTxn

TestTxn_CPPFLAGS=-I@egen_path@/inc -I../include
TestTxn_LDFLAGS=$(DB_LDFLAGS)
TestTxn_SOURCES = $(EGEN)/ReadRowFunctions.cpp \
                  $(EGEN)/BaseLogger.cpp \
                  $(EGEN)/EGenLogFormatterTab.cpp \
                  $(EGEN)/InputFlatFilesStructure.cpp \
                  $(EGEN)/error.cpp \
                  $(EGEN)/EGenVersion.cpp \
                  $(EGEN)/AddressTable.cpp \
                  $(EGEN)/CustomerTable.cpp \
                  $(EGEN)/CustomerSelection.cpp \
                  $(EGEN)/Random.cpp \
                  $(EGEN)/Person.cpp \
                  $(EGEN)/DateTime.cpp \
                  $(EGEN)/CETxnInputGenerator.cpp \
                  $(EGEN)/DM.cpp \
                  $(EGEN)/WheelTime.cpp \
                  $(EGEN)/MEESecurity.cpp \
                  $(EGEN)/MEEPriceBoard.cpp \
                  $(EGEN)/MEETickerTape.cpp \
                  $(EGEN)/MEETradingFloor.cpp \
                  $(EGEN)/MEE.cpp \
                  $(EGEN)/locking.cpp \
                  $(TXN)/@database_to_use@/DBConnection.cpp \
                  $(TXN)/TxnBaseDB.cpp \
                  $(INTF)/MEESUTtest.cpp \
                  $(INTF)/DMSUTtest.cpp \
                  $(TXN)/MarketFeedDB.cpp \
                  $(TXN)/TradeCleanupDB.cpp \
                  $(TXN)/DataMaintenanceDB.cpp \
                  $(TXN)/MarketWatchDB.cpp \
                  $(TXN)/SecurityDetailDB.cpp \
                  $(TXN)/BrokerVolumeDB.cpp \
                  $(TXN)/CustomerPositionDB.cpp \
                  $(TXN)/TradeUpdateDB.cpp \
                  $(TXN)/TradeLookupDB.cpp \
                  $(TXN)/TradeResultDB.cpp \
                  $(TXN)/TradeOrderDB.cpp \
                  $(TXN)/TradeStatusDB.cpp \
                  $(INTF)/BaseInterface.cpp \
                  $(INTF)/CESUT.cpp \
                  $(INTF)/CSocket.cpp \
                  $(INTF)/TxnHarnessSendToMarketTest.cpp \
                  $(INTF)/TxnHarnessSendToMarket.cpp \
                  ../BrokerageHouse/BrokerageHouse.cpp \
                  TestTxn.cpp
