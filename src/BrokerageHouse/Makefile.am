# Makefile
# 2006-2010 Mark Wong & Rilson Nascimento

AUTOMAKE_OPTIONS = no-dependencies

TXN=../transactions
INTF=../interfaces
EGEN=@egen_path@/src

bin_PROGRAMS = BrokerageHouseMain

BrokerageHouseMain_CPPFLAGS=-I@egen_path@/inc -I../include @DB_CFLAGS@
BrokerageHouseMain_LDFLAGS=$(DB_LDFLAGS)
BrokerageHouseMain_SOURCES = $(EGEN)/DateTime.cpp \
                             $(EGEN)/locking.cpp \
                             $(INTF)/CSocket.cpp \
                             $(INTF)/TxnHarnessSendToMarket.cpp \
                             $(TXN)/DBConnection.cpp \
                             $(TXN)/TxnBaseDB.cpp \
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
                             BrokerageHouseMain.cpp \
                             BrokerageHouse.cpp
