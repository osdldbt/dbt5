# Makefile for the Trade Status txn test
# 2006 Rilson Nascimento

CCFLAGS=-D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS #-ggdb
LDFLAGS=-L/usr/postgresql-8.1.3/src/interfaces/libpq -lpqxx -lpq
APPDEFINES=-DCOMPILE_PLSQL_FUNCTION

TXN=../transactions/

testTS : DBConnection.o TxnBaseDB.o TradeStatusDB.o TradeStatusTest.o
	g++ -o testTS TxnBaseDB.o DBConnection.o TradeStatusDB.o TradeStatusTest.o $(LDFLAGS)

DBConnection.o : $(TXN)DBConnection.cpp
	g++ $(CCFLAGS) $(APPDEFINES) -c $(TXN)DBConnection.cpp

TxnBaseDB.o : $(TXN)TxnBaseDB.cpp
	g++ $(CCFLAGS) $(APPDEFINES) -c $(TXN)TxnBaseDB.cpp

TradeStatusDB.o : $(TXN)TradeStatusDB.cpp
	g++ $(CCFLAGS) $(APPDEFINES) -c $(TXN)TradeStatusDB.cpp

TradeStatusTest.o : TradeStatusTest.cpp
	g++ $(CCFLAGS) $(APPDEFINES) -c TradeStatusTest.cpp


clean:
	rm -f DBConnection.o TxnBaseDB.o TradeStatusDB.o TradeStatusTest.o testTS
