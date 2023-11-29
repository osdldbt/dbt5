add_subdirectory (pgsql)

install (FILES BrokerVolumeDB.cpp
               CustomerPositionDB.cpp
               DataMaintenanceDB.cpp
               MarketFeedDB.cpp
               MarketWatchDB.cpp
               SecurityDetailDB.cpp
               TradeCleanupDB.cpp
               TradeLookupDB.cpp
               TradeOrderDB.cpp
               TradeResultDB.cpp
               TradeStatusDB.cpp
               TradeUpdateDB.cpp
               TxnBaseDB.cpp
         DESTINATION "share/dbt5/src/transactions")
