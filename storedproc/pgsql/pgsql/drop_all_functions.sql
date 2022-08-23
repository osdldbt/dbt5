/*
 * This file is released under the terms of the Artistic License.  Please see
 * the file LICENSE, included in this package, for details.
 */

-- Simple script that might help make sure there aren't overloaded functions.

DROP FUNCTION IF EXISTS BrokerVolumeFrame1;
DROP FUNCTION IF EXISTS CustomerPositionFrame1;
DROP FUNCTION IF EXISTS CustomerPositionFrame2;
DROP FUNCTION IF EXISTS DataMaintenanceFrame1;
DROP FUNCTION IF EXISTS MarketFeedFrame1;
DROP FUNCTION IF EXISTS MarketWatchFrame1;
DROP FUNCTION IF EXISTS SecurityDetailFrame1;
DROP FUNCTION IF EXISTS TradeCleanupFrame1;
DROP FUNCTION IF EXISTS TradeLookupFrame1;
DROP FUNCTION IF EXISTS TradeLookupFrame2;
DROP FUNCTION IF EXISTS TradeLookupFrame3;
DROP FUNCTION IF EXISTS TradeLookupFrame4;
DROP FUNCTION IF EXISTS TradeOrderFrame1;
DROP FUNCTION IF EXISTS TradeOrderFrame2;
DROP FUNCTION IF EXISTS TradeOrderFrame3;
DROP FUNCTION IF EXISTS TradeOrderFrame4;
DROP FUNCTION IF EXISTS TradeResultFrame1;
DROP FUNCTION IF EXISTS TradeResultFrame2;
DROP FUNCTION IF EXISTS TradeResultFrame3;
DROP FUNCTION IF EXISTS TradeResultFrame4;
DROP FUNCTION IF EXISTS TradeResultFrame5;
DROP FUNCTION IF EXISTS TradeResultFrame6;
DROP FUNCTION IF EXISTS TradeStatusFrame1;
DROP FUNCTION IF EXISTS TradeUpdateFrame1;
DROP FUNCTION IF EXISTS TradeUpdateFrame2;
DROP FUNCTION IF EXISTS TradeUpdateFrame3;

DROP TYPE IF EXISTS SECURITY_DETAIL_DAY;
DROP TYPE IF EXISTS SECURITY_DETAIL_FIN;
DROP TYPE IF EXISTS SECURITY_DETAIL_NEWS;
