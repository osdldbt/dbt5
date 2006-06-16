#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
#
# 2006 Rilson Nascimento

DIR=`dirname $0`
. ${DIR}/pgsql_profile || exit 1

# Trade Status Txn frames
${PSQL} -e -d ${DBNAME} -f $TOP_DIR/storedproc/pgsql/TradeStatusFrame1.sql || exit 1


