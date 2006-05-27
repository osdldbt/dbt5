#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
#

DIR=`dirname $0`
. ${DIR}/pgsql_profile || exit 1

# Create database
echo "Creating database..."
if [ -d ${PGDATA} ]; then
	echo "======================================="
	echo "PGData directory ${PGDATA} already exists"
	echo "Skipping initdb"
	echo "======================================="
else
	${INITDB} -D ${PGDATA} --locale=C || exit 1
fi

${SHELL} ${DIR}/start_db.sh

# Give the database a few seconds to get going
sleep 4

${CREATEDB} ${DBNAME}
${CREATELANG} plpgsql ${DBNAME}

exit 0
