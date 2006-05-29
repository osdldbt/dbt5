#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Rod Taylor & Open Source Development Lab, Inc.
#

DIR=`dirname ${0}`
. ${DIR}/pgsql_profile || exit 1

# We only need to stop the database if it's running.
if [ -f ${PGDATA}/postmaster.pid ]; then
	killall ${PG_AUTOVACUUM} > /dev/null 2>&1
	sleep 1
	${PG_CTL} -D ${PGDATA} stop ${1}
	sleep 1
fi
