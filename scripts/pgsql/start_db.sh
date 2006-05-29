#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Rod Taylor & Open Source Development Lab, Inc.
#

FLAG=${1}

DIR=`dirname ${0}`
. ${DIR}/pgsql_profile || exit 1

LOGFILE="log"
USE_PG_AUTOVACUUM=0
while getopts "afp:" OPT; do
	case ${OPT} in
	a)
		USE_PG_AUTOVACUUM=1
		;;
	f)
		rm -f ${PGDATA}/postmaster.pid
		;;
	p)
		PARAMETERS=$OPTARG
		;;
	esac
done

if [ -f ${PGDATA}/postmaster.pid ]; then
	echo "Database is already started."
	exit 0
fi

# We need the sleeps just in case we start pg_ctl commands too closely
# together.  Only start pg_autovacuum if explicitly called.

sleep 1

if [ "${PARAMETERS}" = "" ]; then
	${PG_CTL} -D ${PGDATA} -l ${LOGFILE} start
else
	${PG_CTL} -D ${PGDATA} -o "${PARAMETERS}" -l ${LOGFILE} start
fi

sleep 10

if [ ${USE_PG_AUTOVACUUM} -eq 1 ]; then
	if [ -z ${PG_AUTOVACUUM} ]; then
		echo "pg_autovacuum is not installed, but this is ok if you are using 8.1"
	else
		echo Waiting for database to start before starting pg_autovacuum...
		sleep 10
		${PG_AUTOVACUUM} -D
	fi
fi

exit 0
