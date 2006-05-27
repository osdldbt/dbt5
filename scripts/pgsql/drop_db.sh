#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
#

DIR=`dirname $0`
. ${DIR}/pgsql_profile || exit

# Double check we have a value for PGDATA
if [ -z ${PGDATA} ] ; then
	echo "PGDATA environment variable is unset"
	exit 1
fi

${DROPDB} ${DBNAME}
