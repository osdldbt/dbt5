#!/bin/sh

#
# This file is released under the terms of the Artistic License.
# Please see the file LICENSE, included in this package, for details.
#
# Copyright (C) 2002 Mark Wong & Open Source Development Lab, Inc.
#

DIR=`dirname $0`
. ${DIR}/pgsql_profile || exit 1

while getopts "o:" opt; do
	case $opt in
	o)
		OUTPUT=$OPTARG
		;;
	esac
done

if [ "$OUTPUT" == "" ]; then
	echo "use -o and specify the backup file"
	exit 1;
fi

_test=`$PGRESTORE -v Fc -a -d $DBNAME $OUTPUT | grep OK`
if [ "$_test" != "" ]; then
	echo "restore failed: $_test"
	exit 1
fi

exit 0
