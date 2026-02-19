#!/bin/sh

sed -i -n '/^#/p; /trust$/p' "$PGDATA/pg_hba.conf"
