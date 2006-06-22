AC_PREREQ(2.59)

dnl Process this file with autoconf to produce a configure script.
AC_INIT(dbt5, 0.1, osdldbt-general@lists.sourceforge.net)
AM_INIT_AUTOMAKE

dnl Check for programs.
AC_PROG_CXX

dnl
dnl Database related configuration.
dnl

dnl PostgreSQL
AC_ARG_WITH(postgresql,
  [AC_HELP_STRING([--with-postgresql=DIR],
      [Default on. Set to the path of the PostgreSQL's installation, or leave
          unset if the path is already in the compiler search path.])],
  [pgsql_path=$withval database_to_use="pgsql"],
  [database_to_use="pgsql"]
)

dnl Set up rules depending on what database is selected.
if test "$database_to_use" == "pgsql"; then

  dnl check for initdb
  AC_PATH_PROGS(INITDB, initdb, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$INITDB" ; then
    AC_MSG_ERROR([*** initdb is required])
  fi

  dnl check for createdb
  AC_PATH_PROGS(CREATEDB, createdb, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$CREATEDB" ; then
    AC_MSG_ERROR([*** createdb is required])
  fi

  dnl check for createuser
  AC_PATH_PROGS(CREATEUSER, createuser, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$CREATEUSER" ; then
    AC_MSG_ERROR([*** createuser is required])
  fi

  dnl check for dropdb
  AC_PATH_PROGS(DROPDB, dropdb, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$DROPDB" ; then
    AC_MSG_ERROR([*** dropdb is required])
  fi

  dnl check for pg_ctl 
  AC_PATH_PROGS(PG_CTL, pg_ctl, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$PG_CTL" ; then 
    AC_MSG_ERROR([*** pg_ctl is required])
  fi

  dnl check for psql
  AC_PATH_PROGS(PSQL, psql, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$PSQL" ; then
    AC_MSG_ERROR([*** psql is required])
  fi

  dnl check for vacuumdb
  AC_PATH_PROGS(VACUUMDB, vacuumdb, ,
    [$PATH:$pgsql_path/bin:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$VACUUMDB" ; then
    AC_MSG_ERROR([*** vacuumdb is required])
  fi

  AC_CONFIG_FILES([scripts/pgsql/pgsql_profile])
fi

AC_SUBST(database_to_use)
AC_SUBST(DBC_CORE)
AC_SUBST(DBC_FUNCTIONS)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src/Makefile])

AC_OUTPUT
