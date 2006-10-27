AC_PREREQ(2.59)

dnl Process this file with autoconf to produce a configure script.
AC_INIT(dbt5, 0.1, osdldbt-general@lists.sourceforge.net)
AM_INIT_AUTOMAKE

dnl Check for programs.
AC_PROG_CXX
CXXFLAGS="-D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS"

dnl Replace below with this in future release AC_PROG_EGREP
AC_PATH_PROGS([EGREP], ['grep -E'], ,
    [$PATH:/usr/local/bin:/usr/bin:/usr/X11R6/bin])
if test -z "$EGREP"
then
  AC_MSG_ERROR([*** grep -E is required])
fi

AC_PATH_PROGS([VMSTAT], [vmstat], ,
    [$PATH:/usr/local/bin:/usr/bin:/usr/X11R6/bin])
if test -z "$VMSTAT"
then
  AC_MSG_WARN([*** vmstat not found some scripts may not be usable])
fi

AC_PATH_PROGS([IOSTAT], [iostat], ,
    [$PATH:/usr/local/bin:/usr/bin:/usr/X11R6/bin])
if test -z "$IOSTAT"
then
  AC_MSG_WARN([*** iostat not found some scripts may not be usable])
fi

AC_PATH_PROGS([GNUPLOT], [gnuplot], ,
    [$PATH:/usr/local/bin:/usr/bin:/usr/X11R6/bin])
if test -z "$GNUPLOT"
then
  AC_MSG_WARN([*** gnuplot not found some scripts may not be usable])
fi

AC_PATH_PROGS([SAR], [sar bsdsar], ,
             [$PATH:/usr/local/bin:/usr/bin])
if test -z "$SAR"
then
  AC_MSG_WARN([*** sar or bsdsar not found some scripts may not be usable])
fi

AC_PATH_PROGS([SYSCTL], [sysctl], ,
    [$PATH:/usr/local/bin:/usr/bin:/sbin:/bin])
if test -z "$SYSCTL"
then
  AC_MSG_WARN([*** sysctl not found some scripts may not be usable])
fi

dnl
dnl Thread Library (required)
dnl check for `pthread_create' in -lpthread
dnl
AC_CHECK_HEADER(semaphore.h, [], [
    AC_MSG_ERROR([could not locate sempahore headers)])
])
AC_CHECK_HEADER(pthread.h, [], [
    AC_MSG_ERROR([could not locate pthread headers)])
])
AC_CHECK_LIB(pthread, pthread_create)
if test ".`echo $LIBS | grep pthread`" != .; then
dnl
dnl check for `sem_init' in -lposix4
dnl
  AC_CHECK_LIB(posix4, sem_init)
else
  AC_CHECK_LIB(pthreads, pthread_create)
  if test ".`echo $LIBS | grep pthread`" != .; then
dnl
dnl check for `sem_init' in -lposix4
dnl
    AC_CHECK_LIB(posix4, sem_init)
  else
    AC_CHECK_LIB(c_r, pthread_create)
    if test ".`echo $LIBS | grep c_r`" == .; then
      threaded=no;
    fi
  fi
fi
if test "$threaded" == "no" ; then
  AC_MSG_ERROR([Multithread support not available.])
fi

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

AC_ARG_WITH(egen,
  [AC_HELP_STRING([--with-egen=DIR],
      [Default is to use EGen distributed with his kit.])],
  [egen_path=$withval],
  [egen_path="EGen_v3.14"]
)

AC_SUBST(egen_path)

dnl Set up rules depending on what database is selected.
if test "$database_to_use" == "pgsql"; then

  dnl check for initdb
  AC_PATH_PROGS(INITDB, initdb, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$INITDB" ; then
    AC_MSG_ERROR([*** initdb is required])
  fi

  dnl check for createdb
  AC_PATH_PROGS(CREATEDB, createdb, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$CREATEDB" ; then
    AC_MSG_ERROR([*** createdb is required])
  fi

  AC_PATH_PROGS(CREATELANG, createlang, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$CREATELANG" ; then
    AC_MSG_ERROR([*** createlang is required])
  fi

  dnl check for createuser
  AC_PATH_PROGS(CREATEUSER, createuser, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$CREATEUSER" ; then
    AC_MSG_ERROR([*** createuser is required])
  fi

  dnl check for dropdb
  AC_PATH_PROGS(DROPDB, dropdb, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$DROPDB" ; then
    AC_MSG_ERROR([*** dropdb is required])
  fi

  dnl check for pg_ctl 
  AC_PATH_PROGS(PG_CTL, pg_ctl, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$PG_CTL" ; then 
    AC_MSG_ERROR([*** pg_ctl is required])
  fi

  dnl check for psql
  AC_PATH_PROGS(PSQL, psql, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$PSQL" ; then
    AC_MSG_ERROR([*** psql is required])
  fi

  dnl check for vacuumdb
  AC_PATH_PROGS(VACUUMDB, vacuumdb, ,
    [$pgsql_path/bin:$PATH:/bin:/usr/bin:/usr/local/bin]
  )
  if test -z "$VACUUMDB" ; then
    AC_MSG_ERROR([*** vacuumdb is required])
  fi

  AC_CONFIG_FILES([scripts/pgsql/build-db],
	[chmod +x scripts/pgsql/build-db])
  AC_CONFIG_FILES([scripts/pgsql/create-db],
	[chmod +x scripts/pgsql/create-db])
  AC_CONFIG_FILES([scripts/pgsql/create-indexes],
	[chmod +x scripts/pgsql/create-indexes])
  AC_CONFIG_FILES([scripts/pgsql/create-tables],
	[chmod +x scripts/pgsql/create-tables])
  AC_CONFIG_FILES([scripts/pgsql/db_stat.sh],
	[chmod +x scripts/pgsql/db_stat.sh])
  AC_CONFIG_FILES([scripts/pgsql/drop-db],
	[chmod +x scripts/pgsql/drop-db])
  AC_CONFIG_FILES([scripts/pgsql/load-stored-procs],
	[chmod +x scripts/pgsql/load-stored-procs])
  AC_CONFIG_FILES([scripts/pgsql/pgsql_profile])

  dnl
  dnl Find pqxx libraries
  dnl

  AC_PATH_PROGS(PQXX_CONFIG, pqxx-config)
  if test -z "$PQXX_CONFIG" || test ! -r "$PQXX_CONFIG"; then
	  AC_MSG_ERROR([
Libpqxx configuration script pqxx-config not found.  Make sure this is in your
command path before configuring.  Without it, the configure script has no way to
find the right location for the libpqxx library.])
  fi

  DB_CFLAGS=`$PQXX_CONFIG --cflags`
  DB_LDFLAGS=`$PQXX_CONFIG --libs`
  AC_MSG_NOTICE([using libpqxx headers at $DB_CFLAGS])
  AC_MSG_NOTICE([using libpqxx libraries at $DB_LDFLAGS])

  STORED_PROC_TYPE="COMPILE_PLSQL_FUNCTION"
fi

AC_SUBST(DB_CFLAGS)
AC_SUBST(DB_LDFLAGS)
AC_SUBST(STORED_PROC_TYPE)

AC_SUBST(database_to_use)
AC_SUBST(DBC_CORE)
AC_SUBST(DBC_FUNCTIONS)

AC_CONFIG_FILES([scripts/run_workload.sh],
	[chmod +x scripts/run_workload.sh])
AC_CONFIG_FILES([scripts/transform_sar.sh],
	[chmod +x scripts/transform_sar.sh])
AC_CONFIG_FILES([scripts/sysstats.sh],
 	[chmod +x scripts/sysstats.sh])
AC_CONFIG_FILES([scripts/Linux/get_os_info.sh],
	[chmod +x scripts/Linux/get_os_info.sh])
AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([scripts/dbt5_profile])
AC_CONFIG_FILES([src/Makefile])

AC_OUTPUT
