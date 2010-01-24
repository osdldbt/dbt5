AC_PREREQ(2.59)

dnl Process this file with autoconf to produce a configure script.
AC_INIT(dbt5, 0.1, osdldbt-general@lists.sourceforge.net)
AM_INIT_AUTOMAKE

dnl Check for programs.
AC_PROG_CXX
CXXFLAGS="-g -D__STDC_FORMAT_MACROS -D__STDC_CONSTANT_MACROS"

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

AC_PATH_PROGS([RSYNC], [rsync], ,
    [$PATH:/usr/local/bin:/usr/bin:/sbin:/bin])
if test -z "$RSYNC"
then
  AC_MSG_ERROR([*** rsync not found])
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

AC_ARG_ENABLE(debug, "Build with DEBUG symbol.", [enable_debug="yes"],
    [enable_debug="no"])

DEBUG_FLAG=
if test "$enable_debug" = "yes"; then
  DEBUG_FLAG="-DDEBUG"
  CFLAGS="$CFLAGS $DEBUG_FLAG"
  CPPFLAGS="$CPPFLAGS $DEBUG_FLAG"
fi
AC_SUBST(DEBUG_FLAG)

dnl
dnl Database related configuration.
dnl

dnl PostgreSQL
AC_ARG_WITH(postgresql,
  [AC_HELP_STRING([--with-postgresql=DIR],
      [Default on. Set to the path of the PostgreSQL's installation, or leave
          unset if the path is already your search path.])],
  [pgsql_path=$withval database_to_use="pgsql"]
)

AC_ARG_WITH(pqxx,
  [AC_HELP_STRING([--with-pqxx=DIR],
      [Default on. Set to the path of the libpqxx installation, or leave
          unset if the path is already in the compiler search path.])],
  [pqxx_path=$withval]
)

AC_ARG_WITH(egen,
  [AC_HELP_STRING([--with-egen=DIR],
      [Default is to use EGen distributed with his kit.])],
  [egen_path=$withval],
  [egen_path="egen"]
)

AC_SUBST(egen_path)

dnl Set up rules depending on what database is selected.
if test "$database_to_use" = "pgsql"; then
  if test "$pgsql_path" = "yes"; then
    path_to_check="$PATH"
  else
    path_to_check="$pgsql_path/bin"
  fi

  dnl check for pkg_config
  AC_PATH_PROGS(PKG_CONFIG, pkg-config, no, [])
  if test -z "$PKG_CONFIG" ; then
    AC_MSG_ERROR([*** pkg-config is required])
  fi

  dnl check for pg_config
  AC_PATH_PROGS(PG_CONFIG, pg_config, no, [$path_to_check])
  if test -z "$PG_CONFIG" ; then
    AC_MSG_ERROR([*** pg_config is required])
  fi
  AC_SUBST(PG_CONFIG)

  dnl check for initdb
  AC_PATH_PROGS(INITDB, initdb, no, [$path_to_check])
  if test -z "$INITDB" ; then
    AC_MSG_ERROR([*** initdb is required])
  fi

  dnl check for createdb
  AC_PATH_PROGS(CREATEDB, createdb, no, [$path_to_check])
  if test -z "$CREATEDB" ; then
    AC_MSG_ERROR([*** createdb is required])
  fi

  AC_PATH_PROGS(CREATELANG, createlang, no, [$path_to_check])
  if test -z "$CREATELANG" ; then
    AC_MSG_ERROR([*** createlang is required])
  fi

  dnl check for createuser
  AC_PATH_PROGS(CREATEUSER, createuser, no, [$path_to_check])
  if test -z "$CREATEUSER" ; then
    AC_MSG_ERROR([*** createuser is required])
  fi

  dnl check for dropdb
  AC_PATH_PROGS(DROPDB, dropdb, no, [$path_to_check])
  if test -z "$DROPDB" ; then
    AC_MSG_ERROR([*** dropdb is required])
  fi

  dnl check for pg_ctl 
  AC_PATH_PROGS(PG_CTL, pg_ctl, no, [$path_to_check])
  if test -z "$PG_CTL" ; then 
    AC_MSG_ERROR([*** pg_ctl is required])
  fi

  dnl check for psql
  AC_PATH_PROGS(PSQL, psql, no, [$path_to_check])
  if test -z "$PSQL" ; then
    AC_MSG_ERROR([*** psql is required])
  fi

  dnl check for vacuumdb
  AC_PATH_PROGS(VACUUMDB, vacuumdb, no, [$path_to_check])
  if test -z "$VACUUMDB" ; then
    AC_MSG_ERROR([*** vacuumdb is required])
  fi

  if test "$PG_CONFIG" != "no"; then
    AC_MSG_CHECKING([for PostgreSQL libraries])

    BINDIR=`$PG_CONFIG --bindir`
    AC_SUBST(BINDIR)
    INCLUDEDIR=`$PG_CONFIG --includedir`
    PKGINCLUDEDIR=`$PG_CONFIG --pkgincludedir`
    INCLUDEDIR_SERVER=`$PG_CONFIG --includedir-server`

    DB_CFLAGS="-I$INCLUDEDIR -I$PKGINCLUDEDIR -I$INCLUDEDIR_SERVER"
    LIBDIR=`$PG_CONFIG --libdir`
    DB_LDFLAGS="-L$LIBDIR -Wl,-R$LIBDIR -lpq $LDFLAGS"

    POSTGRESQL_VERSION=`$PG_CONFIG --version | sed -e 's#PostgreSQL ##'`

    AC_DEFINE([HAVE_POSTGRESQL], [1],
        [Define to 1 if PostgreSQL libraries are available])

    found_postgresql="yes"
    AC_MSG_RESULT([yes])
  fi

  dnl
  dnl Find pqxx libraries
  dnl

  LIBPQXX_CFLAGS=`$PKG_CONFIG --cflags libpqxx`
  LIBPQXX_LDFLAGS=`$PKG_CONFIG --libs libpqxx`
  DB_CFLAGS="$DB_CFLAGS $LIBPQXX_CFLAGS"
  DB_LDFLAGS="$DB_LDFLAGS $LIBPQXX_LDFLAGS"
  AC_MSG_NOTICE([using libpqxx headers at $LIBPQXX_CFLAGS])
  AC_MSG_NOTICE([using libpqxx libraries at $LIBPQXX_LDFLAGS])

  STORED_PROC_TYPE="COMPILE_PLSQL_FUNCTION"
  CFLAGS="$CFLAGS $DEBUG_FLAG -DCOMPILE_PGSQL_LOAD"
fi

AC_SUBST(DB_CFLAGS)
AC_SUBST(DB_LDFLAGS)
AC_SUBST(STORED_PROC_TYPE)

AC_SUBST(database_to_use)
AC_SUBST(DBC_CORE)
AC_SUBST(DBC_FUNCTIONS)

AC_CONFIG_FILES([Makefile])
AC_CONFIG_FILES([src/Makefile])

AC_OUTPUT
