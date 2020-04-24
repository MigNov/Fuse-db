AC_INIT([fuse-db], [0.0.1], [mignov@gmail.com])
AM_INIT_AUTOMAKE([-Wall -Werror])
AC_CHECK_LIB([fuse], [fuse_main], [], AC_MSG_ERROR([FUSE Library is missing. Cannot continue]))

CFLAGS+=" -D_FILE_OFFSET_BITS=64"
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
