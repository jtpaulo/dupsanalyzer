#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.68])
AC_INIT([FULL-PACKAGE-NAME], [VERSION], [BUG-REPORT-ADDRESS])
AC_CONFIG_HEADERS([config.h])

AM_INIT_AUTOMAKE
AM_PROG_CC_C_O

# Checks for programs.
#AC_PROG_CC
AC_PROG_MAKE_SET

# Checks for libraries.
# FIXME: Replace `main' with a function in `-ldb':
AC_CHECK_LIB([db], [main])
AC_CHECK_LIB([ssl], [main])
AC_CHECK_LIB([crypto], [main])

# Checks for header files.
AC_CHECK_HEADERS([fcntl.h inttypes.h netinet/in.h stdlib.h string.h strings.h sys/socket.h sys/time.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_PID_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T

# Checks for library functions.
AC_FUNC_FORK
AC_FUNC_MALLOC
AC_FUNC_MMAP
AC_CHECK_FUNCS([bzero gettimeofday memset])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
