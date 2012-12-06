#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.


AC_PREREQ([2.63])
AC_INIT([octopus],[2.0],[mon@unformed.ru])
AC_CONFIG_SRCDIR([src/octopus.m])
AC_CONFIG_HEADERS([include/config.h])
AC_CONFIG_FILES([Makefile scripts/gen-lua-registry.sh])

# We use same compiler to compile both C and Objective C sources.
# Due to limited support of Objective C in autoconf, pretend that we are
# configuring a C compiler.
AS_IF([test -n "$OBJC"], AC_MSG_ERROR([Please use CC to define compiler.]))

AC_USE_SYSTEM_EXTENSIONS

m4_include([third_party/libev/libev.m4])

# Checks for programs.
AC_PROG_CC
AC_PROG_MAKE_SET
AC_PROG_SED
AC_PROG_RANLIB

AC_ARG_VAR([RAGEL], [Ragel generator command])
AC_ARG_VAR([RAGELFLAGS], [Ragel generator flags])
AC_CHECK_PROG([RAGEL], [ragel], [ragel])
AS_IF([test -z "$RAGEL"],
  [AC_MSG_CHECKING([whether we need ragel to regenerate sources])
  AS_IF([test -f "${srcdir}/src/admin.m"], [ragel_needed=no], [ragel_needed=yes])
  AC_MSG_RESULT([$ragel_needed])
  AS_IF([test $ragel_needed = yes],
      [AC_MSG_ERROR([dnl
You need Ragel to build from DARCS/GIT checkouts.
You can find Ragel at http://www.complang.org/ragel/dnl
      ])])
   ],
   [AC_SUBST([HAVE_RAGEL], 1)])

AC_ARG_VAR([CONFETTI], [Confetti config generator command])
AC_ARG_VAR([CONFETTIFLAGS], [Confetti config generator flags])
AC_CHECK_PROG([CONFETTI], [confetti], [confetti])
AS_IF([test -z "$CONFETTI"],
 [AC_MSG_CHECKING([whether we need confetti to regenerate sources])
  AS_IF([test -f "${srcdir}/cfg/octopus.c"], [confetti_needed=no], [confetti_needed=yes])
  AC_MSG_RESULT([$confetti_needed])
  AS_IF([test $confetti_needed = yes],
        [AC_MSG_ERROR([dnl
You need Confetti to build from DARCS/GIT checkouts.
You can find Confetti at http://github.com/mailru/confetti/dnl
])])
 ],
 [AC_SUBST([HAVE_CONFETTI], 1)])

AC_ARG_VAR([GIT], [GIT version control])
AC_CHECK_PROG([GIT], [git], [git])
AS_IF([test -n "$GIT" -a -d "${srcdir}/.git" ], [AC_SUBST(HAVE_GIT, 1)])
AC_ARG_VAR([DARCS], [DARCS version control])
AC_CHECK_PROG([DARCS], [darcs], [darcs])
AS_IF([test -n "$DARCS" -a -d "${srcdir}/_darcs"], [AC_SUBST(HAVE_DARCS, 1)])
AC_CHECK_PROG([ECHO], [echo], [echo])
AC_CHECK_PROG([DOT], [dot], [dot])
AC_CHECK_PROG([NM], [nm], [nm])
AC_CHECK_PROG([OBJCOPY], [objcopy], [objcopy])
AS_IF([test -n $DOT], [AC_SUBST(HAVE_DOT, 1)])


# Checks for libraries.
AC_SEARCH_LIBS([objc_msg_lookup], [objc], [objc_gnu_runtime=yes])
AC_SEARCH_LIBS([_objc_msgSend], [objc], [objc_apple_runtime=yes])
AS_IF([test -z "$objc_gnu_runtime" -a -z "$objc_apple_runtime"],
      [AC_MSG_ERROR([Unable to locate Objective C runtime library.])])
AC_SEARCH_LIBS([dlopen], [dl dld], [], [AC_MSG_ERROR([unable to find the dlopen() function])])
AC_SEARCH_LIBS(setproctitle, util)

# Checks for header files.
AC_HEADER_ASSERT
AC_HEADER_STDBOOL
AC_CHECK_HEADERS([ucontext.h sys/prctl.h sys/pstat.h sys/param.h valgrind/valgrind.h])
AC_DEFINE(HAVE_THIRD_PARTY_QUEUE_H, 1, [x])

OBJCFLAGS=$CFLAGS
OBJC=$CC
AC_LANG_PUSH([Objective C])
AC_CHECK_HEADER([objc/Object.h], [], [AC_MSG_ERROR([dnl
objc/Object.h is not found.
You can probably fix that setting environment variable CPPFLAGS=-I/path/to/objc/runtime/headers dnl
])])

AC_TRY_RUN([
#include <objc/Object.h>
@interface OneClass : Object
- (int) one;
@end
@implementation OneClass
- (int) one
{
        return 1;
}
@end

int main(void)
{
        OneClass *o = [[OneClass alloc] init];
        return [o one] - 1;
}
], [], [AC_MSG_ERROR([$CC failed to produce working _AC_LANG executable.])])

AC_LANG_POP

AC_CHECK_HEADER(libelf.h)
AC_CHECK_HEADER(gelf.h)
AC_SEARCH_LIBS([elf_begin], [elf])
AS_IF([test -n "$ac_cv_header_libelf_h" -a -n "$ac_cv_header_gelf_h" -a "$ac_cv_search_elf_begin" != no],
      [AC_MSG_NOTICE([Will use libelf to resolve symbol names])]
      [AC_DEFINE(HAVE_LIBELF, 1, [Define to 1 if you have libelf installed])])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_INLINE
AC_C_BIGENDIAN
AC_C_RESTRICT
AC_CHECK_SIZEOF([void *])

# Check largefile support.  You might think this is a system service not a
# compiler characteristic, but you'd be wrong.  We must check this before
# probing existence of related functions such as fseeko, since the largefile
# defines can affect what is generated for that.
AC_SYS_LARGEFILE

# Check for largefile support (must be after AC_SYS_LARGEFILE)
AC_CHECK_SIZEOF([off_t])
AS_IF([test $ac_cv_sizeof_off_t -ne 8 -a $ac_cv_sizeof_off_t -ne 4],
      [AC_MSG_ERROR([Unsupported sizeof(off_t) == $ac_cv_sizeof_off_t.])])

# Checks for required library functions.
AC_FUNC_ALLOCA
AC_CHECK_FUNCS([setproctitle sigaltstack prctl fdatasync  posix_fadvise madvise sysconf])
AC_CHECK_DECL(PAGE_SIZE, [AC_DEFINE(HAVE_PAGE_SIZE, 1, [Define to 1 if PAGE_SIZE provided])], [], [[#include <sys/param.h>]])
AC_CHECK_DECL(strdupa, [AC_DEFINE(HAVE_STRDUPA, 1, [Define to 1 if strdupa provided])], [])
AC_CHECK_DECL(TCP_KEEPIDLE, AC_DEFINE(HAVE_TCP_KEEPIDLE, 1, [Define to 1 if setsockopt(fd, SOL_TCP, TCP_KEEPIDLE) available]),
          [], [[#include <sys/socket.h>]
[#include <netinet/in.h>]
[#include <netinet/tcp.h>]])

# checks for compiler characteristics

AC_DEFUN([AC_CC_FLAG_CHECK], [
  AC_REQUIRE([AC_PROG_CC])
  AC_MSG_CHECKING([whether the _AC_LANG compiler accepts the $1 flag])
  CFLAGS_saved="$CFLAGS"
  CFLAGS="$CFLAGS $1"
  AC_COMPILE_IFELSE([AC_LANG_SOURCE([[char foobar;]])],
                    [AS_TR_SH([ac_cv$1])=yes
                     $2],
                    [AS_TR_SH([ac_cv$1])=no]
                     $3)
  AC_MSG_RESULT($[AS_TR_SH([ac_cv$1])])
  CFLAGS="$CFLAGS_saved"
])

AC_PROG_CC_C99

AC_MSG_CHECKING([whether the compiler supports __builtin_expect])
ac_cv__builtin_expect=no
AC_COMPILE_IFELSE(
  [AC_LANG_SOURCE([int x() { if(__builtin_expect(2>1,1)) return 1; return 0;}])],
  [ac_cv__builtin_expect=yes
   AC_DEFINE([HAVE__BUILTIN_EXPECT], [1], [Define to 1 if the C compiler supports __builtin_expect])])
AC_MSG_RESULT($ac_cv__builtin_expect)

AC_MSG_CHECKING([whether the compiler supports __attribute__((cold))])
ac_cv__attribute_cold=no
CFLAGS_saved="$CFLAGS"
CFLAGS="$CFLAGS -Werror"
AC_COMPILE_IFELSE(
  [AC_LANG_SOURCE([int __attribute__((cold)) x() { return 0;}])],
  [ac_cv__attribute_cold=yes
   AC_DEFINE([HAVE__ATTRIBUTE_COLD], [1], [Define to 1 if the C compiler supports __attribute__((cold))])])
AC_MSG_RESULT($ac_cv__attribute_cold)
CFLAGS="$CFLAGS_saved"

AC_CC_FLAG_CHECK([-fno-stack-protector])
AS_IF([test "x$ac_cv_no_stack_protector" == xyes]
      CFLAGS="$CFLAGS -fno-stack-protector")
AC_CC_FLAG_CHECK([-pipe])
AS_IF([test "x$ac_cv_pipe" == xyes]
      CFLAGS="$CFLAGS -pipe")

AC_CC_FLAG_CHECK([-Werror], [CFLAGS_WARNS="-Werror $CFLAGS_WARNS"])
AC_CC_FLAG_CHECK([-Wno-sign-compare], [CFLAGS_WARNS="-Wno-sign-compare $CFLAGS_WARNS"])
AC_CC_FLAG_CHECK([-Wextra], [CFLAGS_WARNS="-Wextra $CFLAGS_WARNS"])
AC_CC_FLAG_CHECK([-Wall], [CFLAGS_WARNS="-Wall $CFLAGS_WARNS"])
AC_SUBST(CFLAGS_WARNS)

AC_CC_FLAG_CHECK([-MD -MP])
AS_IF([test $ac_cv_MD__MP == yes],
      CFLAGS="-MD -MP $CFLAGS")

AC_CC_FLAG_CHECK([-Wno-comment], [AC_SUBST(HAVE_CC_FLAG_W_NO_COMMENT, 1)])
AC_CC_FLAG_CHECK([-Wno-unused-value], [AC_SUBST(HAVE_CC_FLAG_W_NO_UNUSED_VALUE, 1)])
AC_CC_FLAG_CHECK([-Wno-unused-parameter], [AC_SUBST(HAVE_CC_FLAG_W_NO_UNUSED_PARAMETER, 1)])

AC_ARG_ENABLE([coverage],
              [AS_HELP_STRING([--enable-coverage],
                              [compile with code coverage])],
            [coverage=$enableval])
AS_IF([test "x$coverage" = xyes],
      [AC_CHECK_PROG([LCOV], [lcov], [lcov])
       AS_IF([test -n $LCOV], [AC_SUBST(HAVE_LCOV, 1)])
       AC_SUBST(COVERAGE, 1)
       CFLAGS="--coverage $CFLAGS";
       LDFLAGS="--coverage $LDFLAGS"])

AC_ARG_ENABLE([valgrind],
              [AS_HELP_STRING([--enable-valgrind],
                              [compile with Valgring support])],
            [valgrind=$enableval])
AS_IF([test "x$valgrind" == xyes],
      [AS_IF([test -z "$ac_cv_header_valgrind_valgrind_h"], AC_MSG_ERROR([unable to find valgrind headers]))],
      [CFLAGS="-DNVALGRIND $CFLAGS"])

AC_ARG_ENABLE([modules],
              [AS_HELP_STRING([--enable-modules[[="box feeder"]]],
                              [select module to build, silver(box) and feeder by default])],
              [octopus_modules=$enableval],
              [octopus_modules="box feeder"])
AC_SUBST(modules, [$octopus_modules])
for module in $octopus_modules; do
    AS_IF([test -r mod/$module/Makefile], [], AC_MSG_ERROR([No such module: $module]))
done

AC_ARG_ENABLE([clients],
              [AS_HELP_STRING([--enable-clients[[=libiproto]]],
                              [select clients to build, libiproto by default])],
              [octopus_clients=$enableval],
              [octopus_clients="libiproto"])
AC_SUBST(clients, [$octopus_clients])
for client in $octopus_clients; do
    AS_IF([test -r client/$client/Makefile], [], AC_MSG_ERROR([No such client: $client]))
done

AC_OUTPUT

AC_MSG_RESULT([
Configuration:

Modules enabled........$octopus_modules
Clients enabled........$octopus_clients

Objective C compiler...$CC
CPPFLAGS...............$CPPFLAGS
CFLAGS.................$CFLAGS
CFLAGS_WARNS...........$CFLAGS_WARNS
LDFLAGS................$LDFLAGS
LIBS...................$LIBS

Ragel..................$RAGEL
Ragel flags............$RAGELFLAGS
Confetti...............$CONFETTI
Confetti flags.........$CONFETTIFLAGS])
