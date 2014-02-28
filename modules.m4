AC_SUBST([BUNDLE])
AC_ARG_ENABLE([modules],
              [AS_HELP_STRING([--enable-modules[[="box feeder"]]],
                              [select module to build, silver(box) and feeder by default])],
              [octopus_modules=$enableval],
              [octopus_modules=${bundled_modules:-box feeder}])

AC_MSG_NOTICE([Fetching modules])
"${srcdir}"/scripts/fetch-modules.sh octopus_modules
