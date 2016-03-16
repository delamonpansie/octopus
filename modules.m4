AC_SUBST([BUNDLE])
AC_ARG_ENABLE([modules],
              [AS_HELP_STRING([--enable-modules[[="box feeder"]]],
                              [select module to build, silver(box) and feeder by default])],
              [octopus_modules=$enableval],
              [octopus_modules=${bundled_modules:-box feeder}])

AC_MSG_NOTICE([Fetching modules/($octopus_modules) clients/($octopus_clients)])
(
  cd "${srcdir}"
  scripts/fetch-modules.sh `for m in $octopus_modules; do echo mod/$m; done; for c in $octopus_clients; do echo client/$c; done`
)
