#include <stdarg.h>

extern char *cfg_err;
extern int cfg_err_len;

#include <tarantool.h>
#include <util.h>
#include <third_party/confetti/prscfg.h>

void
out_warning(ConfettyError v __attribute__((unused)), char *format, ...)
{
	va_list ap;

	if (cfg_err_len < 0)
		return;

	cfg_err_len -= snprintf(cfg_err, cfg_err_len, "\r\n - ");

	if (cfg_err_len < 0)
		return;

	va_start(ap, format);
	cfg_err_len -= vsnprintf(cfg_err, cfg_err_len, format, ap);
	va_end(ap);
}
