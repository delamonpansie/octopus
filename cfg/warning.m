#include <stdarg.h>

extern char *cfg_err;
extern int cfg_err_len, cfg_err_offt;

#include <octopus.h>
#include <util.h>
#include <third_party/confetti/prscfg.h>

void
out_warning(ConfettyError v __attribute__((unused)), char *format, ...)
{
	va_list ap;

	if (cfg_err_len < 0)
		return;

	int r = snprintf(cfg_err + cfg_err_offt, cfg_err_len, "\r\n - ");
	cfg_err_len -= r;
	cfg_err_offt += r;

	if (cfg_err_len < 0)
		return;

	va_start(ap, format);
	r = vsnprintf(cfg_err + cfg_err_offt, cfg_err_len, format, ap);
	cfg_err_len -= r;
	cfg_err_offt += r;
	va_end(ap);
}
