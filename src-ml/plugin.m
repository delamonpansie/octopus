#include <caml/mlvalues.h>
#include <caml/memory.h>
#include <caml/callback.h>
#include <caml/alloc.h>
#include <caml/threads.h>

#import <octopus.h>

void
oct_caml_plugins()
{
	caml_leave_blocking_section();
	caml_callback(*caml_named_value("plugin_loader"),
		      caml_copy_string(cfg.caml_path));
	caml_enter_blocking_section();
}
