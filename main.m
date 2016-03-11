#import <iproto.h>
#import <log_io.h>
#import <pickle.h>
#import <mod/example/example.h>
#import <mod/example/example_version.h>
#import <mod/feeder/feeder.h>

struct pair { u32 car, cdr; };
static int key_compare(const void *a_, const void *b_, void* arg)
{
	const struct pair *a = a_, *b = b_;
	if (a->car == b->car)
		if (b->cdr == a->cdr)
			return 0;
		else
			return a->cdr > b->cdr ? 1 : -1;
	else
		return a->car > b->car ? 1 : -1;
}

struct twltree_conf_t twl_conf = { .tuple_key_cmp = key_compare };

static Example *mod;
static void
modify(struct netmsg_head *wbuf, struct iproto *req, void *arg)
{
	if (req->data_len != 8)
		return iproto_error(wbuf, req, 0x102, "bad request");
	/* WAL write */
	if ([mod->shard submit:req->data len:8 tag:req->msg_code<<5|TAG_WAL] != 1)
		return iproto_error(wbuf, req, 0x201, "io error");
	/* modify */
	if (req->msg_code == UPSERT)
		twltree_insert(&mod->index, req->data, true);
	else
		twltree_delete(&mod->index, req->data);
	iproto_reply_small(wbuf, req, 0);
}

static void
query(struct netmsg_head *wbuf, struct iproto *req, void *arg)
{
	if (req->data_len != 8)
		return iproto_error(wbuf, req, 0x102, "bad request");
	struct iproto_retcode *reply = iproto_reply_small(wbuf, req, 0);
	twliterator_t it;
	struct pair *ptr, *pair = (void *)req->data;
	twltree_iterator_init_set(&mod->index, &it, pair, twlscan_forward);
	int found = 0;
	while ((ptr = twltree_iterator_next(&it)) != NULL) {
		if (ptr->car != pair->car)
			break;
		net_add_iov_dup(wbuf, ptr, 8);
		reply->data_len += 8;
		found++;
		break;
	}
	assert(found == 1);
}

@implementation Example
- (id)
init_shard:(Shard<Shard> *)shard_
{
	[super init_shard:shard_];

	index.sizeof_index_key = 8;
	index.sizeof_tuple_key = 8;
	index.conf = &twl_conf;
	extern void* twl_realloc(void *old, size_t new_size);
	index.tlrealloc = twl_realloc;
	twltree_init(&index);

	return (mod = self);
}
- (void)
apply:(struct tbuf *)data tag:(u16)tag
{
	switch ((tag & TAG_MASK) >> 5) {
	case UPSERT:
		twltree_insert(&index, data->ptr, true);
		break;
	case DELETE:
		twltree_delete(&index, data->ptr);
		break;
	}
}

- (int)
snapshot_write_rows:(XLog *)l
{
	struct pair *ptr;
	twliterator_t it;
	twltree_iterator_init(&mod->index, &it, twlscan_forward);
	while ((ptr = twltree_iterator_next(&it)) != NULL)
		if ([l append_row:ptr len:8 shard:shard tag:UPSERT<<5|TAG_SNAP] == NULL)
			return -1;
	return 0;
}
@end

static void
init_second_stage(va_list ap)
{
	static struct iproto_service svc;
	iproto_service(&svc, cfg.primary_addr);
	[recovery simple:&svc];
	service_register_iproto(&svc, UPSERT, modify, 0);
	service_register_iproto(&svc, DELETE, modify, 0);
	service_register_iproto(&svc, QUERY, query, IPROTO_NONBLOCK);
	feeder_service(&svc); /* enable replication */

	for (int i = 0; i < MAX(1, cfg.wal_writer_inbox_size); i++)
		fiber_create("worker", iproto_worker, &svc);
}

static void
init(void)
{
	recovery = [[Recovery alloc] init];
	recovery->default_exec_class = [Example class];
	if (init_storage) {
		[recovery shard_add_dummy:NULL]; /* no µsharding */
		return;
	}
	fiber_create("example_init", init_second_stage);
}

static struct tnt_module example_mod = {
	.name = "example",
	.version = example_version_string,
	.init = init,
};

register_module(example_mod);
