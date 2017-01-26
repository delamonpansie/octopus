/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2016 Yuriy Vostrikov
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

#ifndef TARANTOOL_SILVERBOX_H
#define TARANTOOL_SILVERBOX_H

#include <index.h>
#import <iproto.h>
#import <net_io.h>
#import <objc.h>
#import <log_io.h>

@class Box;
struct index;

/* if you change this struct also change definition in box.lua */
#define MAX_IDX 10
struct object_space {
	int n;
	bool ignored, snap, wal;
	int cardinality;
	size_t obj_bytes;
	size_t slab_bytes;
	Index<BasicIndex> *index[MAX_IDX];
};

#define foreach_index(ivar, obj_space)					\
	for (Index<BasicIndex> *ivar = (obj_space)->index[0]; ivar; ivar = ivar->next)

#define foreach_indexi(i, ivar, obj_space)				\
	for (Index<BasicIndex> *ivar = (i == 0 ? (obj_space)->index[0] : (obj_space)->index[0]->next); ivar; ivar = ivar->next)

enum object_type {
	BOX_TUPLE = 1,
	BOX_SMALL_TUPLE = 2,
	BOX_PHI = 3
};

struct box_tuple {
	u32 bsize; /* byte size of data[] */
	u32 cardinality;
	u8 data[0];
} __attribute__((packed));

struct box_small_tuple {
	uint8_t bsize;
	uint8_t cardinality;
	uint8_t data[0];
};

TAILQ_HEAD(phi_tailq, box_phi_cell);
struct box_phi {
	struct tnt_object header;
	struct tnt_object *obj; /* commited object */
	struct phi_tailq tailq;
	struct box_op *bop; /* for debug purposes */
	Index<BasicIndex> *index;
};

struct box_phi_cell {
	struct tnt_object *obj; /* to be commited */
	struct box_phi *head;
	struct box_op *bop; /* for debug purposes */
	TAILQ_ENTRY(box_phi_cell) link, bop_link;
};

union box_phi_union {
	struct box_phi phi;
	struct box_phi_cell cell;
};

struct tnt_object *phi_left(struct tnt_object *obj);
struct tnt_object *phi_right(struct tnt_object *obj);
struct tnt_object *phi_obj(const struct tnt_object *obj);

struct tnt_object *tuple_visible_left(struct tnt_object *obj);
struct tnt_object *tuple_visible_right(struct tnt_object *obj);


struct box_snap_row {
	u32 object_space;
	u32 tuple_size;
	u32 data_size;
	u8 data[];
} __attribute((packed));

static inline struct box_snap_row *
box_snap_row(const struct tbuf *t)
{
	return (struct box_snap_row *)t->ptr;
}

extern struct dtor_conf box_tuple_dtor;
struct index_conf * cfg_box2index_conf(struct octopus_cfg_object_space_index *c, int sno, int ino, int panic);

struct box_op {
	u16 op;
	u32 flags;
	struct object_space *object_space;
	struct tnt_object *old_obj, *obj, *ret_obj;
	struct phi_tailq phi;
	u32 obj_affected;
	TAILQ_ENTRY(box_op) link;
	struct box_txn *txn; /* for debug purposes */
	int data_len;
	char data[];
};

enum txn_mode { RO, RW } mode;
enum txn_state { UNDECIDED, COMMIT, ROLLBACK };
struct box_txn {
	Box *box;
	enum txn_mode mode;
	enum txn_state state;
	u32 obj_affected, submit;
	int id;
	struct Fiber *fiber; /* for debug purposes */

	TAILQ_HEAD(box_op_tailq, box_op) ops;
};

struct box_txn *box_txn_alloc(int shard_id, enum txn_mode mode);

struct box_meta_txn {
	u16 op;
	u32 flags;

	Box *box;
	struct object_space *object_space;
	Index<BasicIndex> *index;
};

struct tlv {
	u16 tag;
	u32 len;
	char val[];
} __attribute((packed));

enum tlv_tag {
	BOX_OP = 127,
	BOX_MULTI_OP
};


struct box_op *box_prepare(struct box_txn *txn, int op, const void *data, u32 data_len);
int  box_submit(struct box_txn *txn) __attribute__ ((warn_unused_result));
void box_commit(struct box_txn *txn);
void box_rollback(struct box_txn *txn);
void prepare_replace(struct box_op *bop, size_t cardinality, const void *data, u32 data_len);
void snap_insert_row(struct object_space *object_space, size_t cardinality, const void *data, u32 data_len);

void box_prepare_meta(struct box_meta_txn *txn, struct tbuf *data);
void box_commit_meta(struct box_meta_txn *txn);
void box_rollback_meta(struct box_meta_txn *txn);

void box_shard_create(int n, int type);

void box_service(struct iproto_service *s);
void box_service_ro(struct iproto_service *s);

#define BOX_RETURN_TUPLE 1
#define BOX_ADD 2
#define BOX_REPLACE 4

/*
    deprecated commands:
        _(INSERT, 1)
        _(DELETE, 2)
        _(SET_FIELD, 3)
        _(ARITH, 5)
        _(SET_FIELD, 6)
        _(ARITH, 7)
        _(SELECT, 4)
        _(DELETE, 8)
        _(UPDATE_FIELDS, 9)
        _(INSERT,10)
        _(SELECT_LIMIT, 12)
        _(SELECT_OLD, 14)
        _(UPDATE_FIELDS_OLD, 16)
        _(JUBOX_ALIVE, 11)

    DO NOT use those ids!
 */
#define MESSAGES(_)				\
	_(NOP, 1)				\
        _(INSERT, 13)				\
        _(SELECT_LIMIT, 15)			\
	_(SELECT, 17)				\
	_(UPDATE_FIELDS, 19)			\
	_(DELETE_1_3, 20)			\
	_(DELETE, 21)				\
	_(EXEC_LUA, 22)				\
	_(PAXOS_LEADER, 90)			\
	_(SELECT_KEYS, 99)			\
	_(SELECT_TUPLES, 100)			\
	_(SUBMIT_ERROR, 101)			\
	_(CREATE_OBJECT_SPACE, 240)		\
	_(CREATE_INDEX, 241)			\
	_(DROP_OBJECT_SPACE, 242)		\
	_(DROP_INDEX, 243)			\
	_(TRUNCATE, 244)

enum messages ENUM_INITIALIZER(MESSAGES);
extern char const * const box_ops[];

#define OBJECT_SPACE_MAX (256)
@interface Box : DefaultExecutor <Executor> {
@public
	struct object_space *object_space_registry[OBJECT_SPACE_MAX];
	const int object_space_max_idx;
	int version;
}
@end
struct object_space *object_space(Box *box, int n);
int box_version();

void *next_field(void *f);
ssize_t fields_bsize(u32 cardinality, const void *data, u32 max_len);
void __attribute__((noreturn)) bad_object_type(void);
#define box_tuple(obj) ((struct box_tuple *)((obj) + 1))
#define box_small_tuple(obj) ((struct box_small_tuple *)((obj) + 1))
static inline int tuple_bsize(const struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE:
		return box_tuple(obj)->bsize;
	case BOX_SMALL_TUPLE:
		return box_small_tuple(obj)->bsize;
	default:
		bad_object_type();
	}
}
static inline int tuple_cardinality(const struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE:
		return box_tuple(obj)->cardinality;
	case BOX_SMALL_TUPLE:
		return box_small_tuple(obj)->cardinality;
	case BOX_PHI:
		return tuple_cardinality(phi_obj(obj));
	default:
		bad_object_type();
	}
}
static inline void * tuple_data(struct tnt_object *obj)
{
	switch (obj->type) {
	case BOX_TUPLE:
		return box_tuple(obj)->data;
	case BOX_SMALL_TUPLE:
		return box_small_tuple(obj)->data;
	case BOX_PHI:
		return tuple_data(phi_obj(obj));
	default:
		bad_object_type();
	}
}
void * tuple_field(struct tnt_object *obj, size_t i);
int tuple_valid(struct tnt_object *obj);
void tuple_free(struct tnt_object *obj);
void net_tuple_add(struct netmsg_head *h, struct tnt_object *obj);

int box_cat_scn(i64 stop_scn);
int box_cat(const char *filename);
void box_print_row(struct tbuf *out, u16 tag, struct tbuf *r);
const char *box_row_to_a(u16 tag, struct tbuf *r);

struct print_dups_arg {
	int space, index;
	struct tbuf *positions;
};
void box_idx_print_dups(void *arg, struct index_node* a, struct index_node* b, uint32_t position);

void box_init_phi_cache(void);
#endif
