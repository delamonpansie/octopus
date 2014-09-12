#ifndef BINARY_SEARCH_H
#define BINARY_SEARCH_H
/*
 * Copyright (C) 2014 Mail.RU
 * Copyright (C) 2014 Sokolov Yura aka funny_falcon
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

/*
	BSEARCH_STRUCT(long) bs;
	BSEARCH_INIT_SEARCH_LAST(&bs, count);
	while (BSEARCH_LAST_NOT_FOUND(&bs)) {
		BSEARCH_STEP_TO_LAST_LE(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of last less or equal %d equal %d", bs.mid, bs.equal);

	BSEARCH_INIT_SEARCH_FIRST(&bs, count);
	while (BSEARCH_FIRST_NOT_FOUND(&bs)) {
		BSEARCH_STEP_TO_FIRST_GT(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of first greater %d", bs.mid);

	BSEARCH_INIT_SEARCH_EQUAL(&bs, count);
	while (BSEARCH_EQUAL_NOT_FOUND(&bs)) {
		BSEARCH_STEP_TO_EQUAL(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of some equal or first greater %d equal %d", bs.mid, bs.equal);
*/
#define BSEARCH_STRUCT(index_t) struct { index_t low, high, mid; int equal; }
#define BSEARCH_STRUCT_NAME(index_t, name) struct name { index_t low, high, mid; int equal; }

#define BSEARCH_INIT_SEARCH_LAST(bs_, count) do { \
	__typeof__(bs_) _bs_ = (bs_); _bs_->low = _bs_->mid = -1; _bs_->high = (count) - 1; _bs_->equal = 0; \
	assert(_bs_->low <= _bs_->high); \
} while(0)

#define BSEARCH_LAST_NOT_FOUND(bs_) ({ \
	__typeof__(bs_) _bs_ = (bs_); \
	_bs_->mid = _bs_->low + ((_bs_->high - _bs_->low + 1) / 2); \
	_bs_->low < _bs_->high; \
})

#define BSEARCH_STEP_TO_LAST_LE(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ >= 0) { _bs_->low = _bs_->mid; _bs_->equal = _cmp_ == 0; } \
	else { _bs_->high = _bs_->mid - 1; } \
} while(0)

#define BSEARCH_STEP_TO_LAST_LT(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ > 0) { _bs_->low = _bs_->mid; } \
	else { _bs_->high = _bs_->mid - 1; } \
} while(0)

#define BSEARCH_INIT_SEARCH_FIRST(bs_, count) do { \
	__typeof__(bs_) _bs_ = (bs_); _bs_->low = _bs_->mid = 0; _bs_->high = (count); _bs_->equal = 0; \
	assert(_bs_->low <= _bs_->high); \
} while(0)

#define BSEARCH_FIRST_NOT_FOUND(bs_) ({ \
	__typeof__(bs_) _bs_ = (bs_); \
	_bs_->mid = _bs_->low + ((_bs_->high - _bs_->low) / 2); \
	_bs_->low < _bs_->high; \
})

#define BSEARCH_STEP_TO_FIRST_GE(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ <= 0) { _bs_->high = _bs_->mid; _bs_->equal = _cmp_ == 0; } \
	else { _bs_->low = _bs_->mid + 1; } \
} while(0)

#define BSEARCH_STEP_TO_FIRST_GT(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ < 0) { _bs_->high = _bs_->mid; } \
	else { _bs_->low = _bs_->mid + 1; } \
} while(0)

#define BSEARCH_INIT_SEARCH_EQUAL(bs_, count) BSEARCH_INIT_SEARCH_FIRST((bs_), (count))
#define BSEARCH_EQUAL_NOT_FOUND(bs_) BSEARCH_FIRST_NOT_FOUND(bs_)
#define BSEARCH_STEP_TO_EQUAL(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ < 0) { _bs_->high = _bs_->mid; } \
	else if (_cmp_ > 0) { _bs_->low = _bs_->mid + 1; } \
	else { _bs_->low = _bs_->high = _bs_->mid; _bs_->equal = 1; } \
} while(0)

/*
	// if int indices are just enough
	bs_t bs;
	bs_init_search_last(&bs, count);
	while (bs_last_not_found(&bs)) {
		bs_step_to_last_le(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of last less or equal %d equal %d", bs.mid, bs.equal);

	bs_init_search_first(&bs, count);
	while (bs_first_not_found(&bs)) {
		bs_step_to_first_gt(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of first greater %d", bs.mid);

	bs_init_search_equal(&bs, count);
	while (bs_equal_not_found(&bs)) {
		bs_step_to_equal(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of some equal or first greater %d equal %d", bs.mid, bs.equal);
*/
typedef BSEARCH_STRUCT(int) bs_t;
static inline void bs_init_search_last(bs_t *bs, int count) { BSEARCH_INIT_SEARCH_LAST(bs, count); }
static inline void bs_init_search_first(bs_t *bs, int count) { BSEARCH_INIT_SEARCH_FIRST(bs, count); }
static inline void bs_init_search_equal(bs_t *bs, int count) { BSEARCH_INIT_SEARCH_EQUAL(bs, count); }
static inline int  bs_last_not_found(bs_t *bs) { return BSEARCH_LAST_NOT_FOUND(bs); }
static inline int  bs_first_not_found(bs_t *bs) { return BSEARCH_FIRST_NOT_FOUND(bs); }
static inline int  bs_equal_not_found(bs_t *bs) { return BSEARCH_EQUAL_NOT_FOUND(bs); }
static inline void bs_step_to_last_le(bs_t *bs, int64_t cmp) { BSEARCH_STEP_TO_LAST_LE(bs, cmp); }
static inline void bs_step_to_last_lt(bs_t *bs, int64_t cmp) { BSEARCH_STEP_TO_LAST_LT(bs, cmp); }
static inline void bs_step_to_first_ge(bs_t *bs, int64_t cmp) { BSEARCH_STEP_TO_FIRST_GE(bs, cmp); }
static inline void bs_step_to_first_gt(bs_t *bs, int64_t cmp) { BSEARCH_STEP_TO_FIRST_GT(bs, cmp); }
static inline void bs_step_to_equal(bs_t *bs, int64_t cmp) { BSEARCH_STEP_TO_EQUAL(bs, cmp); }

#endif
