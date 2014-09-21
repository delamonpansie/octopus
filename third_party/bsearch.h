#ifndef BINARY_SEARCH_H
#define BINARY_SEARCH_H
/*
	BSEARCH_STRUCT(long) bs;

	BSEARCH_INIT(&bs, count);
	while (BSEARCH_NOT_FOUND(&bs)) {
		BSEARCH_STEP_TO_FIRST_GT(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of first greater %d", bs.mid);

	BSEARCH_INIT(&bs, count);
	while (BSEARCH_NOT_FOUND(&bs)) {
		BSEARCH_STEP_TO_EQUAL_MAY_BREAK(&bs, cmp(key, objs[bs.mid]));
		//BSEARCH_STEP_TO_EQUAL(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of some equal or first greater %d equal %d", bs.mid, bs.equal);
*/
#define BSEARCH_STRUCT(index_t) struct { index_t low, high, mid; int equal; }
#define BSEARCH_STRUCT_NAME(index_t, name) struct name { index_t low, high, mid; int equal; }

#define BSEARCH_INIT(bs_, count) do { \
	__typeof__(bs_) _bs_ = (bs_); _bs_->low = _bs_->mid = 0; _bs_->high = (count); _bs_->equal = 0; \
	assert(_bs_->low <= _bs_->high); \
} while(0)

#define BSEARCH_NOT_FOUND(bs_) ({ \
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

#define BSEARCH_STEP_TO_EQUAL(bs_, cmp_) do { \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ < 0) { _bs_->high = _bs_->mid; } \
	else if (_cmp_ > 0) { _bs_->low = _bs_->mid + 1; } \
	else { _bs_->low = _bs_->high = _bs_->mid; _bs_->equal = 1; } \
} while(0)

/* as optimisation for clang */
#define BSEARCH_STEP_TO_EQUAL_MAY_BREAK(bs_, cmp_) ({ \
	__typeof__(bs_) _bs_ = (bs_); \
	__typeof__(cmp_) _cmp_ = (cmp_); \
	if (_cmp_ < 0) { _bs_->high = _bs_->mid; } \
	else if (_cmp_ > 0) { _bs_->low = _bs_->mid + 1; } \
	else { _bs_->low = _bs_->high = _bs_->mid; _bs_->equal = 1; break; } \
})

/*
	// if int indices are just enough
	bs_t bs;

	bs_init(&bs, count);
	while (bs_not_found(&bs)) {
		bs_step_to_first_gt(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of first greater %d", bs.mid);

	bs_init(&bs, count);
	while (bs_not_found(&bs)) {
		bs_step_to_equal(&bs, cmp(key, objs[bs.mid]));
	}
	print("index of some equal or first greater %d equal %d", bs.mid, bs.equal);
*/
typedef BSEARCH_STRUCT(unsigned) bs_t;
static inline void bs_init(bs_t *bs, int count) { BSEARCH_INIT(bs, count); }
static inline int  bs_not_found(bs_t *bs) { return BSEARCH_NOT_FOUND(bs); }
static inline void bs_step_to_first_ge(bs_t *bs, int cmp) { BSEARCH_STEP_TO_FIRST_GE(bs, cmp); }
static inline void bs_step_to_first_gt(bs_t *bs, int cmp) { BSEARCH_STEP_TO_FIRST_GT(bs, cmp); }
static inline void bs_step_to_equal(bs_t *bs, int cmp) { BSEARCH_STEP_TO_EQUAL(bs, cmp); }
#define bs_step_to_equal_may_break(bs, cmp) BSEARCH_STEP_TO_EQUAL_MAY_BREAK((bs), (cmp))

#endif
