/*
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2015, 2016 Mail.RU
 * Copyright (C) 2010, 2011, 2012, 2013, 2014, 2015, 2016 Yuriy Vostrikov
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

#import <util.h>
#import <log_io.h>
#import <fiber.h>
#import <palloc.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>
#import <shard.h>

#include <third_party/crc32.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/file.h>

#if !HAVE_DECL_FDATASYNC
extern int fdatasync(int fd);
#endif

#if !HAVE_MEMRCHR
/* os x doesn't have memrchr */
static void *
memrchr(const void *s, int c, size_t n)
{
    const unsigned char *cp;

    if (n != 0) {
        cp = (unsigned char *)s + n;
        do {
            if (*(--cp) == (unsigned char)c)
                return((void *)cp);
        } while (--n != 0);
    }
    return(NULL);
}
#endif

const u64 default_cookie = 0;
const u32 default_version = 12;
const u32 version_11 = 11;
const char *v11 = "0.11\n";
const char *v12 = "0.12\n";
const char *v04 = "0.04\n";
const char *v03 = "0.03\n";
const char *snap_mark = "SNAP\n";
const char *xlog_mark = "XLOG\n";
const char *inprogress_suffix = ".inprogress";
const u32 marker = 0xba0babed;
const u32 eof_marker = 0x10adab1e;
Class version3 = nil;
Class version4 = nil;
Class version11 = nil;


struct row_v12 *
dummy_row(i64 lsn, i64 scn, u16 tag)
{
	struct row_v12 *r = palloc(fiber->pool, sizeof(struct row_v12));

	r->lsn = lsn;
	r->scn = scn;
	r->tm = ev_now();
	r->tag = tag;
	r->len = 0;
	r->data_crc32c = crc32c(0, (unsigned char *)"", 0);
	r->header_crc32c = crc32c(0, (unsigned char *)r + sizeof(r->header_crc32c),
				  sizeof(*r) - sizeof(r->header_crc32c));
	return r;
}

const char *
xlog_tag_to_a(u16 tag)
{
	static char buf[32];
	char *p = buf;
	u16 tag_type = tag & ~TAG_MASK;
	tag &= TAG_MASK;

	if (tag == 0)
		return "nil";

	switch (tag_type) {
	case TAG_SNAP: p += sprintf(p, "snap/"); break;
	case TAG_WAL: p += sprintf(p, "wal/"); break;
	case TAG_SYS: p += sprintf(p, "sys/"); break;
	default: p += sprintf(p, "%i/", tag_type >> TAG_SIZE);
	}

	switch (tag) {
	case snap_initial:	strcat(p, "snap_initial"); break;
	case snap_data:		strcat(p, "snap_data"); break;
	case snap_final:	strcat(p, "snap_final"); break;
	case wal_data:		strcat(p, "wal_data"); break;
	case wal_final:		strcat(p, "wal_final"); break;
	case shard_create:	strcat(p, "shard_create"); break;
	case shard_alter:	strcat(p, "shard_alter"); break;
	case shard_final:	strcat(p, "shard_final"); break;
	case run_crc:		strcat(p, "run_crc"); break;
	case nop:		strcat(p, "nop"); break;
	case paxos_promise:	strcat(p, "paxos_promise"); break;
	case paxos_accept:	strcat(p, "paxos_accept"); break;
	case tlv:		strcat(p, "tlv"); break;
	default:
		if (tag < user_tag)
			sprintf(p, "sys%i", tag);
		else
			sprintf(p, "usr%i", tag >> 5);
	}
	return buf;
}

static char *
set_file_buf(FILE *fd, const int bufsize)
{
	char	*vbuf;

	vbuf = xmalloc(bufsize);
	setvbuf(fd, vbuf, _IOFBF, bufsize);

	return vbuf;
}

@implementation XLog
- (bool) eof { return eof; }
- (u32) version { return 0; }
- (i64) last_read_lsn { return last_read_lsn; }

- (XLog *)
init_filename:(const char *)filename_
           fd:(FILE *)fd_
          dir:(XLogDir *)dir_
	  vbuf:(char*)vbuf_
{
	[super init];
	filename = strdup(filename_);
	fd = fd_;
	mode = LOG_READ;
	dir = dir_;
	vbuf = vbuf_;

	tag_mask = TAG_WAL;
	offset = ftello(fd);

	wet_rows_offset_size = 16;
	wet_rows_offset = xmalloc(wet_rows_offset_size * sizeof(*wet_rows_offset));
	return self;
}

+ (XLog *)
open_for_read_filename:(const char *)filename dir:(XLogDir *)dir
{
	char filetype_[32], version_[32];
	XLog *l = nil;
	FILE *fd;
	char *fbuf;

	if ((fd = fopen(filename, "r")) == NULL)
		return nil; /* no cleanup needed */
	/* libc will try prepread sizeof(vbuf) bytes on every fseeko,
	   so no reason to make vbuf particulary large */
	fbuf = set_file_buf(fd, 64 * 1024);

	if (fgets(filetype_, sizeof(filetype_), fd) == NULL ||
	    fgets(version_, sizeof(version_), fd) == NULL)
	{
		if (feof(fd))
			say_error("unexpected EOF reading %s", filename);
		else
			say_syserror("can't read header of %s", filename);
		goto error;
	}

	if (dir != NULL && strncmp(dir->filetype, filetype_, sizeof(filetype_)) != 0) {
		say_error("filetype mismatch of %s", filename);
		goto error;
	}

	if (strcmp(version_, v11) == 0) {
		if (version11 != nil) {
			l = [version11 alloc];
		} else {
			l = [XLog11 alloc];
		}
	} else if (strcmp(version_, v12) == 0) {
		l = [XLog12 alloc];
	} else if (strcmp(version_, v04) == 0) {
		if (version4 != nil) {
			l = [version4 alloc];
		} else {
			l = [XLog04 alloc];
		}
	} else if (strcmp(version_, v03) == 0) {
		if (version3 != nil) {
			l = [version3 alloc];
		}
	}
	if (l == nil) {
		say_error("bad version `%s' of %s", version_, filename);
		goto error;
	}

	[l init_filename:filename fd:fd dir:dir vbuf:fbuf];
	if ([l read_header] < 0) {
		if (feof(fd))
			say_error("unexpected EOF reading %s", filename);
		else
			say_syserror("can't read header of %s", filename);
		[l free]; /* will do correct cleanup */
		return nil;
	}

	return l;

error:
	fclose(fd);
	free(fbuf);
	return nil;
}

+ (void)
register_version3: (Class)xlog
{
	version3 = xlog;
}

+ (void)
register_version4: (Class)xlog
{
	version4 = xlog;
}

+ (void)
register_version11: (Class)xlog
{
	version11 = xlog;
}

- (id)
free
{
	ev_stat_stop(&stat);

	if (mode == LOG_READ && rows == 0 && access(filename, F_OK) == 0) {
		bool legacy_snap = ![self isMemberOf:[XLog12 class]] &&
				   [dir isMemberOf:[SnapDir class]];
		if (!legacy_snap)
			panic("no valid rows were read");
	}

	if (fd) {
		if (mode == LOG_WRITE)
			[self write_eof_marker];
		[self close];
	}

	free(wet_rows_offset);
	free(filename);
	free(vbuf);
	return [super free];
}

- (size_t)
rows
{
	return rows + wet_rows;
}

- (int)
inprogress_rename
{
	int result = 0;

	char *final_filename = strdup(filename);
	char *suffix = strrchr(final_filename, '.');
	assert(strcmp(suffix, inprogress_suffix) == 0);
	*suffix = 0;

	if (rename(filename, final_filename) != 0) {
		say_syserror("can't rename %s to %s", filename, final_filename);
		result = -1;
	} else {
		assert(inprogress);
		inprogress = 0;
		*(strrchr(filename, '.')) = 0;
	}

	free(final_filename);
	if (result >= 0) {
		result = [dir sync];
		if (result < 0)
			say_syserror("can't fsync dir");
	}
	return result;
}

- (int)
close
{
	if (fd == NULL)
		return 0;
	if (fclose(fd) < 0) {
		say_syserror("can't close");
		return -1;
	}
	fd = NULL;
	return 0;
}

- (int)
write_eof_marker
{
	assert(mode == LOG_WRITE);
	assert(fd != NULL);

	if (fwrite(&eof_marker, sizeof(eof_marker), 1, fd) != 1) {
		say_syserror("can't write eof_marker");
		return -1;
	}

	if ([self flush] == -1)
		return -1;

	if ([self close] == -1)
		return -1;
	return 0;
}

- (int)
flush
{
	if (fflush(fd) < 0) {
		say_syserror("fflush");
		/* prevent silent drop of wet rows.
		   it's required to call [confirm_write] in case of wet file */
		assert(wet_rows == 0);
		return -1;
	}

#if HAVE_FDATASYNC
	if (fdatasync(fileno(fd)) < 0) {
		say_syserror("fdatasync");
		return -1;
	}
#else
	if (fsync(fileno(fd)) < 0) {
		say_syserror("fsync");
		return -1;
	}
#endif
	return 0;
}

- (void)
fadvise_dont_need
{
#if HAVE_POSIX_FADVISE
	off_t end = ftello(fd);
	/* на всякий случай :-) */
	if (end < 128*1024 + 4096)
		return;
	end -= 128*1024 + end % 4096;
	posix_fadvise(fileno(fd), 0, end, POSIX_FADV_DONTNEED);
#endif
}

- (void)
write_header
{
	assert(false);
}

- (int)
read_header
{
        char buf[256];
        char *r;
        for (;;) {
                r = fgets(buf, sizeof(buf), fd);
                if (r == NULL) {
			say_syserror("fgets");
			return -1;
		}

                if (strcmp(r, "\n") == 0 || strcmp(r, "\r\n") == 0)
                        break;
        }
        return 0;
}

- (struct row_v12 *)
read_row
{
	return NULL;
}

- (marker_desc_t)
marker_desc
{
	return (marker_desc_t){
		.marker = (u64)marker,
		.eof = (u64)eof_marker,
		.size = 4,
		.eof_size = 4
	};
}

- (struct row_v12 *)
fetch_row
{
	struct row_v12 *row;
	u64 magic, magic_shift;
	off_t marker_offset = 0, good_offset, eof_offset;
	marker_desc_t mdesc = [self marker_desc];

	magic = 0;
	magic_shift = (mdesc.size - 1) * 8;
	good_offset = ftello(fd);

restart:

	/*
	 * reset stream status if we reached eof before,
	 * subsequent fread() call could cache (at least on
	 * FreeBSD) eof cache status
	 */
	if (feof(fd))
		clearerr(fd);

	if (marker_offset > 0)
		fseeko(fd, marker_offset + 1, SEEK_SET);

	say_debug3("%s: start offt %08" PRIofft, __func__, ftello(fd));
	if (fread(&magic, mdesc.size, 1, fd) != 1)
		goto eof;

	while (magic != mdesc.marker) {
		int c = fgetc(fd);
		if (c == EOF)
			goto eof;
		magic >>= 8;
		magic |= ((u64)c & 0xff) << magic_shift;
	}
	marker_offset = ftello(fd) - mdesc.size;
	if (good_offset != marker_offset)
		say_warn("skipped %" PRIofft " bytes after %08" PRIofft " offset",
			 marker_offset - good_offset, good_offset);
	say_debug3("	magic found at %08" PRIofft, marker_offset);

	row = [self read_row];

	if (row == NULL) {
		if (feof(fd))
			goto eof;
		say_warn("failed to read row");
		clearerr(fd);
		goto restart;
	}

	++rows;
	last_read_lsn = row->lsn;
	return row;
eof:
	eof_offset = ftello(fd);
	if (eof_offset == good_offset + mdesc.eof_size) {
		if (mdesc.eof_size == 0) {
			eof = 1;
			return NULL;
		}

		fseeko(fd, good_offset, SEEK_SET);

		magic = 0;
		/* reset stream status if we reached eof before */
		if (feof(fd))
			clearerr(fd);

		if (fread(&magic, mdesc.eof_size, 1, fd) != 1) {
			fseeko(fd, good_offset, SEEK_SET);
			return NULL;
		}

		if (magic != mdesc.eof) {
			fseeko(fd, good_offset, SEEK_SET);
			return NULL;
		}

		eof = 1;
		return NULL;
	}
	/* libc will try prepread sizeof(vbuf) bytes on fseeko,
	   and this behavior will trash system on continous log follow mode
	   since every fetch_row will result in seek + read(sizeof(vbuf)) */
	if (eof_offset != good_offset)
		fseeko(fd, good_offset, SEEK_SET);
	return NULL;
}

- (void)
follow:(follow_cb *)cb data:(void *)data
{
	if (ev_is_active(&stat))
		return;

	if (cb == NULL) {
		ev_stat_stop(&stat);
		return;
	}

	ev_stat_init(&stat, cb, filename, 0.);
	stat.interval = (ev_tstamp)cfg.wal_dir_rescan_delay / 10;
	stat.data = data;
	ev_stat_start(&stat);
}

- (i64)
next_lsn
{
	assert(next_lsn != 0);
	if ([dir isKindOf:[SnapDir class]])
		return next_lsn;
	return next_lsn + wet_rows;
}

- (void)
append_successful:(size_t)bytes
{
	if (no_wet) {
		rows++;
		return;
	}

	if (wet_rows_offset_size == wet_rows) {
		wet_rows_offset_size *= 2;
		wet_rows_offset = xrealloc(wet_rows_offset,
					   wet_rows_offset_size * sizeof(*wet_rows_offset));
	}

	off_t prev_offt = wet_rows == 0 ? offset : wet_rows_offset[wet_rows - 1];
	wet_rows_offset[wet_rows] = prev_offt + bytes;
	wet_rows++;
}

static void
assert_row(const struct row_v12 *row)
{
	(void)row;
	assert(row->tag & ~TAG_MASK);
	assert(row->len > 0); /* fwrite() has funny behavior if size == 0 */
}

- (const struct row_v12 *)
append_row:(struct row_v12 *)row data:(const void *)data
{
	(void)row; (void)data;
	panic("%s: virtual", __func__);
}

- (const struct row_v12 *)
append_row:(const void *)data len:(u32)len scn:(i64)scn tag:(u16)tag
{
	static struct row_v12 row;
	row = (struct row_v12){ .scn = scn,
				.tm = ev_now(),
				.tag = tag,
				.len = len };

	return [self append_row:&row data:data];
}

- (const struct row_v12 *)
append_row:(const void *)data len:(u32)len shard:(Shard *)shard tag:(u16)tag
{
	static struct row_v12 row;
	row = (struct row_v12){ .scn = shard->scn,
				.tm = ev_now(),
				.tag = tag,
				.shard_id = shard->id,
				.len = len };

	return [self append_row:&row data:data];
}

- (i64)
confirm_write
{
	assert(next_lsn != 0);
	assert(mode == LOG_WRITE);
	/* XXX teodor
	 * assert(!no_wet);
	 */

	off_t tail;

	if (wet_rows == 0)
		goto exit;

	if (fflush(fd) < 0) {
		say_syserror("fflush");

		tail = ftello(fd);

		say_debug3("%s offset:%llu tail:%lli", __func__, (long long)offset, (long long)tail);

		off_t confirmed_offset = 0;
		for (int i = 0; i < wet_rows; i++) {
			if (wet_rows_offset[i] > tail) {
				say_error("failed to sync %lli rows", (long long)(wet_rows - i));
				if (confirmed_offset) {
					if (fseeko(fd, confirmed_offset, SEEK_SET) == -1)
						say_syserror("fseeko");
					if (ftruncate(fileno(fd), confirmed_offset) == -1)
						say_syserror("ftruncate");
				}
				break;
			}
			confirmed_offset = wet_rows_offset[i];
			say_debug3("confirmed offset %lli", (long long)confirmed_offset);
			next_lsn++;
			rows++;
		}
	} else {
		tail = wet_rows_offset[wet_rows - 1];
		next_lsn += wet_rows;
		rows += wet_rows;
	}
#if HAVE_SYNC_FILE_RANGE
	sync_bytes += tail - offset;
	if (unlikely(sync_bytes > 32 * 4096)) {
		sync_file_range(fileno(fd), sync_offset, 0, SYNC_FILE_RANGE_WRITE);
		sync_offset += sync_bytes;
		sync_bytes = 0;
	}
#endif
	bytes_written += tail - offset;
	offset = tail;
	wet_rows = 0;
exit:
	return next_lsn - 1;
}

- (int)
fileno
{
	return fileno(fd);
}
@end

@implementation XLog04
- (u32) version { return 4; }

- (marker_desc_t)
marker_desc
{
	return (marker_desc_t){
		.marker = (u64)-1,
		.eof = (u64)0,
		.size = 8,
		.eof_size = 8
	};
}

- (int)
read_header
{
	char buf[256];
	char *r;
	int n, year, mon, day, hour, min, sec;
	r = fgets(buf, sizeof(buf), fd);
	if (r == NULL)
		return -1;
	n = sscanf(r, "%d %d %d %d:%d:%d\n", &year, &mon, &day, &hour, &min, &sec);
	if (n != 6) {
		return -1;
	}
	return 0;
}

struct tbuf *
convert_row_v04_to_v12(struct tbuf *m)
{
	struct tbuf *n = tbuf_alloc(m->pool);
	tbuf_append(n, NULL, sizeof(struct row_v12));
	row_v12(n)->scn = row_v12(n)->lsn = _row_v04(m)->lsn;
	row_v12(n)->tm = 0;
	row_v12(n)->len = _row_v04(m)->len + sizeof(u16); /* tag */
	row_v12(n)->tag = wal_data | TAG_WAL;

	tbuf_add_dup(n, &_row_v04(m)->type);
	tbuf_ltrim(m, sizeof(struct _row_v04));
	tbuf_append(n, m->ptr, row_v12(n)->len - sizeof(u16));

	row_v12(n)->data_crc32c = crc32c(0, row_v12(n)->data, row_v12(n)->len);
	row_v12(n)->header_crc32c = crc32c(0, n->ptr + field_sizeof(struct row_v12, header_crc32c),
					   sizeof(struct row_v12) - field_sizeof(struct row_v12, header_crc32c));

	return n;
}

- (struct row_v12 *)
read_row
{
	struct tbuf *m = tbuf_alloc(fiber->pool);

	u32 row_crc, calc_crc;
	tbuf_append(m, NULL, sizeof(struct _row_v04));
	if (fread(m->ptr, sizeof(struct _row_v04), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	tbuf_append(m, NULL, _row_v04(m)->len);
	if (fread(_row_v04(m)->data, _row_v04(m)->len, 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	if (fread(&row_crc, sizeof(row_crc), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	calc_crc = crc32(m->ptr, tbuf_len(m));
	if (row_crc != calc_crc) {
		say_error("data crc32c mismatch %x %x", row_crc, calc_crc);
		return NULL;
	}

	say_debug2("%s: LSN:%"PRIu64, __func__, _row_v04(m)->lsn);

	return convert_row_v04_to_v12(m)->ptr;
}

@end

@implementation XLog03Template
- (u32) version { return 3; }

- (int)
read_header
{
	char buf[256];
	char *r;
	int n, year, mon, day, hour, min, sec;
	r = fgets(buf, sizeof(buf), fd);
	if (r == NULL)
		return -1;
	n = sscanf(r, "%d %d %d %d:%d:%d\n", &year, &mon, &day, &hour, &min, &sec);
	if (n != 6) {
		return -1;
	}
	return 0;
}

- (marker_desc_t)
marker_desc
{
	return (marker_desc_t){
		.marker = (u64)0xffffffff,
		.size = 4,
		.eof_size = 0
	};
}
@end

@implementation XLog11
- (u32) version { return 11; }

struct tbuf *
convert_row_v11_to_v12(struct tbuf *m)
{
	struct tbuf *n = tbuf_alloc(m->pool);
	tbuf_append(n, NULL, sizeof(struct row_v12));
	row_v12(n)->scn = row_v12(n)->lsn = _row_v11(m)->lsn;
	row_v12(n)->tm = _row_v11(m)->tm;
	row_v12(n)->len = _row_v11(m)->len - sizeof(u16) - sizeof(u64); /* tag & cookie */

	tbuf_ltrim(m, sizeof(struct _row_v11));

	u16 tag = read_u16(m);
	if (tag == (u16)-1) {
		row_v12(n)->tag = snap_data|TAG_SNAP;
	} else if (tag == (u16)-2) {
		row_v12(n)->tag = wal_data|TAG_WAL;
	} else {
		say_error("unknown tag %i", (int)tag);
		return NULL;
	}

	(void)read_u64(m); /* ignore cookie */
	tbuf_append(n, m->ptr, row_v12(n)->len);

	row_v12(n)->data_crc32c = crc32c(0, m->ptr, row_v12(n)->len);
	row_v12(n)->header_crc32c = crc32c(0, n->ptr + field_sizeof(struct row_v12, header_crc32c),
					   sizeof(struct row_v12) - field_sizeof(struct row_v12, header_crc32c));

	return n;
}

- (int)
write_header
{
	fwrite(dir->filetype, strlen(dir->filetype), 1, fd);
	fwrite(v11, strlen(v11), 1, fd);
	fwrite("\n", 1, 1, fd);
	if ((offset = ftello(fd)) < 0 || ferror(fd))
		return -1;
	return 0;
}

- (struct row_v12 *)
read_row
{
	struct tbuf *m = tbuf_alloc(fiber->pool);

	u32 header_crc, data_crc;

	tbuf_ensure(m, sizeof(struct _row_v11));
	if (fread(m->ptr, sizeof(struct _row_v11), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	tbuf_append(m, NULL, offsetof(struct _row_v11, data));

	/* header crc32c calculated on <lsn, tm, len, data_crc32c> */
	header_crc = crc32c(0, m->ptr + offsetof(struct _row_v11, lsn),
			    sizeof(struct _row_v11) - offsetof(struct _row_v11, lsn));

	if (_row_v11(m)->header_crc32c != header_crc) {
		say_error("header crc32c mismatch");
		return NULL;
	}

	tbuf_ensure(m, tbuf_len(m) + _row_v11(m)->len);
	if (fread(_row_v11(m)->data, _row_v11(m)->len, 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	tbuf_append(m, NULL, _row_v11(m)->len);

	data_crc = crc32c(0, _row_v11(m)->data, _row_v11(m)->len);
	if (_row_v11(m)->data_crc32c != data_crc) {
		say_error("data crc32c mismatch");
		return NULL;
	}

	if (tbuf_len(m) < sizeof(struct _row_v11) + sizeof(u16)) {
		say_error("row is too short");
		return NULL;
	}

	say_debug2("%s: LSN:%" PRIi64, __func__, _row_v11(m)->lsn);

	struct row_v12 *r = convert_row_v11_to_v12(m)->ptr;

	/* some of old tarantool snapshots has all rows with lsn == 0,
	   so using lsn from record will reset recovery lsn set by snap_initial_tag to 0,
	   also v11 snapshots imply that SCN === LSN */
	if (r->lsn == 0 && (r->tag & TAG_SNAP) == TAG_SNAP)
		r->scn = r->lsn = self->lsn;

	return r;
}


- (const struct row_v12 *)
append_row:(struct row_v12 *)row12 data:(const void *)data
{
	assert_row(row12);
	struct _row_v11 row;
	u16 tag = row12->tag & TAG_MASK;
	u32 data_len = row12->len;
	u64 cookie = 0;

	if (tag == snap_data) {
		tag = (u16)-1;
	} else if (tag == wal_data) {
		tag = (u16)-2;
	} else if (tag == snap_initial ||
		   tag == snap_final ||
		   tag == wal_final)
	{
		/* SEGV value non equal to NULL */
		return (const struct row_v12 *)(intptr_t)1;
	} else {
		say_error("unknown tag %i", (int)tag);
		errno = EINVAL;
		return NULL;
	}


	row12->lsn = row.lsn = [self next_lsn];
	if (row12->scn == 0)
		row12->scn = row.lsn;

	/* When running remote recovery of octopus (read: we'r replica) remote rows
	   come in v12 format with SCN != 0.
	   If cfg.io_compat enabled, ensure invariant LSN == SCN, since in this mode
	   rows doesn't have distinct SCN field. */

	if (row12->scn != row12->lsn) {
		say_error("io_compat mode doesn't support SCN tagged rows");
		errno = EINVAL;
		return NULL;
	}


	row.tm = ev_now();
	row.len = sizeof(tag) + sizeof(cookie) + data_len;

	row.data_crc32c = crc32c(0, (void *)&tag, sizeof(tag));
	row.data_crc32c = crc32c(row.data_crc32c, (void *)&cookie, sizeof(cookie));
	row.data_crc32c = crc32c(row.data_crc32c, data, data_len);

	row.header_crc32c = crc32c(0, (unsigned char *)&row + sizeof(row.header_crc32c),
				   sizeof(row) - sizeof(row.header_crc32c));

	if (fwrite(&marker, sizeof(marker), 1, fd) != 1 ||
	    fwrite(&row, sizeof(row), 1, fd) != 1 ||
	    fwrite(&tag, sizeof(tag), 1, fd) != 1 ||
	    fwrite(&cookie, sizeof(cookie), 1, fd) != 1 ||
	    fwrite(data, data_len, 1, fd) != 1)
	{
		say_syserror("fwrite");
		return NULL;
	}

	[self append_successful:sizeof(marker) + sizeof(row) +
				sizeof(tag) + sizeof(cookie) +
	                        data_len];
	return row12;
}

@end


@implementation XLog12
- (u32) version { return 12; }

- (int)
read_header
{
        char buf[256];
        char *r;
        for (;;) {
                r = fgets(buf, sizeof(buf), fd);
                if (r == NULL)
                        return -1;
		if (strcmp(r, "\n") == 0 || strcmp(r, "\r\n") == 0)
                        break;
        }
        return 0;
}

- (void)
write_header
{
	fwrite(dir->filetype, strlen(dir->filetype), 1, fd);
	fwrite(v12, strlen(v12), 1, fd);
	fprintf(fd, "Created-by: octopus\n");
	fprintf(fd, "Octopus-version: %s\n", octopus_version());
}

- (void)
write_header_scn:(const i64 *)scn
{
	if (scn[0])
		fprintf(fd, "SCN: %"PRIi64"\n", scn[0] + 1);
	for (int i = 0; i < MAX_SHARD; i++)
		if (scn[i])
			fprintf(fd, "SCN-%i: %"PRIi64"\n", i, scn[i] + 1);
}


- (struct row_v12 *)
read_row
{
	struct tbuf *m = tbuf_alloc(fiber->pool);

	u32 header_crc, data_crc;

	tbuf_ensure(m, sizeof(struct row_v12));
	if (fread(m->ptr, sizeof(struct row_v12), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	tbuf_append(m, NULL, offsetof(struct row_v12, data));

	/* header crc32c calculated on all fields before data_crc32c> */
	header_crc = crc32c(0, m->ptr + offsetof(struct row_v12, lsn),
			    sizeof(struct row_v12) - offsetof(struct row_v12, lsn));

	if (row_v12(m)->header_crc32c != header_crc) {
		say_error("header crc32c mismatch");
		return NULL;
	}

	tbuf_ensure(m, tbuf_len(m) + row_v12(m)->len);
	if (fread(row_v12(m)->data, row_v12(m)->len, 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	tbuf_append(m, NULL, row_v12(m)->len);

	data_crc = crc32c(0, row_v12(m)->data, row_v12(m)->len);
	if (row_v12(m)->data_crc32c != data_crc) {
		say_error("data crc32c mismatch");
		return NULL;
	}

	if (tbuf_len(m) < sizeof(struct row_v12)) {
		say_error("row is too short");
		return NULL;
	}

	say_debug2("%s: LSN:%" PRIi64, __func__, row_v12(m)->lsn);

	return m->ptr;
}

- (const struct row_v12 *)
append_row:(struct row_v12 *)row data:(const void *)data
{
	if (!header_written) {
		if (fputc('\n', fd) == EOF)
			return NULL;
		if ((offset = ftello(fd)) < 0 || ferror(fd))
		return NULL;
		header_written = true;
	}

	if ((row->tag & ~TAG_MASK) == 0)
		row->tag |= tag_mask;

	assert_row(row);

	row->lsn = [self next_lsn];
	row->scn = row->scn ?: row->lsn;
	row->data_crc32c = crc32c(0, data, row->len);
	row->header_crc32c = crc32c(0, (unsigned char *)row + sizeof(row->header_crc32c),
				   sizeof(*row) - sizeof(row->header_crc32c));

#if LOG_IO_ERROR_INJECT
	const void *ptr = data;
	int len = row->len;
	while ((ptr = memmem(ptr, len, "sleep", 5))) {
		sleep(3);
		ptr += 5;
		len = row->len - (ptr - data);
	}
	if (memmem(data, row->len, "error", 5))
		return NULL;
#endif

	if (fwrite(&marker, sizeof(marker), 1, fd) != 1 ||
	    fwrite(row, sizeof(*row), 1, fd) != 1 ||
	    fwrite(data, row->len, 1, fd) != 1)
	{
		say_syserror("fwrite");
		return NULL;
	}

	[self append_successful:sizeof(marker) + sizeof(*row) + row->len];
	return row;
}
@end

@implementation XLogDir
- (id)
init_dirname:(const char *)dirname_
{
        dirname = dirname_;
	fd = open(dirname, O_RDONLY);
	if (fd < 0)
		panic("can't open dir: %s: %s", dirname, strerror_o(errno));
	xlog_class = [XLog12 class];
        return self;
}

- (int)
sync
{
	return fsync(fd);
}

- (id)
free
{
	close(fd);
	return [super free];
}

- (int)
lock
{
	return flock(fd, LOCK_EX|LOCK_NB);
}

- (int)
stat:(struct stat *)buf
{
	return fstat(fd, buf);
}

static int
cmp_i64(const void *_a, const void *_b)
{
	const i64 *a = _a, *b = _b;
	if (*a == *b)
		return 0;
	return (*a > *b) ? 1 : -1;
}

- (ssize_t)
scan_dir:(i64 **)ret_lsn
{
	DIR *dh = NULL;
	struct dirent *dent;
	i64 *lsn;
	size_t i = 0, size = 1024;
	char *parse_suffix;
	ssize_t result = -1;

	dh = opendir(dirname);
	if (dh == NULL)
		goto out;

	lsn = palloc(fiber->pool, sizeof(i64) * size);
	if (lsn == NULL)
		goto out;

	errno = 0;
	while ((dent = readdir(dh)) != NULL) {
		char *file_suffix = strrchr(dent->d_name, '.');

		if (file_suffix == NULL)
			continue;

		bool valid_suffix = strcmp(file_suffix, suffix) == 0;
		if (!valid_suffix)
			continue;

		lsn[i] = strtoll(dent->d_name, &parse_suffix, 10);
		if (strncmp(parse_suffix, suffix, strlen(suffix)) != 0) {
			/* d_name doesn't parse entirely, ignore it */
			say_warn("can't parse `%s', skipping", dent->d_name);
			continue;
		}

		if (lsn[i] == LLONG_MAX || lsn[i] == LLONG_MIN) {
			say_warn("can't parse `%s', skipping", dent->d_name);
			continue;
		}

		i++;
		if (i == size) {
			i64 *n = palloc(fiber->pool, sizeof(i64) * size * 2);
			if (n == NULL)
				goto out;
			memcpy(n, lsn, sizeof(i64) * size);
			lsn = n;
			size = size * 2;
		}
	}

	qsort(lsn, i, sizeof(i64), cmp_i64);

	*ret_lsn = lsn;
	result = i;
      out:
	if (errno != 0)
		say_syserror("error reading directory `%s'", dirname);

	if (dh != NULL)
		closedir(dh);
	return result;
}

- (i64)
greatest_lsn
{
	i64 *lsn;
	ssize_t count = [self scan_dir:&lsn];

	if (count <= 0)
		return count;

	return lsn[count - 1];
}

- (XLog *)
containg_lsn:(i64)target_lsn
{
	i64 *lsn;
	ssize_t count = [self scan_dir:&lsn];

	if (count <= 0)
		return nil;

	if (target_lsn < *lsn) {
		say_warn("%s: requested LSN:%"PRIi64" is missing", __func__, target_lsn);
		return nil;
	}

	while (count > 1) {
		if (*lsn <= target_lsn && target_lsn < *(lsn + 1))
			goto out;
		lsn++;
		count--;
	}

	/*
	 * we can't check here for sure will or will not last file
	 * contain record with desired lsn since number of rows in file
	 * is not known beforehand. so, we simply return the last one.
	 */
out:
	say_debug2("%s: target_lsn:%"PRIi64 " file_lsn:%"PRIi64, __func__, target_lsn, *lsn);
	return [self open_for_read:*lsn];
}


- (const char *)
format_filename:(i64)lsn prefix:(const char *)prefix suffix:(const char *)extra_suffix
{
	static char filename[PATH_MAX + 1];
	snprintf(filename, sizeof(filename),
		 "%s%s/%020" PRIi64 "%s%s",
		 prefix, dirname, lsn, suffix, extra_suffix);
	return filename;
}

- (const char *)
format_filename:(i64)lsn suffix:(const char *)extra_suffix
{
	return [self format_filename:lsn prefix:"" suffix:extra_suffix];
}

- (const char *)
format_filename:(i64)lsn
{
	return [self format_filename:lsn suffix:""];
}


- (XLog *)
open_for_read:(i64)lsn
{
	const char *filename = [self format_filename:lsn];
	XLog *xlog = [XLog open_for_read_filename:filename dir:self];
	if (xlog)
		xlog->lsn = lsn;
	return xlog;
}

int allow_snap_overwrite = 0;
- (XLog *)
open_for_write:(i64)lsn
{
        XLog *l = nil;
        FILE *file = NULL;
        assert(lsn > 0);
	char *fbuf = NULL;

	const char *final_filename = [self format_filename:lsn];
	if (!allow_snap_overwrite && access(final_filename, F_OK) == 0) {
		errno = EEXIST;
		say_error("failed to create '%s': file already exists", final_filename);
		goto error;
	}

	const char *filename = [self format_filename:lsn suffix:inprogress_suffix];

	/* .inprogress file can't contain confirmed records, overwrite it silently */
	file = fopen(filename, "w");
	if (file == NULL) {
		say_syserror("fopen of %s for writing failed", filename);
		goto error;
	}
	fbuf = set_file_buf(file, 1024 * 1024);

	l = [[xlog_class alloc] init_filename:filename fd:file dir:self vbuf:fbuf];

	/* reset local variables: they are included in l */
	fbuf = NULL;
	file = NULL;

	l->next_lsn = lsn;
	l->mode = LOG_WRITE;
	l->inprogress = 1;

	[l write_header];
	return l;
      error:
        if (file != NULL)
                fclose(file);
	free(fbuf);
        [l free];
	return NULL;
}


static i64
find(int count, const char *type, i64 needle, i64 *haystack, i64 *lsn)
{
	haystack += count - 1;
	lsn += count - 1;

	while (count) {
		if (*haystack < 0) /* error */
			return -1;
		/* if *haystack == 0 то вернуть последний файл, потому что мы не знаем заранее ,
		 есть ли в этом файле нужная запись или нет */
		if (*haystack <= needle) {
			say_debug2("%s: %s:%"PRIi64 " file_lsn:%"PRIi64, __func__, type, needle, *lsn);
			return *lsn;
		}

		haystack--;
		lsn--;
		count --;
	}

	say_warn("%s: requested %s:%"PRIi64" is missing", __func__, type, needle);
	return -1;
}

- (i64 *)
scan_scn_shard:(int)target_shard_id lsn:(i64 *)lsn count:(int)count
{
	i64 *scn = p0alloc(fiber->pool, sizeof(i64) * count);

	/* scn[i] == -1 if error,
	              0 if not present
		      b otherwise */

	for (int i = 0; i < count; i++) {
		const char *filename = [self format_filename:lsn[i]];
		FILE *file = fopen(filename, "r");
		if (file == NULL) {
			say_syserror("fopen of %s for reading failed", filename);
			scn[i] = -1;
			continue;
		}

		for (;;) {
			i64 tmp;
			int shard_id;
			char buf[256];

			if (fgets(buf, sizeof(buf), file) == NULL) {
				say_syserror("fgets");
				scn[i] = -1;
				break;
			}

			if (strcmp(buf, "\n") == 0 || strcmp(buf, "\r\n") == 0)
				break;

			if (sscanf(buf, "SCN-%i: %"PRIi64, &shard_id, &tmp) == 2) {
				if (target_shard_id == shard_id) {
					scn[i] = tmp;
					break;
				}
			}
		}
	}
	return scn;
}

- (XLog *)
find_with_lsn:(i64)lsn
{
	i64 *dir_lsn;
	ssize_t count = [self scan_dir:&dir_lsn];

	i64 file_lsn = find(count, "LSN", lsn, dir_lsn, dir_lsn);
	if (file_lsn <= 0)
		return nil;
	return [self open_for_read:file_lsn];
}

- (XLog *)
find_with_scn:(i64)scn shard:(int)shard_id
{
	i64 *dir_lsn, *dir_scn;
	ssize_t count = [self scan_dir:&dir_lsn];
	dir_scn = [self scan_scn_shard:shard_id lsn:dir_lsn count:count];

	i64 file_lsn = find(count, "SCN", scn, dir_scn, dir_lsn);
	if (file_lsn <= 0)
		return nil;
	return [self open_for_read:file_lsn];
}

@end

@implementation WALDir
- (XLogDir *)
init_dirname:(const char *)dirname_
{
        if ((self = [super init_dirname:dirname_])) {
		filetype = xlog_mark;
		suffix = ".xlog";
	}
	return self;
}
@end

@interface Snap12 : XLog12 {
	size_t bytes;
	ev_tstamp step_ts, last_ts;
}@end

@implementation Snap12
- (XLog *)
init_filename:(const char *)filename_
           fd:(FILE *)fd_
          dir:(XLogDir *)dir_
	  vbuf:(char*)vbuf_
{
	[super init_filename:filename_ fd:fd_ dir:dir_ vbuf:vbuf_];
	tag_mask = TAG_SNAP;
	return self;
}

- (const struct row_v12 *)
append_row:(struct row_v12 *)row12 data:(const void *)data
{
	const struct row_v12 *ret = [super append_row:row12 data:data];
	if (ret == NULL)
		return NULL;

	bytes += sizeof(*row12) + row12->len;

	if (rows & 31)
		return ret;

	ev_now_update();
	if (last_ts == 0) {
		last_ts = ev_now();
		step_ts = ev_now();
	}

	const int io_rate_limit = cfg.snap_io_rate_limit * 1024 * 1024;
	if (io_rate_limit <= 0) {
		if (ev_now() - step_ts > 0.1) {
			if ([self flush] < 0)
				return NULL;
			if (cfg.snap_fadvise_dont_need)
				[self fadvise_dont_need];
			ev_now_update();
			step_ts = ev_now();
		}
		return ret;
	}

	if (ev_now() - step_ts > 0.02) {
		double delta = ev_now() - last_ts;
		size_t bps = bytes / delta;

		if (bps > io_rate_limit) {
			if ([self flush] < 0)
				return NULL;
			if (cfg.snap_fadvise_dont_need)
				[self fadvise_dont_need];
			ev_now_update();
			delta = ev_now() - last_ts;
			bps = bytes / delta;
		}

		if (bps > io_rate_limit) {
			double sec = delta * (bps - io_rate_limit) / io_rate_limit;
			usleep(sec * 1e6);
			ev_now_update();
		}
		step_ts = ev_now();
	}

	if (ev_now() > last_ts + 1) {
		bytes = 0;
		last_ts = step_ts = ev_now();
	}

	return ret;
}

#if HAVE_POSIX_FADVISE
- (int)
close
{
	if (fd)
		posix_fadvise(fileno(fd), 0, ftello(fd), POSIX_FADV_DONTNEED);
	return [super close];
}
#endif
@end


@implementation SnapDir
- (id)
init_dirname:(const char *)dirname_
{
        if ((self = [super init_dirname:dirname_])) {
		filetype = snap_mark;
		suffix = ".snap";
	}
	// rate limiting only v12 snapshots
	if (xlog_class == [XLog12 class])
		xlog_class = [Snap12 class];
        return self;
}
@end


static void
hexdump(struct tbuf *out, u16 tag __attribute__((unused)), struct tbuf *row)
{
	tbuf_printf(out, "%s", tbuf_to_hex(row));
}

void
print_row(struct tbuf *buf, const struct row_v12 *row,
	  void (*handler)(struct tbuf *out, u16 tag, struct tbuf *row))
{
	static struct tbuf row_data;
	row_data = TBUF(row->data, row->len, fiber->pool);

	int tag = row->tag & TAG_MASK;
	int inner_tag;
	u64 ballot;
	u32 value_len;

	static int print_header = -1;
	if (print_header == -1) {
		if (getenv("OCTOPUS_CAT_ROW_HEADER"))
			print_header = atoi(getenv("OCTOPUS_CAT_ROW_HEADER"));
		else
			print_header = 1;
	}
	if (print_header == 1) {
		//tbuf_printf(buf, "lsn:%" PRIi64, row->lsn);
		tbuf_append_lit(buf, "lsn:");
		tbuf_putl(buf, row->lsn);
		if (row->scn != -1) {
			//tbuf_printf(buf, " shard:%i", row->shard_id);
			tbuf_append_lit(buf, " shard:");
			tbuf_putu(buf, row->shard_id);
			i64 rem_scn = 0;
			memcpy(&rem_scn, row->remote_scn, 6);
			if (rem_scn) {
				tbuf_printf(buf, " rem_scn:%"PRIi64, rem_scn);
				tbuf_append_lit(buf, " rem_scn:");
				tbuf_putl(buf, rem_scn);
			}
		}

		//tbuf_printf(buf, " scn:%" PRIi64 " tm:%.3f t:%s ",
			    //row->scn, row->tm,
			    //xlog_tag_to_a(row->tag));
		tbuf_append_lit(buf, " scn:");
		tbuf_putl(buf, row->scn);
		tbuf_append_lit(buf, " tm:");
		tbuf_putl(buf, row->tm);
		tbuf_putc(buf, '.');
		tbuf_putl(buf, (long)((row->tm-(long)row->tm)*1000));
		tbuf_append_lit(buf, " t:");
		tbuf_append_lit(buf, xlog_tag_to_a(row->tag));
		tbuf_putc(buf, ' ');
	}

	if (!handler)
		handler = hexdump;

	if (!print_header && (tag == wal_data || tag == snap_data || tag >= user_tag)) {
		handler(buf, row->tag, &row_data);
		return;
	}

	switch (tag) {
	case snap_initial:
		if (tbuf_len(&row_data) == sizeof(u32) * 3) {
			u32 count = read_u32(&row_data);
			u32 log = read_u32(&row_data);
			u32 mod = read_u32(&row_data);
			tbuf_printf(buf, "count:%u run_crc_log:0x%08x run_crc_mod:0x%08x",
				    count, log, mod);
		} else if (row->scn == -1) {
			u8 ver = read_u8(&row_data);
			u32 count = read_u32(&row_data);
			u32 flags = read_u32(&row_data);
			tbuf_printf(buf, "ver:%i count:%u flags:0x%08x", ver, count, flags);
		} else {
			tbuf_printf(buf, "unknow format");
		}

		break;
	case run_crc: {
		i64 scn = -1;
		if (tbuf_len(&row_data) == sizeof(i64) + 2 * sizeof(u32))
			scn = read_u64(&row_data);
		u32 log = read_u32(&row_data);
		(void)read_u32(&row_data); /* ignore run_crc_mod */
		tbuf_printf(buf, "SCN:%"PRIi64 " log:0x%08x", scn, log);
		break;
	}
	case shard_alter:
	case shard_create: {
		int ver = read_u8(&row_data);
		if (ver != 0) {
			tbuf_printf(buf, "unknow version: %i", ver);
			break;
		}

		int type = read_u8(&row_data);
		u32 estimated_row_count = read_u32(&row_data);
		u32 run_crc = read_u32(&row_data);
		const char *mod_name = read_bytes(&row_data, 16);

		switch (tag & TAG_MASK) {
		case shard_create: tbuf_printf(buf, "SHARD_CREATE"); break;
		case shard_alter: tbuf_printf(buf, "SHARD_ALTER"); break;
		default: assert(false);
		}
		tbuf_printf(buf, " shard_id:%i", row->shard_id);

		switch (type) {
		case SHARD_TYPE_PAXOS: tbuf_printf(buf, " PAXOS"); break;
		case SHARD_TYPE_POR: tbuf_printf(buf, " POR"); break;
		case SHARD_TYPE_PART: tbuf_printf(buf, " PART"); break;
		default: assert(false);
		}
		tbuf_printf(buf, " %s", mod_name);
		tbuf_printf(buf, " count:%i run_crc:0x%08x", estimated_row_count, run_crc);
		tbuf_printf(buf, " master:%s", (const char *)read_bytes(&row_data, 16));
		while (tbuf_len(&row_data) > 0) {
			const char *str = read_bytes(&row_data, 16);
			if (strlen(str))
			    tbuf_printf(buf, " repl:%s", str);
		}

		break;
	}
	case shard_final:
	case snap_final:
	case nop:
		break;

	case paxos_promise:
	case paxos_nop:
		ballot = read_u64(&row_data);
		tbuf_printf(buf, "ballot:%"PRIx64, ballot);
		break;
	case paxos_accept:
		ballot = read_u64(&row_data);
		inner_tag = read_u16(&row_data);
		value_len = read_u32(&row_data);
		(void)value_len;
		assert(value_len == tbuf_len(&row_data));
		tbuf_printf(buf, "ballot:%"PRIx64" it:%s ", ballot, xlog_tag_to_a(inner_tag));

		switch(inner_tag & TAG_MASK) {
		case run_crc: {
			i64 scn = read_u64(&row_data);
			u32 log = read_u32(&row_data);
			(void)read_u32(&row_data); /* ignore run_crc_mod */
			tbuf_printf(buf, "SCN:%"PRIi64 " log:0x%08x", scn, log);
			break;
		}
		case nop:
			break;
		default:
			handler(buf, inner_tag, &row_data);
			break;
		}
		break;
	default:
		handler(buf, row->tag, &row_data);
	}
}



register_source();
