/*
 * Copyright (C) 2010, 2011, 2012 Mail.RU
 * Copyright (C) 2010, 2011, 2012 Yuriy Vostrikov
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

#include <third_party/crc32.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
const char *snap_mark = "SNAP\n";
const char *xlog_mark = "XLOG\n";
const char *inprogress_suffix = ".inprogress";
const u32 marker = 0xba0babed;
const u32 eof_marker = 0x10adab1e;

const char *
xlog_tag_to_a(u16 tag)
{
	static char buf[16];
	char *p = buf;
	u16 tag_type = tag & ~TAG_MASK;
	tag &= TAG_MASK;

	switch (tag_type) {
	case TAG_SNAP: p += sprintf(p, "snap/"); break;
	case TAG_WAL: p += sprintf(p, "wal/"); break;
	case TAG_SYS: p += sprintf(p, "sys/"); break;
	default: p += sprintf(p, "%i/", tag_type >> TAG_SIZE);
	}

	switch (tag) {
	case snap_initial_tag:	strcat(p, "snap_initial_tag"); break;
	case snap_tag:		strcat(p, "snap_tag"); break;
	case wal_tag:		strcat(p, "wal_tag"); break;
	case snap_final_tag:	strcat(p, "snap_final_tag"); break;
	case wal_final_tag:	strcat(p, "wal_final_tag"); break;
	case run_crc:		strcat(p, "run_crc"); break;
	case nop:		strcat(p, "nop"); break;
	case paxos_prepare:	strcat(p, "paxos_prepare"); break;
	case paxos_promise:	strcat(p, "paxos_promise"); break;
	case paxos_propose:	strcat(p, "paxos_propose"); break;
	case paxos_accept:	strcat(p, "paxos_accept"); break;
	case snap_skip_scn:	strcat(p, "snap_skip_scn"); break;
	default:
		if (tag < user_tag)
			sprintf(p, "sys%i", tag);
		else
			sprintf(p, "usr%i", tag >> 5);
	}
	return buf;
}

@implementation XLog
- (bool) eof { return eof; }
- (u32) version { return 0; }

- (XLog *)
init_filename:(const char *)filename_
           fd:(FILE *)fd_
          dir:(XLogDir *)dir_
{
	[super init];
	filename = strdup(filename_);
	fd = fd_;
	mode = LOG_READ;
	dir = dir_;

#ifdef __GLIBC__
	/* libc will try prepread sizeof(vbuf) bytes on every fseeko,
	   so no reason to make vbuf particulary large */
	const int bufsize = 64 * 1024;
	vbuf = xmalloc(bufsize);
	setvbuf(fd, vbuf, _IOFBF, bufsize);
#endif
	offset = ftello(fd);
	return self;
}

+ (XLog *)
open_for_read_filename:(const char *)filename dir:(XLogDir *)dir
{
	char filetype_[32], version_[32];
	XLog *l = nil;
	FILE *fd;

	if ((fd = fopen(filename, "r")) == NULL)
		return nil;

	if (fgets(filetype_, sizeof(filetype_), fd) == NULL ||
	    fgets(version_, sizeof(version_), fd) == NULL)
	{
		if (feof(fd))
			say_error("unexpected EOF reading %s", filename);
		else
			say_syserror("can't read header of %s", filename);
		fclose(fd);
		return nil;
	}

	if (dir != NULL && strncmp(dir->filetype, filetype_, sizeof(filetype_)) != 0) {
		say_error("filetype mismatch of %s", filename);
		fclose(fd);
		return nil;
	}

	if (strcmp(version_, v11) == 0) {
		l = [XLog11 alloc];
	} else if (strcmp(version_, v12) == 0) {
		l = [XLog12 alloc];
	} else {
		say_error("bad version `%s' of %s", version_, filename);
		fclose(fd);
		return nil;
	}

	[l init_filename:filename fd:fd dir:dir];
	if ([l read_header] < 0) {
		if (feof(fd))
			say_error("unexpected EOF reading %s", filename);
		else
			say_syserror("can't read header of %s", filename);
		[l free];
		return nil;
	}

	return l;
}

- (id)
free
{
	if (fclose(fd) < 0)
		say_syserror("can't close");
	fd = NULL;

	free(filename);
	free(vbuf);
	return [super free];
}

- (size_t)
rows
{
	return rows + wet_rows;
}

- (size_t)
wet_rows_offset_available
{
	return nelem(wet_rows_offset) - wet_rows;
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
	return result;
}

- (int)
close
{
	int result = 0;

	if (mode == LOG_WRITE) {
		if (fwrite(&eof_marker, sizeof(eof_marker), 1, fd) != 1) {
			result = -1;
			say_error("can't write eof_marker");
		}
		/* partially written tail of WAL will be cause of LSN
		   gap, which will prevent server from startup.
		   NB: it's ok to lose tail from last WAL. */
		if ([self flush] == -1)
			result = -1;
	} else {
		if (rows == 0 && access(filename, F_OK) == 0) {
			bool legacy_snap = [self isMemberOf:[XLog11 class]] &&
					   [dir isMemberOf:[SnapDir class]];
			if (!legacy_snap)
				panic("no valid rows were read");
		}
#if HAVE_SYNC_FILE_RANGE
		if (sync_file_range(fileno(fd), 0, 0, SYNC_FILE_RANGE_WRITE) < 0)
			say_syserror("sync_file_range");
#endif
	}

	ev_stat_stop(&stat);

	[self free];
	return result;
}

- (int)
flush
{
	if (fflush(fd) < 0) {
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
	end -= end % 4096;
	posix_fadvise(fileno(fd), 0, end, POSIX_FADV_DONTNEED);
#endif
}

- (int)
write_header
{
	assert(false);
	return 0;
}

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

- (struct row_v12 *)
read_row
{
	return NULL;
}

- (struct row_v12 *)
fetch_row
{
	struct row_v12 *row;
	u32 magic;
	off_t marker_offset = 0, good_offset, eof_offset;

	assert(sizeof(magic) == sizeof(marker));
	good_offset = ftello(fd);

restart:
	if (marker_offset > 0)
		fseeko(fd, marker_offset + 1, SEEK_SET);

	say_debug("%s: start offt %08" PRIofft, __func__, ftello(fd));
	if (fread(&magic, sizeof(marker), 1, fd) != 1)
		goto eof;

	while (magic != marker) {
		int c = fgetc(fd);
		if (c == EOF)
			goto eof;
		magic >>= 8;
		magic |= (((u32)c & 0xff) << ((sizeof(magic) - 1) * 8));
	}
	marker_offset = ftello(fd) - sizeof(marker);
	if (good_offset != marker_offset)
		say_warn("skipped %" PRIofft " bytes after %08" PRIofft " offset",
			 marker_offset - good_offset, good_offset);
	say_debug("	magic found at %08" PRIofft, marker_offset);

	row = [self read_row];

	if (row == NULL) {
		if (feof(fd))
			goto eof;
		say_warn("failed to read row");
		clearerr(fd);
		goto restart;
	}

	++rows;
	if ((row->tag & ~TAG_MASK) == 0) /* old style row */
		row->tag = fix_tag(row->tag);
	return row;
eof:
	eof_offset = ftello(fd);
	if (eof_offset == good_offset + sizeof(eof_marker)) {
		fseeko(fd, good_offset, SEEK_SET);

		if (fread(&magic, sizeof(eof_marker), 1, fd) != 1) {
			fseeko(fd, good_offset, SEEK_SET);
			return NULL;
		}

		if (memcmp(&magic, &eof_marker, sizeof(eof_marker)) != 0) {
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
	ev_stat_stop(&stat);
	ev_stat_init(&stat, cb, filename, 0.);
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
append_row:(void *)data len:(u32)len scn:(i64)scn tag:(u16)tag
{
	assert(wet_rows < nelem(wet_rows_offset));
	static struct row_v12 row;
	row = (struct row_v12){ .scn = scn,
				.tm = ev_now(),
				.tag = tag,
				.cookie = default_cookie,
				.len = len };

	return [self append_row:&row data:data];
}

- (const struct row_v12 *)
append_row:(const void *)data len:(u32)len scn:(i64)scn tag:(u16)tag cookie:(u64)cookie
{
	assert(wet_rows < nelem(wet_rows_offset));
	static struct row_v12 row;
	row = (struct row_v12){ .scn = scn,
				.tm = ev_now(),
				.tag = tag,
				.cookie = cookie,
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

	if (fflush(fd) < 0) {
		say_syserror("fflush");

		tail = ftello(fd);

		say_debug("%s offset:%llu tail:%lli", __func__, (long long)offset, (long long)tail);

		for (int i = 0; i < wet_rows; i++) {
			if (wet_rows_offset[i] > tail) {
				say_error("failed to sync %lli rows", (long long)(wet_rows - i));
				break;
			}
			say_debug("confirm offset %lli", (long long)wet_rows_offset[i]);
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

	return next_lsn - 1;
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
		row_v12(n)->tag = snap_tag;
	} else if (tag == (u16)-2) {
		row_v12(n)->tag = wal_tag;
	} else {
		say_error("unknown tag %i", (int)tag);
		return NULL;
	}

	row_v12(n)->cookie = read_u64(m);
	tbuf_append(n, m->ptr, row_v12(n)->len);

	row_v12(n)->data_crc32c = crc32c(0, m->ptr, row_v12(n)->len);
	row_v12(n)->header_crc32c = crc32c(0, n->ptr + field_sizeof(struct row_v12, header_crc32c),
					   sizeof(struct row_v12) - field_sizeof(struct row_v12, header_crc32c));

	return n;
}

- (int)
write_header
{
	if (fwrite(dir->filetype, strlen(dir->filetype), 1, fd) != 1)
		return -1;
	if (fwrite(v11, strlen(v11), 1, fd) != 1)
		return -1;
	if (fwrite("\n", 1, 1, fd) != 1)
                return -1;
	if ((offset = ftello(fd)) < 0)
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

	say_debug("read row v11 success lsn:%" PRIi64, _row_v11(m)->lsn);

	return convert_row_v11_to_v12(m)->ptr;
}


- (const struct row_v12 *)
append_row:(struct row_v12 *)row12 data:(const void *)data
{
	assert_row(row12);
	struct _row_v11 row;
	u16 tag = row12->tag & TAG_MASK;
	u32 data_len = row12->len;
	u64 cookie = row12->cookie;

	assert(wet_rows < nelem(wet_rows_offset));

	if (tag == snap_tag) {
		tag = (u16)-1;
	} else if (tag == wal_tag) {
		tag = (u16)-2;
	} else if (tag == snap_initial_tag ||
		   tag == snap_final_tag ||
		   tag == wal_final_tag)
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

- (i64)
scn
{
	return next_scn;
}

- (int)
read_header
{
        char buf[256];
        char *r;
        for (;;) {
                r = fgets(buf, sizeof(buf), fd);
                if (r == NULL)
                        return -1;
		sscanf(r, "SCN: %"PRIi64"\n", &next_scn);
		if (strcmp(r, "\n") == 0 || strcmp(r, "\r\n") == 0)
                        break;
        }
        return 0;
}

- (int)
write_header
{
	const char *comment = "Created-by: octopus\n";
	char buf[64];
	if (fwrite(dir->filetype, strlen(dir->filetype), 1, fd) != 1)
		return -1;
	if (fwrite(v12, strlen(v12), 1, fd) != 1)
		return -1;
	if (fwrite(comment, strlen(comment), 1, fd) != 1)
                return -1;
	snprintf(buf, sizeof(buf), "SCN: %"PRIi64"\n", next_scn);
	if (fwrite(buf, strlen(buf), 1, fd) != 1)
                return -1;
	if (fwrite("\n", 1, 1, fd) != 1)
                return -1;
	if ((offset = ftello(fd)) < 0)
		return -1;
	return 0;
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

	if (tbuf_len(m) < sizeof(struct row_v12) + sizeof(u16)) {
		say_error("row is too short");
		return NULL;
	}

	say_debug("read row v12 success lsn:%" PRIi64, row_v12(m)->lsn);

	return m->ptr;
}

- (const struct row_v12 *)
append_row:(struct row_v12 *)row data:(const void *)data
{
	assert_row(row);

	row->lsn = [self next_lsn];
	row->scn = row->scn ?: row->lsn;
	row->data_crc32c = crc32c(0, data, row->len);
	row->header_crc32c = crc32c(0, (unsigned char *)row + sizeof(row->header_crc32c),
				   sizeof(*row) - sizeof(row->header_crc32c));

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
        return self;
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
	say_debug("%s: target_lsn:%"PRIi64 " file_lsn:%"PRIi64, __func__, target_lsn, *lsn);
	return [self open_for_read:*lsn];
}

- (i64)
containg_scn:(i64)target_scn
{
	i64 *lsn;
	ssize_t count = [self scan_dir:&lsn];
	XLog *l = nil;
	const i64 initial_lsn = 2;

	/* new born master without a single commit */
	if (count == 0 && target_scn <= 2)
		return initial_lsn;

	if (count <= 0) {
		say_error("%s: WAL dir is either empty or unreadable", __func__);
		return -1;
	}

	for (int i = 0; i < count; i++) {
		l = [self open_for_read:lsn[i]];
		i64 scn = [l respondsTo:@selector(scn)] ? [(id)l scn] : lsn[i];
		[l fetch_row];
		[l close];

		/* handly buggy headers where "SCN: 0" :
		   assume they were written with cfg.sync_scn_with_lsn=1 */
		if (scn == 0)
			scn = lsn[i];

		if (scn >= target_scn)
			return i > 0 ? lsn[i - 1] : initial_lsn;
	}

	return lsn[count - 1];
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
	return [XLog open_for_read_filename:filename dir:self];
}

- (XLog *)
open_for_write:(i64)lsn scn:(i64)scn
{
        XLog *l = nil;
        FILE *file = NULL;
	int fd = -1;
        assert(lsn > 0);


	const char *final_filename = [self format_filename:lsn];
	if (access(final_filename, F_OK) == 0) {
		errno = EEXIST;
		say_error("failed to create '%s': file already exists", final_filename);
		goto error;
	}

	const char *filename = [self format_filename:lsn suffix:inprogress_suffix];

	/* .inprogress file can't contain confirmed records, overwrite it silently */
	file = fopen(filename, "w");
	if (file == NULL) {
		say_syserror("fopen failed");
		goto error;
	}

	if (cfg.io_compat) {
		l = [[XLog11 alloc] init_filename:filename fd:file dir:self];
		l->next_lsn = lsn;

	} else {
		l = [[XLog12 alloc] init_filename:filename fd:file dir:self];
		l->next_lsn = lsn;
		((XLog12 *)l)->next_scn = scn;
	}

	l->mode = LOG_WRITE;
	l->inprogress = 1;

	if ([l write_header] < 0) {
		say_syserror("failed to write header");
		goto error;
	}

	return l;
      error:
	if (fd >= 0)
		close(fd);
        if (file != NULL)
                fclose(file);
        [l free];
	return NULL;
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


@implementation SnapDir
- (id)
init_dirname:(const char *)dirname_
{
        if ((self = [super init_dirname:dirname_])) {
		filetype = snap_mark;
		suffix = ".snap";
	}
        return self;
}
@end



register_source();
