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

#import <config.h>
#import <util.h>
#import <fiber.h>
#import <log_io.h>
#import <palloc.h>
#import <say.h>
#import <pickle.h>
#import <tbuf.h>

#include <third_party/crc32.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include <objc/objc-api.h>

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
	switch (tag) {
	case snap_initial_tag: return "snap_initial_tag";
	case snap_tag: return "snap_tag";
	case wal_tag: return "wal_tag";
	case snap_final_tag: return "snap_final_tag";
	case wal_final_tag: return "wal_final_tag";
	}
	return "<unknow tag>";
}

@implementation XLogDir
- (id) init_dirname:(const char *)dirname_
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

		char *sub_suffix;
		if (recover_from_inprogress)
			sub_suffix = memrchr(dent->d_name, '.', file_suffix - dent->d_name);
		else
			sub_suffix = NULL;

		/*
		 * A valid suffix is either .xlog or * .xlog.inprogress,
                 * given recover_from_inprogress == true && suffix == 'xlog'
		 */

		bool valid_suffix;
		valid_suffix = (strcmp(file_suffix, suffix) == 0 ||
				(sub_suffix != NULL &&
				 strcmp(file_suffix, inprogress_suffix) == 0 &&
				 strncmp(sub_suffix, suffix, strlen(suffix)) == 0));

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

- (i64)
find_file_containg_lsn:(i64)target_lsn
{
	i64 *lsn;
	ssize_t count = [self scan_dir:&lsn];

	if (count <= 0)
		return count;

	while (count > 1) {
		if (*lsn <= target_lsn && target_lsn < *(lsn + 1)) {
			goto out;
			return *lsn;
		}
		lsn++;
		count--;
	}

	/*
	 * we can't check here for sure will or will not last file
	 * contain record with desired lsn since number of rows in file
	 * is not known beforehand. so, we simply return the last one.
	 */

      out:
	return *lsn;
}

- (const char *)
format_filename:(i64)lsn in_progress:(bool)in_progress
{
	static char filename[PATH_MAX + 1];

	if (in_progress)
                snprintf(filename, PATH_MAX, "%s/%020" PRIi64 "%s%s",
                         dirname, lsn, suffix, inprogress_suffix);
	else
		snprintf(filename, PATH_MAX, "%s/%020" PRIi64 "%s",
			 dirname, lsn, suffix);
	return filename;
}


- (id)
open_for_read_filename:(const char *)filename fd:(FILE *)fd lsn:(i64)lsn
{
	char filetype_[32], version_[32];
	char *error = "unknown error";
	XLog *l = nil;

	if (fgets(filetype_, sizeof(filetype_), fd) == NULL) {
		error = "header reading failed";
		goto error;
	}

	if (fgets(version_, sizeof(version_), fd) == NULL) {
		error = "header reading failed";
		goto error;
	}

	if (strcmp(version_, v11) == 0) {
		l = [XLog11 alloc];
	} else if (strcmp(version_, v12) == 0) {
		l = [XLog12 alloc];
	} else {
		error = "unknown version";
		goto error;
	}

        if (strncmp(filetype, filetype_, sizeof(filetype_)) != 0) {
                error = "unknown file type";
                goto error;
        }

	[l init_filename:filename
		      fd:fd
		    mode:LOG_READ
		     dir:self
		     lsn:lsn];

        if ([l read_header] < 0) {
                error = "header reading failed";
                goto error;
        }

	return l;

error:
	say_warn("[open_for_read_filename `%s']: %s", filename, error);
	[l free];
	l = [[XLog alloc] init_filename:filename
				     fd:fd
				   mode:LOG_READ
				    dir:self
				    lsn:lsn];
	l->valid = false;
	return l;
}


- (id)
open_for_read:(i64)lsn
{
        const char *filename = [self format_filename:lsn in_progress:false];
	FILE *fd = fopen(filename, "r");
	if (fd == NULL) {
		say_syserror("[open_for_read %"PRIi64"]: filename:`%s'",
			     lsn, filename);
		return nil;
	}
	return [self open_for_read_filename:filename
					 fd:fd
					lsn:lsn];
}

- (id)
open_for_write:(i64)lsn
{
        XLog *l = nil;
        FILE *file = NULL;
        assert(lsn > 0);

        // FIXME: filename leak
	const char *filename = strdup([self format_filename:lsn in_progress:true]);
	say_debug("[open_for_write `%s']", filename);

	if (access(filename, F_OK) == 0) {
		say_error("failed to open `%s': file already exists", filename);
		return NULL;
	}
	const char *final_filename = [self format_filename:lsn in_progress:false];
	if (access(final_filename, F_OK) == 0) {
		say_error("failed to open `%s': '%s' is in the way",
			  filename, final_filename);
		return NULL;
	}

	/*
	 * Open the <lsn>.<suffix>.inprogress file. If it
	 * exists, open will fail.
	 */
	int fd = open(filename, O_WRONLY | O_CREAT | O_EXCL | O_APPEND, 0664);
	if (fd < 0)
		goto error;

	file = fdopen(fd, "a");
	if (file == NULL)
		goto error;

	l = cfg.io_compat ? [XLog11 alloc] : [XLog12 alloc];
	l = [l init_filename:filename
			  fd:file
			mode:LOG_WRITE
			 dir:self
			 lsn:lsn];

	say_info("creating `%s'", l->filename);
	if ([l write_header] < 0) {
                say_error("failed to write header");
                goto error;
        }

	return l;
      error:
	say_error("failed to open `%s': %s", filename, strerror(errno));
        if (file != NULL)
                fclose(file);
        [l free];
	return NULL;
}
@end

@implementation WALDir
- (id)
init_dirname:(const char *)dirname_
{
        if ((self = [super init_dirname:dirname_])) {
		filetype = xlog_mark;
		suffix = ".xlog";
		recover_from_inprogress = true;
	}
	return self;
}

- (id)
open_for_read:(i64)lsn
{
	FILE *fd;
	const char *filename = [self format_filename:lsn in_progress:true];
	if ((fd = fopen(filename, "r")) != NULL) {
		XLog *l = [self open_for_read_filename:filename
						    fd:fd
						   lsn:lsn];
		l->inprogress = true;
		return l;
	}

	return [super open_for_read:lsn];
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


@implementation XLog
- (XLog *)
init_filename:(const char *)filename_
           fd:(FILE *)fd_
	 mode:(int)mode_
          dir:(XLogDir *)dir_
	  lsn:(i64)lsn
{
	[super init];
	valid = true;

	strncpy(filename, filename_, sizeof(filename));
	pool = palloc_create_pool(filename);
	fd = fd_;
	mode = mode_;
	dir = dir_;
	next_lsn = lsn;
	stat.data = dir->recovery_state;

	const int bufsize = 1024 * 1024;
	vbuf = malloc(bufsize);
	setvbuf(fd, vbuf, _IOFBF, bufsize);

        return self;
}

- (void)
free
{
	free(vbuf);
	palloc_destroy_pool(pool);
	[super free];
}

- (int)
inprogress_unlink
{
#ifndef NDEBUG
	char *suffix = strrchr(filename, '.');
	assert(suffix);
	assert(strcmp(suffix, inprogress_suffix) == 0);
#endif
        say_warn("unlink broken %s wal", filename);

	if (unlink(filename) != 0) {
		if (errno == ENOENT)
			return 0;

		say_syserror("can't unlink %s", filename);
		return -1;
	}

	return 0;
}

- (const char *)
final_filename
{
	char *final;
	char *suffix = strrchr(filename, '.');

	assert(suffix);
	assert(strcmp(suffix, inprogress_suffix) == 0);

	/* Create a new filename without '.inprogress' suffix. */
        final = palloc(pool, suffix - filename + 1);
        memcpy(final, filename, suffix - filename);
        final[suffix - filename] = '\0';
	return final;
}

- (void)
reset_inprogress
{
	strcpy(filename, [self final_filename]);
	inprogress = false;
}

- (int)
inprogress_rename
{
	const char *final_filename = [self final_filename];
	say_crit("renaming %s to %s", filename, final_filename);

	if (rename(filename, final_filename) != 0) {
		say_syserror("can't rename %s to %s", filename, final_filename);
		return -1;
	}
	strcpy(filename, final_filename);
	inprogress = false;
	return 0;
}

- (int)
close
{
	if (mode == LOG_WRITE) {
		if (fwrite(&eof_marker, sizeof(eof_marker), 1, fd) != 1)
			say_error("can't write eof_marker");
		[self flush];
	} else {
		if (rows == 0) {
			bool legacy_snap = cfg.io_compat &&
					   [dir isMemberOf:[SnapDir class]] &&
					   [dir->recovery_state lsn] == 1; /* TODO: check file lsn */
			if (!legacy_snap)
				panic("no valid rows were read");
		}
	}

	if (ev_is_active(&stat))
		ev_stat_stop(&stat);


	int r = fclose(fd);
	if (r < 0)
		say_syserror("can't close");

	[self free];
	return r;
}

- (int)
flush
{
	if (fflush(fd) < 0)
		return -1;

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

- (struct tbuf *)
read_row
{
	return NULL;
}

- (struct tbuf *)
next_row
{
	struct tbuf *row;
	u32 magic;
	off_t marker_offset = 0, good_offset;

	assert(sizeof(magic) == sizeof(marker));
	good_offset = ftello(fd);

restart:
	if (marker_offset > 0)
		fseeko(fd, marker_offset + 1, SEEK_SET);

	say_debug("next_row: start offt %08" PRIofft, ftello(fd));
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
	say_debug("magic found at %08" PRIofft, marker_offset);

	row = [self read_row];

	if (row == NULL) {
		if (feof(fd))
			goto eof;
		say_warn("failed to read row");
		clearerr(fd);
		goto restart;
	}

	if (++rows % 100000 == 0)
		say_info("%.1fM rows processed", rows / 1000000.);
	return row;
eof:
	if (ftello(fd) == good_offset + sizeof(eof_marker)) {
		fseeko(fd, good_offset, SEEK_SET);

		if (fread(&magic, sizeof(eof_marker), 1, fd) != 1) {
			fseeko(fd, good_offset, SEEK_SET);	/* seek back to last known good offset */
			return NULL;
		}

		if (memcmp(&magic, &eof_marker, sizeof(eof_marker)) != 0)
			return NULL;

		eof = 1;
		return NULL;
	}
	return NULL;
}

- (void)
follow:(follow_cb *)cb
{
	ev_stat_stop(&stat);
	ev_stat_init(&stat, cb, filename, 0.);
	ev_stat_start(&stat);
}

- (int)
append_row:(struct tbuf *)data tag:(u16)tag cookie:(u64)cookie
{
	(void)data; (void)tag; (void)cookie;
	assert(false);
}

- (void)
append_successful
{
	if (![dir isKindOf:[SnapDir class]])
		next_lsn++;
	rows++;
}
@end


@implementation XLog11

struct tbuf *
convert_row_v11_to_v12(struct tbuf *m)
{
	struct tbuf *n = tbuf_alloc(m->pool);
	tbuf_ensure(n, sizeof(struct row_v12));
	n->len = sizeof(struct row_v12);
	row_v12(n)->lsn = _row_v11(m)->lsn;
	row_v12(n)->tm = _row_v11(m)->tm;
	row_v12(n)->len = _row_v11(m)->len - sizeof(u16) - sizeof(u64); /* tag & cookie */

	tbuf_peek(m, sizeof(struct _row_v11));

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

	tbuf_append(n, m->data, row_v12(n)->len);
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
	return 0;
}

- (struct tbuf *)
read_row
{
	struct tbuf *m = tbuf_alloc(pool);

	u32 header_crc, data_crc;

	tbuf_ensure(m, sizeof(struct _row_v11));
	if (fread(m->data, sizeof(struct _row_v11), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	m->len = offsetof(struct _row_v11, data);

	/* header crc32c calculated on <lsn, tm, len, data_crc32c> */
	header_crc = crc32c(0, m->data + offsetof(struct _row_v11, lsn),
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

	m->len += _row_v11(m)->len;

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

	return convert_row_v11_to_v12(m);
}


- (int)
append_row:(struct tbuf *)data tag:(u16)tag cookie:(u64)cookie
{
	struct _row_v11 row;

	if (tag == snap_tag) {
		tag = (u16)-1;
	} else if (tag == wal_tag) {
		tag = (u16)-2;
	} else if (tag == snap_initial_tag ||
		   tag == snap_final_tag ||
		   tag == wal_final_tag)
	{
		return 0;
	} else {
		say_error("unknown tag %i", (int)tag);
		return -1;
	}

	row.lsn = next_lsn;
	row.tm = ev_now();
	row.len = sizeof(tag) + sizeof(cookie) + tbuf_len(data);

	row.data_crc32c = crc32c(0, (void *)&tag, sizeof(tag));
	row.data_crc32c = crc32c(row.data_crc32c, (void *)&cookie, sizeof(cookie));
	row.data_crc32c = crc32c(row.data_crc32c, data->data, tbuf_len(data));

	row.header_crc32c = crc32c(0, (unsigned char *)&row + sizeof(row.header_crc32c),
				   sizeof(row) - sizeof(row.header_crc32c));

	off_t offt = ftello(fd);
	if (fwrite(&marker, sizeof(marker), 1, fd) != 1 ||
	    fwrite(&row, sizeof(row), 1, fd) != 1 ||
	    fwrite(&tag, sizeof(tag), 1, fd) != 1 ||
	    fwrite(&cookie, sizeof(cookie), 1, fd) != 1 ||
	    fwrite(data->data, tbuf_len(data), 1, fd) != 1)
		goto fail;

	[self append_successful];
	return 0;
fail:
	say_syserror("fwrite");
	fflush(fd);
	fseeko(fd, offt, SEEK_SET);
	if (ftruncate(fileno(fd), offt) == -1)
		say_syserror("ftruncate(%s)", filename);
	return -1;
}

@end


@implementation XLog12

- (int)
write_header
{
	const char *comment = "Created-by: octopus\n";
	if (fwrite(dir->filetype, strlen(dir->filetype), 1, fd) != 1)
		return -1;
	if (fwrite(v12, strlen(v12), 1, fd) != 1)
		return -1;
	if (fwrite(comment, strlen(comment), 1, fd) != 1)
                return -1;
	if (fwrite("\n", 1, 1, fd) != 1)
                return -1;
	return 0;
}

- (struct tbuf *)
read_row
{
	struct tbuf *m = tbuf_alloc(pool);

	u32 header_crc, data_crc;

	tbuf_ensure(m, sizeof(struct row_v12));
	if (fread(m->data, sizeof(struct row_v12), 1, fd) != 1) {
		if (ferror(fd))
			say_error("fread error");
		return NULL;
	}

	m->len = offsetof(struct row_v12, data);

	/* header crc32c calculated on all fields before data_crc32c> */
	header_crc = crc32c(0, m->data + offsetof(struct row_v12, lsn),
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

	m->len += row_v12(m)->len;

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

	return m;
}

- (int)
append_row:(struct tbuf *)data tag:(u16)tag cookie:(u64)cookie
{
	Recovery *r = dir->recovery_state;

	struct row_v12 row;
	row.scn = [r scn];
	row.lsn = next_lsn;
	row.tm = ev_now();
	row.tag = tag;
	row.cookie = cookie;
	row.len = tbuf_len(data);
	row.data_crc32c = crc32c(0, data->data, tbuf_len(data));
	row.header_crc32c = crc32c(0, (unsigned char *)&row + sizeof(row.header_crc32c),
				   sizeof(row) - sizeof(row.header_crc32c));

	off_t offt = ftello(fd);
	if (fwrite(&marker, sizeof(marker), 1, fd) != 1 ||
	    fwrite(&row, sizeof(row), 1, fd) != 1 ||
	    fwrite(data->data, tbuf_len(data), 1, fd) != 1)
		goto fail;

	[self append_successful];
	return 0;
fail:
	say_syserror("fwrite");
	fflush(fd);
	fseeko(fd, offt, SEEK_SET);
	if (ftruncate(fileno(fd), offt) == -1)
		say_syserror("ftruncate(%s)", filename);
	return -1;

}

@end
