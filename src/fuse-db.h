#ifndef FUSE_DB_H
#define FUSE_DB_H

#define FUSE_USE_VERSION 26
#define EXT_LOG_SIZE     40960 /* 40 kiB */

#include <fuse.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <errno.h>
#include <stdarg.h>
#include <signal.h>
#include <sys/mount.h>
#include <mysql/mysql.h>

#define TYPE_NOENT      -1
#define TYPE_FILE       0
#define TYPE_DIR        1
#define TYPE_DIR_NOPK   2

#define FLAG_READONLY           4
#define FLAG_CORRECT_CODES      8
#define FLAG_UNMOUNT            16
#define FLAG_FORCE              32
#define FLAG_DEBUGPWD           64
#define FLAG_DEBUG              128

MYSQL sql;

/* Core functions */
unsigned char *unbase64(char *input);
int countChars(const char *str, int c);
int getLevel(const char *path);
int flagIsSet(int flag);
int getErrorCode(int err, int code, int retTrue, int retFalse);
char *replace(char *input, char *what, char *with);
char *escape(char *input);
char *getPathComponent(const char *path, int idx);

/* MySQL functions */
int getFieldNumber(MYSQL sql, char *qry, char *fieldName);
char *getValue(MYSQL sql, char *qry, char *fieldName, unsigned long long *numRows);
char *getPrimaryKeyName(MYSQL sql, char *table, int *error);
int getSize(MYSQL sql, char *path, int *error);
int getMySQLResults(MYSQL sql, char *qry, char *field, fuse_fill_dir_t filler, void *buf);
int isReadOnly(MYSQL sql, const char *path, char *tab);
int getType(char *path, int *error);
int fmysql_getattr(const char *path, struct stat *stbuf);
char *mysql_read(MYSQL sql, char *path, unsigned int *len);
int fmysql_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int fmysql_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi);
int fmysql_open(const char *path, struct fuse_file_info *fi);
int fmysql_mkdir(const char *path, mode_t mode);
int fmysql_rmdir(const char *path);
int fmysql_rm(const char *path);
int fmysql_create(const char *path, mode_t mode, struct fuse_file_info *fi);
int fmysql_truncate(const char *path, off_t size);
int fmysql_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);

#endif
