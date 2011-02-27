/*
  MySQL FUSE Connector
  Designed and written by Michal Novotny <mignov@gmail.com> in 2010

  Based on:
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  Compile using:
  $ gcc -o fuse-mysql fuse-mysql.c -Wall `pkg-config fuse --cflags` `pkg-config fuse --libs` $(MYSQL_LIBS) $(MYSQL_CFLAGS)

  e.g. something like:
  $ gcc -o fuse-mysql fuse-mysql.c -Wall `pkg-config fuse --cflags` -lmysqlclient `pkg-config fuse --libs` -L/usr/lib/mysql
*/

#define FUSE_USE_VERSION 26
#define EXT_LOG_SIZE	 40960 /* 40 kiB */

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

#define TYPE_NOENT	-1
#define TYPE_FILE	0
#define TYPE_DIR	1
#define TYPE_DIR_NOPK	2

#define FLAG_READONLY		4
#define FLAG_CORRECT_CODES	8
#define FLAG_UNMOUNT		16
#define FLAG_FORCE		32
#define FLAG_DEBUGPWD		64
#define FLAG_DEBUG		128

long flags = 0;
MYSQL sql;

/* Connection parameters */
char *mServer   = NULL;
char *mUser     = NULL;
char *mPass     = NULL;
char *mMntPoint = NULL;
char *mLogFile  = NULL;
char *mPwdType  = "plain";

void DPRINTF(const char *fmt, ...)
{
    if (mLogFile == NULL)
        return;

    FILE *fp;
    va_list ap;
    char tmp[4096];

    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    fp = fopen(mLogFile, "a");
    fprintf(fp, "%s\n", tmp);
    fclose(fp);

}

void DPRINTF_EXT(const char *fmt, ...)
{
    /* Enable only when debug flag is set */
    if ((mLogFile == NULL) || (!(flags & FLAG_DEBUG)))
        return;

    FILE *fp;
    va_list ap;
    char tmp[EXT_LOG_SIZE];

    va_start(ap, fmt);
    vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);

    fp = fopen(mLogFile, "a");
    fprintf(fp, "%s\n", tmp);
    fclose(fp);

}

char *unbase64(char *input) {
    FILE *fp;
    char *val = NULL;
    char cmd[1024];

    val = (char *)malloc( 1024 * sizeof(char));
    snprintf(cmd, sizeof(cmd), "echo \"%s\" | base64 -d", input);
    fp = popen(cmd, "r");
    if (fp == NULL) {
        DPRINTF("Cannot open process '%s'\n", cmd);
        return NULL;
    }
    fgets(val, 1024, fp);
    fclose(fp);

    if (strlen(val) > 1)
        val[strlen(val)-1] = 0;

    return val;
}

int countChars(const char *str, int c) {
    int num, i;

    num = 0;

    for (i = 0; i < strlen(str); i++)
        if (str[i] == c)
            num++;

    return num;
}

int getLevel(const char *path) {
    if (strcmp(path, "/") == 0)
        return 0;

    return countChars(path, '/');
}

int flagIsSet(int flag)
{
    return (flags & flag) ? 1 : 0;
}

int getErrorCode(int err, int code, int retTrue, int retFalse)
{
    if ((err == code) && flagIsSet(FLAG_CORRECT_CODES))
        return retTrue;
    
    return retFalse;
}

char *replace(char *input, char *what, char *with)
{
    char *out, *tmp, *str, *save, *token;
    int num = 0, size = 0;

    if ((what == NULL) || (strlen(what) == 0))
        return input;

    num = countChars((const char *)input, what[0]);
    size = (strlen(input) + num + 1);

    out = (char *)malloc( size * sizeof(char) );
    memset(out, 0, size);

    tmp = strdup( (char *)input );
    for (str = tmp; ; str = NULL) {
        token = strtok_r(str, what, &save);
        if (token == NULL)
            break;
        strcat(out, token);
        strcat(out, with);
    }
    out[ size - 1 ] = 0;

    return out;
}

char *escape(char *input)
{
    input = replace(input, "\'", "\\'");
    input = replace(input, "\\", "\\\\");

    return input;
}

char *getPathComponent(const char *path, int idx) {
    char *tmp, *str, *save, *token;
    int n = 0;

    tmp = strdup( (char *)path );
    for (str = tmp; ; str = NULL) {
        token = strtok_r(str, "/", &save);
        if (token == NULL)
            break;
        if (n == idx)
            return token;
        n++;
    }

    return NULL;
}

int getFieldNumber(MYSQL sql, char *qry, char *fieldName) {
    unsigned int i, num_fields;
    MYSQL_FIELD *fields;
    MYSQL_RES *res;
    int ret = -1, newSize;
    char *newQry;

    if (fieldName == NULL)
        return ret;

    /* Add LIMIT clause if appropriate */
    if ((strstr(qry, "SELECT") && (strstr(qry, "LIMIT") == NULL))) {
        newSize = (strlen(qry) + 9);
        newQry = (char *)malloc( newSize * sizeof(char));
        snprintf(newQry, newSize, "%s LIMIT 1", qry);
    }
    else
        newQry = strdup(qry);

    DPRINTF_EXT("%s: New query is \"%s\" to get field \"%s\"", __FUNCTION__, newQry, fieldName);
    if (mysql_real_query(&sql, newQry, strlen(newQry)) != 0) {
        DPRINTF("%s: Error #%d = \"%s\"", __FUNCTION__, mysql_errno(&sql),
                mysql_error(&sql));
        free(newQry);
        return ret;
    }

    res = mysql_store_result(&sql);

    num_fields = mysql_num_fields(res);
    fields = mysql_fetch_fields(res);
    for(i = 0; i < num_fields; i++)
        if (strcmp(fields[i].name, fieldName) == 0) {
            ret = i;
            break;
        }

    mysql_free_result(res);
    free(newQry);

    DPRINTF("%s: Field '%s' index is %d", __FUNCTION__, fieldName, ret);
    return ret;
}

char *getValue(MYSQL sql, char *qry, char *fieldName, unsigned long long *numRows) {
    MYSQL_RES *res;
    MYSQL_ROW row;
    char *val, *endptr;
    int idx, idxEP, iVal, rowCount;

    if ((qry == NULL) || (strlen(qry) == 0)) {
        DPRINTF("Invalid query for %s", __FUNCTION__);
        return NULL;
    }

    if ((fieldName == NULL) || (strlen(fieldName) == 0)) {
        DPRINTF("Invalid fieldName for %s", __FUNCTION__);
        return NULL;
    }

    if (numRows != NULL)
        *numRows = -1;

    errno = 0;
    iVal = strtol(fieldName, &endptr, 10);
    if ((iVal >= 0) && (errno == 0))
        idx = iVal;
    else
        idx = getFieldNumber(sql, qry, fieldName);

    if (idx < 0) {
        DPRINTF("%s: Negative index value = %d", __FUNCTION__, idx);
        return NULL;
    }

    idxEP = -1;
    if ((endptr) && (strlen(endptr) > 0)) {
        errno = 0;
        iVal = strtol(endptr+1, &endptr, 10);
        if ((iVal >= 0) && (errno == 0))
            idxEP = iVal;
    }

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        return NULL;
    }

    res = mysql_store_result(&sql);
    rowCount = mysql_num_rows(res);
    if (numRows != NULL)
        *numRows = rowCount;
    row = mysql_fetch_row(res);
    if (row == NULL) {
        mysql_free_result(res);
        return NULL;
    }
    if (row[idx] == NULL) {
        DPRINTF("%s: Row[%d] is NULL", __FUNCTION__, idx);
        mysql_free_result(res);
        return NULL;
    }
    val = strdup(row[idx]);
    if ((idxEP >= 0) && (row[idxEP] != NULL)) {
        if (numRows != NULL) {
            *numRows = atoi(row[idxEP]);
            DPRINTF("%s: Additional field requested at index %d = value is %lld",
                    __FUNCTION__, idxEP, *numRows);
        }
    }
    mysql_free_result(res);

    DPRINTF_EXT("%s: Value from \"%s\" is \"%s\" (row count %d%s)", __FUNCTION__,
            qry, val, rowCount, (numRows == NULL) ? " but not requested" : "");
    return val;
}

char *getPrimaryKeyName(MYSQL sql, char *table, int *error) {
    int size, field_key, field_name;
    MYSQL_ROW row;
    MYSQL_RES *res;
    char *qry, *val = NULL;

    if (error != NULL)
        *error = 0;

    size = (strlen(table) + 18);
    qry = (char *)malloc( size * sizeof(char));
    snprintf(qry, size, "SHOW FIELDS FROM %s", table);

    DPRINTF_EXT("%s: Query is \"%s\"", __FUNCTION__, qry);
    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF("%s: Error #%d = \"%s\"", __FUNCTION__,
                mysql_errno(&sql), mysql_error(&sql));
        if (error != NULL)
            *error = mysql_errno(&sql);
        return NULL;
    }

    res = mysql_store_result(&sql);
    field_key = getFieldNumber(sql, qry, "Key");
    field_name = getFieldNumber(sql, qry, "Field");
    DPRINTF("%s: Name field idx = %d, key field idx = %d", __FUNCTION__,
            field_name, field_key);
    if ((field_key < 0) || (field_name < 0)) {
        mysql_free_result(res);
        return NULL;
    }

    while ((row = mysql_fetch_row(res)))
        if (strcmp(row[field_key], "PRI") == 0) {
            val = row[field_name];
            break;
        }

    DPRINTF("%s: Primary key for \"%s\" is found in \"%s\" column", __FUNCTION__, table, val);
    mysql_free_result(res);
    return val;
}

int getSize(MYSQL sql, char *path, int *error) {
    char *db, *tab, *tmp, *pk, *pkVal;
    char qry[2048] = { 0 };
    int level, err = 0;
    unsigned long long nr;

    level = getLevel(path);
    if (level > 1) {
        db = getPathComponent(path, 0);
        tab = getPathComponent(path, 1);
        pkVal = getPathComponent(path, 2);
        mysql_select_db(&sql, db);
        pk = getPrimaryKeyName(sql, tab, &err);
        DPRINTF("%s: Primary key for table \"%s\" is \"%s\"", __FUNCTION__, tab, pk);
    }

    if (error != NULL)
        *error = err;

    DPRINTF("%s: Path = %s, level = %d (err = %d)", __FUNCTION__, path, level, err);

    if (level == 0) { /* Get number of databases */
      snprintf(qry, sizeof(qry), "SHOW DATABASES");
      if (getValue(sql, qry, "0", &nr) == NULL)
          return 0;

      return nr;
    }
    else
    if (level == 1) { /* Get number of tables in the database */
        snprintf(qry, sizeof(qry), "SHOW TABLES");
        if (getValue(sql, qry, "0", &nr) == NULL)
            return 0;

        return nr;
    }
    else
    if (level == 2) { /* Get number of entries in the table */
        snprintf(qry, sizeof(qry), "SELECT COUNT(*) FROM %s", tab);
        tmp = getValue(sql, qry, "0", NULL);
        if (tmp == NULL)
            return 0;

        return atoi(tmp);
    }
    else
    if (level == 3) { /* Get number of fields in the table */
        snprintf(qry, sizeof(qry), "SHOW FIELDS FROM %s", tab);
        if (getValue(sql, qry, "0", &nr) == NULL)
            return 0;

        return nr;
    }
    else
    if (level == 4) { /* Get size of the "data" */
        int ret;

        snprintf(qry, sizeof(qry), "SELECT `%s` FROM %s WHERE `%s` = '%s'",
                 getPathComponent(path, 3), tab, pk, pkVal);
        DPRINTF_EXT("%s: Querying size \"%s\"", __FUNCTION__, qry);

        if ((tmp = getValue(sql, qry, "0", NULL)) != NULL) {
            DPRINTF("%s: Size query returned \"%s\"", __FUNCTION__, tmp);
            ret = strlen(tmp) + 1;
         }
         else
            ret = 0;

        DPRINTF("%s: Returning value %d", __FUNCTION__, ret);

        return ret;
    }

    return 0;
}

int getMySQLResults(MYSQL sql, char *qry, char *field, fuse_fill_dir_t filler, void *buf) {
    int num;
    char *dbname, *fField;
    MYSQL_ROW row;
    MYSQL_RES *res;

    DPRINTF_EXT("%s(sql, '%s', '%s', %p, %p)", __FUNCTION__, qry, field, filler, buf);

    if (strcmp(field, "$PRI$") == 0) {
        if (strstr(qry, "SELECT") != NULL) {
            char *tmp;
            int size;

            DPRINTF("%s: Getting primary key for query '%s'", __FUNCTION__,
                    qry);
            tmp = strstr(qry, "FROM") + 5;
            if (strstr(tmp, " ") != NULL) {
                size = strlen(tmp) - strlen(strstr(tmp, " "));
                dbname = (char *)malloc( (size + 1) * sizeof(char) );
                memset(dbname, 0, size+1);
                strncpy(dbname, tmp, size);
            }
            else
                dbname = strdup(tmp);

            if (dbname != NULL) {
                fField = getPrimaryKeyName(sql, dbname, NULL);
                DPRINTF("%s: Got primary key for '%s': %s", __FUNCTION__,
                        dbname, fField);
                free(dbname);
            }
        }
    }
    else
        fField = strdup(field);

    DPRINTF_EXT("%s: Query is '%s', fieldName = %s", __FUNCTION__, qry, fField);
    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        free(fField);
        return -1;
    }

    res = mysql_store_result(&sql);
    num = mysql_num_rows(res);

    if ((filler != NULL) && (fField != NULL)) {
        int field_num;
        DPRINTF("%s: Field is \"%s\"", __FUNCTION__, fField);
        field_num = getFieldNumber(sql, qry, fField);
        if (field_num > -1) {
            DPRINTF("%s: Field number for \"%s\" is %d", __FUNCTION__, fField, field_num);
            while ((row = mysql_fetch_row(res))) {
                if (row[field_num] == NULL)
                    DPRINTF("Row[%d] is NULL", field_num);
                else {
                    DPRINTF("%s: Field \"%s\" value is \"%s\"", __FUNCTION__,
                            fField, row[field_num]);
                    filler(buf, row[field_num], NULL, 0);
                }
            }
        }
        else
            DPRINTF("%s: Field \"%s\" not found in the result", __FUNCTION__,
                    fField);
    }
    mysql_free_result(res);
    free(fField);

    return num;
}

int isReadOnly(MYSQL sql, const char *path, char *tab)
{
    char *pk, *fn;

    fn = getPathComponent( (char *)path, 3);
    pk = getPrimaryKeyName(sql, tab, NULL);

    return (strcmp(fn, pk) == 0) ? 1 : 0;
}

int getType(char *path, int *error) {
    int level;

    if (error != NULL)
        *error = 0;

    level = getLevel(path);
    if (level == 0)
        return TYPE_DIR;
    if (level > 0)
        if (mysql_select_db(&sql, getPathComponent(path, 0)) != 0) {
            DPRINTF("%s: Error getting level %d information for %s: %s (%d)",
                __FUNCTION__, level, path, mysql_error(&sql), mysql_errno(&sql));
            if (error != NULL)
                *error = mysql_errno(&sql);
            return TYPE_NOENT;
        }
    if (level <= 2) {
        if (level == 2)
            if (getPrimaryKeyName(sql, getPathComponent(path, 1), NULL) == NULL) {
                DPRINTF("Primary key not found for %s", getPathComponent(path, 1));
                return TYPE_DIR_NOPK;
            }

        return TYPE_DIR;
    }
    else
    if (level == 3) {
        char qry[2048] = { 0 };
        char *tmp, *tab;

        tab = getPathComponent(path, 1);
        snprintf(qry, sizeof(qry), "SELECT COUNT(*) FROM %s WHERE %s = '%s'",
                 tab, getPrimaryKeyName(sql, tab, NULL), getPathComponent(path, 2));

        tmp = getValue(sql, qry, "0", NULL);
        if (tmp == NULL) {
            DPRINTF_EXT("%s: Cannot get result for '%s'", __FUNCTION__, qry);
            return TYPE_NOENT;
        }
        if (atoi(tmp) == 0) {
            DPRINTF_EXT("%s: No entry found for '%s'", __FUNCTION__, qry);
            return TYPE_NOENT;
        }
        free(tmp);

        return TYPE_DIR;
    }
    else 
    if (level == 4) {
        char qry[2048] = { 0 };
        char *tab;

        tab = getPathComponent(path, 1);
        snprintf(qry, sizeof(qry), "SELECT `%s` FROM %s WHERE %s = '%s'",
                 getPathComponent(path, 3), tab, getPrimaryKeyName(sql, tab, NULL),
                 getPathComponent(path, 2));
        if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
            DPRINTF("%s: Error: %s", __FUNCTION__, mysql_error(&sql));

            if (error != NULL)
                *error = mysql_errno(&sql);

            return TYPE_NOENT;
        }
        // Reset the result set
        mysql_free_result( mysql_store_result(&sql) );

        return TYPE_FILE;
    }

    DPRINTF("%s: Error: Level %d = \"%s\"", __FUNCTION__, level, path);
    return TYPE_NOENT;
}

static int fmysql_getattr(const char *path, struct stat *stbuf)
{
    int type, err;

    stbuf->st_uid = getuid();
    stbuf->st_gid = getgid();
    stbuf->st_atime = stbuf->st_mtime = time(NULL);

    type = getType( (char *)path, &err );
    DPRINTF("%s: Path %s, type = %d (error %d)", __FUNCTION__, path, type, err);
    if ((type == TYPE_DIR) || (type == TYPE_DIR_NOPK)) {
        if (getPathComponent(path, 0) != NULL)
            if (mysql_select_db(&sql, getPathComponent(path, 0)) != 0)
                return getErrorCode( err, 1044, -EPERM, -ENOENT);

        stbuf->st_mode = S_IFDIR | ((type == TYPE_DIR) ? 0755 : 0444);
        stbuf->st_nlink = 1;
        stbuf->st_size = getSize(sql, (char *)path, &err );
        if (err == 1146)
            return -ENOENT;
        else
        if (err > 0)
            DPRINTF("Directory %s size returned error %d", (char *)path, err);
    }
    else
    if (type == TYPE_FILE) {
        char *tab;

        if (err == 1146)
            return -ENOENT;

        tab = getPathComponent( (char *)path, 1);

        if (mysql_select_db(&sql, getPathComponent(path, 0)) != 0)
            return getErrorCode( err, 1044, -EPERM, -ENOENT);

        stbuf->st_mode = S_IFREG | (isReadOnly(sql, path, tab) ? 0444 : 0666);
        stbuf->st_nlink = 1;
        stbuf->st_size = getSize(sql, (char *)path, &err );
        DPRINTF("Setting up file information %s, size is %ld bytes", (char *)path, stbuf->st_size);
        if (err > 0)
            DPRINTF("File %s size returned error %d", (char *)path, err);
    }
    else
        return getErrorCode(err, 1044, -EPERM, -ENOENT);

    return 0;
}

char *mysql_read(MYSQL sql, char *path, unsigned int *len)
{
    char *val = NULL;
    unsigned int sz;
    char *db, *tab, *col, *pk, *pkVal;
    char qry[2048] = { 0 };
    MYSQL_RES *res;
    MYSQL_ROW row;

    db = getPathComponent(path, 0);
    tab = getPathComponent(path, 1);
    pkVal = getPathComponent(path, 2);
    col = getPathComponent(path, 3);
    pk  = getPrimaryKeyName(sql, tab, NULL);

    if ((col == NULL) || (pk == NULL))
        return NULL;

    mysql_select_db(&sql, db);
    snprintf(qry, sizeof(qry), "SELECT `%s`, LENGTH(`%s`) FROM %s WHERE %s = '%s'",
             col, col, tab, pk, pkVal);
    DPRINTF_EXT("%s: mysql_read query: %s", __FUNCTION__, qry);

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF("%s: Error in query '%s': %s", __FUNCTION__, qry,
                mysql_error(&sql));
        return NULL;
    }

    res = mysql_store_result(&sql);
    if ((row = mysql_fetch_row(res))) {
        if (row[1] != NULL) {
            sz = atoi(row[1]);
            val = (char *)malloc( (sz + 1) * sizeof(char) );
            val = strdup(row[0]);
        }
        else
            sz = 0;
    }
    mysql_free_result(res);

    if (sz == 0) {
        if (len != NULL)
            *len = 0;
        return "\n";
    }

    memcpy(val + sz, "\n", 1);
    if (len != NULL)
        *len = sz + 1;

    DPRINTF("%s: Return value is '%s' (%d)", __FUNCTION__, val, sz + 1);
    return val;
}

static int fmysql_read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi)
{
    int t;
    size_t len;
    char *buf1;

    t = getType( (char *)path, NULL );
    DPRINTF("%s: Path = %s, type = %d", __FUNCTION__, (char *)path, t);

    if ((t == TYPE_DIR) || (t == TYPE_DIR_NOPK))
        return -EISDIR;
    if (t == TYPE_NOENT)
        return -ENOENT;

    if ((buf1 = mysql_read(sql, (char *)path, &len)) == NULL)
        return -EIO;

    if (offset < len) {
        if (offset + size > len)
            size = len - offset;
        memcpy(buf, buf1 + offset, size);
    } else
        size = 0;

    DPRINTF("%s returning buffer '%s' (size: %d)", __FUNCTION__, buf, size);
    return size;
}

static int fmysql_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                          off_t offset, struct fuse_file_info *fi)
{
    int level;
    (void) offset;
    (void) fi;

    level = getLevel(path);
    DPRINTF("%s: Path %s (level = %d)", __FUNCTION__, path, level );

    if (level == 0) { /* Database */
        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);

        if (getMySQLResults(sql, "SHOW DATABASES", "Database", filler, buf) < 0)
            return -EIO;
    }
    else
    if (level == 1) { /* Table */
        char *db, *tmp;
        int size;

        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);

        db = getPathComponent(path, 0);
        size = 12 + strlen(db);
        tmp = (char *)malloc( size * sizeof(char) );
        memset(tmp, 0, size);
        snprintf(tmp, size, "Tables_in_%s", db);
        DPRINTF("Column name: '%s'", tmp);
        mysql_select_db(&sql, db);
        getMySQLResults(sql, "SHOW TABLES", tmp, filler, buf);
        free(tmp);
    }
    else
    if (level == 2) { /* Directory entries sorted by primary key */
        char *db, *tab, *pk;
        char qry[2048] = { 0 };
        int err;

        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);

        db = getPathComponent(path, 0);
        tab = getPathComponent(path, 1);
        mysql_select_db(&sql, db);
        pk = getPrimaryKeyName(sql, tab, &err);

        if ((db == NULL) || (tab == NULL) || (pk == NULL))
            return -ENOENT;

        snprintf(qry, sizeof(qry), "SELECT `%s` FROM %s ORDER BY %s", pk, tab, pk);
        DPRINTF("%s: Query \"%s\" returned error code %d", __FUNCTION__, qry, err);

        getMySQLResults(sql, qry, pk, filler, buf);
    }
    else
    if (level == 3) { /* File entries are DB columns */
        char *db, *tab, *pk, *pkVal, *tmp;
        char qry[2048] = { 0 };
        int num;

        filler(buf, ".", NULL, 0);
        filler(buf, "..", NULL, 0);

        db = getPathComponent(path, 0);
        tab = getPathComponent(path, 1);
        pkVal = getPathComponent(path, 2);
        mysql_select_db(&sql, db);
        pk = getPrimaryKeyName(sql, tab, NULL);

        snprintf(qry, sizeof(qry), "SELECT COUNT(*) FROM %s WHERE %s = '%s'",
                 tab, pk, pkVal);

        DPRINTF_EXT("%s: Setting up query: %s", __FUNCTION__, qry);
        tmp = getValue(sql, qry, "0", NULL);
        if (tmp == NULL)
            return -ENOENT;
        num = atoi(tmp);
        DPRINTF("Result at index 0 is %d", num);

        if ((db == NULL) || (tab == NULL) || (num == 0))
            return -ENOENT;

        snprintf(qry, sizeof(qry), "SHOW FIELDS FROM %s", tab);
        DPRINTF_EXT("%s: Query is '%s'", __FUNCTION__, qry);

        getMySQLResults(sql, qry, "Field", filler, buf);
    }

    return 0;
}

static int fmysql_open(const char *path, struct fuse_file_info *fi)
{
    int type, ret;

    ret = 0;
    type = getType( (char *)path, NULL );

    DPRINTF("%s: Path = %s, type = %d, flags = %d", __FUNCTION__, (char *)path,
            type, fi->flags);

    if ((type == TYPE_DIR) || (type == TYPE_DIR_NOPK))
        ret = -EISDIR;
    else
    if (type == TYPE_FILE) {
        if ((fi->flags & O_WRONLY) || (fi->flags & O_RDWR)) {
            if (flagIsSet(FLAG_READONLY))
                ret = -EPERM;
            else
            if (mysql_select_db(&sql, getPathComponent(path, 0)) != 0)
                ret = -EPERM;
            else
            if (isReadOnly(sql, path, getPathComponent( (char *)path, 1)))
                ret = -EPERM;
        }
    }
    else
    if (type == TYPE_NOENT)
        ret = -ENOENT;

    DPRINTF("%s(%s): returning %d", __FUNCTION__, path, ret);
    return ret;
}

static int fmysql_mkdir(const char *path, mode_t mode)
{
    int level, ret;
    char qry[1024] = { 0 };
    (void)mode;

    if (flagIsSet(FLAG_READONLY))
        return -EPERM;

    ret = 0;
    level = getLevel(path);
    DPRINTF("%s: Path %s, mode=%o, level = %d", __FUNCTION__, path, mode, level);

    if (level == 1)
        snprintf(qry, sizeof(qry), "CREATE DATABASE %s", getPathComponent(path, 0));
    else
    if (level == 2) 
        snprintf(qry, sizeof(qry), "CREATE TABLE %s(id varchar(255), PRIMARY KEY(id))",
                 getPathComponent(path, 1));
    else
    if (level == 3)
        snprintf(qry, sizeof(qry), "INSERT INTO %s(%s) VALUES('%s')", getPathComponent(path, 1),
                 getPrimaryKeyName(sql, getPathComponent(path, 1), NULL), getPathComponent(path, 2));
    else
        return -EPERM;

    if (level > 1)
        mysql_select_db(&sql, getPathComponent(path, 0));

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }

    DPRINTF_EXT("%s: Query '%s' returned %d", __FUNCTION__, qry, ret);
    return ret;
}

static int fmysql_rmdir(const char *path)
{
    int level, ret;
    char qry[1024] = { 0 };

    if (flagIsSet(FLAG_READONLY))
        return -EPERM;

    ret = 0;
    level = getLevel(path);
    DPRINTF("%s: Path %s, level = %d", __FUNCTION__, path, level);

    if (level == 1)
        snprintf(qry, sizeof(qry), "DROP DATABASE %s", getPathComponent(path, 0));
    else
    if (level == 2)
        snprintf(qry, sizeof(qry), "DROP TABLE %s", getPathComponent(path, 1));
    else
    if (level == 3)
        snprintf(qry, sizeof(qry), "DELETE FROM %s WHERE %s = '%s'", getPathComponent(path, 1),
                 getPrimaryKeyName(sql, getPathComponent(path, 1), NULL), getPathComponent(path, 2));
    else
        return -EPERM;

    if (level > 1)
        mysql_select_db(&sql, getPathComponent(path, 0));

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }

    DPRINTF_EXT("%s for query '%s' returned %d", __FUNCTION__, qry, ret);
    return ret;
}

static int fmysql_rm(const char *path)
{
    int level, ret;
    char qry[1024] = { 0 };
    char *tab;

    ret = 0;
    level = getLevel(path);
    if ((level < 4) || (flagIsSet(FLAG_READONLY)))
        return -EPERM;

    tab = getPathComponent( (char *)path, 1);

    DPRINTF("%s: Path %s, level = %d, tab = %s", __FUNCTION__, path, level, tab);

    mysql_select_db(&sql, getPathComponent(path, 0));
    if (isReadOnly(sql, path, tab))
        return -EPERM;

    snprintf(qry, sizeof(qry), "UPDATE `%s` SET `%s` = NULL WHERE `%s` = '%s';",
             getPathComponent(path, 1), getPathComponent(path, 3),
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }

    DPRINTF_EXT("%s for query '%s' returned %d", __FUNCTION__, qry, ret);
    return ret;
}


static int fmysql_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    int ret, level;
    char *tmp;
    char qry[1024] = { 0 };

    ret = 0;
    level = getLevel(path);
    if ((level < 4) || (flagIsSet(FLAG_READONLY)))
        return -EPERM;

    /* Disallow invisible file creation */
    tmp = getPathComponent(path, 3);
    if ((tmp != NULL) && (strlen(tmp) > 0) && (tmp[0] == '.'))
        return -EPERM;

    DPRINTF("%s: Path %s, level = %d", __FUNCTION__, path, level);

    snprintf(qry, sizeof(qry), "ALTER TABLE `%s` ADD `%s` text",
             getPathComponent(path, 1), getPathComponent(path, 3));

    mysql_select_db(&sql, getPathComponent(path, 0));

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }

    DPRINTF_EXT("%s for query '%s' returned %d", __FUNCTION__, qry, ret);
    return ret;
}

static int fmysql_truncate(const char *path, off_t size)
{
    int ret, level;
    char *tmp;
    char *qry = NULL;
    unsigned long long len;

    ret = 0;
    level = getLevel(path);
    if ((level < 4) || (flagIsSet(FLAG_READONLY)))
        return -EPERM;

    DPRINTF("%s: Path %s, level = %d, size = %lld", __FUNCTION__, path, level, size);

    mysql_select_db(&sql, getPathComponent(path, 0));

    qry = malloc( 1024 * sizeof(char) );
    snprintf(qry, 1024, "SELECT `%s`, LENGTH(`%s`) FROM %s WHERE `%s` = '%s'",
             getPathComponent(path, 3), getPathComponent(path, 3), getPathComponent(path, 1),
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));

    DPRINTF_EXT("%s: Select query is \"%s\"", __FUNCTION__, qry);
    tmp = getValue(sql, qry, "0l1", &len);
    if (tmp == NULL)
        return 0;


    tmp[size] = 0;

    qry = (char *)realloc(qry, (256 + len) * sizeof(char) );
    memset(qry, 0, (256 + len) * sizeof(char) );
    snprintf(qry, 256 + len, "UPDATE `%s` SET %s = '%s' WHERE `%s` = '%s'",
             getPathComponent(path, 1), getPathComponent(path, 3), tmp,
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));

    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }

    DPRINTF_EXT("%s for query '%s' returned %d", __FUNCTION__, qry, ret);
    free(qry);
    return ret;
}

static int fmysql_write(const char *path, const char *buf, size_t size,
                      off_t offset, struct fuse_file_info *fi)
{
    unsigned long long len;
    int level, ret;
    char *tmp = NULL, *qry = NULL;

    ret = 0;

    level = getLevel(path);
    if ((level < 4) || (flagIsSet(FLAG_READONLY)))
        return -EPERM;

    DPRINTF_EXT("%s: Requested write of %d bytes (%s)", __FUNCTION__, size, buf);

    mysql_select_db(&sql, getPathComponent(path, 0));
    if (isReadOnly(sql, path, getPathComponent(path, 1)))
        return -EPERM;

    qry = malloc( 1024 * sizeof(char) );
    snprintf(qry, 1024, "SELECT `%s`, LENGTH(`%s`) FROM %s WHERE `%s` = '%s'",
             getPathComponent(path, 3), getPathComponent(path, 3), getPathComponent(path, 1),
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));

    DPRINTF_EXT("%s: Select query is \"%s\"", __FUNCTION__, qry);
    tmp = getValue(sql, qry, "0l1", &len);
    if (tmp != NULL) {
        if ( offset + size > len ) {
            len = offset + size;
            tmp = (char *) realloc( tmp, (len + 1) * sizeof(char) );
        }
        /* Make new buffer pointer empty */
        memset(tmp + offset, 0, len);
        memcpy(tmp + offset, buf, size);
    }
    else
        len = size;

    DPRINTF("%s: Before replace = %lld, string = \"%s\"", __FUNCTION__, len, tmp);

    //tmp = escape(tmp);

    DPRINTF("%s: New length = %lld, string = \"%s\"", __FUNCTION__, len, tmp);

    qry = (char *)realloc(qry, (512 + len) * sizeof(char) );
    DPRINTF("Reallocation to %d is done to\n", 512 + len);

    memset(qry, 0, (512 + len) * sizeof(char) );
    snprintf(qry, 512 + len, "UPDATE `%s` SET %s = '%s' WHERE `%s` = '%s'",
             getPathComponent(path, 1), getPathComponent(path, 3), tmp,
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));

    DPRINTF_EXT("%s: Setting up query '%s'...", __FUNCTION__, qry);

/*
    snprintf(qry, sizeof(qry), "UPDATE `%s` SET %s = '%s' WHERE `%s` = '%s'",
             getPathComponent(path, 1), getPathComponent(path, 3), buf,
             getPrimaryKeyName(sql, getPathComponent(path, 1), NULL),
             getPathComponent(path, 2));
*/
    if (mysql_real_query(&sql, qry, strlen(qry)) != 0) {
        DPRINTF_EXT("%s: Query '%s' failed: %s", __FUNCTION__, qry,
                mysql_error(&sql));
        ret = -EIO;
    }
    free(tmp);

    DPRINTF_EXT("%s for query '%s' returned %d", __FUNCTION__, qry, ret);
    free(qry);
    return (ret == 0) ? size : ret;
}

static struct fuse_operations fmysql_oper = {
    /* Directories/files listing */
    .getattr    = fmysql_getattr,
    .readdir    = fmysql_readdir,
    /* Read functions */
    .read       = fmysql_read,
    .open       = fmysql_open,
    /* Directory operations */
    .mkdir      = fmysql_mkdir,
    .rmdir      = fmysql_rmdir,
    /* Write operations */
    .create     = fmysql_create,
    .write      = fmysql_write,
    /* File operations */
    .unlink     = fmysql_rm,
    .truncate   = fmysql_truncate,
};

void dumpArgs() {
    if (!flagIsSet(FLAG_DEBUG))
        return;
    printf("\nDump argument settings:\n");
    printf("\tServer: %s\n", mServer);
    printf("\tUser: %s\n", mUser);
    if (flagIsSet(FLAG_DEBUGPWD))
        printf("\tPassword: %s\n", mPass);
    else
        printf("\tPassword: %s\n", mPass ? "Set" : "Not set");
    printf("\tPassword type: %s\n", mPwdType);
    printf("\tRead-only: %s\n", flagIsSet(FLAG_READONLY) ? "True" : "False");
    printf("\tLog file: %s\n", mLogFile);
    printf("\tMountpoint: %s\n", mMntPoint);
    printf("\tForce: %s\n", flagIsSet(FLAG_FORCE) ? "True" : "False");
    printf("\tUnmount: %s\n", flagIsSet(FLAG_UNMOUNT) ? "True" : "False");
    printf("\n");
}

void usage(char *name) {
    fprintf(stderr, "Syntax: %s --server <server> --user <user> --password <password> --password-type <type*1>\n"
                    "        --mountpoint <mountpoint> [--log-file <log-file>] [--debug] [--force-password-dump]\n"
                    "        [--force] [--use-correct-codes] [--read-only] [--unmount]\n\n"
                    "You can also use short version of the parameters by using the lowercase first letters except for\n"
                    "-t for password type and -g for debugging. Forcing the password dump will enforce dumping the\n"
                    "password in the debug output if enabled.\nFor the password-type you can use plain text type"
                    "which is the default or you can use 'b64' type\nthat specifies the password is in base64 encoded "
                    "format.\n", name);

    dumpArgs();
    exit(EXIT_FAILURE);
}

long parseArgs(int argc, char * const argv[]) {
    int option_index = 0, c;
    unsigned int retVal = 0;
    struct option long_options[] = {
        {"server", 1, 0, 's'},
        {"user", 1, 0, 'u'},
        {"password", 1, 0, 'p'},
        {"password-type", 1, 0, 't'},
        {"mountpoint", 1, 0, 'm'},
        {"log-file", 1, 0, 'l'},
        {"debug", 0, 0, 'g'},
        {"force-password-dump", 0, 0, 'd'},
        {"force", 0, 0, 'f'},
        {"unmount", 0, 0, 'n'},
        {"use-correct-codes", 0, 0, 'c'},
        {"read-only", 0, 0, 'r'},
        {0, 0, 0, 0}
    };

    char *optstring = "s:u:p:t:m:l:gfdn";

    while (1) {
        c = getopt_long(argc, argv, optstring,
                   long_options, &option_index);
        if (c == -1)
            break;

        switch (c) {
            case 's':
                mServer = strdup(optarg);
                break;
            case 'u':
                mUser = strdup(optarg);
                break;
            case 'p':
                mPass = strdup(optarg);
                break;
            case 'l':
                mLogFile = strdup(optarg);
                unlink(mLogFile);
                break;
            case 't':
                mPwdType = strdup(optarg);
                break;
            case 'm':
                mMntPoint = strdup(optarg);
                break;
            case 'g':
                retVal |= FLAG_DEBUG;
                break;
            case 'd':
                retVal |= FLAG_DEBUGPWD;
                break;
            case 'f':
                retVal |= FLAG_FORCE;
                break;
            case 'n':
                retVal |= FLAG_UNMOUNT;
                break;
            case 'c':
                retVal |= FLAG_CORRECT_CODES;
                break;
            case 'r':
                retVal |= FLAG_READONLY;
                break;
            default:
                usage(argv[0]);
        }
    }

    return retVal;
}

int unmount(char *binaryPath, char *mountpoint, int suppressMessages)
{
    char cmd[256], *tmp;
    FILE *fp;
    int pid, killed;

    tmp = (char *)malloc( 512 * sizeof(char) );
    memset(tmp, 0, 512);

    snprintf(cmd, sizeof(cmd), "umount %s 2> /dev/null", mountpoint);
    system(cmd);

    killed = 0;
    snprintf(cmd, sizeof(cmd), "ps -C %s -o pid=", binaryPath);
    fp = popen(cmd, "r");
    while (!feof(fp)) {
        fgets(tmp, 512, fp);
        if ((pid = atoi(tmp)) != getpid() ) {
            killed++;
            kill(pid, SIGKILL);
        }
    }
    fclose(fp);

    if (killed > 0) {
        if (!suppressMessages)
            printf("Path %s unmounted and process %d killed\n", mountpoint,
                    pid);

        return 0;
    }
    else
        if (!suppressMessages)
            printf("Cannot find any existing running instance of %s\n", binaryPath);

    return EXIT_FAILURE;
}

int main(int argc, char *argv[])
{
    int rc, i;
    struct statvfs vfs;

    if (getuid() != 0) {
       fprintf(stderr, "Error: You need to run %s as root!\n", argv[0]);
       return EXIT_FAILURE;
    }

    flags = parseArgs(argc, argv);

    if (!mServer || !mUser || !mPass || !mMntPoint)
        usage(argv[0]);

    if (strcmp(mPwdType, "b64") == 0)
        mPass = strdup(unbase64(mPass));

    dumpArgs();

    if (flagIsSet(FLAG_UNMOUNT) || (flagIsSet(FLAG_FORCE))) {
        rc = unmount( replace(argv[0], "./", ""), mMntPoint, flagIsSet(FLAG_FORCE) );

        if (flagIsSet(FLAG_UNMOUNT))
            return rc;

        if ((rc != 0) && (flagIsSet(FLAG_UNMOUNT)))
            fprintf(stderr, "Warning: Error unmounting mountpoint %s\n", mMntPoint);
    }

    if (statvfs(mMntPoint, &vfs) == 0) {
        if (vfs.f_fsid == 0) { /* Observed when mounted (FUSE mount) */
            fprintf(stderr, "Path %s seems to be already mounted. Cannot "
                            "continue. Quiting...\n", mMntPoint);
            return EXIT_FAILURE;
        }
    }

    if (mysql_init(&sql) == NULL)
        return EXIT_FAILURE;

    if (!mysql_real_connect(&sql, mServer, mUser, mPass, NULL, 0, NULL, 0)) {
        fprintf(stderr, "MySQL connection error: %s (%d)\n", mysql_error(&sql),
                mysql_errno(&sql));
        mysql_close(&sql);
        return EXIT_FAILURE;
    }

    /* Unset all the arguments for fuse_main */
    for (i = 1; i > argc; i++)
        free(argv[i]);
    argc = 2;
    /* Set only the mountpoint argument */
    argv[1] = mMntPoint;

    printf("Process %s started successfully\n", argv[0]);

    rc = fuse_main(argc, argv, &fmysql_oper, NULL);
    mysql_close(&sql);

    return rc;
}

