/*
  MySQL FUSE Connector
  Designed and written by Michal Novotny <mignov@gmail.com> in 2010

  Based on:
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
*/

#define DEBUG_MAIN

#ifdef DEBUG_MAIN
#define DPRINTF(fmt, ...) \
do { fprintf(stderr, "main: " fmt , ## __VA_ARGS__); } while (0)
#else
#define DPRINTF(fmt, ...) \
do {} while(0)
#endif

#include "fuse-db.h"

long flags = 0;

/* Connection parameters */
char *mServer   = NULL;
char *mUser     = NULL;
char *mPass     = NULL;
char *mMntPoint = NULL;
char *mLogFile  = NULL;
char *mPwdType  = "plain";

unsigned char *unbase64(char *input) {
    size_t size = 0;
    unsigned char *val = NULL;

    val = strdup( (char *)base64_decode(input, &size) );

    if (size > 3) {
        if (val[size-3] == '\r')
            val[size-3] = 0;
        else
        if (val[size-2] == '\n')
            val[size-2] = 0;
    }

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

