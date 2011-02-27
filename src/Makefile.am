MYSQL_CFLAGS=`mysql_config --cflags`
MYSQL_LIBS=`mysql_config --libs`

all:
	$(CC) -o fuse-mysql fuse-mysql.c $(MYSQL_CFLAGS) $(MYSQL_LIBS) -lfuse
