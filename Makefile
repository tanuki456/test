CC=gcc

all:
	$(CC) example.c -Wall -O3 -I/usr/local/include/hiredis/ -L/usr/local/lib  -lhiredis -lm
