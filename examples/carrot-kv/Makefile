#sources
SOURCES = carrot_kv_server.cpp carrot_kv.cpp carrot_kv_command_parser.cpp carrot_kv_connection.cpp

#object files
OBJECTS = $(SOURCES:.cpp=.o) 
#executable
PROGRAM = carrot-kv
#compiler
CC = g++

#includes
INCLUDE = -I/usr/include/raftcore
#linker params
LINKPARAMS = -lraftcore -lboost_system -lpthread
#options for development
CFLAGS = --std=c++11 -g -O0 -Wall -Werror
#options for release
#CFLAGS = --std=c++11 -g -O2 -Wall -Werror -fpic -shared

all:
	$(CC) -o $(PROGRAM) $(SOURCES) $(CFLAGS) $(INCLUDE) $(LINKPARAMS) 

clean:
	-rm $(PROGRAM)
	-rm *.o
