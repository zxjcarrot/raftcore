#sources
RAFTCORE_SRC = core_simple_logger.cpp core_config.cpp core_filemap.cpp core_log.cpp core_utils.cpp raftcore.cpp

#object files
RAFTCORE_OBJS = $(RAFTCORE_SRC:.cpp=.o) raft.pb.o http_server.o http_reply.o http_dispatcher.o http_connection.o http_request_parser.o http_connection_manager.o http_mime_types.o
#executable
#PROGRAM = libraftcore.so
PROGRAM = libraftcore.so
#compiler
CC = g++

#includes
INCLUDE = -Iraftcore -Iprotob -I.
#linker params
LINKPARAMS = -fpic -lprotobuf -lcarrotrpc -lpthread -lboost_system -lboost_log_setup -lboost_log -lboost_thread 
#options for development
CFLAGS = --std=c++11 -g -O0 -Wall -Werror -fpic -DBOOST_LOG_DYN_LINK
#options for release
#CFLAGS = --std=c++11 -g -O2 -Wall -Werror -fpic -shared

all: $(RAFTCORE_OBJS)
	$(CC) -o $(PROGRAM) $(CFLAGS) $(RAFTCORE_OBJS) $(LINKPARAMS) -shared 

log_reader: log_reader.cpp core_simple_logger.o core_config.o core_filemap.o core_log.o core_utils.o
	$(CC) -o log_reader $(CFLAGS) $(INCLUDE) log_reader.cpp core_simple_logger.o core_config.o core_filemap.o core_log.o core_utils.o $(LINKPARAMS)

raft.pb.o: raftcore/protob/raft.pb.h raftcore/protob/raft.pb.cc 
	$(CC) -c $(CFLAGS) $(INCLUDE) raftcore/protob/raft.pb.cc -o raft.pb.o

core_simple_logger.o: raftcore/core_simple_logger.h core_simple_logger.cpp 
	$(CC) -c $(CFLAGS) $(INCLUDE) core_simple_logger.cpp

core_config.o: raftcore/core_config.h core_config.cpp	
	$(CC) -c $(CFLAGS) $(INCLUDE) core_config.cpp

core_filemap.o: raftcore/core_filemap.h core_filemap.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) core_filemap.cpp

core_log.o: raftcore/core_log.h core_log.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) core_log.cpp

core_utils.o: raftcore/core_utils.h core_utils.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) core_utils.cpp

raftcore.o: raftcore/raftcore.h raftcore.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) raftcore.cpp

main.o: $(RAFTCORE_SRC)
	$(CC) -c $(CFLAGS) $(INCLUDE) main.cpp

http_server.o: raftcore/http/server/server.hpp raftcore/http/server/server.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/server.cpp -o http_server.o

http_request_parser.o: raftcore/http/server/request_parser.hpp raftcore/http/server/request_parser.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/request_parser.cpp -o http_request_parser.o

http_reply.o: raftcore/http/server/reply.hpp raftcore/http/server/reply.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/reply.cpp -o http_reply.o

http_mime_types.o: raftcore/http/server/mime_types.hpp raftcore/http/server/mime_types.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/mime_types.cpp -o http_mime_types.o

http_dispatcher.o: raftcore/http/server/dispatcher.hpp raftcore/http/server/dispatcher.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/dispatcher.cpp -o http_dispatcher.o

http_connection_manager.o: raftcore/http/server/connection_manager.hpp raftcore/http/server/connection_manager.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/connection_manager.cpp -o http_connection_manager.o
	
http_connection.o: raftcore/http/server/connection.hpp raftcore/http/server/connection.cpp
	$(CC) -c $(CFLAGS) raftcore/http/server/connection.cpp -o http_connection.o

install:
	cp -R raftcore /usr/include/raftcore
	cp $(PROGRAM) 	 /usr/lib

uninstall:
	rm -rf /usr/include/raftcore
	rm -f  /usr/lib/$(PROGRAM)
clean:
	-rm $(PROGRAM)
	-rm *.o
