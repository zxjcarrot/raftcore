##Overview
a library implements raft consensus protocol core functionalities, intended to be used as a building block for distributed systems.
###Features supported
Leader election.  
Log replication.
Leadership transfer.   
Membership changes.   
###Features in development
Log compaction.  
###Prerequsites
[protobuf 2.6.0](https://developers.google.com/protocol-buffers/)  
[boost.asio boost.log](http://www.boost.org/users/history/version_1_57_0.html)  
[carrot-rpc](https://github.com/zxjcarrot/carrot-rpc)  
##Installation
    git clone https://github.com/zxjcarrot/raftcore
    cd raftcore/src
    make
###Configuration
see conf file [exmaple](https://github.com/zxjcarrot/raftcore/blob/master/src/raft.conf).
