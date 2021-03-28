## Overview
a library implements raft consensus protocol core functionalities, intended to be used as a building block for distributed systems.
### Features supported
Leader election.  
Log replication.  
Leadership transfer.   
Membership changes.   
### Features in development
Log compaction.  
### Prerequsites
[protobuf 2.6.0](https://developers.google.com/protocol-buffers/)  
[boost.asio boost.log](http://www.boost.org/users/history/version_1_57_0.html)  
[carrot-rpc](https://github.com/zxjcarrot/carrot-rpc)  
## Installation
    git clone https://github.com/zxjcarrot/raftcore
    cd raftcore/src
    make
### Configuration
see [exemplary conf file](https://github.com/zxjcarrot/raftcore/blob/master/src/raft.conf) for reference.
####Bootstrap the cluster
Say we have a cluster of 3 servers(A 192.168.1.1, B 192.168.1.2, C 192.168.1.3), in order to bootstrap the cluster, we have to choose one of the three servers as the initiating server, say server A.
In the configuration file of A, set the @servers attribute to the address of server A itself,  that is:  
    ```servers=192.168.1.1 ```  

After that, run the binary **raftc** on the server A, this will create a cluster consists of only one server(also a leader), namely server A.  
    ```./raftc```  
From now on, **raftcore** will maintain the server addresses in its replicated log.  

Now in the configuration file of B and C, comment out the @servers attribute to state this server is a subsequently-added server or non-voting server, and run the binary on both servers.
    ```#server=```  
Then use the script **reconfigure.py** to add the two servers to the cluster.  
    ```./reconfigure.py --leader_address=192.168.1.1 --add_servers=192.168.1.2,192.168.1.3```  
This completes the startup of the cluster.  

### Cluster reconfiguration
If we want to remove one of the server out of the cluster, say server B, run following:  
    ```./reconfigure.py --leader_address=192.168.1.1 --del_servers=192.168.1.2```  
This will remove the server B out of the cluster safely.  

If for some reason, we want to transfer the leadership to other server in the cluster, say transfer current leader A's leadership to server B, we run following:  
    ```./reconfigure.py --leader_address=192.168.1.1 --target_server=192.168.1.2```  
This will make leader A transfer its leadership to server B stably, A will become a follower once the transfer is complete.  

## Examples  
A trivial key-value replicated store can be easily implemented on top of **raftcore**, see [carrot-kv](https://github.com/zxjcarrot/raftcore/tree/master/examples/carrot-kv).
