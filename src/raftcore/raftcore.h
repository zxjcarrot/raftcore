/*
* Copyright (C) Xinjing Cho
*/
#ifndef RAFTCORE_RAFTCORE_H_
#define RAFTCORE_RAFTCORE_H_
#include <memory>
#include <random>
#include <chrono>
#include <atomic>
#include <thread>
#include <utility>
#include <functional>

#include <boost/asio.hpp>

#include <carrot-rpc/client/carrot_async_channel.h>
#include <carrot-rpc/server/carrot_async_server.h>
#include <google/protobuf/service.h>

#include <raftcore/core_log.h>
#include <raftcore/core_filemap.h>
#include <raftcore/core_utils.h>
#include <raftcore/core_config.h>
#include <raftcore/core_queue.h>
#include <raftcore/protob/raft.pb.h>

#include <http/server/server.hpp>

namespace raftcore {

class core_service_impl;

typedef std::function<void(const log_entry *)>                      log_committed_callback;
typedef std::function<void(void)>                                   void_callback;
typedef std::unique_ptr<core_config>                                core_config_uptr;
typedef std::unique_ptr<core_logger>                                core_logger_uptr;
typedef std::unique_ptr<core_filemap>                               core_filemap_uptr;
typedef std::unique_ptr<carrot::CarrotAsyncServer>                  async_rpc_server_uptr;
typedef std::unique_ptr<carrot::CarrotAsyncChannel>                 async_rpc_channel_uptr;
typedef std::unique_ptr<::google::protobuf::Closure>                service_done_callback_uptr;
typedef std::unique_ptr<std::mt19937>                               mt19937_uptr;
typedef std::unique_ptr<std::uniform_int_distribution<int>>         uniform_int_dist_uptr;
typedef std::lock_guard<std::mutex>                                 mutex_guard;
typedef std::unique_lock<std::mutex>                                unique_mutex;
typedef std::unique_ptr<RaftCoreService>                            raftcore_service_uptr;
typedef std::unique_ptr<carrot::CarrotController>                   carrot_controller_uptr;
typedef std::unique_ptr<RequestVote>                                request_vote_uptr;
typedef std::unique_ptr<RequestVoteRes>                             request_vote_res_uptr;
typedef std::unique_ptr<AppendEntries>                              append_entries_uptr;
typedef std::unique_ptr<AppendEntriesRes>                           append_entries_res_uptr;
typedef std::unique_ptr<http::server::server>                       http_server_uptr;

enum raft_role {
    LEADER,
    CANDIDATE,
    FOLLOWER
};

struct raftcore_map {
    uint64_t cur_term;        // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    char     voted_for[32];   // candidateId that received vote in current term (or null if none)
};

struct raft_server;

using namespace std::chrono;

/* struct for timeout_now rpc */
struct timeout_now_task {
    uint64_t                        id;
    raft_server*                    s;
    RequestVote                     request;
    RequestVoteRes                  response;
    carrot::CarrotController        controller;
    ::google::protobuf::Closure*    done;
    /* time taken for this call to complete or timed out in microseconds */
    high_resolution_clock::time_point time_start;
    high_resolution_clock::time_point time_end;
};

/* struct for each individual pre_vote rpc */
struct pre_vote_task {
    uint64_t                        id;
    raft_server*                    s;
    RequestVote                     request;
    RequestVoteRes                  response;
    carrot::CarrotController        controller;
    ::google::protobuf::Closure*    done;
    /* time taken for this call to complete or timed out in microseconds */
    high_resolution_clock::time_point time_start;
    high_resolution_clock::time_point time_end;
};

/* struct for each individual request_vote rpc */
struct request_vote_task {
    uint64_t                        id;
    raft_server*                    s;
    RequestVote                     request;
    RequestVoteRes                  response;
    carrot::CarrotController        controller;
    ::google::protobuf::Closure*    done;
    /* time taken for this call to complete or timed out in microseconds */
    high_resolution_clock::time_point time_start;
    high_resolution_clock::time_point time_end;
};

/* struct for each individual append_entries rpc */
struct append_entries_task {
    uint64_t                        id;
    raft_server*                    s;
    AppendEntries                   request;
    AppendEntriesRes                response;
    carrot::CarrotController        controller;
    ::google::protobuf::Closure*    done;
    /* time taken for this call to complete or timed out in microseconds */
    high_resolution_clock::time_point time_start;
    high_resolution_clock::time_point time_end;
};

typedef std::shared_ptr<timeout_now_task>               timeout_now_task_sptr;
typedef std::shared_ptr<pre_vote_task>                  pre_vote_task_sptr;
typedef std::shared_ptr<request_vote_task>              request_vote_task_sptr;
typedef std::shared_ptr<append_entries_task>            append_entries_task_sptr;
typedef std::weak_ptr<request_vote_task>                request_vote_task_wptr;
typedef std::weak_ptr<append_entries_task>              append_entries_task_wptr;
typedef thread_safe_queue<append_entries_task_sptr>     append_entries_task_queue_;
typedef std::function<void(append_entries_task_sptr)>   append_entries_done_callback;

struct raft_server {
    /* server's network address sort of stuff */
    char id[32];

    /* if this server is up */
    bool alive;

    /*
    * for each server, index of the next log entry to send 
    * to that server (initialized to leader's last log index + 1).
    */
    uint64_t next_idx;
    
    /*
    * for each server, index of highest log entry known to
    * be replicated on server (initialized to 0, increases monotonically)
    */
    uint64_t match_idx;

    /* service stub for request_votes */
    raftcore_service_uptr       request_vote_service;
    
    /* service stub for append_entries */
    raftcore_service_uptr       append_entries_service;

    /* if leader is transferring leadership to this server */
    bool                        transfer_leader_to; 
};


typedef std::shared_ptr<raft_server>            raft_server_sptr;
typedef std::map<std::string, raft_server_sptr> raft_server_map;
typedef thread_safe_queue<raft_server*>         server_queue;

/* 
* struct for a fresh server being added 
* to the cluster to catch up log with the leader.
*/
struct server_catching_up {
    raft_server_sptr                  s;
    /*
    * number of rounds of replication since adding the server.
    */
    uint32_t                          rounds;
    /* time at which last round of replication is started */
    high_resolution_clock::time_point last_round;
};

typedef std::shared_ptr<server_catching_up>  server_catching_up_sptr;


#define RAFTCORE_MAP_FILE_PROT   PROT_WRITE | PROT_READ
#define RAFTCORE_MAP_FILE_FLAGS  MAP_SHARED

#define RAFTCORE_DEFAULT_HEARTBEAT_RATE 80
#define RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT 150
#define RAFTCORE_DEFAULT_MAX_ELEC_TIMEOUT 300
#define RAFTCORE_DEFAULT_ELECTION_RPC_TIMEOUT 70
#define RAFTCORE_DEFAULT_HEARTBEAT_RPC_TIMEOUT 70
#define RAFTCORE_DEFAULT_SERVER_CATCH_UP_ROUNDS 10
#define RAFTCORE_DEFAULT_RPC_PORT 5758

typedef std::unique_ptr<core_service_impl> core_service_impl_uptr;
typedef std::unique_ptr<boost::asio::io_service::work> ios_work_uptr;

/* raft protocol core implementation */
class raft: public noncopyable {
public:
    friend class core_service_impl;

    raft(std::string cfg_filename, log_committed_callback callback):
            work_(new boost::asio::io_service::work(ios_)), log_committed_cb_(callback), cfg_filename_(cfg_filename),
            cfg_(new core_config(cfg_filename)), election_timer_(ios_), heartbeat_timer_(ios_),
            inited_(false), started_(false), cur_role_(FOLLOWER),  
            server_transfer_leader_to_(nullptr), leader_transfer_timer_(ios_), 
            commit_idx_(0), last_applied_(0),
            timeout_now_id_(0), pre_vote_id_(0), request_vote_id_(0), append_entries_id_(0)
            {}

    raft(const core_config & config, log_committed_callback callback):
            work_(new boost::asio::io_service::work(ios_)), log_committed_cb_(callback), cfg_(new core_config(config)),
            election_timer_(ios_), heartbeat_timer_(ios_),
            inited_(false), started_(false), cur_role_(FOLLOWER),
            server_transfer_leader_to_(nullptr), leader_transfer_timer_(ios_),
            commit_idx_(0), last_applied_(0),
            timeout_now_id_(0), pre_vote_id_(0), request_vote_id_(0), append_entries_id_(0)
            {}
    
    /* initialize raft protocol, configurations, timers and heartbeating */
    rc_errno init();

    /* start raft protocol */
    rc_errno start();
    /* stop raft protocol */
    rc_errno stop();

    /*
    *  append a log entry to local storage and replicate over a majority of servers.
    */
    rc_errno append_log_entry(const std::string & data);

    void log_committed_cb(const log_committed_callback & cb) { log_committed_cb_ = cb; }

    log_committed_callback log_committed_cb() { return log_committed_cb_; }
private:
    raft();
    /*
    * use network interface address(@self_id_) to bootstrap the cluster if the log contains no configuration entry, 
    * after bootstrapping, this cluster contains only this server which is also the leader.
    * other servers could be added by membership change mechanism.
    */
    rc_errno bootstrap_cluster_config();
    /* serialize currnet servers configuration to a string */
    std::string serialize_configuration();

    /* create a server struct given an address */
    raft_server_sptr make_raft_server(std::string address);

    void handle_catch_up_server_append_entries();
    /* add a new server to current configuration and replicate configuration to other servers */
    rc_errno add_server(std::string address);
    /* remove a server from current configuration and replicate configuration to other servers */
    rc_errno remove_server(std::string address);
    /* adjust current confituration when a new configuration entry is stored on the server */
    void adjust_configuration();
        
    /*
    * find the server who has the most up-to-datee log,
    * return @self_id_, if there is only one server in the cluster(which is this server itself).
    */
    std::string find_most_up_to_date_server();
    
    void timeout_now_done(timeout_now_task_sptr task);;
    void timeout_now(raft_server * s);
    /* issue a TimeoutNow request to the transferee if its log is up-to-date*/
    void handle_transfer_leader_to(raft_server * s);
    /* handle timeout events associated with leadership transfer */
    void handle_leader_transfer_timeout(raft_server * s, const boost::system::error_code& error);
    /* transfer leadership to the server with address @address */
    rc_errno leadership_transfer(std::string address);

    /* callback gets called asynchronously after a pre_vote call returned from one server */
    void pre_vote_done(pre_vote_task_sptr task);
    /* 
    * send pre_vote rpc to every server to see if can get a vote,
    */
    void pre_vote(bool early_vote = false);

    /* 
    * send request_vote rpc to a single server,
    */
    void request_vote(raft_server * s);

    /* kicks off a new round of election. */
    void leader_election();
    /* callback gets called asynchronously after a request_vote call returned from one server */
    void leader_election_done(request_vote_task_sptr task);
    /* 
    * regenerate a random number within [min_election_timeout_, max_election_timeout_] 
    * as timeout to start next round of election.
    */
    void reset_election_timer();
    void remove_election_timer();

    /* 
    * send append_entries rpc to a single server,
    */
    void append_entries_single(raft_server * s);
    /* as a leader, send regular heartbeat checks to other servers.
    *
    *   If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
    *   If successful: update nextIndex and matchIndex for follower
    *   If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
     */
    void append_entries();
    /* callback gets called asynchronously after a append_entries call returned from one server */
    void append_entries_done(append_entries_task_sptr task);
    /* reset heartbeat checking timer event. */
    void reset_heartbeat_timer();
    void remove_heartbeat_timer();

    /* 
    * transit role from leader to follower, 
    */
    void step_down(uint64_t new_term);

    /* transit role to leader */
    void step_up();

    /* 
    * only leader calls this function.
    * If there exists an N such that N > commitIndex, a majority
    * of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
    */
    void adjust_commit_idx();

    /* function for thread of calling callback when some log entries are committed */
    void commit_callback_thread();

    /* convenient function to get raftcore_map out of cmap_ */
    inline struct raftcore_map* core_map() { return static_cast<struct raftcore_map*>(cmap_->addr()); }

    /* gets call when eletion timer expired */
    void handle_election_timeout(const boost::system::error_code& error);
    /* gets call when heartbeat timer expired */
    void handle_heartbeat_timeout(const boost::system::error_code& error);
    /* generate stats in html */
    void handle_stat(const http::server::request& req, http::server::reply& rep);
    /* web interface to append data to log */
    void handle_append_log_entry(const http::server::request& req, http::server::reply& rep);
    /* web interface to add server to currnet cluster */
    void handle_add_server(const http::server::request& req, http::server::reply& rep);
    /* web interface to remove server to currnet cluster */
    void handle_remove_server(const http::server::request& req, http::server::reply& rep);
    /* web interface to transfer leadership */
    void handle_leader_transfer(const http::server::request& req, http::server::reply& rep);
    boost::asio::io_service     ios_;
    /* work keeps ios_ running even if there is no real work to do*/
    ios_work_uptr               work_;

    /*
    * For leader: called when a log entry has been replicated over a majority of servers.
    * For others: called when a log entry has been committed.
    * this is where the log could safely apply to state machine.
    */
    log_committed_callback      log_committed_cb_;

    /* configuration file */
    std::string                 cfg_filename_;

    /* configuration */
    core_config_uptr            cfg_;
    /* core_logger to persist log entries*/
    core_logger_uptr            log_;
    std::mutex                  lmtx_;
    /* file mapping for storing state need stay persistent */
    core_filemap_uptr           cmap_;
    std::mutex                  mmtx_;
    
    /* rpc server */
    async_rpc_server_uptr       rpc_server_;
    std::string                 rpc_bind_ip_;
    uint32_t                    rpc_port_;

    /* rpc service */
    core_service_impl_uptr      rpc_service_;
    /* called at the end of a rpc service method */
    service_done_callback_uptr  done_;

    /* channels for leader election and heartbeat checking */
    async_rpc_channel_uptr      election_channel_;
    async_rpc_channel_uptr      data_channel_;

    /* global mutex for this raft object */
    std::mutex                  gmtx_;
    
    /* kicks off a new round of election when this timer exprired */
    boost::asio::deadline_timer election_timer_;
    /* time range in milliseconds in which a random value will be picked to be the next election timeout */
    uint32_t                    min_election_timeout_;
    uint32_t                    max_election_timeout_;
    /* time after which a election rpc should be considered timed out, default is 100. */
    uint32_t                    election_rpc_timeout_;

    /* kicks off a new round of heartbeat check when this timer exprired */
    boost::asio::deadline_timer heartbeat_timer_;
    /* time in milliseconds after which a new round of heartbeat check will begin */
    uint32_t                    heartbeat_rate_;
    /* time after which a heartbeat checking rpc should be considered timed out, default is 100. */
    uint32_t                    heartbeat_rpc_timeout_;

    /* if raftcore is initialized */
    bool                        inited_;
    /* if raftcore is started */
    bool                        started_;
    /* current role of this protocol */
    raft_role                   cur_role_;
    /* lock for cur_role_*/
    std::mutex                  rmtx_;

    /* current server that leader is transferring leadership to, null if there is no leadership transfer */ 
    raft_server*                server_transfer_leader_to_;
    /* timer for timeout in leadership transfer */
    boost::asio::deadline_timer leader_transfer_timer_;

    /* index of highest log entry known to be committed (initialized to 0, increases monotonically) */
    std::atomic<uint64_t>       commit_idx_;
    /* index of highest log entry applied to state machine (initialized to 0, increases monotonically) */
    std::atomic<uint64_t>       last_applied_;

    /* # of pre votes received from other servers, pre_votes_+pre_votes_fails_ = # of servers in the cluster */
    uint64_t                    pre_votes_;
        /* # of pre vote rpc failure, to restart the election timer if can't get a majority of pre votes. */
    uint64_t                    pre_votes_fails_;
    /* # of votes received fromo ther servers in current term */
    uint64_t                    votes_;

    /* thread-safe queue for notifying some log entries are committed */
    thread_safe_queue<bool>     commit_queue_;

    /* stuff about the server that is catching up with the leader, null if there is no server being caught up */
    server_catching_up_sptr     cu_server_;
    uint32_t                    server_catch_up_rounds_;
    /* this server id */
    std::string                 self_id_;

    /* map of servers */
    raft_server_map             servers_;
    std::mutex                  servers_mtx_;

    /* non-deterministic source of random numbers*/
    std::random_device          rd_;
    mt19937_uptr                mt_;
    uniform_int_dist_uptr       dist_;

    uint64_t                    timeout_now_id_;
    uint64_t                    pre_vote_id_;
    uint64_t                    request_vote_id_;
    uint64_t                    append_entries_id_;

    std::string                 http_server_port_;
    http_server_uptr            http_server_;

    /* statistics */
    /* when did the server started */
    high_resolution_clock::time_point started_at_;
    /* current leader */
    std::string                cur_leader_;
    /* time of last heartbeat message */
    high_resolution_clock::time_point last_heartbeat_;
};

/* rpc service implementaion */
class core_service_impl: public RaftCoreService {
public:
    core_service_impl(raft* r):raft_(r) {}
    ~core_service_impl() {}

    /*
    * tell the server to time out immediately to start a new election
    */
    void timeout_now(::google::protobuf::RpcController* controller,
                      const ::raftcore::RequestVote*,
                      ::raftcore::RequestVoteRes*,
                      ::google::protobuf::Closure* done);

    /* pre-vote phase to see if this candidate will get the vote,
    * to prevent disruptions brought by servers rejoined the cluster from network partition.
    */
    void pre_vote(::google::protobuf::RpcController* controller,
                      const ::raftcore::RequestVote*,
                      ::raftcore::RequestVoteRes*,
                      ::google::protobuf::Closure* done);

    void request_vote(::google::protobuf::RpcController* controller,
                      const ::raftcore::RequestVote*,
                      ::raftcore::RequestVoteRes*,
                      ::google::protobuf::Closure* done);

    void append_entries(::google::protobuf::RpcController* controller,
                        const ::raftcore::AppendEntries*,
                        ::raftcore::AppendEntriesRes*,
                        ::google::protobuf::Closure* done);
    void done() {}
private:
    /* backpointer to raftcore, used to modify raftcore internal state*/
    raft* raft_;
};

}
#endif