/*
* Copyright (C) Xinjing Cho
*/
#include <signal.h>
#include <algorithm>

#include <raftcore/core_simple_logger.h>
#include <raftcore/raftcore.h>

namespace raftcore {

#define TIMING_START { \
    std::chrono::system_clock::time_point tp1 = std::chrono::system_clock::now(); \

#define TIMING_STOP(expr) \
    std::chrono::system_clock::time_point tp2 = std::chrono::system_clock::now();\
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(tp2 - tp1);\
    LOG_DEBUG << "time taken for " << expr << ": " << time_span.count();\
}

uint64_t raft::append_log_entry(const std::string & data) {
    if (cur_role_ != LEADER)
        return RC_NOT_LEADER;

    log_entry_sptr new_entry = make_log_entry(data.size());

    new_entry->idx = log_->last_entry_idx() + 1;
    new_entry->term = core_map()->cur_term;
    new_entry->cfg = false;
    log_->copy_log_data(new_entry.get(), data);

    if(log_->append(new_entry, true) != RC_GOOD)
        return 0;
    return new_entry->idx;
}

void raft::pre_vote_done(pre_vote_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    RequestVote              * req = &task->request;
    RequestVoteRes           * res = &task->response;

    raft_server              * s   = task->s;
    task->time_end = high_resolution_clock::now();
    LOG_DEBUG << "counter: " << counter 
              << " time taken for pre_vote[" << task->id
              << "] at term[" << req->term()
              << "] to finish: " << duration_cast<microseconds>(task->time_end - task->time_start).count() << "us";
    if (cur_role_ == CANDIDATE || cur_role_ == LEADER) {
        LOG_INFO << "pre_vote[" << task->id
                 << "]: election already started or elected as leader, ignoring stale response...";
        return;
    }

    if (ctl->ErrorCode() != carrot::EC_SUCCESS) {
        LOG_DEBUG << "pre_vote[" << task->id
                  << "] to server " << s->id
                  << " failed: " << ctl->ErrorText();
        ++pre_votes_fails_;
        goto checkout;
    }

    LOG_DEBUG << "pre_vote[" << task->id 
              << "] returned from server[" << s->id 
              << " with result: " << res->ShortDebugString();

    if (res->vote_granted() == true) {
        ++pre_votes_;
    } else {
        ++pre_votes_fails_;
    }

checkout:

    if (pre_votes_ > servers_.size() / 2) {
        LOG_INFO << "pre_vote phase succeed, initiating election.";
        /* now we are confident to start the election */
        leader_election();
    } else if (pre_votes_ + pre_votes_fails_ == servers_.size()) {
        LOG_INFO << "pre_vote phase failed, quit initiating election, restart election timer.";
        reset_election_timer();
    }
}


void raft::pre_vote(bool early_vote) {
    if (servers_.size() == 1 && servers_.find(self_id_) != servers_.end()) {
        /* we are bootstrapping, start election right away */
        leader_election();
        return;
    }

    /* pre vote for self only in the configuration */
    pre_votes_ = servers_.find(self_id_) == servers_.end() ? 0 : 1;
    pre_votes_fails_ = 0;

    cur_role_ = FOLLOWER;

    LOG_INFO << "election timer timed out, start pre_vote phase";
    for (auto it : servers_) {
        raft_server * s = it.second.get();

        if (s->id == self_id_)
            continue;

        pre_vote_task_sptr task = pre_vote_task_sptr(new pre_vote_task);

        task->s = s;
        task->id = ++pre_vote_id_;
        task->controller.host(s->address);
        task->controller.service(s->port);
        task->controller.timeout(election_rpc_timeout_);

        task->request.set_term(core_map()->cur_term);
        task->request.set_candidate_id(self_id_);
        task->request.set_last_log_idx(log_->last_entry_idx());
        task->request.set_last_log_term(log_->last_entry_term());
        task->request.set_early_vote(early_vote);

        task->done = ::google::protobuf::NewCallback(this, &raft::pre_vote_done, task);
        task->time_start = high_resolution_clock::now();
        /* issue a asynchronoous request_vote call to this server */
        s->request_vote_service->pre_vote(&task->controller,
                                              &task->request,
                                              &task->response,
                                              task->done);
        LOG_TRACE << "issued asynchronous pre_vote rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString();
    }

}
/* 
* send request_vote rpc to a single server,
* this function should called with this->lmtx_ and this->mmtx_ held
*/
void raft::request_vote(raft_server * s) {
    request_vote_task_sptr task = request_vote_task_sptr(new request_vote_task);

    task->s = s;
    task->id = ++request_vote_id_;
    task->controller.host(s->address);
    task->controller.service(s->port);
    task->controller.timeout(election_rpc_timeout_);

    task->request.set_term(core_map()->cur_term);
    task->request.set_candidate_id(self_id_);
    task->request.set_last_log_idx(log_->last_entry_idx());
    task->request.set_last_log_term(log_->last_entry_term());

    task->done = ::google::protobuf::NewCallback(this, &raft::leader_election_done, task);
    task->time_start = high_resolution_clock::now();
    /* issue a asynchronoous request_vote call to this server */
    s->request_vote_service->request_vote(&task->controller,
                                          &task->request,
                                          &task->response,
                                          task->done);
    LOG_TRACE << "issued asynchronous request_vote rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString();
}

void raft::leader_election_done(request_vote_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    RequestVote              * req = &task->request;
    RequestVoteRes           * res = &task->response;

    raft_server              * s   = task->s;
    task->time_end = high_resolution_clock::now();
    
    LOG_DEBUG << "counter: " << counter
              << " time taken for request_vote[" << task->id
              << "] at term[" << req->term()
              << "] to finish: " << duration_cast<microseconds>(task->time_end - task->time_start).count() << "us";

    if (ctl->ErrorCode() != carrot::EC_SUCCESS) {
        LOG_DEBUG << "request_vote[" << task->id
                  << "] to server " << s->id
                  << " failed: " << ctl->ErrorText();
        return;
    }


    LOG_DEBUG << "request_vote[" << task->id 
              << "] returned from server[" << s->id 
              << " with result: " << res->ShortDebugString();

    if (cur_role_ != CANDIDATE) {
        LOG_INFO << "request_vote[" << task->id
                 << "]: currnet role is not candidate, ignoring stale response...";
        return;
    }

    if (res->vote_granted() == true) {
        if (res->term() < core_map()->cur_term) {
            LOG_DEBUG << "request_vote[" << task->id
                      << "]: vote's term " << req->term()
                      << " cur_term " << core_map()->cur_term
                      << ", stale vote, discarding...";
            return;
        }
        /* no need for protection as only one thread mutates vote count. */
        ++votes_;
        LOG_INFO << "request_vote[" << task->id
                 << "] at term " << req->term()
                 << " got granted from " << ctl->host();

        /* one for self, check if we have enough votes. */
        if (votes_ > servers_.size() / 2) {
            /* we are leader now */
            step_up();
        }
    } else if (res->term() > core_map()->cur_term) {
        /* step down if there is a higher term going on*/
        LOG_INFO << "calling step_down from raft::leader_election_done";
        step_down(res->term(), "");
    }
}

void raft::leader_election()
{
    cur_leader_ = "unknown";

    /* convert to candidate */
    cur_role_ = CANDIDATE;

    TIMING_START
    /* increment current term, clear votedFor and persist it to disk */
    ++core_map()->cur_term;
    ::strncpy(core_map()->voted_for, self_id_.c_str(), sizeof(core_map()->voted_for));
    cmap_->sync_all();
    TIMING_STOP("leader_election cmap_->sync_all();")

    /* vote for self only if in the configration */
    votes_ = servers_.find(self_id_) == servers_.end() ? 0 : 1;

    LOG_INFO << "start a new election at term: " << core_map()->cur_term;
    if (servers_.size() == 1 && servers_.find(self_id_) != servers_.end()) {
        /* we are bootstrapping, become leader right away */
        step_up();
        return;
    }

    /* send request vote to all servers */
    for (auto it : servers_) {
        raft_server_sptr s = it.second;

        if (s->id == self_id_)
            continue;
        
        request_vote(s.get());
    }

    /* reset election timer for next round */
    reset_election_timer();
}

void raft::handle_election_timeout(const boost::system::error_code& error)
{
    if (!error) {
        /* 
        * First we do a poll among other servers to see
        * if we can get enough votes to win the real election,
        * if not, do not initiate the election to prevent disruptions.
        */
        pre_vote();
    } else if (error.value() != boost::system::errc::operation_canceled){
        LOG_ERROR << "raft::handle_election_timeout error: " << error.message();
    } else {
        LOG_TRACE << "election timer canceled";
    }
}

void raft::reset_election_timer() {
    uint32_t timeout = (*dist_.get())(*mt_.get());
    LOG_TRACE << "next election timeout: " << timeout;
    election_timer_.expires_from_now(boost::posix_time::milliseconds(timeout));
    election_timer_.async_wait(boost::bind(&raft::handle_election_timeout, this,
        boost::asio::placeholders::error));
}

void raft::remove_election_timer() {
    boost::system::error_code error;
    election_timer_.cancel(error);
    if (error)
        LOG_ERROR << "election_timer_.cancel error: " << error.message();
}

/* 
* send append_entries rpc to a single server,
* this function should called with this->lmtx_ and this->mmtx_ held
*/
void raft::append_entries_single(raft_server * s) {
    append_entries_task_sptr task = append_entries_task_sptr(new append_entries_task);
    if (task.get() == nullptr) {
        LOG_ERROR << "failed to allocate memory for append_entries_task";
        return;
    }

    task->s = s;
    task->id = ++append_entries_id_;
    task->controller.host(s->address);
    task->controller.service(s->port);
    task->controller.timeout(heartbeat_rpc_timeout_);
    task->request.set_term(core_map()->cur_term);
    task->request.set_leader_id(self_id_);
    log_entry * prev_log = (*log_)[s->next_idx - 1];
    if (prev_log == nullptr) {
        throw std::runtime_error("prev_log is nullptr, this should never happen");
    }
    task->request.set_prev_log_idx(prev_log->idx);
    task->request.set_prev_log_term(prev_log->term);
    task->request.set_leader_commit(commit_idx_);
    task->request.set_id(task->id);
    task->done = ::google::protobuf::NewCallback(this, &raft::append_entries_done, task);
    /* 
    *  If last log index ≥ next_idx for a follower:
    *     send AppendEntries RPC with log entries starting at next_idx.
    */
    for (uint64_t i = s->next_idx; i <= log_->last_entry_idx(); ++i) {
        LogEntry * e = task->request.add_entries();
        if (e == nullptr) {
            LOG_ERROR << "failed to allocate memory for LogEntry for server " << s->id << ", skipping...";
            return;
        }
        log_entry * l = (*log_)[i];
        e->set_term(l->term);
        e->set_idx(l->idx);
        e->set_data(std::move(std::string(l->data, LOG_ENTRY_DATA_LEN(l))));
        e->set_config(l->cfg);
    }
    task->time_start = high_resolution_clock::now();
    s->append_entries_service->append_entries(&task->controller,
                                              &task->request,
                                              &task->response,
                                              task->done);
    LOG_TRACE << "issued asynchronous append_entries rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString();
}
    
/*
*   If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
*       If successful: update nextIndex and matchIndex for follower
*       If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
*/
void raft::append_entries() {
    reset_heartbeat_timer();

    if (cur_role_ != LEADER) {
        return;
    }

    /* update last heartbeat time of leader it self */
    last_heartbeat_ = high_resolution_clock::now();

    for (auto it : servers_) {
        raft_server_sptr s = it.second;

        if (s->id == self_id_)
            continue;

        append_entries_single(s.get());
    }

    /* adjust commit index if there is only one server(leader) in the cluster */
    if (servers_.size() == 1 && servers_.find(self_id_) != servers_.end())
        adjust_commit_idx();
}

void raft::append_entries_done(append_entries_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    AppendEntries            * req = &task->request;
    AppendEntriesRes         * res = &task->response;
    raft_server              * s   = task->s;

    task->time_end = high_resolution_clock::now();

    LOG_DEBUG << "counter: " << counter
              << " time taken for append_entries[" << task->id
              << "] at term[" << req->term()
              << "] to finish: " << duration_cast<microseconds>(task->time_end - task->time_start).count() << "us";

    if (ctl->Failed() || ctl->ErrorCode() != carrot::EC_SUCCESS) {
        LOG_DEBUG << "append_entries[" << task->id
                  << "]: failed to append_entries to server " << s->id
                  << ", error code: " << ctl->ErrorCode()
                  << ", error text: " << ctl->ErrorText();
        if (recfg_.get() && recfg_->s.get() == s && recfg_->type == server_addition) {
            handle_catch_up_server_append_entries();
        }
        return;
    }

    LOG_DEBUG << "append_entries[" << task->id 
              << "] returned from server[" << s->id 
              << " with result: " << res->ShortDebugString();

    if (cur_role_ != LEADER) {
        LOG_DEBUG << "append_entries[" << task->id << "]: not leader anymore, discarding stale append_entries_done call";
        return;
    }

    if (res->success() == true) {
        /* update nextIndex and matchIndex for follower except for heartbeat checks */
        s->next_idx = res->match_idx() + 1;
        s->match_idx = res->match_idx();
        adjust_commit_idx();
        if (s->transfer_leader_to) {
            handle_transfer_leader_to(s);
        }
        if (recfg_.get() && recfg_->s.get() == s && recfg_->type == server_addition) {
            handle_catch_up_server_append_entries();
        }
    } else if (res->term() > core_map()->cur_term) { /* if we have a higher term */
        /* we've already had a another leader, convert to follower */
        LOG_INFO << "calling step_down from raft::append_entries_done";
        step_down(res->term(), s->id);
    } else {
        /* failed due to log inconsistency: use log length responded by server and retry */
        s->next_idx = res->match_idx() + 1;
        LOG_DEBUG << "append_entries[" << task->id << "]: log inconsistency, retrying with nextIndex[" << s->next_idx << "]...";
        append_entries_single(s);
    }
}

void raft::handle_heartbeat_timeout(const boost::system::error_code& error) {
    if (!error) {
        append_entries();
    } else if (error.value() != boost::system::errc::operation_canceled){
        LOG_DEBUG << "raft::handle_heartbeat_timeout error: " << error.message();
    } else {
        LOG_DEBUG << "heartbeat check timer canceled";
    }
}

void raft::reset_heartbeat_timer() {
    heartbeat_timer_.expires_from_now(boost::posix_time::milliseconds(heartbeat_rate_));
    heartbeat_timer_.async_wait(boost::bind(&raft::handle_heartbeat_timeout, this,
        boost::asio::placeholders::error));
}

void raft::remove_heartbeat_timer() {
    boost::system::error_code error;
    heartbeat_timer_.cancel(error);
    if (error)
        LOG_ERROR << "election_timer_.cancel error: " << error.message();
}

void raft::step_down(uint64_t new_term, std::string cur_leader) {
    LOG_INFO << "stepped down at term " << core_map()->cur_term << " to new term " << new_term;
    remove_heartbeat_timer();

    cur_role_ = FOLLOWER;
    TIMING_START
    core_map()->cur_term = new_term;
    cmap_->sync_all();
    TIMING_STOP("step_down cmap_->sync_all()")

    LOG_DEBUG << "resetting election timer by step_down...";
    reset_election_timer();

    if (server_transfer_leader_to_){
        LOG_INFO << "Leaership successfully transfered to " << server_transfer_leader_to_->id;
        server_transfer_leader_to_->transfer_leader_to = false;
        server_transfer_leader_to_ = nullptr;
        if (recfg_.get() && recfg_->type == server_removal) {
            cur_leader_ = cur_leader;
            complete_reconfiguration("OK");
            LOG_INFO << "recfg_.reset() step_down, current leader now is " << cur_leader_;

            /* cancel leader transfer timer */
            boost::system::error_code ec;
            leader_transfer_timer_.cancel(ec);
        }
    }
}

void raft::step_up() {
    cur_role_ = LEADER;
    cur_leader_ = self_id_;
    LOG_INFO << "now i'm the leader[" << self_id_ << "] at term " << core_map()->cur_term << ", sending out heartbeats.";
    for (auto it : servers_) {
        it.second->next_idx = log_->last_entry_idx() + 1;
        it.second->match_idx = 0;
    }

    remove_election_timer();

    /* send heartbeat checks to declare leadership(append_entries) */
    append_entries();
}

void raft::complete_reconfiguration(std::string status) {
    recfg_->http_reply.status = http::server::reply::ok;
    recfg_->http_reply.content.append(status + " " + cur_leader_);
    recfg_->http_reply.headers[0].value = std::to_string(recfg_->http_reply.content.size());
    recfg_->http_reply.ready();
    /* empty pointer to handle next reconfiguration */
    recfg_.reset();
}
/* 
* only leader calls this function.
* If there exists an N such that N > commitIndex, a majority
* of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N.
* That is, find the highest index of the entry that a majority of servers have accepted.
* If a configuration entry is considered committed and the current leader is not in the 
* configuration, we simply step down and shut down the server.
*/
void raft::adjust_commit_idx() {
    uint64_t max_match = 0;
    uint64_t min_match = log_->last_entry_idx();
    uint32_t old_commit = commit_idx_;

    /* find out a possible range of commit_idx */
    for (auto it : servers_) {
        raft_server_sptr s = it.second;
        if (s->id == self_id_)
            continue;
        if (s->match_idx > commit_idx_ && (*log_)[s->match_idx]->term == core_map()->cur_term) {
            max_match = std::max(max_match, s->match_idx);
            min_match = std::min(min_match, s->match_idx);
        }
    }

    /* 
    * loop from the highest index to the lowest to see if
    * there is a majority servers have accepted the entry.
    */
    for (uint64_t i = max_match; i >= min_match; --i) {
        /* count leader in the majority only if in the configuration */
        uint32_t count = servers_.find(self_id_) == servers_.end() ? 0 : 1;
        for (auto it : servers_) {
            raft_server_sptr s = it.second;
            if (s->id == self_id_)
                continue;

            if (s->match_idx >= i)
                ++count;
        }

        /* see if fomrs a majority at index @i */
        if (count > servers_.size() / 2){
            commit_idx_ = i;
            /* notify commit_callback_thread */
            commit_queue_.push(false);
            break;
        }
    }
    /* if there is only one server in the cluster, set commit idx to last log entry's index*/
    if (servers_.size() == 1 && servers_.find(self_id_) != servers_.end()){
        commit_idx_ = log_->last_entry_idx();
        if (old_commit < commit_idx_)
            commit_queue_.push(false);
    }


    if (old_commit < log_->cfg_entry_idx() && 
        log_->cfg_entry_idx() <= commit_idx_ &&
        recfg_.get()) {
        /* a reconfiguration entry is committed, reply to client */
        assert(recfg_->s.get());
        switch(recfg_->type) {
            case server_addition:
            LOG_INFO << "successfully added the server["
                     << recfg_->s->id
                     << "] to the configuration";

            complete_reconfiguration("OK");
            break;
            case server_removal:
            LOG_INFO << "successfully removed the server["
                     << recfg_->s->id
                     << "] from the configuration";
            complete_reconfiguration("OK");
            break;
        }
        LOG_INFO << "recfg_.reset() adjust_commit_idx";
    }
}

std::string raft::serialize_configuration() {
    std::string res;

    for (auto it : servers_) {
        res.append(it.second->id);
        res.append(",");
    }
    size_t pos = res.find_last_of(',');

    if (pos != std::string::npos) {
        res.erase(res.begin() + pos);
    }
    
    return res;
}

void raft::make_server_id(std::string & address) {
    std::vector<std::string> v = split(address, ":");

    if (v.size() == 1) {
        if (address == interface_address_)
            address += ":" + rpc_port_;
        else
            address += ":" + std::string(RAFTCORE_DEFAULT_RPC_PORT);
    }
}

raft_server_sptr raft::make_raft_server(const std::string & address, const std::string & default_port) {
    raft_server_sptr server = raft_server_sptr(new raft_server);
        
    if (server.get() == nullptr) {
        LOG_FATAL << "failed to allocate space for server " << address << ", out of memory";
        return raft_server_sptr(nullptr);
    }

    std::vector<std::string> v = split(address, ":");

    if (v.size() == 1){
        server->address = address;
        LOG_INFO << "make_raft_server: missing rpc port for server " << address 
                 << ", " << default_port << " is used as default";
        server->port = default_port;
    } else {
        server->address = v[0];
        server->port = v[1];
    }

    ::strncpy(server->id, (server->address + ":" + server->port).c_str(), sizeof(server->id));

    server->alive = false;

    server->next_idx = log_->last_entry_idx() + 1;

    server->match_idx = 0;

    server->transfer_leader_to = false;

    server->request_vote_service = raftcore_service_uptr(new RaftCoreService::Stub(election_channel_.get()));

    server->append_entries_service = raftcore_service_uptr(new RaftCoreService::Stub(data_channel_.get()));

    return server;
}

std::string raft::find_most_up_to_date_server() {
    raft_server_sptr best;
    for (auto it : servers_) {
        raft_server_sptr s = it.second;

        if (s->id == self_id_)
            continue;

        if (best.get() == nullptr || best->match_idx < s->match_idx)
            best = s;
    }

    return best.get() == nullptr ? self_id_ : best->id;
}

void raft::handle_catch_up_server_append_entries() {
    assert(recfg_.get() && recfg_->type == server_addition);
    raft_server_sptr    s = recfg_->s;
    rc_errno            rc;

    if (recfg_.get() == nullptr) {
        LOG_INFO << "server " << recfg_->s->id << " is no longer catching up , ignoring...";
        return;
    }

    high_resolution_clock::time_point now = high_resolution_clock::now();
    
    uint64_t msecs = duration_cast<milliseconds>(now - recfg_->last_round).count();

    if (recfg_->rounds < server_catch_up_rounds_) {
        /* keep working */
        recfg_->rounds++;
        recfg_->last_round = high_resolution_clock::now();
        LOG_INFO << "start round " << recfg_->rounds << " of replication to server " << s->id;
        append_entries_single(s.get());
    } else if (msecs < min_election_timeout_ && log_->last_entry_idx() - s->match_idx <= 5) { 
        LOG_INFO << "after " << server_catch_up_rounds_ << " rounds of replication, "
                 << "the new server [" << s->id
                 << "] caught up with leader, deploying the reconfiguration...";
        
        if(!recfg_->entry_added) {
            /*
            * add to the configuration only if last round
            * of replication finished within the minimum
            * election timeout
            */
            servers_.insert(std::make_pair(s->id, s));

            std::string cfg_serialized = serialize_configuration();
            log_entry_sptr cfg_entry = make_log_entry(cfg_serialized.size(), true);
            cfg_entry->idx = log_->last_entry_idx() + 1;
            cfg_entry->term = core_map()->cur_term;
            cfg_entry->cfg = true;
            log_->copy_log_data(cfg_entry.get(), cfg_serialized, true);
            
            if (log_->append(cfg_entry, true) != RC_GOOD) {
                LOG_ERROR << "append_entries: failed to append configuration entry to log, error code: " << rc;
                complete_reconfiguration("ERROR");
                LOG_INFO << "recfg_.reset() handle_catch_up_server_append_entries 1";
                return;
            }
            recfg_->entry_added = true;
        }
        /* replicate new configuration entry to the rest of the cluster */
        append_entries();
    } else if (recfg_->rounds >= server_catch_up_rounds_) {
        /*
        * after @server_catch_up_rounds_ rounds of replication,
        * the server is still not caught up with the leader,
        * abort the configuration
        */
        LOG_INFO << "after " << server_catch_up_rounds_ << " rounds of replication, "
                 << "the new server [" << s->id
                 << "] is still " << (int)((1 - (double)s->match_idx / log_->last_entry_idx()) * 100)
                 << "% behind the leader, abort the reconfiguration.";
        complete_reconfiguration("TIMEOUT");
        LOG_INFO << "recfg_.reset() handle_catch_up_server_append_entries 2";
    }

}
rc_errno raft::add_server(std::string address, http::server::reply& rep) {
    make_server_id(address);
    if (cur_role_ != LEADER) {
        LOG_INFO << "add_new_server: current role is not leader, ignoring...";
        rep.content.append("NOT_LEADER " + cur_leader_);
        return RC_ERROR;
    } else if (servers_.find(address) != servers_.end()) {
        LOG_INFO << "add_new_server: server " << address << " already in the configuration, ignoring...";
        rep.content.append("IN_CFG " + cur_leader_);
        return RC_ERROR;
    } else if (!is_valid_ipv4_address(address)) {
        LOG_INFO << "add_new_server: invalid server address " << address << ", ignoring...";
        rep.content.append("INVALID_SERVER " + cur_leader_);
        return RC_ERROR;
    } else if (recfg_.get()) {
        LOG_INFO << "add_new_server: reconfiguration of server "<< recfg_->s->id
                 << " is in progress...";
        rep.content.append("LIMIT " + cur_leader_);
        return RC_ERROR;
    }

    raft_server_sptr s = make_raft_server(address, RAFTCORE_DEFAULT_RPC_PORT);

    if (s.get() == nullptr) {
        LOG_FATAL << "failed to allocate space for server " << address << ", out of memory";
        rep.content.append("IN_CFG " + cur_leader_);
        return RC_OOM;
    }

    recfg_ = server_reconfiguration_sptr(new server_reconfiguration(rep));
    recfg_->address = address;
    recfg_->type = server_addition;
    recfg_->s = s;
    recfg_->rounds = 1;
    recfg_->last_round = high_resolution_clock::now();

    LOG_INFO << "start catching up phase for server[" << s->id << "].";
    LOG_INFO << "start round " << recfg_->rounds << " of replication to server " << s->id;

    rep.ready_to_go = false;
    
    append_entries_single(s.get());
    return RC_GOOD;
}

rc_errno raft::remove_server(std::string address, http::server::reply& rep) {
    rc_errno rc;
    make_server_id(address);
    if (cur_role_ != LEADER) {
        LOG_INFO << "remove_server: current role is not leader, ignoring...";
        rep.content.append("NOT_LEADER " + cur_leader_);
        return RC_ERROR;
    } else if (servers_.find(address) == servers_.end()) {
        LOG_INFO << "remove_server: server " << address << " not in the configuration, ignoring...";
        rep.content.append("NOT_IN_CFG " + cur_leader_);
        return RC_ERROR;
    } else if (recfg_.get()) {
        LOG_INFO << "remove_server: reconfiguration of server "<< recfg_->s->id
                 << " is in progress...";
        rep.content.append("LIMIT " + cur_leader_);
        return RC_ERROR;
    }

    raft_server_sptr s = servers_.find(address)->second;

    servers_.erase(address);

    std::string cfg_serialized = serialize_configuration();
    log_entry_sptr cfg_entry = make_log_entry(cfg_serialized.size(), true);
    cfg_entry->idx = log_->last_entry_idx() + 1;
    cfg_entry->term = core_map()->cur_term;
    cfg_entry->cfg = true;
    log_->copy_log_data(cfg_entry.get(), cfg_serialized, true);
    
    if ((rc = log_->append(cfg_entry, true)) != RC_GOOD) {
        LOG_ERROR << "remove_server: failed to append configuration entry to log, error code: " << rc;
        rep.content.append("ERROR " + cur_leader_);
        return RC_ERROR;
    }

    /* remove leader itself from the configuration */
    if (address == self_id_) {
        /* 
        * pick the best server to transfer leadership to,
        * let the new leader to complete the reconfiguration.
        */
        if ((rc = leadership_transfer(find_most_up_to_date_server())) == RC_GOOD) {
            rep.ready_to_go = false;
            recfg_ = server_reconfiguration_sptr(new server_reconfiguration(rep));
            recfg_->address = address;
            recfg_->type = server_removal;
            recfg_->s = s;
            return RC_GOOD;
        } else {
            rep.content.append("ERROR " + cur_leader_);
            return RC_ERROR;
        }
    } else {
        rep.ready_to_go = false;
        recfg_ = server_reconfiguration_sptr(new server_reconfiguration(rep));
        recfg_->address = address;
        recfg_->type = server_removal;
        recfg_->s = s;
        /* replicate new configuration entry to the rest of the cluster */
        append_entries();
        return RC_GOOD;
    }
}

void raft::timeout_now_done(timeout_now_task_sptr task) {
    carrot::CarrotController * ctl = &task->controller;

    if (ctl->ErrorCode() != carrot::EC_SUCCESS) {
        LOG_DEBUG << "timeout_now[" << task->id
                  << "] to server " << task->s->id
                  << " failed: " << ctl->ErrorText();
        if (task->s->transfer_leader_to) {
            LOG_INFO << "retry timeout_now to " << task->s->id;
            timeout_now(task->s);
        }
    }
}

void raft::timeout_now(raft_server * s) {
    timeout_now_task_sptr task = timeout_now_task_sptr(new timeout_now_task);

    task->s = s;
    task->id = ++timeout_now_id_;
    task->controller.host(s->address);
    task->controller.service(s->port);
    task->controller.timeout(election_rpc_timeout_);

    task->request.set_term(core_map()->cur_term);
    task->request.set_candidate_id(self_id_);
    task->request.set_last_log_idx(log_->last_entry_idx());
    task->request.set_last_log_term(log_->last_entry_term());

    task->done = ::google::protobuf::NewCallback(this, &raft::timeout_now_done, task);
    task->time_start = high_resolution_clock::now();
    /* issue a asynchronoous request_vote call to this server */
    s->request_vote_service->timeout_now(&task->controller,
                                          &task->request,
                                          &task->response,
                                          task->done);
    LOG_TRACE << "issued asynchronous timeout_now rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString();
}

void raft::handle_transfer_leader_to(raft_server * s) {
    if (s->match_idx == log_->last_entry_idx()) {
        /* transferee's log is up-to-date to leader's, issue TimeoutNow rpc */
        timeout_now(s);
    }
}

void raft::handle_leader_transfer_timeout(raft_server * s, const boost::system::error_code& error)
{
    if (!error) {
        LOG_INFO << "failed to transfer leadership to " << s->id << ", timed out.";
        s->transfer_leader_to = false;
        server_transfer_leader_to_ = nullptr;
        if (recfg_.get() && recfg_->type == server_removal) {
            complete_reconfiguration("TIMEOUT");
            LOG_INFO << "recfg_.reset() handle_leader_transfer_timeout";
        }
    } else if (error.value() != boost::system::errc::operation_canceled){
        LOG_ERROR << "raft::handle_leader_transfer_timeout error: " << error.message();
    } else {
        LOG_TRACE << "leadership transfer timer canceled";
    }
}

rc_errno raft::leadership_transfer(std::string address) {
    make_server_id(address);
    if (cur_role_ != LEADER) {
        LOG_INFO << "leadership_transfer: current role is not leader, ignoring...";
        return RC_ERROR;
    } else if (address == "") {
        LOG_INFO << "leadership_transfer: couldn't transfer leadership to server whose address is mepty";
        return RC_ERROR;
    } else if (server_transfer_leader_to_ != nullptr) {
        LOG_INFO << "leadership_transfer: a leadership transfer is going on, ignoring...";
        return RC_ERROR;
    } else if (address == self_id_) {
        LOG_INFO << "leadership_transfer: already leader, ignoring...";
        return RC_ERROR;
    } else if (servers_.find(address) == servers_.end()) {
        LOG_INFO << "leadership_transfer: target server " << address << " not in the configuration, ignoring...";
        return RC_ERROR;
    }

    raft_server_sptr s = servers_.find(address)->second;
    s->transfer_leader_to = true;
    server_transfer_leader_to_ = s.get();

    LOG_INFO << "prepare to transfer leadership to " << s->id;
    if (s->match_idx == log_->last_entry_idx()) {
        /* transferee's log is already up-to-date to leader's, issue TimeoutNow rpc */
        timeout_now(s.get());
    } else {
        /* transfer missing entries to server if any */
        append_entries_single(s.get());
        /* set up timer */
        leader_transfer_timer_.expires_from_now(boost::posix_time::milliseconds(min_election_timeout_));
        leader_transfer_timer_.async_wait(boost::bind(&raft::handle_leader_transfer_timeout, this, s.get(),
            boost::asio::placeholders::error));
    }
    
    return RC_GOOD;
}

void raft::adjust_configuration() {
    std::string new_config = log_->config();
    LOG_INFO << "adjust_configuration: new_config " << new_config;
    std::vector<std::string> addresses = split(new_config, ",");
    /* server_addition */
    for (auto address : addresses) {
        if (servers_.find(address) == servers_.end()) {
            raft_server_sptr s = make_raft_server(address, RAFTCORE_DEFAULT_RPC_PORT);
            if (s.get() == nullptr)
                throw std::bad_alloc();
            servers_.insert(std::make_pair(address, s));
            LOG_INFO << "adjust_configuration: added new server " << address << " to configuration.";
        }
    }
    /* server_removal */
    for (auto it = servers_.begin(); it != servers_.end();) {
        std::string address = it->first;

        if (std::find(addresses.begin(), addresses.end(), address) == addresses.end()) {
            servers_.erase(it++);
            LOG_INFO << "adjust_configuration: removed server " << address << " from configuration.";
        } else {
            ++it;
        }
    }
}

void raft::commit_callback_thread() {
    bool get_out = false;
    while ((get_out = commit_queue_.pop()) == false) {
        for (; last_applied_ < commit_idx_;) {
            log_entry* entry = (*log_)[++last_applied_];

            /* skip configuration entry */
            if (entry->cfg)
                continue;

            log_committed_cb_(entry);
        }
    }
}

void raft::handle_stat(const http::server::request& req, http::server::reply& rep) {
    high_resolution_clock::time_point now = high_resolution_clock::now();
    uint64_t secs = duration_cast<seconds>(now - started_at_).count();

    std::vector<std::string> v = split(cur_leader_, ":");
    std::string adderss = v[0];

    rep.status = http::server::reply::ok;
    rep.content.append("<html><head><title>raftcore@" + self_id_ + "</title></head>");
    // style for table
    rep.content.append("<style type='text/css'>table{border-collapse:collapse;border-spacing:0;border-left:1px solid #888;border-top:1px solid #888;background:#efefef;}\
th,td{border-right:1px solid #888;border-bottom:1px solid #888;padding:5px 15px;}\
th{font-weight:bold;background:#ccc;}</style>");

    rep.content.append("<body><div><h2>Hello, this is raftcore.</h2>");
    rep.content.append(std::string("<h3>server ip: ") + self_id_ + "</h3>");
    rep.content.append(std::string("<h3>rpc port: ") + rpc_port_ + "</h3>");
    rep.content.append(std::string("<h3>uptime: ") + 
                       std::to_string(secs / 86400) + "d " +
                       std::to_string((secs / 3600) % 24) + "h " +
                       std::to_string((secs / 60) % 60) + "m " +
                       std::to_string(secs % 60) + "s </h3>");
    rep.content.append(std::string("<h3>current role: " + 
                       std::string(cur_role_ == FOLLOWER ? "<font color='blue'>Follower</font>" : (cur_role_ == CANDIDATE ? "<font color='green'>Candidate</font>": "<font color='red'>Leader</font>")) 
                       + "</h3>"));
    rep.content.append(std::string("<h3>commit index: ") + std::to_string(commit_idx_) + "</h2>");
    rep.content.append(std::string("<h3>currnet term: ") + std::to_string(core_map()->cur_term) + "</h2>");
    rep.content.append(std::string("<h3>servers: ") + serialize_configuration() + "</h3>");
    rep.content.append(std::string("<h3>current leader: <a target='_blank' href='http://" + adderss+ ":" + http_server_port_ + "'>" + cur_leader_ + "</a></h3></div>"));

    if (cur_role_ == LEADER) {
        for (auto it : servers_) {
            if (it.second->id == self_id_)
                continue;
            rep.content.append("<div style='float: left;'><a href='/leader_transfer?server=" + std::string(it.second->id) + "'>transfer leadership</a></br><iframe style='margin:10px;' height='100%' width=500 frameborder=1 src='http://" + std::string(it.second->address) + ":" + http_server_port_ + "/'></iframe></div>");
        }
    }

    rep.content.append("<table>");
    rep.content.append("<thead><tr><td>index</td><td>term</td><td>data</td></tr></thead>");
    for (uint64_t i = log_->first_entry_idx(); i <= log_->last_entry_idx(); ++i) {
        log_entry * entry = (*log_)[i];
        if (i > commit_idx_){
            rep.content.append("<tr>");
        } else {
            rep.content.append("<tr style='background:green;'>");
        }
        rep.content.append("<td>" + std::to_string(entry->idx) + "</td>");
        rep.content.append("<td>" + std::to_string(entry->term) + "</td>");
        rep.content.append("<td>" + std::string(entry->data, LOG_ENTRY_DATA_LEN(entry)) + "</td>");
        rep.content.append("</tr>");
    }
    rep.content.append("</table>");
    if (cur_role_ == LEADER){
        rep.content.append("<form method='GET' action='/append_log_entry'><label>entry content:</label><input name='content' type='text'/><input type='submit' value='append'/></form>");
        rep.content.append("<form method='GET' action='/add_server'><label>add server:</label><input name='server' type='text'/><input type='submit' value='add'/></form>");
        rep.content.append("<form method='GET' action='/remove_server'><label>remove server:</label><input name='server' type='text'/><input type='submit' value='remove'/></form>");
    }
    rep.content.append("</body></html>");
    rep.headers.resize(2);
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("htm");
}

static std::string find_reuqest_param(std::string params, std::string key) {
    std::vector<std::string> amp_splitted_v = split(params, "&");

    for (auto pair : amp_splitted_v) {
        std::vector<std::string> equal_spliited_v = split(pair, "=");
        if (equal_spliited_v.size() == 2 && equal_spliited_v[0] == key) {
            return equal_spliited_v[1];
        }
    }

    return std::string("");
}

void raft::handle_append_log_entry(const http::server::request& req, http::server::reply& rep) {
    if (req.method != "GET"){
        LOG_DEBUG << "append_log_entry: request method is not GET: " << req.method;
        rep = http::server::reply::stock_reply(http::server::reply::bad_request);
        return;
    }
    rep.content.append("<html>");
    
    std::vector<std::string> v = split(self_id_, ":");
    std::string address = v[0];
    if (cur_role_ != LEADER) {
        LOG_DEBUG << "append_log_entry: currnt role is not leader";
        rep.content.append("<head><script type='text/javascript'>alert('not leader!');window.location='http://" + address + ":" + http_server_port_ + "/';</script></head>");
    } else {
        size_t question_mark_pos = req.uri.find_last_of('?');
        if (question_mark_pos == std::string::npos) {
            LOG_DEBUG << "append_log_entry: parameter \'content\' is empty or not found";
            rep.content.append("<head><script type='text/javascript'>alert('\'content\' not found in the parameter!');window.location='http://" + address + ":" + http_server_port_ + "/';</script></head>");
        } else {
            std::string params = std::string(req.uri.begin() + question_mark_pos + 1, req.uri.end());
            std::string content = find_reuqest_param(params, "content");
            if (content == "") {
                LOG_DEBUG << "append_log_entry: parameter \'content\' is empty or not found";
                rep.content.append("<head><script type='text/javascript'>alert('\'content\' is empty or not found in the parameter!');window.location='http://" + address + ":" + http_server_port_ + "/';</script></head>");
            } else {
                log_entry_sptr new_entry = make_log_entry(content.size());
                new_entry->idx = log_->last_entry_idx() + 1;
                new_entry->term = core_map()->cur_term;
                new_entry->cfg = false;
                log_->copy_log_data(new_entry.get(), content);
                log_->append(new_entry, true);
                LOG_DEBUG << "append_log_entry: log entry[" << content << "] appended";
                rep.content.append("<head><script type='text/javascript'>window.location='http://" + address + ":" + http_server_port_ + "/';</script></head>");
            }
        }
    }
    rep.content.append("</html>");
    rep.headers.resize(2);
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("htm");
}

void raft::handle_add_server(const http::server::request& req, http::server::reply& rep) {
    rep.headers.resize(2);
    if (req.method != "GET"){
        LOG_DEBUG << "handle_add-server: request method is not GET: " << req.method;
        rep = http::server::reply::stock_reply(http::server::reply::bad_request);
        return;
    }
    if (cur_role_ != LEADER) {
        LOG_DEBUG << "handle_add_server: currnt role is not leader";
        rep.content.append("NOT_LEADER " + cur_leader_ );
    } else {
        size_t question_mark_pos = req.uri.find_last_of('?');
        if (question_mark_pos == std::string::npos) {
            LOG_DEBUG << "handle_add_server: parameter \'server\' is empty or not found";
            rep.content.append("EMPTY_SERVER " + cur_leader_);
        } else {
            std::string params = std::string(req.uri.begin() + question_mark_pos + 1, req.uri.end());
            std::string server = find_reuqest_param(params, "server");
            if (server == "") {
                LOG_DEBUG << "handle_add_server: parameter \'server\' is empty or not found";
                rep.content.append("EMPTY_SERVER " + cur_leader_);
            } else {
                rc_errno rc = add_server(server, rep);
                if (rc != RC_GOOD)
                    LOG_INFO << "handle_add_server: errno " << rc;
            }
        }
    }
    rep.status = http::server::reply::ok;
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("text/plain");
}

void raft::handle_remove_server(const http::server::request& req, http::server::reply& rep) {
    rep.headers.resize(2);
    if (req.method != "GET"){
        LOG_DEBUG << "handle_remove_server: request method is not GET: " << req.method;
        rep = http::server::reply::stock_reply(http::server::reply::bad_request);
        return;
    }
    

    if (cur_role_ != LEADER) {
        LOG_DEBUG << "handle_remove_server: currnt role is not leader";
        rep.content.append("NOT_LEADER " + cur_leader_ );
    } else {
        size_t question_mark_pos = req.uri.find_last_of('?');
        if (question_mark_pos == std::string::npos) {
            LOG_DEBUG << "handle_remove_server: parameter \'server\' is empty or not found";
            rep.content.append("EMPTY_SERVER " + cur_leader_);
        } else {
            std::string params = std::string(req.uri.begin() + question_mark_pos + 1, req.uri.end());
            std::string server = find_reuqest_param(params, "server");
            if (server == "") {
                LOG_DEBUG << "handle_remove_server: parameter \'server\' is empty or not found";
                rep.content.append("EMPTY_SERVER " + cur_leader_);
            } else {
                rc_errno rc = remove_server(server, rep);
                if (rc != RC_GOOD)
                    LOG_INFO << "handle_remove_server: errno " << rc;
            } 
        }
    }
    rep.status = http::server::reply::ok;
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("text/plain");
}

void raft::handle_list_server(const http::server::request& req, http::server::reply& rep) {
    rep.content.append(serialize_configuration());
    rep.status = http::server::reply::ok;
    rep.headers.resize(2);
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("htm");
}

void raft::handle_leader_transfer(const http::server::request& req, http::server::reply& rep) {
    if (req.method != "GET"){
        LOG_DEBUG << "handle_leader_transfer: request method is not GET: " << req.method;
        rep = http::server::reply::stock_reply(http::server::reply::bad_request);
        return;
    }
    rep.content.append("<html>");
    
    if (cur_role_ != LEADER) {
        LOG_DEBUG << "handle_leader_transfer: currnt role is not leader";
        rep.content.append("<head><script type='text/javascript'>alert('not leader!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
    } else {
        size_t question_mark_pos = req.uri.find_last_of('?');
        if (question_mark_pos == std::string::npos) {
            LOG_DEBUG << "handle_leader_transfer: parameter \'server\' is empty or not found";
            rep.content.append("<head><script type='text/javascript'>alert('\'server\' not found in the parameter!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
        } else {
            std::string params = std::string(req.uri.begin() + question_mark_pos + 1, req.uri.end());
            std::string server = find_reuqest_param(params, "server");
            if (server == "") {
                LOG_DEBUG << "handle_leader_transfer: parameter \'server\' is empty or not found";
                rep.content.append("<head><script type='text/javascript'>alert('\'server\' is empty or not found in the parameter!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
            } else {
                rc_errno rc = leadership_transfer(server);
                if (rc != RC_GOOD)
                    LOG_INFO << "handle_leader_transfer: errno " << rc;
                rep.content.append("<head><script type='text/javascript'>window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
            }
        }
    }
    rep.content.append("</html>");
    rep.status = http::server::reply::ok;
    rep.headers.resize(2);
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("htm");
}

rc_errno raft::bootstrap_cluster_config() {
    rc_errno rc;

    std::vector<std::string> servers;

    if (cfg_->get("servers") != nullptr) {
        servers = split(*cfg_->get("servers"), ",");
    }

    if (servers.size() > 0) {
        std::string address = servers[0];

        if (!is_valid_ipv4_address(address)) {
            LOG_FATAL << "invalid server ip: " << address;
            return RC_CONF_ERROR;
        }

        make_server_id(address);

        if (self_id_ != address) {
            LOG_FATAL << "conf: when bootstrapping, first entry in servers line must be the same as network interface address[" << self_id_ << "]";
            return RC_CONF_ERROR;
        }

        LOG_INFO << "bootstrap_cluster_config: take " << address
                 << " as the first server in the cluster.";
        raft_server_sptr server = make_raft_server(address, rpc_port_);
        
        if (server.get() == nullptr) {
            LOG_FATAL << "bootstrap_cluster_config: failed to allocate space for server " << address
                      << ", out of memory";
            return RC_OOM;
        }

        servers_.insert(std::pair<std::string, raft_server_sptr>(address, server));

        std::string cfg_serialized = serialize_configuration();
        log_entry_sptr cfg_entry = make_log_entry(cfg_serialized.size(), true);
        cfg_entry->idx = log_->last_entry_idx() + 1;
        /* since it's bootstrapping, we're sure this server will become leader at term 1.*/
        cfg_entry->term = 1;
        cfg_entry->cfg = true;
        log_->copy_log_data(cfg_entry.get(), cfg_serialized, true);

        if ((rc = log_->append(cfg_entry, true)) != RC_GOOD) {
            LOG_ERROR << "bootstrap_cluster_config: failed to append configuration entry to log, error code: " << rc;
            return rc;
        }

        server->next_idx = log_->last_entry_idx() + 1;
        server->match_idx = log_->last_entry_idx();

        commit_idx_ = cfg_entry->idx;

    } else {
        LOG_INFO << "conf: no servers line found, started as a non-voting server";
    }

    LOG_INFO << "bootstrap_cluster_config: finished bootstrapping.";
    return RC_GOOD;
}

rc_errno raft::init() {
    rc_errno res;

    if (inited_)
        return RC_GOOD;
    
    res = cfg_->parse();

    if (res != RC_GOOD) {
        LOG_FATAL << "failed in init() of configuration.";
        return res;
    }

    /* core_map_file */
    std::string * core_map_file = cfg_->get("core_map_file");

    if (core_map_file == nullptr) {
        LOG_FATAL << "must specify core_map_file in conf file.";
        return RC_CONF_ERROR;
    }

    cmap_ = core_filemap_uptr(new core_filemap(*core_map_file,
                                       RAFTCORE_MAP_FILE_PROT,
                                       RAFTCORE_MAP_FILE_FLAGS,
                                       sizeof(struct raftcore_map)));

    if (cmap_.get() == nullptr) {
        LOG_FATAL << "couldn't allocate memory for core file_map, out of memory.";
        return RC_OOM;
    }

    res = cmap_->map();
    
    if (res != RC_GOOD && res != RC_MMAP_NEW_FILE)
        return res;

    cmap_->sync_all();
    /* end of core_map_file */

    /* log_file */
    std::string * log_file = cfg_->get("log_file");

    if (log_file == nullptr) {
        LOG_FATAL << "must specify log_file in conf file.";
        return RC_CONF_ERROR;
    }

    log_ = std::unique_ptr<core_logger>(new core_logger(*log_file));

    if (log_.get() == nullptr) {
        LOG_FATAL << "couldn't allocate memory for logger_, out of memory.";
        return RC_OOM;
    }

    if ((res = log_->init()) != RC_GOOD)
        return res;
    /* end of log_file */

    /* rpc_bind_ip rpc_port */
    std::string * rpc_bind_ip = cfg_->get("rpc_bind_ip");

    if (rpc_bind_ip == nullptr) {
        LOG_INFO << "missing rpc_bind_ip, ADDR_ANY is used as default.";
    } else {
        rpc_bind_ip_ = *rpc_bind_ip;
    }

    std::string * rpc_port = cfg_->get("rpc_port");

    if (rpc_port == nullptr) {
        LOG_INFO << "missing rpc_port, " << RAFTCORE_DEFAULT_RPC_PORT
                 << " is used as default.";
        rpc_port_ = RAFTCORE_DEFAULT_RPC_PORT;
    }else if (std::stoi(*rpc_port) < 1024 || std::stoi(*rpc_port) > 65535) {
        LOG_FATAL << "rpc_port out of range[1024-65535]";
        return RC_CONF_ERROR;
    } else {
        rpc_port_ = *rpc_port;
    }

    /* setup rpc server */
    if (rpc_bind_ip_.empty())
        rpc_server_ = async_rpc_server_uptr(new carrot::CarrotAsyncServer(ios_, std::stoi(rpc_port_)));
    else
        rpc_server_ = async_rpc_server_uptr(new carrot::CarrotAsyncServer(ios_, rpc_bind_ip_, std::stoi(rpc_port_)));

    if (rpc_server_.get() == nullptr){
        LOG_FATAL << "couldn't allocate memory for rpc_server_, out of memory.";
        return RC_OOM;
    }

    /* set up rpc service */
    rpc_service_ = core_service_impl_uptr(new core_service_impl(this));

    if (rpc_service_.get() == nullptr) {
        LOG_FATAL << "couldn't allocate memory for rpc_service_, out of memory.";
        return RC_OOM;
    }

    done_ = service_done_callback_uptr(::google::protobuf::NewPermanentCallback(rpc_service_.get(),
                                                                               &core_service_impl::done));

    rpc_server_->RegisterService(rpc_service_.get(), done_.get());

    /* end of rpc_bind_ip rpc_port */

    /* interface */
    std::string * interface = cfg_->get("interface");

    if (interface == nullptr) {
        LOG_INFO << "missing interface, eth0 is used as default.";
        self_id_ = interface_address_ = get_if_ipv4_address("eth0");
    } else {
        self_id_ = interface_address_ = get_if_ipv4_address(*interface);
    }

    if (self_id_ == "" || !is_valid_ipv4_address(self_id_)) {
        LOG_FATAL << "invalid ip address[" << self_id_
                  << "] of interface " << interface ? *interface : "eth0";
        return RC_CONF_ERROR;
    }
    
    make_server_id(self_id_);

    LOG_INFO << "interface " << (interface ? *interface : "eth0") << "'s address: " << interface_address_;
    /* end of interface */

    /* min_election_timeout */
    std::string * min_elec_timeout = cfg_->get("min_election_timeout");

    if (min_elec_timeout == nullptr) {
        LOG_INFO << "missing min_election_timeout, " << RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT
                 << "ms is used as default.";
        min_election_timeout_ = RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT;
    } else {
        min_election_timeout_ = std::stoi(*min_elec_timeout);
    }
    /* end of min_election_timeout_ */

    /* max_election_timeout */
    std::string * max_elec_timeout = cfg_->get("max_election_timeout");

    if (max_elec_timeout == nullptr) {
        LOG_INFO << "missing max_election_timeout, " << RAFTCORE_DEFAULT_MAX_ELEC_TIMEOUT
                 << "ms is used as default.";
        max_election_timeout_ = RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT;
    } else {
        max_election_timeout_ = std::stoi(*max_elec_timeout);
    }
    /* end of max_election_timeout */
    
    /* election_rpc_timeout */
    std::string * election_rpc_timeout = cfg_->get("election_rpc_timeout");

    if (election_rpc_timeout == nullptr) {
        LOG_INFO << "missing election_rpc_timeout, " << RAFTCORE_DEFAULT_ELECTION_RPC_TIMEOUT
                 << "ms is used as default.";
        election_rpc_timeout_ = RAFTCORE_DEFAULT_ELECTION_RPC_TIMEOUT;
    } else {
        election_rpc_timeout_ = std::stoi(*election_rpc_timeout);
    }
    /* end of election_rpc_timeout*/


    /* heartbeat_rate */
    std::string * heartbeat_rate = cfg_->get("heartbeat_rate");

    if (heartbeat_rate == nullptr) {
        LOG_INFO << "missing heartbeat_rate, " << RAFTCORE_DEFAULT_HEARTBEAT_RATE
                 << "ms is used as default.";
        heartbeat_rate_ = RAFTCORE_DEFAULT_HEARTBEAT_RATE;
    } else {
        heartbeat_rate_ = std::stoi(*heartbeat_rate);
    }
    /* end of heartbeat_rate */    

    /* heartbeat_rpc_timeout */
    std::string * heartbeat_rpc_timeout = cfg_->get("heartbeat_rpc_timeout");

    if (heartbeat_rpc_timeout == nullptr) {
        LOG_INFO << "missing heartbeat_rpc_timeout, " << RAFTCORE_DEFAULT_HEARTBEAT_RPC_TIMEOUT
                 << "ms is used as default.";
        heartbeat_rpc_timeout_ = RAFTCORE_DEFAULT_HEARTBEAT_RPC_TIMEOUT;
    } else {
        heartbeat_rpc_timeout_ = std::stoi(*heartbeat_rpc_timeout);
    }
    /* end of heartbeat_rpc_timeout*/

    /* min_election_timeout */
    std::string * server_catch_up_rounds = cfg_->get("server_catch_up_rounds");

    if (server_catch_up_rounds == nullptr) {
        LOG_INFO << "missing server_catch_up_rounds, " << RAFTCORE_DEFAULT_SERVER_CATCH_UP_ROUNDS
                 << " rounds is used as default.";
        server_catch_up_rounds_ = RAFTCORE_DEFAULT_SERVER_CATCH_UP_ROUNDS;
    } else {
        server_catch_up_rounds_ = std::stoi(*server_catch_up_rounds);
    }
    /* end of min_election_timeout_ */

    election_channel_ = async_rpc_channel_uptr(new carrot::CarrotAsyncChannel(ios_));
    data_channel_ = async_rpc_channel_uptr(new carrot::CarrotAsyncChannel(ios_));
    
    commit_idx_ = 0;

    /* servers */
    if (log_->cfg_entry_idx() == 0) {
        LOG_INFO << "found no configuration entry, bootstrapping cluster...";
        if ((res = bootstrap_cluster_config()) != RC_GOOD)
            return res;
    } else {
        std::string config = log_->config();
        LOG_INFO << "found configuration entry " << config << ", initing cluster config...";
        std::vector<std::string> addresses = split(config, ",");

        for (auto address : addresses) {
            if (servers_.find(address) == servers_.end()) {
                raft_server_sptr s = make_raft_server(address, address == self_id_ ? rpc_port_ : RAFTCORE_DEFAULT_RPC_PORT);

                if (s.get() == nullptr){
                    LOG_FATAL << "failed to allocate space for server " << address << ", out of memory";
                    return RC_OOM;
                }

                servers_.insert(std::make_pair(address, s));

                LOG_INFO << "added server " << address << " to configuration.";
            }
        }
    }
    /* end of servers */

    /* http_server_port */
    std::string * http_server_port = cfg_->get("http_server_port");

    if (http_server_port == nullptr) {
        http_server_port_ = "29998";
        LOG_INFO << "missing http_server_port, 29998 is used as default.";
        http_server_ = http_server_uptr(new http::server::server(ios_, "0.0.0.0", "29998"));
    } else {
        http_server_port_ = *http_server_port;
        http_server_ = http_server_uptr(new http::server::server(ios_, "0.0.0.0", *http_server_port));
    }

    http_server_->register_handler("/", std::bind(&raft::handle_stat, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/stat", std::bind(&raft::handle_stat, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/list_server", std::bind(&raft::handle_list_server, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/append_log_entry", std::bind(&raft::handle_append_log_entry, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/add_server", std::bind(&raft::handle_add_server, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/remove_server", std::bind(&raft::handle_remove_server, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/leader_transfer", std::bind(&raft::handle_leader_transfer, this, std::placeholders::_1, std::placeholders::_2));
    /* end of http_server_port */

    /* initialize random number engine */

    mt_ = mt19937_uptr(new std::mt19937(rd_()));
    dist_ = uniform_int_dist_uptr(new std::uniform_int_distribution<int>(min_election_timeout_,
                                                                         max_election_timeout_));
    
    inited_ = true;

    return RC_GOOD;
}

rc_errno raft::start() {
    if (!inited_) {
        LOG_WARNING << "raftcore's not been initialized.";
        return RC_ERROR;
    }

    if (started_) {
        LOG_WARNING << "raftcore's already been started";
        return RC_GOOD;
    }

    http_server_->start();

    started_at_ = high_resolution_clock::now();
    last_heartbeat_ = high_resolution_clock::time_point::min();

    cur_role_ = FOLLOWER;

    /* set election timer only if this is not a non-voting member */
    if (servers_.size() > 0){
        /* set up election timer event */
        LOG_DEBUG << "resetting election timer by start...";
        reset_election_timer();
    }

    commit_thread_ = std::thread(&raft::commit_callback_thread, this);
    started_ = true;

    ios_.reset();

    work_.reset(new boost::asio::io_service::work(ios_));

    for ( ; started_ ; ) {
        try {
            ios_.run();
        } catch(std::exception& e) {
            LOG_ERROR << e.what();
        }
    }

    return RC_GOOD;
}

rc_errno raft::stop() {
    if (started_) {
        started_ = false;
        commit_queue_.push(true);
        work_.reset();
        http_server_->stop();
        ios_.stop();
        commit_thread_.join();
    }
    return RC_GOOD;
}

void core_service_impl::timeout_now(::google::protobuf::RpcController* ctl,
                                 const ::raftcore::RequestVote* req,
                                 ::raftcore::RequestVoteRes* res,
                                 ::google::protobuf::Closure* done) {
    LOG_DEBUG  << "timeout_now got called from server[" << req->candidate_id() 
               << "]: req: " << req->ShortDebugString()
               << ", raft_->core_map()->cur_term: " << raft_->core_map()->cur_term
               << ", raft_->core_map()->voted_for: " << raft_->core_map()->voted_for;
    LOG_INFO << "leader " << req->candidate_id() 
             << " tells to start new election, about to do so.";

    raft_->remove_election_timer();
    raft_->pre_vote(true);
    res->set_term(raft_->core_map()->cur_term);
    res->set_vote_granted(false);
    done->Run();
}

void core_service_impl::pre_vote(::google::protobuf::RpcController* ctl,
                                 const ::raftcore::RequestVote* req,
                                 ::raftcore::RequestVoteRes* res,
                                 ::google::protobuf::Closure* done) {
    high_resolution_clock::time_point now = high_resolution_clock::now();
    uint64_t msecs = duration_cast<milliseconds>(now - raft_->last_heartbeat_).count();

    LOG_DEBUG  << "pre_vote got called from server[" << req->candidate_id() 
               << "]: req: " << req->ShortDebugString()
               << ", raft_->core_map()->cur_term: " << raft_->core_map()->cur_term
               << ", raft_->core_map()->voted_for: " << raft_->core_map()->voted_for;
    /* reject pre-vote if one of the following is true:
     * 1. candidate's log is not as up-to-date as this server 
     * 2. recevices the request within the minimum election timeout of hearing from the current leader
     *    and this is not an early request vote that leader told this candidate to initiate
     */
    if (raft_->log_->last_entry_term() > req->last_log_term() ||
        raft_->log_->last_entry_idx() > req->last_log_idx()){
        LOG_DEBUG << "pre_vote: requester[" << req->candidate_id() 
         << "]'s log("  << req->last_log_idx() << "," << req->last_log_term() 
         << ") is older than this server(" << raft_->log_->last_entry_idx()
         << "," << raft_->log_->last_entry_term() << "), rejecting...";
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (msecs < raft_->min_election_timeout_ && req->early_vote() == false) {
        LOG_DEBUG << "pre_vote: last heartbeat receiving time still in good range(time between this request and the last heartheart is " << msecs
                 << "ms), rejecting requester[" << req->candidate_id() << "]...";
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else {
        LOG_DEBUG << "pre_vote: would granted vote for requester[" << req->candidate_id()
                 << "] log(" << req->last_log_idx() << "," << req->last_log_term();
        res->set_term(req->term());
        res->set_vote_granted(true);
    }
    LOG_DEBUG << "pre_vote: done invoking request_vote from " << req->candidate_id()
              << " with result: " << res->ShortDebugString();
    done->Run();
}

/* response to request_vote call */
void core_service_impl::request_vote(::google::protobuf::RpcController* ctl,
                                     const ::raftcore::RequestVote* req,
                                     ::raftcore::RequestVoteRes* res,
                                     ::google::protobuf::Closure* done) {
    
    if (req->term() > raft_->core_map()->cur_term) {
        LOG_INFO << "request_vote: a higher term " << req->term()
                 << " from " << req->candidate_id()
                 << " exists, stepping down";
        raft_->core_map()->voted_for[0] = 0;
        LOG_INFO << "calling step_down from core_service_impl::request_vote 1";
        raft_->step_down(req->term(), "");
    }
    /* reject vote if one of the following is true:
     * 1. term < cur_term
     * 2. already voted for someone else and that someone is not the candidate in question
     * 3. candidate's log is not as up-to-date as this server
     */
    if (req->term() < raft_->core_map()->cur_term){
        LOG_INFO << "request_vote: request's term is smaller than current term, rejecting...";
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (raft_->core_map()->voted_for[0] &&
        req->candidate_id() != std::string(raft_->core_map()->voted_for)) {
        LOG_INFO << "request_vote: already voted for " << raft_->core_map()->voted_for
                 << ", rejecting request from " << req->candidate_id();
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (raft_->log_->last_entry_term() > req->last_log_term() ||
        raft_->log_->last_entry_idx() > req->last_log_idx()){
        LOG_INFO << "request_vote: requester[" << req->candidate_id() 
                 << "]'s log("  << req->last_log_idx() << "," << req->last_log_term() 
                 << ") is older than this server(" << raft_->log_->last_entry_idx()
                 << "," << raft_->log_->last_entry_term() << "), rejecting...";
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else {
        /* step down if this is a higher term */
        if (req->term() > raft_->core_map()->cur_term) {
            strncpy(raft_->core_map()->voted_for, req->candidate_id().c_str(), sizeof(raft_->core_map()->voted_for));
            LOG_INFO << "calling step_down from core_service_impl::request_vote 2";
            raft_->step_down(req->term(), "");
        }/* save the sync if already voted for this candidate */
        else if (req->candidate_id() != std::string(raft_->core_map()->voted_for)) {
            raft_->core_map()->cur_term = req->term();
            strncpy(raft_->core_map()->voted_for, req->candidate_id().c_str(), sizeof(raft_->core_map()->voted_for));
            TIMING_START
            raft_->cmap_->sync_all();
            TIMING_STOP("core_service_impl::request_vote raft_->cmap_->sync_all()")
        }
        LOG_INFO << "request_vote: granted vote for requester[" << req->candidate_id()
                 << "] log(" << req->last_log_idx() << "," << req->last_log_term() << ")";
        res->set_term(req->term());
        res->set_vote_granted(true);
    }
    LOG_DEBUG << "request_vote: done invoking request_vote from " << req->candidate_id()
              << " with result: " << res->ShortDebugString();
    done->Run();
}

/* response to append_entries call */
void core_service_impl::append_entries(::google::protobuf::RpcController* ctl,
                    const ::raftcore::AppendEntries* req,
                    ::raftcore::AppendEntriesRes* res,
                    ::google::protobuf::Closure* done) {
    rc_errno rc;
    bool need_adjust_cfg = false;

    LOG_DEBUG << "append_entries got called from leader[" << req->leader_id() 
              << "]: req: " << req->ShortDebugString() 
              << ", raft_->core_map()->cur_term: " << raft_->core_map()->cur_term 
              << ", raft_->core_map()->voted_for: " << raft_->core_map()->voted_for;

    
    if (req->term() < raft_->core_map()->cur_term) {
        LOG_INFO << "append_entries: rpc from older leader " << req->leader_id()
                 << " with term[" << req->term()
                 << "], rejecting...";
        /* old leader */
        goto failed;
    } else if (req->term() > raft_->core_map()->cur_term) {
        LOG_INFO << "append_entries: a new term[" << req->term()
                 << "] from " << req->leader_id()
                 << " begins, resetting election timer event...";
        /* set voted_for to null if a new term begins  */
        raft_->core_map()->voted_for[0] = 0;
        LOG_INFO << "calling step_down from core_service_impl::append_entries";
        raft_->step_down(req->term(), req->leader_id());
    } else {
        /* append entries rpc from current leader, reset election timer */
        LOG_DEBUG << "append_entries: rpc from current leader, resetting election timer event...";
        raft_->reset_election_timer();
        raft_->cur_role_ = FOLLOWER;
    }

    if (raft_->log_->has_log_entry(req->prev_log_idx(), req->prev_log_term()) == false) {
        /* log inconsistency */
        LOG_INFO << "append_entries: rpc from " << req->leader_id()
                 << " not such log entry as idx:" << req->prev_log_idx()
                 << ", term: " << req->prev_log_term();
        goto failed;
    } else {
        /* <= 0 means heartbeat message */
        if (req->entries_size() > 0) {
            LOG_INFO << "append_entries: about to append log entries: " << req->ShortDebugString();

            /* all goes well, append entries to local log */
            std::vector<log_entry_sptr> v;
            int i = 0;
            for (; i < req->entries_size(); ++i) {
                const LogEntry & e = req->entries(i);
                need_adjust_cfg = need_adjust_cfg || e.config();
                if (raft_->log_->log_entry_conflicted(e.idx(), e.term())) {
                    uint64_t old_cfg_entry_idx = raft_->log_->cfg_entry_idx();
                    /* chop off inconsistent entries if any */
                    LOG_INFO << "append_entries: entry " << e.ShortDebugString()
                             << " conflicted with eixsted one " << LOG_ENTRY_TO_STRING((*(raft_->log_))[e.idx()])
                             << ", chopping off...";
                    if ((rc = raft_->log_->chop(e.idx())) != RC_GOOD) {
                        LOG_ERROR << "append_entries: failed to chop entries starting at index " << e.idx()
                                  << ", error code: " << rc;
                        ctl->SetFailed("server internal error");
                        goto failed;
                    }
                    /* if current configuration has been chopped off, rollback to previous configuration */
                    if (old_cfg_entry_idx != raft_->log_->cfg_entry_idx()) {
                        need_adjust_cfg = true;
                    }
                } else if (raft_->log_->has_log_entry(e.idx(), e.term())) {
                    LOG_INFO << "append_entries: entry " << e.ShortDebugString()
                             << " already existed in the log, skipping...";
                    continue;
                }
                break;
            }


            std::string entries_content = "";

            for (; i < req->entries_size(); ++i) {
                const LogEntry & e = req->entries(i);
                need_adjust_cfg = need_adjust_cfg || e.config();
                log_entry_sptr entry = log_entry_sptr(make_log_entry(e.data().size()));
                
                if (entry.get() == nullptr) {
                    LOG_ERROR << "append_entries: failed to allocate memory for incoming log_entry, out of memory.";
                    ctl->SetFailed("out of memory");
                    goto failed;
                }

                entry->idx = e.idx();
                entry->term = e.term();
                entry->cfg = e.config(); 
                raft_->log_->copy_log_data(entry.get(), e.data());

                v.push_back(entry);

                if (i < req->entries_size() - 1)
                    entries_content.append(e.ShortDebugString() + ",");
                else
                    entries_content.append(e.ShortDebugString());
            }

            /* persist to disk */
            if (v.size() > 0 && (rc = raft_->log_->append(v, true)) != RC_GOOD) {
                LOG_ERROR << "append_entries: failed to append entries to log, error code: " << rc;
                ctl->SetFailed("server internal error");
                goto failed;
            }
            LOG_INFO << "append_entries: entries (" << entries_content
                     << ") actually appened";
        }
        /* If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
        if (req->leader_commit() > raft_->commit_idx_) {
            LOG_INFO << "append_entries: set commit index to min(leader_commit[" << req->leader_commit() 
                     << "], last_entry_idx[" << raft_->log_->last_entry_idx() << "])";
            raft_->commit_idx_ = std::min(req->leader_commit(), raft_->log_->last_entry_idx());
            raft_->commit_queue_.push(false);
        }
    }

    raft_->cur_leader_ = req->leader_id();
    raft_->last_heartbeat_ = high_resolution_clock::now();

    if (need_adjust_cfg) {
        LOG_INFO << "append_entries: configuration entry changed, adjusting...";
        raft_->adjust_configuration();
    }

/* succeed: */
    res->set_term(raft_->core_map()->cur_term);
    res->set_success(true);
    res->set_match_idx(raft_->log_->last_entry_idx());
    res->set_id(req->id());
    done->Run();
    LOG_DEBUG << "append_entries: done invoking append_entries from " << req->leader_id()
              << " with result: " <<res->ShortDebugString();
    return;

failed:
    res->set_term(raft_->core_map()->cur_term);
    res->set_success(false);
    res->set_match_idx(raft_->log_->last_entry_idx());
    res->set_id(req->id());
    done->Run();
    LOG_DEBUG << "append_entries: done invoking append_entries from " << req->leader_id()
              << " with result: " <<res->ShortDebugString();
    return;
}

}