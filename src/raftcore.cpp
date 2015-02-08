/*
* Copyright (C) Xinjing Cho
*/
#include <signal.h>

#include <raftcore/raftcore.h>

namespace raftcore {

#define TIMING_START { \
    std::chrono::system_clock::time_point tp1 = std::chrono::system_clock::now(); \

#define TIMING_STOP(expr) \
    std::chrono::system_clock::time_point tp2 = std::chrono::system_clock::now();\
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(tp2 - tp1);\
    glogger::l(DEBUG, "time taken for %s: %lf", expr , time_span.count());\
}


void raft::pre_vote_done(pre_vote_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    RequestVote              * req = &task->request;
    RequestVoteRes           * res = &task->response;

    raft_server              * s   = task->s;
    task->time_end = high_resolution_clock::now();
    glogger::l(DEBUG, "counter: %d time taken for pre_vote[%lld] at term[%lld] to finish: %lldus", ++counter, task->id, req->term(), duration_cast<microseconds>(task->time_end - task->time_start));

    if (cur_role_ != CANDIDATE) {
        glogger::l(INFO, "pre_vote[%lld]: currnet role is not candidate, ignoring stale response...", task->id);
        return;
    }

    if (ctl->ErrorCode() != carrot::EC_SUCCESS) {
        glogger::l(DEBUG, "pre_vote[%lld] to server %s failed: %s.", task->id, s->id, ctl->ErrorText().c_str());
        ++pre_votes_fails_;
        goto checkout;
    }

    glogger::l(DEBUG, "pre_vote[%lld] returned from server[%s] with result: %s", task->id, s->id, res->ShortDebugString().c_str());

    if (res->vote_granted() == true) {
        ++pre_votes_;
    } else {
        ++pre_votes_fails_;
    }

checkout:

    if (pre_votes_ > (servers_.size() + 1) / 2) {
        glogger::l(INFO, "pre_vote phase succeed, initiating election.");
        /* now we are confident to start the election */
        leader_election();
    } else if (pre_votes_ + pre_votes_fails_ == servers_.size() + 1) {
        glogger::l(INFO, "pre_vote phase failed, quit initiating election, restart election timer.");
        reset_election_timer();
    }
}


void raft::pre_vote() {
    /* pre vote for self */
    pre_votes_ = 1;
    pre_votes_fails_ = 0;

    cur_role_ = CANDIDATE;

    glogger::l(INFO, "election timer timed out, start pre_vote phase");

    for (auto it : servers_) {
        raft_server * s = it.second.get();

        pre_vote_task_sptr task = pre_vote_task_sptr(new pre_vote_task);

        task->s = s;
        task->id = ++pre_vote_id_;
        task->controller.host(s->id);
        task->controller.service(std::to_string(rpc_port_));
        task->controller.timeout(election_rpc_timeout_);

        task->request.set_term(core_map()->cur_term);
        task->request.set_candidate_id(self_id_);
        task->request.set_last_log_idx(log_->last_entry_idx());
        task->request.set_last_log_term(log_->last_entry_term());

        task->done = ::google::protobuf::NewCallback(this, &raft::pre_vote_done, task);
        task->time_start = high_resolution_clock::now();
        /* issue a asynchronoous request_vote call to this server */
        s->request_vote_service->pre_vote(&task->controller,
                                              &task->request,
                                              &task->response,
                                              task->done);
        glogger::l(TRACE, "issued asynchronous pre_vote rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString());
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
    task->controller.host(s->id);
    task->controller.service(std::to_string(rpc_port_));
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
    glogger::l(TRACE, "issued asynchronous request_vote rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString());
}

void raft::leader_election_done(request_vote_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    RequestVote              * req = &task->request;
    RequestVoteRes           * res = &task->response;

    raft_server              * s   = task->s;
    task->time_end = high_resolution_clock::now();
    glogger::l(DEBUG, "counter: %d time taken for request_vote[%lld] at term[%lld] to finish: %lldus", ++counter, task->id, req->term(), duration_cast<microseconds>(task->time_end - task->time_start));

    if (ctl->ErrorCode() != carrot::EC_SUCCESS) {
        glogger::l(DEBUG, "request_vote[%lld] to server %s failed: %s, retrying...", task->id, s->id, ctl->ErrorText().c_str());        return;
        return;
    }

    glogger::l(DEBUG, "request_vote[%lld] returned from server[%s]'s request_vote with result: %s", task->id, s->id, res->ShortDebugString().c_str());

    if (cur_role_ != CANDIDATE) {
        glogger::l(INFO, "request_vote[%lld]: currnet role is not candidate, ignoring stale response...", task->id);
        return;
    }

    if (res->vote_granted() == true) {
        if (res->term() < core_map()->cur_term) {
            glogger::l(DEBUG, "request_vote[%lld]: vote's term %d cur_term %d, stale vote, discarding...", task->id, res->term(), core_map()->cur_term);
            return;
        }
        /* no need for protection as only one thread mutates vote count. */
        ++votes_;
        glogger::l(INFO, "request_vote[%lld] at term %lld got granted from %s", task->id, req->term() , ctl->host().c_str());

        /* one for self, check if we have enough votes. */
        if (votes_ > (servers_.size() + 1) / 2) {
            /* we are leader now */
            step_up();
        }
    } else if (res->term() > core_map()->cur_term) {
        /* step down if there is a higher term going on*/
        step_down(res->term());
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

    /* vote for self */
    votes_ = 1;

    glogger::l(INFO, "start a new election at term: %d", core_map()->cur_term);

    /* send request vote to all servers */
    for (auto it : servers_) {
        raft_server_sptr s = it.second;

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
        glogger::l(ERROR, "raft::handle_election_timeout error: %s", error.message().c_str());
    } else {
        glogger::l(TRACE, "election timer canceled");
    }
}

void raft::reset_election_timer() {
    uint32_t timeout = (*dist_.get())(*mt_.get());
    glogger::l(TRACE, "next election timeout: %d", timeout);

    election_timer_.expires_from_now(boost::posix_time::milliseconds(timeout));
    election_timer_.async_wait(boost::bind(&raft::handle_election_timeout, this,
        boost::asio::placeholders::error));
}

void raft::remove_election_timer() {
    boost::system::error_code error;
    election_timer_.cancel(error);
    if (error)
        glogger::l(ERROR, "election_timer_.cancel error: %s", error.message().c_str());
}

/* 
* send append_entries rpc to a single server,
* this function should called with this->lmtx_ and this->mmtx_ held
*/
void raft::append_entries_single(raft_server * s) {
    append_entries_task_sptr task = append_entries_task_sptr(new append_entries_task);
    if (task.get() == nullptr) {
        glogger::l(ERROR, "failed to allocate memory for append_entries_task");
        return;
    }

    task->s = s;
    task->id = ++append_entries_id_;
    task->controller.host(s->id);
    task->controller.service(std::to_string(rpc_port_));
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
            glogger::l(ERROR, "failed to allocate memory for LogEntry for server %s, skipping...", s->id);
            return;
        }
        log_entry * l = (*log_)[i];
        e->set_term(l->term);
        e->set_idx(l->idx);
        e->set_data(std::move(std::string(l->data, LOG_ENTRY_DATA_LEN(l))));
    }
    task->time_start = high_resolution_clock::now();
    s->append_entries_service->append_entries(&task->controller,
                                              &task->request,
                                              &task->response,
                                              task->done);
    glogger::l(TRACE, "issued asynchronous append_entries rpc to server[" + std::string(s->id) + "] with params: " + task->request.ShortDebugString());
}

#define TIMING_START2 std::chrono::system_clock::time_point tp1 = std::chrono::system_clock::now();

#define TIMING_STOP2(expr) std::chrono::system_clock::time_point tp2 = std::chrono::system_clock::now();\
    std::chrono::duration<double> time_span = std::chrono::duration_cast<std::chrono::duration<double>>(tp2 - tp1);\
    glogger::l(DEBUG, "time taken for %s: %lf", expr , time_span.count());
    
/*
*   If last log index ≥ nextIndex for a follower: send AppendEntries RPC with log entries starting at nextIndex
*       If successful: update nextIndex and matchIndex for follower
*       If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
*/
void raft::append_entries() {
//  glogger::l("resetting heartbeat timer by append_entries_thread...");
    /* reset heartbeat check timer for next round */
    reset_heartbeat_timer();

    if (cur_role_ != LEADER) {
        return;
    }

    /* update last heartbeat time of leader it self */
    last_heartbeat_ = high_resolution_clock::now();

//    glogger::l("append_entries() got called");
    for (auto it : servers_) {
        raft_server_sptr s = it.second;

        append_entries_single(s.get());
    }
}

void raft::append_entries_done(append_entries_task_sptr task) {
    static int counter = 0;
    carrot::CarrotController * ctl = &task->controller;
    AppendEntries            * req = &task->request;
    AppendEntriesRes         * res = &task->response;
    raft_server              * s   = task->s;

    task->time_end = high_resolution_clock::now();
    glogger::l(DEBUG, "counter: %d time taken for append_entries[%lld] at term[%lld] to finish: %lldus", ++counter, task->id, req->term(), duration_cast<microseconds>(task->time_end - task->time_start));
    if (ctl->Failed() || ctl->ErrorCode() != carrot::EC_SUCCESS) {
        glogger::l(DEBUG, "append_entries[%lld]: failed to append_entries to server %s, error code:  %d, error text: %s.",
                   task->id, s->id, ctl->ErrorCode(), ctl->ErrorText().c_str());
        return;
    }

    glogger::l(DEBUG, "append_entries[%lld] returned from server[%s]'s append_entries with result: %s", task->id, s->id, res->ShortDebugString().c_str());

    if (cur_role_ != LEADER) {
        glogger::l(DEBUG, "append_entries[%lld]: not leader anymore, discarding stale append_entries_done call", task->id);
        return;
    }

    if (res->success() == true) {
        /* update nextIndex and matchIndex for follower except for heartbeat checks */
        s->next_idx = res->match_idx() + 1;
        s->match_idx = res->match_idx();
        adjust_commit_idx();
    } else if (res->term() > core_map()->cur_term) { /* if we have a higher term */
        /* we've already had a another leader, convert to follower */
        step_down(res->term());
    } else {
        /* failed due to log inconsistency: use log length responded by server and retry */
        s->next_idx = res->match_idx() + 1;
        glogger::l(DEBUG, "append_entries[%lld]: log inconsistency, retrying with nextIndex[%d]...", task->id, s->next_idx);
        append_entries_single(s);
    }
}

void raft::handle_heartbeat_timeout(const boost::system::error_code& error) {
    if (!error) {
        append_entries();
    } else if (error.value() != boost::system::errc::operation_canceled){
        glogger::l(ERROR, "raft::handle_heartbeat_timeout error: %s", error.message().c_str());
    } else {
        glogger::l(DEBUG, "heartbeat check timer canceled");
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
        glogger::l(ERROR, "election_timer_.cancel error: %s", error.message().c_str());
}

void raft::step_down(uint64_t new_term) {
    glogger::l(INFO, "stepped down at term %lld to new term %lld.", core_map()->cur_term, new_term);
    remove_heartbeat_timer();

    cur_role_ = FOLLOWER;
    TIMING_START
    core_map()->cur_term = new_term;
    cmap_->sync_all();
    TIMING_STOP("step_down cmap_->sync_all()")

    glogger::l(DEBUG, "resetting election timer by step_down...");
    reset_election_timer();
}

void raft::step_up() {
    cur_role_ = LEADER;
    cur_leader_ = self_id_;
    glogger::l(INFO, "now i'm the leader[%s] at term %lld, sending out heartbeats.", self_id_.c_str(), core_map()->cur_term);

    for (auto it : servers_) {
        it.second->next_idx = log_->last_entry_idx() + 1;
        it.second->match_idx = 0;
    }

    remove_election_timer();

    /* send heartbeat checks to declare leadership(append_entries) */
    append_entries();
}

/* 
* only leader calls this function.
* If there exists an N such that N > commitIndex, a majority
* of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N.
* That is, find the highest index of the entry that a majority of servers have accepted.
*/
void raft::adjust_commit_idx() {
    uint64_t max_match = 0;
    uint64_t min_match = log_->last_entry_idx();

    /* find out a possible range of commit_idx */
    for (auto it : servers_) {
        raft_server_sptr s = it.second;
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
        uint32_t count = 0;
        for (auto it : servers_) {
            raft_server_sptr s = it.second;
            if (s->match_idx >= i)
                ++count;
        }


        if (count >= servers_.size() / 2){
            commit_idx_ = i;
            /* notify commit_callback_thread */
            commit_queue_.push(false);
            break;
        }
    }
}

void raft::commit_callback_thread() {
    bool get_out = false;
    while ((get_out = commit_queue_.pop()) == false) {
        for (; last_applied_ <= commit_idx_;) {
            log_committed_cb_((*log_)[++last_applied_]);
        }
    }
}

void raft::handle_stat(const http::server::request& req, http::server::reply& rep) {
    high_resolution_clock::time_point now = high_resolution_clock::now();
    uint64_t secs = duration_cast<seconds>(now - started_at_).count();

    rep.status = http::server::reply::ok;
    rep.content.append("<html><head><title>raftcore@" + self_id_ + "</title></head>");
    // style for table
    rep.content.append("<style type='text/css'>table{border-collapse:collapse;border-spacing:0;border-left:1px solid #888;border-top:1px solid #888;background:#efefef;}\
th,td{border-right:1px solid #888;border-bottom:1px solid #888;padding:5px 15px;}\
th{font-weight:bold;background:#ccc;}</style>");

    rep.content.append("<body><div><h2>Hello, this is raftcore.</h2>");
    rep.content.append(std::string("<h3>server ip: ") + self_id_ + "</h3>");
    rep.content.append(std::string("<h3>rpc port: ") + std::to_string(rpc_port_) + "</h3>");
    rep.content.append(std::string("<h3>uptime: ") + 
                       std::to_string(secs / 86400) + "d " +
                       std::to_string((secs / 3600) % 24) + "h " +
                       std::to_string((secs / 60) % 60) + "m " +
                       std::to_string(secs % 60) + "s </h3>");
    rep.content.append(std::string("<h3>current role: " + 
                       std::string(cur_role_ == FOLLOWER ? "<font color='blue'>Follower</font>" : (cur_role_ == CANDIDATE ? "<font color='green'>Candidate</font>": "<font color='red'>Leader</font>")) 
                       + "</h3>"));

    rep.content.append(std::string("<h3>currnet term: ") + std::to_string(core_map()->cur_term) + "</h2>");
    rep.content.append(std::string("<h3>current leader: <a target='_blank' href='http://" + cur_leader_+ ":" + http_server_port_ + "'>" + cur_leader_ + "</a></h3></div>"));

    if (cur_role_ == LEADER) {
        for (auto it : servers_) {
            rep.content.append("<iframe style='margin:10px;' height='100%' width=500 frameborder=1 src='http://" + std::string(it.second->id) + ":" + http_server_port_ + "/'></iframe>");
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
    }
    rep.content.append("</body></html>");
    rep.headers.resize(2);
    rep.headers[0].name = "Content-Length";
    rep.headers[0].value = std::to_string(rep.content.size());
    rep.headers[1].name = "Content-Type";
    rep.headers[1].value = http::server::mime_types::extension_to_type("htm");
}

static std::vector<std::string> split(const std::string & src, const std::string & delimiter) {
    std::vector<std::string> v;
    size_t pos = 0;
    size_t found_pos = 0;

    while ((found_pos = src.find_first_of(delimiter, pos)) != std::string::npos){
        v.emplace_back(std::string(src.begin() + pos, src.begin() + found_pos));
        pos = found_pos + 1;
    }
    if (v.empty()) {
        v.emplace_back(src);
    } else if (v.size() == 1) {
        v.emplace_back(std::string(src.begin() + pos, src.end()));
    }
    return v;
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
        glogger::l(DEBUG, "append_log_entry: request method is not GET: %s", req.method.c_str());
        rep = http::server::reply::stock_reply(http::server::reply::bad_request);
        return;
    }
    rep.content.append("<html>");
    
    if (cur_role_ != LEADER) {
        glogger::l(DEBUG, "append_log_entry: currnt role is not leader");
        rep.content.append("<head><script type='text/javascript'>alert('not leader!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
    } else {
        size_t question_mark_pos = req.uri.find_last_of('?');
        if (question_mark_pos == std::string::npos) {
            glogger::l(DEBUG, "append_log_entry: parameter \'content\' is empty or not found");
            rep.content.append("<head><script type='text/javascript'>alert('\'content\' not found in the parameter!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
        } else {
            std::string params = std::string(req.uri.begin() + question_mark_pos + 1, req.uri.end());
            std::string content = find_reuqest_param(params, "content");
            if (content == "") {
                glogger::l(DEBUG, "append_log_entry: parameter \'content\' is empty or not found");
                rep.content.append("<head><script type='text/javascript'>alert('\'content\' is empty or not found in the parameter!');window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
            } else {
                log_entry_sptr new_entry = make_log_entry(content.size());
                new_entry->idx = log_->last_entry_idx() + 1;
                new_entry->term = core_map()->cur_term;
                ::memcpy(new_entry->data, content.c_str(), LOG_ENTRY_DATA_LEN(new_entry));
                log_->append(new_entry, true);
                glogger::l(DEBUG, "append_log_entry: log entry[%s] appended ", content.c_str());
                rep.content.append("<head><script type='text/javascript'>window.location='http://" + self_id_ + ":" + http_server_port_ + "/';</script></head>");
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

rc_errno raft::init() {
    rc_errno res;

    if (inited_)
        return RC_GOOD;
    
    res = cfg_->parse();

    if (res != RC_GOOD) {
        glogger::l(FATAL, "failed in init() of configuration.");
        return res;
    }

    /* core_map_file */
    std::string * core_map_file = cfg_->get("core_map_file");

    if (core_map_file == nullptr) {
        glogger::l(FATAL, "must specify core_map_file in conf file.");
        return RC_CONF_ERROR;
    }

    cmap_ = core_filemap_uptr(new core_filemap(*core_map_file,
                                       RAFTCORE_MAP_FILE_PROT,
                                       RAFTCORE_MAP_FILE_FLAGS,
                                       sizeof(struct raftcore_map)));

    if (cmap_.get() == nullptr) {
        glogger::l(FATAL, "couldn't allocate memory for core file_map, out of memory.");
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
        glogger::l(FATAL, "must specify log_file in conf file.");
        return RC_CONF_ERROR;
    }

    log_ = std::unique_ptr<core_logger>(new core_logger(*log_file));

    if (log_.get() == nullptr) {
        glogger::l(FATAL, "couldn't allocate memory for logger_, out of memory.");
        return RC_OOM;
    }

    if ((res = log_->init()) != RC_GOOD)
        return res;
    /* end of log_file */

    /* rpc_bind_ip rpc_port */
    std::string * rpc_bind_ip = cfg_->get("rpc_bind_ip");

    if (rpc_bind_ip == nullptr) {
        glogger::l(INFO, "missing rpc_bind_ip, ADDR_ANY is used as default.");
    } else {
        rpc_bind_ip_ = *rpc_bind_ip;
    }

    std::string * rpc_port = cfg_->get("rpc_port");

    if (rpc_port == nullptr) {
        glogger::l(INFO, "missing rpc_port, %d is used as default.", RAFTCORE_DEFAULT_RPC_PORT);
        rpc_port_ = RAFTCORE_DEFAULT_RPC_PORT;
    }else if (std::stoi(*rpc_port) < 1024 || std::stoi(*rpc_port) > 65535) {
        glogger::l(FATAL, "rpc_port out of range[1024-65535]");
        return RC_CONF_ERROR;
    } else {
        rpc_port_ = std::stoi(*rpc_port);
    }

    /* setup rpc server */
    if (rpc_bind_ip_.empty())
        rpc_server_ = async_rpc_server_uptr(new carrot::CarrotAsyncServer(ios_, rpc_port_));
    else
        rpc_server_ = async_rpc_server_uptr(new carrot::CarrotAsyncServer(ios_, rpc_bind_ip_, rpc_port_));

    if (rpc_server_.get() == nullptr){
        glogger::l(FATAL, "couldn't allocate memory for rpc_server_, out of memory.");
        return RC_OOM;
    }

    /* set up rpc service */
    rpc_service_ = core_service_impl_uptr(new core_service_impl(this));

    if (rpc_service_.get() == nullptr) {
        glogger::l(FATAL, "couldn't allocate memory for rpc_service_, out of memory.");
        return RC_OOM;
    }

    done_ = service_done_callback_uptr(::google::protobuf::NewPermanentCallback(rpc_service_.get(),
                                                                               &core_service_impl::done));

    rpc_server_->RegisterService(rpc_service_.get(), done_.get());

    /* end of rpc_bind_ip rpc_port */

    /* interface */
    std::string * interface = cfg_->get("interface");

    if (interface == nullptr) {
        glogger::l(INFO, "missing interface, eth0 is used as default.");
        self_id_ = get_if_ipv4_address("eth0");
    } else {
        self_id_ = get_if_ipv4_address(*interface);
    }

    if (!is_valid_ipv4_address(self_id_)) {
        glogger::l(FATAL, "invalid ip address %s of interface %s", self_id_.c_str(), interface ? interface->c_str() : "eth0");
        return RC_CONF_ERROR;
    }
    glogger::l(INFO, "self_id: %s", self_id_.c_str());
    /* end of interface */

    /* min_election_timeout */
    std::string * min_elec_timeout = cfg_->get("min_election_timeout");

    if (min_elec_timeout == nullptr) {
        glogger::l(INFO, "missing min_election_timeout, %dms is used as default.", RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT);
        min_election_timeout_ = RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT;
    } else {
        min_election_timeout_ = std::stoi(*min_elec_timeout);
    }
    /* end of min_election_timeout_ */

    /* max_election_timeout */
    std::string * max_elec_timeout = cfg_->get("max_election_timeout");

    if (max_elec_timeout == nullptr) {
        glogger::l(INFO, "missing max_election_timeout, %dms is used as default.", RAFTCORE_DEFAULT_MAX_ELEC_TIMEOUT);
        max_election_timeout_ = RAFTCORE_DEFAULT_MIN_ELEC_TIMEOUT;
    } else {
        max_election_timeout_ = std::stoi(*max_elec_timeout);
    }
    /* end of max_election_timeout */
    
    /* election_rpc_timeout */
    std::string * election_rpc_timeout = cfg_->get("election_rpc_timeout");

    if (election_rpc_timeout == nullptr) {
        glogger::l(INFO, "missing election_rpc_timeout, %dms is used as default.", RAFTCORE_DEFAULT_ELECTION_RPC_TIMEOUT);
        election_rpc_timeout_ = RAFTCORE_DEFAULT_ELECTION_RPC_TIMEOUT;
    } else {
        election_rpc_timeout_ = std::stoi(*election_rpc_timeout);
    }
    /* end of election_rpc_timeout*/


    /* heartbeat_rate */
    std::string * heartbeat_rate = cfg_->get("heartbeat_rate");

    if (heartbeat_rate == nullptr) {
        glogger::l(INFO, "missing heartbeat_rate, %dms is used as default.", RAFTCORE_DEFAULT_HEARTBEAT_RATE);
        heartbeat_rate_ = RAFTCORE_DEFAULT_HEARTBEAT_RATE;
    } else {
        heartbeat_rate_ = std::stoi(*heartbeat_rate);
    }
    /* end of heartbeat_rate */    

    /* heartbeat_rpc_timeout */
    std::string * heartbeat_rpc_timeout = cfg_->get("heartbeat_rpc_timeout");

    if (heartbeat_rpc_timeout == nullptr) {
        glogger::l(INFO, "missing heartbeat_rpc_timeout, %dms is used as default.", RAFTCORE_DEFAULT_HEARTBEAT_RPC_TIMEOUT);
        heartbeat_rpc_timeout_ = RAFTCORE_DEFAULT_HEARTBEAT_RPC_TIMEOUT;
    } else {
        heartbeat_rpc_timeout_ = std::stoi(*heartbeat_rpc_timeout);
    }
    /* end of heartbeat_rpc_timeout*/

    election_channel_ = async_rpc_channel_uptr(new carrot::CarrotAsyncChannel(ios_));
    data_channel_ = async_rpc_channel_uptr(new carrot::CarrotAsyncChannel(ios_));

    /* servers */
    char *p;
    char *save_ptr;

    if (cfg_->get("servers") == nullptr) {
        glogger::l(FATAL, "servers is missing");
        return RC_CONF_ERROR;
    }

    std::unique_ptr<char[]> peer_ips = std::unique_ptr<char[]>(
                                           new char[cfg_->get("servers")->size() + 1]);
    ::strcpy(peer_ips.get(), cfg_->get("servers")->c_str());

    p = ::strtok_r(peer_ips.get(), ",", &save_ptr);

    while (p) {
        if (!is_valid_ipv4_address(p)) {
            glogger::l(FATAL, "invalid server ip: %s", p);
            return RC_CONF_ERROR;
        }

        if (self_id_ == p) {
            p = ::strtok_r(NULL, ",", &save_ptr);
            glogger::l(INFO, "skipped self server %s", self_id_.c_str());
            continue;
        }

        raft_server_sptr server = raft_server_sptr(new raft_server);
        
        if (server.get() == nullptr) {
            glogger::l(FATAL, "failed to allocate space for server %s, out of memory", p);
            return RC_OOM;
        }

        ::strncpy(server->id, p, sizeof(server->id));
        
        server->alive = false;

        server->next_idx = log_->last_entry_idx() + 1;
        /* TODO: sanity checking*/
        server->match_idx = 0;

        server->request_vote_service = raftcore_service_uptr(new RaftCoreService::Stub(election_channel_.get()));

        server->append_entries_service = raftcore_service_uptr(new RaftCoreService::Stub(data_channel_.get()));
        
        servers_.insert(std::pair<std::string, raft_server_sptr>(p, server));

        p = ::strtok_r(NULL, ",", &save_ptr);
    }
    /* end of servers */

    /* http_server_port */
    std::string * http_server_port = cfg_->get("http_server_port");

    if (http_server_port == nullptr) {
        http_server_port_ = "29998";
        glogger::l(INFO, "missing http_server_port, 29998 is used as default.");
        http_server_ = http_server_uptr(new http::server::server(ios_, "0.0.0.0", "29998"));
    } else {
        http_server_port_ = *http_server_port;
        http_server_ = http_server_uptr(new http::server::server(ios_, "0.0.0.0", *http_server_port));
    }

    http_server_->register_handler("/", std::bind(&raft::handle_stat, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/stat", std::bind(&raft::handle_stat, this, std::placeholders::_1, std::placeholders::_2));
    http_server_->register_handler("/append_log_entry", std::bind(&raft::handle_append_log_entry, this, std::placeholders::_1, std::placeholders::_2));

    /* end of http_server_port */

    /* initialize random number engine */

    mt_ = mt19937_uptr(new std::mt19937(rd_()));
    dist_ = uniform_int_dist_uptr(new std::uniform_int_distribution<int>(min_election_timeout_,
                                                                         max_election_timeout_));
    
    commit_idx_ = 0;
    
    inited_ = true;

    return RC_GOOD;
}

rc_errno raft::start() {
    if (!inited_) {
        glogger::l(WARNING, "raftcore's not been initialized.");
        return RC_ERROR;
    }

    if (started_) {
        glogger::l(WARNING, "raftcore's already been started");
        return RC_GOOD;
    }

    http_server_->start();

    started_at_ = high_resolution_clock::now();
    last_heartbeat_ = high_resolution_clock::time_point::min();

    cur_role_ = FOLLOWER;

    /* set up election timer event */
    glogger::l(DEBUG, "resetting election timer by start...");
    reset_election_timer();


    started_ = true;

    for ( ; started_ ; ) {
        try {
            ios_.run();
        } catch(std::exception& e) {
            glogger::l(ERROR, e.what());
        }
    }

    return RC_GOOD;
}

rc_errno raft::stop() {
    started_ = false;

    if (started_) {
        http_server_->stop();
        ios_.stop();
    }
    
    return RC_GOOD;
}

void core_service_impl::pre_vote(::google::protobuf::RpcController* ctl,
                                 const ::raftcore::RequestVote* req,
                                 ::raftcore::RequestVoteRes* res,
                                 ::google::protobuf::Closure* done) {
    high_resolution_clock::time_point now = high_resolution_clock::now();
    uint64_t msecs = duration_cast<milliseconds>(now - raft_->last_heartbeat_).count();

    glogger::l(DEBUG, "pre_vote got called from server[%s]: req: %s, raft_->core_map()->cur_term: %d, raft_->core_map()->voted_for: %s", req->candidate_id().c_str(), req->ShortDebugString().c_str(), raft_->core_map()->cur_term, raft_->core_map()->voted_for);

    /* reject pre-vote if one of the following is true:
     * 1. candidate's log is not as up-to-date as this server 
     * 2. recevices the request within the minimum election timeout of hearing from the current leader
     */
    if (raft_->log_->last_entry_term() > req->last_log_term() ||
        raft_->log_->last_entry_idx() > req->last_log_idx()){
        glogger::l(INFO, "pre_vote: requester[%s]'s log(%lld, %lld) is older than this server(%lld, %lld), rejecting...", req->candidate_id().c_str(), req->last_log_idx(), req->last_log_term(), raft_->log_->last_entry_idx() , raft_->log_->last_entry_term());
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (msecs < raft_->min_election_timeout_) {
        glogger::l(INFO, "pre_vote: last heartbeat receiving time still in good range(time between this request and the last heartheart is %lldms), rejecting requester[%s]...", msecs, req->candidate_id().c_str());
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else {
        glogger::l(INFO, "pre_vote: would granted vote for requester[%s] log(%lld, %lld)", req->candidate_id().c_str(), req->last_log_idx(), req->last_log_term());
        res->set_term(req->term());
        res->set_vote_granted(true);
    }
    glogger::l(DEBUG, "pre_vote: done invoking request_vote from %s with result: %s", req->candidate_id().c_str(), res->ShortDebugString().c_str());
    done->Run();
}


/* response to request_vote call */
void core_service_impl::request_vote(::google::protobuf::RpcController* ctl,
                                     const ::raftcore::RequestVote* req,
                                     ::raftcore::RequestVoteRes* res,
                                     ::google::protobuf::Closure* done) {
    glogger::l(DEBUG, "request_vote got called from server[%s]: req: %s, raft_->core_map()->cur_term: %d, raft_->core_map()->voted_for: %s", req->candidate_id().c_str(), req->ShortDebugString().c_str(), raft_->core_map()->cur_term, raft_->core_map()->voted_for);
    
    if (req->term() > raft_->core_map()->cur_term) {
        glogger::l(INFO, "request_vote: a higher term %lld from %s exists, stepping down", req->term(), req->candidate_id().c_str());
        raft_->core_map()->voted_for[0] = 0;
        raft_->step_down(req->term());
    }
    /* reject vote if one of the following is true:
     * 1. term < cur_term
     * 2. already voted for someone else and that someone is not the candidate in question
     * 3. candidate's log is not as up-to-date as this server
     */
    if (req->term() < raft_->core_map()->cur_term){
        glogger::l(INFO, "request_vote: request's term is smaller than current term, rejecting...");
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (raft_->core_map()->voted_for[0] &&
        req->candidate_id() != std::string(raft_->core_map()->voted_for)) {
        glogger::l(INFO, "request_vote: already voted for %s, rejecting request from %s", raft_->core_map()->voted_for, req->candidate_id().c_str());
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else if (raft_->log_->last_entry_term() > req->last_log_term() ||
        raft_->log_->last_entry_idx() > req->last_log_idx()){
        glogger::l(INFO, "request_vote: requester[%s]'s log(%lld, %lld) is older than this server(%lld, %lld), rejecting...", req->candidate_id().c_str(), req->last_log_idx(), req->last_log_term(), raft_->log_->last_entry_idx() , raft_->log_->last_entry_term());
        res->set_term(raft_->core_map()->cur_term);
        res->set_vote_granted(false);
    } else {
        /* step down if this is a higher term */
        if (req->term() > raft_->core_map()->cur_term) {
            strncpy(raft_->core_map()->voted_for, req->candidate_id().c_str(), sizeof(raft_->core_map()->voted_for));
            raft_->step_down(req->term());
        }/* save the sync if already voted for this candidate */
        else if (req->candidate_id() != std::string(raft_->core_map()->voted_for)) {
            raft_->core_map()->cur_term = req->term();
            strncpy(raft_->core_map()->voted_for, req->candidate_id().c_str(), sizeof(raft_->core_map()->voted_for));
            TIMING_START
            raft_->cmap_->sync_all();
            TIMING_STOP("core_service_impl::request_vote raft_->cmap_->sync_all()")
        }
        glogger::l(INFO, "request_vote: granted vote for requester[%s] log(%lld, %lld)", req->candidate_id().c_str(), req->last_log_idx(), req->last_log_term());
        res->set_term(req->term());
        res->set_vote_granted(true);
    }
    glogger::l(DEBUG, "request_vote: done invoking request_vote from %s with result: %s", req->candidate_id().c_str(), res->ShortDebugString().c_str());
    done->Run();
}

/* response to append_entries call */
void core_service_impl::append_entries(::google::protobuf::RpcController* ctl,
                    const ::raftcore::AppendEntries* req,
                    ::raftcore::AppendEntriesRes* res,
                    ::google::protobuf::Closure* done) {
    rc_errno rc;

    glogger::l(DEBUG, "append_entries got called from leader[%s]: req: %s, raft_->core_map()->cur_term: %d, raft_->core_map()->voted_for: %s",req->leader_id().c_str(), req->ShortDebugString().c_str(), raft_->core_map()->cur_term, raft_->core_map()->voted_for);
    
    if (req->term() < raft_->core_map()->cur_term) {
        glogger::l(INFO, "append_entries: rpc from older leader %s with term[%lld], rejecting...", req->leader_id().c_str(), req->term());
        /* old leader */
        goto failed;
    } else if (req->term() > raft_->core_map()->cur_term) {
        glogger::l(INFO, "append_entries: a new term[%lld] from %s begins, resetting election timer event...", req->term(), req->leader_id().c_str());
        /* set voted_for to null if a new term begins  */
        raft_->core_map()->voted_for[0] = 0;
        raft_->step_down(req->term());
    } else {
        /* append entries rpc from current leader, reset election timer */
        glogger::l(DEBUG, "append_entries: rpc from current leader, resetting election timer event...");
        raft_->reset_election_timer();
        raft_->cur_role_ = FOLLOWER;
    }

    if (raft_->log_->has_log_entry(req->prev_log_idx(), req->prev_log_term()) == false) {
        /* log inconsistency */
        glogger::l(INFO, "append_entries: rpc from %s not such log entry as idx:%d, term: %d", req->leader_id().c_str(), req->prev_log_idx(), req->prev_log_term());
        goto failed;
    } else {
        /* <= 0 means heartbeat message */
        if (req->entries_size() > 0) {
            glogger::l(INFO, "append_entries: about to append log entries: %s", req->ShortDebugString().c_str());

            /* all goes well, append entries to local log */
            std::vector<log_entry_sptr> v;
            int i = 0;
            for (; i < req->entries_size(); ++i) {
                const LogEntry & e = req->entries(i);
                if (raft_->log_->log_entry_conflicted(e.idx(), e.term())) {
                    /* chop off inconsistent entries if any */
                    glogger::l(INFO, "append_entries: entry %s conflicted with eixsted one %s, chopping off...", e.ShortDebugString().c_str(), LOG_ENTRY_TO_STRING((*(raft_->log_))[e.idx()]).c_str());
                    if ((rc = raft_->log_->chop(e.idx())) != RC_GOOD) {
                        glogger::l(ERROR, "append_entries: failed to chop entries starting at index %d, error code: %d.", v.front()->idx, rc);
                        ctl->SetFailed("server internal error");
                        goto failed;
                    }
                } else if (raft_->log_->has_log_entry(e.idx(), e.term())) {
                    glogger::l(INFO, "append_entries: entry %s already existed in the log, skipping...", e.ShortDebugString().c_str());
                    continue;
                }
                break;
            }


            std::string entries_content = "";

            for (; i < req->entries_size(); ++i) {
                const LogEntry & e = req->entries(i);
                log_entry_sptr entry = log_entry_sptr(make_log_entry(e.data().size()));
                
                if (entry.get() == nullptr) {
                    glogger::l(ERROR, "append_entries: failed to allocate memory for incoming log_entry, out of memory.");
                    ctl->SetFailed("out of memory");
                    goto failed;
                }

                entry->idx = e.idx();
                entry->term = e.term();
                ::memcpy(entry->data, e.data().data(), e.data().size());

                v.push_back(entry);

                if (i < req->entries_size() - 1)
                    entries_content.append(e.ShortDebugString() + ",");
                else
                    entries_content.append(e.ShortDebugString());
            }

            /* persist to disk */
            if (v.size() > 0 && (rc = raft_->log_->append(v, true)) != RC_GOOD) {
                glogger::l(ERROR, "append_entries: failed to append entries to log, error code: %d.", rc);
                ctl->SetFailed("server internal error");
                goto failed;
            }
            glogger::l(INFO, "append_entries: entries (%s) actually appened", entries_content.c_str());
        }
        /* If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) */
        if (req->leader_commit() > raft_->commit_idx_) {
            glogger::l(INFO, "append_entries: set commit index to min(leader_commit[%lld], last_entry_idx[%lld])", req->leader_commit(), raft_->log_->last_entry_idx());
            raft_->commit_idx_ = std::min(req->leader_commit(), raft_->log_->last_entry_idx());
            raft_->commit_queue_.push(false);
        }
    }

    raft_->cur_leader_ = req->leader_id();
    raft_->last_heartbeat_ = high_resolution_clock::now();

/* succeed: */
    res->set_term(raft_->core_map()->cur_term);
    res->set_success(true);
    res->set_match_idx(raft_->log_->last_entry_idx());
    res->set_id(req->id());
    done->Run();
    glogger::l(DEBUG, "append_entries: done invoking append_entries from %s with result: %s", req->leader_id().c_str(), res->ShortDebugString().c_str());
    return;

failed:
    res->set_term(raft_->core_map()->cur_term);
    res->set_success(false);
    res->set_match_idx(raft_->log_->last_entry_idx());
    res->set_id(req->id());
    done->Run();
    glogger::l(DEBUG, "append_entries: done invoking append_entries from %s with result: %s", req->leader_id().c_str(), res->ShortDebugString().c_str());
    return;
}

}