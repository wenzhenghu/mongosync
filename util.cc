#include <sys/time.h>
#include <unistd.h>
#include <stdio.h>

#include <iostream>

#include "util.h"
#include "mongosync.h"

namespace util {

	uint64_t Microtime() {
		struct timeval tv;
		gettimeofday(&tv, NULL);
		return tv.tv_sec*1000000 + tv.tv_usec;
	}

	std::string Int2Str(int64_t num) {
	  	char buf[32];
	  	snprintf(buf, sizeof(buf), "%ld", num);
	  	return buf;
	}

	std::string Trim(const std::string &str, const std::string del_str) {
	  	std::string::size_type begin, end;
	  	std::string ret;
		
	  	if ((begin = str.find_first_not_of(del_str)) == std::string::npos) {
	    	return ret;
	  	}
	  	end = str.find_last_not_of(del_str);
	  	return str.substr(begin, end-begin+1);
	}

	std::vector<std::string> &Split(const std::string &s, char delim, 
			std::vector<std::string> &elems) {
	    elems.clear();
	    std::stringstream ss(s);
	    std::string item;
		
	    while (std::getline(ss, item, delim)) {
	        if (!item.empty())
	            elems.push_back(Trim(item, " "));
	    }
	    return elems;
	}

	bool AlmostEqual(int64_t v1, int64_t v2, uint64_t range) {
		if (v1 <= v2 + range && v1 >= v2 - range) {
			return true;
		}
		
		return false;
	}

	std::string GetFormatTime(time_t t) { // Output format is: Aug 23 14:55:02 2001
		char buf[32];
		if (t == -1) {
			t = time(NULL);
		}
		
		strftime(buf, sizeof(buf), "%b %d %T %Y", localtime(&t));
		return std::string(buf);
	}

	static int DoCreatePath(const char *path, mode_t mode) {
	  	struct stat st;
	  	int status = 0;

	  	if (stat(path, &st) != 0) {
	    	/* Directory does not exist. EEXIST for race condition */
	    	if (mkdir(path, mode) != 0 && errno != EEXIST)
	      		status = -1;
	  	} else if (!S_ISDIR(st.st_mode)) {
	    	errno = ENOTDIR;
	    	status = -1;
	  	}

	  	return (status);
	}

	int CreatePath(const std::string &path, mode_t mode) {
	  	char *pp;
	  	char *sp;
	  	int status;
	  	char *copypath = strdup(path.c_str());

	  	status = 0;
	  	pp = copypath;
		
	  	while (status == 0 && (sp = strchr(pp, '/')) != 0) {
	    	if (sp != pp) {
	      		/* Neither root nor double slash in path */
	      		*sp = '\0';
	      		status = DoCreatePath(copypath, mode);
	      		*sp = '/';
	    	}
			
	    	pp = sp + 1;
	  	}
		
	  	if (status == 0)
	   		status = DoCreatePath(path.c_str(), mode);

	  	free(copypath);
	  	return (status);
	}


	/*******************************************************************************************/

	BGThreadGroup::BGThreadGroup(const std::string &srv_ip_port, const std::string &auth_db, 
			const std::string &user, const std::string &passwd, const bool use_mcr, const int32_t bg_thread_num)
	  	:srv_ip_port_(srv_ip_port),
		auth_db_(auth_db),
		user_(user),
		passwd_(passwd),
		running_(false), 
	  	should_exit_(false),
		use_mcr_(use_mcr),
		bg_thread_num_(bg_thread_num) {

	  	pthread_mutex_init(&mlock_, NULL);
	  	pthread_cond_init(&clock_, NULL);

	}

	BGThreadGroup::~BGThreadGroup() {
		should_exit_ = true;
		pthread_cond_broadcast(&clock_);
		for (std::vector<pthread_t>::const_iterator iter = tids_.begin();
				iter != tids_.end();
				++iter) {
			pthread_join(*iter, NULL);
		}

	  	pthread_mutex_destroy(&mlock_);
	  	pthread_cond_destroy(&clock_);
	}

	void BGThreadGroup::StartThreadsIfNeed() {
	  	if (running_) {
	    	return;
	  	}

		pthread_t tid;
		for (int idx = 0; idx != bg_thread_num_; ++idx) {
	  		if (pthread_create(&tid, NULL, BGThreadGroup::Run, this) != -1) {
	  	  		tids_.push_back(tid);
	  		} else {
	  	  		std::cerr << "[WARN]\tBGThread: " << idx << " starts error!" << std::endl;
	  		}
		}
		
		if (tids_.empty()) {
			std::cerr << "[ERROR]\tBGThreadGroup all start fail!" << std::endl;
			exit(-1);	
		}
		
		running_ = true;	
	}

	void BGThreadGroup::AddWriteUnit(const std::string &ns, WriteBatch *batch) {
	  	StartThreadsIfNeed();

	  	WriteUnit unit;
	  	unit.ns = ns;
	  	unit.batch = batch;


	  	pthread_mutex_lock(&mlock_);
	  	while (!write_queue_.empty()) {
	    	pthread_mutex_unlock(&mlock_);
	    	sleep(1);
	    	pthread_mutex_lock(&mlock_);
	  	}

	  	write_queue_.push(unit);
	  	pthread_cond_signal(&clock_);
	  	pthread_mutex_unlock(&mlock_);  
	}

	void *BGThreadGroup::Run(void *arg) {
	  	BGThreadGroup *thread_ptr = reinterpret_cast<BGThreadGroup *>(arg);
		mongo::DBClientConnection *conn = MongoSync::ConnectAndAuth(thread_ptr->srv_ip_port(), 
			thread_ptr->auth_db(), thread_ptr->user(), thread_ptr->passwd(), thread_ptr->use_mcr(), true);

		if (!conn) {
			return NULL;
		}

	  	std::queue<WriteUnit> *queue_p = thread_ptr->write_queue_p();
	  	pthread_mutex_t *queue_mutex_p = thread_ptr->mlock_p();
	  	pthread_cond_t *queue_cond_p = thread_ptr->clock_p();
	  	WriteUnit unit;
		

	  	while (!thread_ptr->should_exit()) {

		  	pthread_mutex_lock(queue_mutex_p);
		    while (queue_p->empty() && !thread_ptr->should_exit()) {
		      	pthread_cond_wait(queue_cond_p, queue_mutex_p);
		    }
		    
		    if (thread_ptr->should_exit()) {
		  		pthread_mutex_unlock(queue_mutex_p);
		      break;
		    }

		    unit = queue_p->front();

			LOG(DEBUG) << " util bsonobj data: " << unit.ns << std::endl;

		    queue_p->pop();
		    pthread_mutex_unlock(queue_mutex_p);
			
			try {
				conn->insert(unit.ns, *(unit.batch), 0, &mongo::WriteConcern::unacknowledged); 
			} catch (mongo::DBException &e) {
				LOG(WARN) << "util exception occurs when insert doc to " << unit.ns << ", we just skip it" << std::endl;
				conn->insert(unit.ns, *(unit.batch), mongo::InsertOption_ContinueOnError, &mongo::WriteConcern::unacknowledged); 
			}

		    delete unit.batch;
	  	}

		delete conn;
	  	return NULL;
	}

}
