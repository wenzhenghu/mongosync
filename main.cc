#include "mongosync.h"
#include "log.h"

#include <iostream>

void *mongo_to_mongos(void *args) {
  	MongoSync *mongosync = reinterpret_cast<MongoSync *>(args);
  	mongosync->Process();

  	delete mongosync;
  	pthread_exit(NULL);
}

int main(int argc, char *argv[]) {
	/*
	 *  first set default log level to DEBUG
	 */
	mlog::Init(mlog::kDebug, "./log", "mongosync");
	LOG(INFO) << "monogosync started, first set log level to DEBUG" << std::endl;

  	mongo::client::GlobalInstance instance;
  	if (!instance.initialized()) {
		LOG(FATAL) << "failed to initialize the client driver: " << instance.status() << std::endl;
    	exit(-1);
  	}
	
  	Options opt;
  	if (argc == 3 && (strncmp(argv[1], "-c", 2) == 0)) {
    	opt.LoadConf(argv[2]);
  	} else {
    	opt.ParseCommand(argc, argv);	
  	}

	if (!opt.log_level.empty()) {
		LOG(INFO) << "with log level option, set log level to " << opt.log_level << std::endl; 
		if (!mlog::SetLogLevel(opt.log_level)) {
			LOG(WARN) << "log level option value invalid, set log level to default to INFO" << std::endl;
		}
	}

	// Connect to src mongodb instance/cluster 
	MongoSync *mongosync = MongoSync::NewMongoSync(&opt);
	if (!mongosync) {
		LOG(FATAL) << "Create mongosync instance failed" << std::endl;
		return -1;
	}

  	if (opt.is_mongos) {
		
    	std::vector<std::string> shards = mongosync->GetShards();
    	if (mongosync->IsBalancerRunning()) {
      		LOG(FATAL) << "Balancer is running" << std::endl;
			delete mongosync;
      		return -1;
    	}	
  	} 

	mongosync->Process();
    delete mongosync;

  	return 0;
}
