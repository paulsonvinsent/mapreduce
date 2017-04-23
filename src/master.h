#pragma once


#include <string>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <map>
#include <sstream>
#include "masterworker.grpc.pb.h"
#include "mapreduce_spec.h"
#include "file_shard.h"
#include "master_helper.h"
using masterworker::WorkerService;
using masterworker::WorkerTask;
using masterworker::TaskAccepted;
using masterworker::Shard;
using masterworker::CheckHeartBeat;
using masterworker::CheckStatus;
using masterworker::TaskStatus;
using masterworker::OutPutFile;

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:

	MapReduceSpec spec;
	std::vector<FileShard> shards;
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
  spec=mr_spec;
  shards=file_shards;
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	return true;
}