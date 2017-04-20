#pragma once

#include <mr_task_factory.h>
#include "masterworkerImpl.h"
#include "mr_tasks.h"
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker: public Delegator{

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

		void initReducer(std::shared_ptr<BaseReducer> reducer,std::string filePathAppender){
		 reducer->impl_->init(filePathAppender,100);
		}


        void initMap(std::shared_ptr<BaseMapper> mapper,std::string filePath,int limit)
        {
         mapper->impl_-> init(limit,filePath,100);
        }
        std::vector<MapFileOutPut> handleMapCompletion(std::shared_ptr<BaseMapper> mapper){
        return mapper->impl_->handleCompletion();
        }

        std::string  handleReduceCompletion(std::shared_ptr<BaseReducer> reducer){
        return reducer->impl_->handleCompletion();
        }

	private:

	  std::string address;
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
   address=ip_addr_port;
}

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
    WorkerServiceImpl service(this);
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << address << address << std::endl;
    server->Wait();
	return true;
}
