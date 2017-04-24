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
#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
#include <pthread.h>

using std::ifstream;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using masterworker::WorkerService;
using masterworker::WorkerTask;
using masterworker::TaskAccepted;
using masterworker::Shard;
using masterworker::CheckHeartBeat;
using masterworker::CheckStatus;
using masterworker::TaskStatus;
using masterworker::OutPutFile;


int total_line_read=0;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker final : public WorkerService::Service{

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

     void readAndMap(Shard shard,std::shared_ptr<BaseMapper> mapper){
            std::cout << "Processing file :"<< shard.file()<< ", offset:"<<shard.offset()<<", end:"<<shard.end()<< std::endl;
           ifstream temporaryfstream(shard.file().c_str(),ifstream::binary);
           temporaryfstream.seekg(shard.offset());
           std::string line;
           while(temporaryfstream.tellg()<shard.end() && temporaryfstream.tellg()!=-1){
                std::getline(temporaryfstream, line);
                mapper->map(line);
                total_line_read++;
           }
        }

          void readAndReduce(const WorkerTask* request,std::shared_ptr<BaseReducer> reducer){

             std::map<std::string,std::vector<std::string>> resultMap;
             int i;
              for(i=0;i<request->shards_size();i++)
              {
                 std::cout << "Processing file: "+request->shards(i).file() << std::endl;
                ifstream temporaryfstream(request->shards(i).file().c_str(),ifstream::binary);
                 while(temporaryfstream.tellg()!=-1){
                            std::string line;
                            std::getline(temporaryfstream, line);
                            int pos = line.find_first_of(',');
                            std::string key = line.substr(pos+1),
                            value = line.substr(0, pos);
                            std::map<std::string,std::vector<std::string>>::iterator it = resultMap.find(key);
                            if(it != resultMap.end())
                                {
                                  it->second.push_back(value);
                                }
                                else
                                {
                                  std::vector<std::string> newVector;
                                  newVector.push_back(value);
                                  resultMap[key]=newVector;
                                }
                       }
              }
            //Map is already ordered on key
            std::map<std::string,std::vector<std::string>>::iterator it = resultMap.begin();
            	while (it != resultMap.end())
            	{
            	    total_line_read++;
            		const std::string key = it->first;
            		const  std::vector<std::string> valueList = it->second;
            		reducer->reduce(key,valueList);
            		it++;
            	}

          }

      Status checkHeartBeat(ServerContext* context, const CheckHeartBeat* request,
                      masterworker::Status* reply) override
          {
             std::cout << "message:checkHeartBeat" << std::endl;
             reply->set_isrunning(isRunning);
             return Status::OK;
          }

      Status runTask(ServerContext* context, const WorkerTask* workerRequest,
                        TaskAccepted* reply) override
            {
               std::cout << "message:runTask" << std::endl;
               request=workerRequest;
               reply->set_accepted(isFree);
               if(isFree)
                {
                    isRunning=true;
                    isFree=false;
                    taskId=request->taskid();
                    isMap=request->ismap();
                    userId=request->userid();
                    std::cout << "taskId : " <<taskId<<", isMap :"<<isMap<<", userId"<<userId<< std::endl;
                    std::cout<<"shard size: "<<request->shards_size()<< std::endl;
                    pthread_t tp_service;
                    pthread_create(&tp_service, NULL, &Worker::runTaskHelperSub, this);
                    pthread_detach(tp_service);
                    std::cout << "message:runTask, Accepted" << std::endl;
                }
                else
                {
                  std::cout << "message:runTask, Rejected" << std::endl;
                }
               return Status::OK;
            }

         void* runTaskHelper(){
                        int i;
                        if(isMap)
                                {
                            std::string filePathAppender=request->outputpath();
                            auto mapper = get_mapper_from_task_factory(userId);
                            initMap(mapper,filePathAppender,request->numberofoutputs());
                            for(i=0;i<request->shards_size();i++)
                                {
                                  readAndMap(request->shards(i),mapper);
                                }
                           outPutFiles=handleMapCompletion(mapper);
                           std::cout<<"TaskId:"<< taskId<< ", Total Lines processed = " << total_line_read<< std::endl;
                           total_line_read=0;
                               }
                           else
                               {
                            std::string outPutPath=request->outputpath();
                            auto reducer = get_reducer_from_task_factory(userId);
                            initReducer(reducer,outPutPath);
                            readAndReduce(request,reducer);
                            MapFileOutPut mapFileOutPut;
                            mapFileOutPut.fileName =handleReduceCompletion(reducer);
                            outPutFiles.push_back(mapFileOutPut);
                            std::cout<<"TaskId:"<< taskId<< ", Total Keys processed = " << total_line_read<< std::endl;
                            total_line_read=0;
                             }
                             isRunning=false;
         }

          static void *runTaskHelperSub(void *workerService)
                {
            return ((Worker *)workerService)->runTaskHelper();
                }

         Status checkTaskStatus(ServerContext* context, const CheckStatus* request,
                         TaskStatus* reply) override
             {
                std::cout << "message:checkTaskStatus" << std::endl;
                if(isFree || request->taskid()!=taskId)
                {
                  reply->set_valid(false);
                }
                else
                {
                    int i;
                    reply->set_valid(true);
                    reply->set_running(isRunning);
                     if(!isRunning)
                       {
                        for(i=0;i<outPutFiles.size();i++)
                         {
                             OutPutFile* outputfile= reply->add_files();
                             outputfile->set_file_name(outPutFiles[i].fileName);
                             outputfile->set_hash(outPutFiles[i].hashId);
                         }
                         reply->set_running(isRunning);
                       }
                    outPutFiles.clear();
                    isFree=true;
                }
                return Status::OK;
             }


    private:
        std::string userId;
    	bool isRunning;
    	bool isMap;
    	std::string taskId;
        bool isFree;
        std::vector<MapFileOutPut> outPutFiles;
        const WorkerTask* request;
	    std::string address;

};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
         address=ip_addr_port;
         isRunning=false;
         isFree=true;
         taskId="NA";
         userId="NA";
}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(this);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout <<"Server listening on "<< address << std::endl;
    server->Wait();
	return true;
}
