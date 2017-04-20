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


extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

int total_line_read=0;



class Delegator{

public:
void initReducer(std::shared_ptr<BaseReducer> reducer,std::string filePathAppender);


void initMap(std::shared_ptr<BaseMapper> mapper,std::string filePath,int limit);

std::vector<MapFileOutPut> handleMapCompletion(std::shared_ptr<BaseMapper> mapper);

std::string  handleReduceCompletion(std::shared_ptr<BaseReducer> reducer);

};



class WorkerServiceImpl final : public WorkerService::Service {

public:
  WorkerServiceImpl(Delegator * DelegatorObject)
      {
         isRunning=false;
         isFree=false;
         taskId="NA";
         userId="NA";
         delegator=DelegatorObject;
      }


    void readAndMap(Shard shard,std::shared_ptr<BaseMapper> mapper){
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
         reply->set_isrunning(isRunning);
         return Status::OK;
      }

  Status runTask(ServerContext* context, const WorkerTask* workerRequest,
                    TaskAccepted* reply) override
        {
           request=workerRequest;
           reply->set_accepted(isFree);
           if(isFree)
            {
                isRunning=true;
                isFree=false;
                taskId=request->taskid();
                isMap=request->ismap();
                userId=request->userid();
                pthread_t tp_service;
                pthread_create(&tp_service, NULL, &WorkerServiceImpl::runTaskHelperSub, this);
                pthread_detach(tp_service);
            }
           isRunning=false;
           return Status::OK;
        }

     void* runTaskHelper(){
                    int i;
                    if(isMap)
                            {
                        std::string filePathAppender=request->outputpath();
                        auto mapper = get_mapper_from_task_factory(userId);
                        delegator->initMap(mapper,filePathAppender,request->numberofoutputs());
                        for(i=0;i<request->shards_size();i++)
                            {
                              readAndMap(request->shards(i),mapper);
                            }
                       outPutFiles=delegator->handleMapCompletion(mapper);
                       std::cout<<"TaskId:"<< taskId<< ", Total Lines processed = " << total_line_read<< std::endl;
                       total_line_read=0;
                           }
                       else
                           {
                        std::string outPutPath=request->outputpath();
                        auto reducer = get_reducer_from_task_factory(userId);
                        delegator -> initReducer(reducer,outPutPath);
                        readAndReduce(request,reducer);
                        MapFileOutPut mapFileOutPut;
                        mapFileOutPut.fileName =delegator->handleReduceCompletion(reducer);
                        outPutFiles.push_back(mapFileOutPut);
                        std::cout<<"TaskId:"<< taskId<< ", Total Keys processed = " << total_line_read<< std::endl;
                        total_line_read=0;
                         }
     }

      static void *runTaskHelperSub(void *workerService)
            {
        return ((WorkerServiceImpl *)workerService)->runTaskHelper();
            }

     Status checkTaskStatus(ServerContext* context, const CheckStatus* request,
                     TaskStatus* reply) override
         {
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
                         outputfile->set_filename(outPutFiles[i].fileName);
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
        Delegator* delegator;
        const WorkerTask* request;
};