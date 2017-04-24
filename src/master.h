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
#include <queue>
#include <sstream>
#include <sys/stat.h>
#include <chrono>
#include <stdio.h>
#include <pthread.h>
#include<stdio.h>
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

std::string intermediateDirectory="map_reduce_temp";

std::string appender_map="map_";

std::string appender_reduce="reduce_";

int max_failures_allowed=20;


//inline long getCurrentTimeInLong(){
//auto now = std::chrono::system_clock::now();
//auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
//auto epoch = now_ms.time_since_epoch();
//auto value = std::chrono::duration_cast<std::chrono::milliseconds>(epoch);
//}

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

		void checkAndUpdateWorkers(bool isMap);

		bool isDone();

		std::string getReduceTaskName(int hashCode);

		std::vector<std::string> getUnDoneTasks(int maxLimit);

		void initReducePhase();

	private:

	MapReduceSpec spec;
	std::map<std::string,WorkerClient*> runningWorkers;
	std::queue<WorkerClient*> freeWorkers;
	std::vector<WorkerClient*> notRespondingWorkers;
	std::map<std::string,bool> taskProgressTracker;
	std::map<std::string,int> taskAttemptCount;
	std::map<std::string,FileShard> mapTaskFileShardTracker;
	std::map<std::string,FileShard> reduceTaskFileShardTracker;
	int numberOfFailures=0;
};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
  ::mkdir(intermediateDirectory.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
  ::mkdir(mr_spec.outputDirectory.c_str(), S_IRUSR | S_IWUSR | S_IXUSR);
  spec=mr_spec;
  int i;
  for(i=0;i<mr_spec.numberOfWorkers;i++)
  {
      std::cout << "() Connecting to address : "<<mr_spec.workers[i] << std::endl;
    WorkerClient* client = new WorkerClient(mr_spec.workers[i]);
    freeWorkers.push(client);
  }
 //map task set up
 for(int i=0;i<file_shards.size();i++)
   {
      std::string taskId=appender_map+mr_spec.userId+"_"+std::to_string(i);
      taskProgressTracker[taskId]=false;
      taskAttemptCount[taskId]=0;
      mapTaskFileShardTracker[taskId]=file_shards[i];
   }
}


std::string Master::getReduceTaskName(int hashCode)
{
   return appender_reduce+spec.userId+'_'+std::to_string(hashCode);
}

void Master::checkAndUpdateWorkers(bool isMap)
{
    std::map<std::string,WorkerClient*> tempWorkers;
    int i;
    for(auto const &ent1 : runningWorkers) {
      std::string taskId=ent1.first;
      WorkerClient* client=ent1.second;
      WorkerTaskStatus  workerTaskStatus=client->checkTaskStatus(taskId);
      if(workerTaskStatus.success && workerTaskStatus.valid)
      {
         if(workerTaskStatus.running)
         {
           tempWorkers[taskId]=client;
         }
         else
         {
           freeWorkers.push(client);
           if(isMap)
           {
               for(auto const &ent2 : workerTaskStatus.files)
               {
                 int hashCode=ent2.first;
                 std::string fileName=ent2.second;
                 std::string taskId=getReduceTaskName(hashCode);
                 std::map<std::string,FileShard>::iterator it = reduceTaskFileShardTracker.find(taskId);
                 FileShard* shard;
                 if(it != reduceTaskFileShardTracker.end())
                 {
                    shard = &(it->second);
                 }
                 else
                 {
                   FileShard tempFileShard;
                   reduceTaskFileShardTracker[taskId]=tempFileShard;
                   shard=&(reduceTaskFileShardTracker[taskId]);
                 }
                 AFileShard aFileShard;
                 aFileShard.filePath=fileName;
                 shard->fileSplits.push_back(aFileShard);
               }
           }
           else
           {
            std::vector<AFileShard> files=reduceTaskFileShardTracker[taskId].fileSplits;
            int j;
            for(j=0;j<files.size();j++)
            {
             remove(files[j].filePath.c_str( ));
            }
           }
           taskProgressTracker[taskId]=true;
         }
      }
      else
      {
          taskAttemptCount[taskId]=taskAttemptCount[taskId]+1;
          numberOfFailures++;
          notRespondingWorkers.push_back(client);
      }
    }

    std::vector<WorkerClient*> tempNotResponding;
    for(i=0;i<notRespondingWorkers.size();i++)
    {
        if(notRespondingWorkers[i]->checkHeartBeat())
        {
           freeWorkers.push(notRespondingWorkers[i]);
        }
        else
        {
          tempNotResponding.push_back(notRespondingWorkers[i]);
        }
    }
    notRespondingWorkers=tempNotResponding;
    runningWorkers=tempWorkers;
}


bool Master::isDone()
{
    for(auto const &ent1 : taskProgressTracker)
        {
         if(!ent1.second)
            {
                  return false;
            }
        }
     return true;
}

std::vector<std::string> Master::getUnDoneTasks(int maxLimit)
   {
     std::cout << "getUnDoneTasks("<<maxLimit<<")" << std::endl;
   int inserted=0;
   std::vector<std::string> taskIds;
   for(auto const &ent1 : taskProgressTracker)
       {
         if(maxLimit==inserted)
          {
            return taskIds;
          }
         std::string taskId=ent1.first;
         bool finished=ent1.second;
         std::cout << "getUnDoneTasks Task Id "<<taskId<< std::endl;
         if(!finished)
           {
           std::map<std::string,WorkerClient*>::iterator it = runningWorkers.find(taskId);
           if(it == runningWorkers.end())
           {
             taskIds.push_back(taskId);
           }
           }
       }
   return taskIds;
   }

void Master::initReducePhase(){
        taskProgressTracker.clear();
        taskAttemptCount.clear();
        for(auto const &ent1 : reduceTaskFileShardTracker)
           {
           std::string taskId=ent1.first;
           taskProgressTracker[taskId]=false;
           taskAttemptCount[taskId]=0;
           }

}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
    std::cout << "Step 1" << std::endl;
   //map phase
    while(!isDone())
    {
      std::cout << "Step 1.1" << std::endl;
        if(numberOfFailures>max_failures_allowed)
        {
         std::cout << "Crossed maximum number of failures, quitting" << std::endl;
         return false;
        }
        std::cout << "Step 1.2" << std::endl;
        checkAndUpdateWorkers(true);
         std::cout << "Step 1.3 free workers: "<<freeWorkers.size() << std::endl;
         if (!freeWorkers.empty())
         {
           std::cout << "Step 1.4" << std::endl;
            std::vector<std::string> taskIds=getUnDoneTasks(freeWorkers.size());
             std::cout << "Step 1.5 pending tasks:"<<taskIds.size() << std::endl;
            int taskIdPos=0;
            while(taskIdPos<taskIds.size() && freeWorkers.size()>0)
            {
               std::cout << "Step 1.6" << std::endl;
               WorkerClient* workerClient=freeWorkers.front();
                std::cout << "Step 1.6.1" << std::endl;
               if(workerClient->checkHeartBeat())
               {
                 std::cout << "Step 1.7" << std::endl;
                 std::string taskId=taskIds[taskIdPos];
                  std::cout << "Step 1.7.1" << std::endl;
                 bool success=workerClient->runTask(taskId,true,spec.userId,mapTaskFileShardTracker[taskId]
                 ,(intermediateDirectory+"/"+taskId+"_"+std::to_string(taskAttemptCount[taskId])),spec.numberOfOutputs);
                  std::cout << "Step 1.8" << std::endl;
                 if(success)
                 {
                   taskIdPos++;
                   freeWorkers.pop();
                   runningWorkers[taskId]=workerClient;
                 }
                 else
                 {
                   taskAttemptCount[taskId]=taskAttemptCount[taskId]+1;
                   freeWorkers.pop();
                   notRespondingWorkers.push_back(workerClient);
                 }
               }
            }
         }
     sleep(3);
    }
    initReducePhase();
    while(!isDone())
        {
            std::cout << "Step 2.1" << std::endl;
            checkAndUpdateWorkers(false);
            if(numberOfFailures>max_failures_allowed)
            {
             std::cout << "Crossed maximum number of failures, quitting" << std::endl;
             return false;
            }
            std::cout << "Step 2.2" << std::endl;
            if (!freeWorkers.empty())
            {
               std::vector<std::string> taskIds=getUnDoneTasks(freeWorkers.size());
               int taskIdPos=0;
               while(taskIdPos<taskIds.size() && freeWorkers.size()>0)
               {
                  WorkerClient* workerClient=freeWorkers.front();
                  if(workerClient->checkHeartBeat())
                  {
                    std::string taskId=taskIds[taskIdPos];
                    bool success=workerClient->runTask(taskId,false,spec.userId,reduceTaskFileShardTracker[taskId]
                    ,(spec.outputDirectory+"/"+taskId+"_"+std::to_string(taskAttemptCount[taskId])),spec.numberOfOutputs);
                    if(success)
                    {
                      taskIdPos++;
                      freeWorkers.pop();
                      runningWorkers[taskId]=workerClient;
                    }
                    else
                    {
                      taskAttemptCount[taskId]=taskAttemptCount[taskId]+1;
                      freeWorkers.pop();
                      notRespondingWorkers.push_back(workerClient);
                    }
                  }
               }
            }
        sleep(3);
        }
	return true;
}
