#pragma once

#include "masterworker.grpc.pb.h"
#include <grpc++/grpc++.h>
#include <grpc/support/log.h>
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


unsigned int connection_timeout = 2;

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::WorkerService;
using masterworker::WorkerTask;
using masterworker::TaskAccepted;
using masterworker::Shard;
using masterworker::CheckHeartBeat;
using masterworker::CheckStatus;
using masterworker::TaskStatus;
using masterworker::OutPutFile;


typedef struct WorkerTaskStatusStruct {
    bool success;
    bool valid;
    bool running;
    std::map<int,std::string> files;
}WorkerTaskStatus;



class WorkerClient {

 public:
  WorkerClient(std::string address)
      : stub_(WorkerService::NewStub(grpc::CreateChannel(
                                           address, grpc::InsecureChannelCredentials()))) {
      //DO Nothing
      }

  bool runTask(std::string taskid,bool ismap,std::string userid,
  const FileShard file_shard,std::string outputpath,int numberofoutputs) {
    WorkerTask task;
    task.set_taskid(taskid);
    task.set_ismap(ismap);
    task.set_userid(userid);
    task.set_outputpath(outputpath);
    task.set_numberofoutputs(numberofoutputs);
    int i;
    for(i=0;i<file_shard.fileSplits.size();i++)
    {
        Shard* shard=task.add_shards();
        shard->set_file(file_shard.fileSplits[i].filePath);
        if(ismap)
        {
            shard->set_offset(file_shard.fileSplits[i].offset);
            shard->set_end(file_shard.fileSplits[i].end);
        }
    }
    TaskAccepted taskAccepted;

    ClientContext context;

    std::chrono::system_clock::time_point deadline =
    std::chrono::system_clock::now() + std::chrono::seconds(connection_timeout);

    context.set_deadline(deadline);

    // The actual RPC.
    Status status = stub_->runTask(&context, task, &taskAccepted);

    // Act upon its status.
    if (status.ok()) {
      return taskAccepted.accepted();
    } else {
      return false;
    }
  }


  bool checkHeartBeat() {
      CheckHeartBeat checkHeartBeatMessage;
      masterworker::Status statusMessage;

      ClientContext context;

      std::chrono::system_clock::time_point deadline =
      std::chrono::system_clock::now() + std::chrono::seconds(connection_timeout);
      context.set_deadline(deadline);    
      // The actual RPC.
      Status status = stub_->checkHeartBeat(&context, checkHeartBeatMessage, &statusMessage);

      // Act upon its status.
      if (status.ok()) {
        return true;
      } else {
        return false;
      }
    }

  WorkerTaskStatus checkTaskStatus(std::string taskid) {
     CheckStatus checkStatus;
     checkStatus.set_taskid(taskid);
     TaskStatus taskStatus;

     ClientContext context;

     std::chrono::system_clock::time_point deadline =
     std::chrono::system_clock::now() + std::chrono::seconds(connection_timeout);

     context.set_deadline(deadline);

     // The actual RPC.
     Status status = stub_->checkTaskStatus(&context, checkStatus, &taskStatus);
     WorkerTaskStatus workerTaskStatus;
     // Act upon its status.
     int i;
     if (status.ok()) {
       workerTaskStatus.success=true;
       workerTaskStatus.valid=taskStatus.valid();
       workerTaskStatus.running=taskStatus.running();
       for(i=0;i<taskStatus.files_size();i++)
       {
          workerTaskStatus.files[taskStatus.files(i).hash()] = taskStatus.files(i).file_name();
       }
     } else {
       workerTaskStatus.success=false;
     }
     return workerTaskStatus;
   }

 private:
  std::unique_ptr<WorkerService::Stub> stub_;
};