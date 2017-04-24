#pragma once

#include <string>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <fstream>
#include <unistd.h>
#include <cstring>
#include <cstdlib>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
    int numberOfWorkers;
    std::vector<std::string> workers;
    std::vector<std::string> inputFiles;
    std::string outputDirectory;
    int numberOfOutputs;
    long mapSize;//in bytes
    std::string userId;
};


inline void splitAndAdd(const std::string& to_split,char splitChar,std::vector<std::string>& array){
    std::size_t pos = 0, found;
    while((found = to_split.find_first_of(splitChar, pos)) != std::string::npos) {
        array.push_back(to_split.substr(pos, found - pos));
        pos = found+1;
    }
    array.push_back(to_split.substr(pos));
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
    std::ifstream myfile (config_filename.c_str());
      if (myfile.is_open()) {
        std::string line;
        while (getline(myfile, line)) {
            std::size_t index = line.find_first_of('=', 0);
            std::string key=line.substr(0,index);
            std::string value=line.substr(index+1);
            if(key.compare("n_workers") == 0){
              mr_spec.numberOfWorkers= atoi(value.c_str());
            }
            else if(key.compare("worker_ipaddr_ports") == 0){
              splitAndAdd(value,',',mr_spec.workers);
            }
            else if(key.compare("input_files") == 0){
              splitAndAdd(value,',',mr_spec.inputFiles);
            }
            else if(key.compare("output_dir") == 0){
              mr_spec.outputDirectory= value;
            }
            else if(key.compare("n_output_files") == 0){
              mr_spec.numberOfOutputs= atoi(value.c_str());
            }
            else if(key.compare("map_kilobytes") == 0){
              mr_spec.mapSize= atoi(value.c_str())*1024;
            }
            else if(key.compare("user_id") == 0){
              mr_spec.userId= value;
            }
        }
        myfile.close();
      }
	return true;
}

inline bool checkWhetherFileExists(std::string fileName){
    std::ifstream f(fileName.c_str());
    return f.good();
}

/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
    if(mr_spec.numberOfWorkers!=mr_spec.workers.size() || mr_spec.numberOfWorkers<=0 || mr_spec.numberOfOutputs<=0|| mr_spec.mapSize<=0 || mr_spec.inputFiles.size()<=0)
    {
       return false;
    }
    int i;
    for(i=0;i<mr_spec.inputFiles.size();i++)
    {
      if(!checkWhetherFileExists(mr_spec.inputFiles[i])){
          std::cerr << "[ERROR] Input file does not exist :" << mr_spec.inputFiles[i] << std::endl;
          return false;
      }
    }
    if(checkWhetherFileExists(mr_spec.outputDirectory)){
      std::cerr << "[ERROR] Output directory  exist, please delete : " << mr_spec.outputDirectory << std::endl;
     return false;
    }
	return true;
}