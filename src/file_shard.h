#pragma once

#include <string>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <fstream>
#include <unistd.h>
#include <cstring>
#include <cstdlib>
#include <limits>

#include "mapreduce_spec.h"

using std::fstream;
using std::ifstream;

typedef struct AFileShardStruct{
 std::string filePath;
 long offset;
 long end;
}AFileShard;
/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct FileShard {
std::vector<AFileShard> fileSplits;
};



inline long getSize(std::string fileName)
{
    ifstream infile (fileName.c_str(),ifstream::binary);
    infile.seekg(0,ifstream::end);
    long fileSize=infile.tellg();
    infile.close();
    return fileSize;
}


inline void getFileShards(std::vector<FileShard>& shards,std::vector<std::string> files,long maxBytes)
    {
            int filesIndex=0;
            int shardsIndex=0;
            int bytesToReadInThisShard=maxBytes;
            ifstream infile((files[0]).c_str(),ifstream::binary);
            long file_size=getSize(files[0]);
            while(filesIndex<files.size())
            {
                if(shards.size()==shardsIndex)
                    {
                        FileShard fileShardNew;
                        shards.push_back(fileShardNew);
                    }
                long currentPosition= infile.tellg();
                long bytesLeft=file_size-currentPosition;
                if(bytesToReadInThisShard>=bytesLeft)
                    {
                        AFileShard aFileShard;
                        aFileShard.filePath=files[filesIndex];
                        aFileShard.offset=currentPosition;
                        aFileShard.end=file_size;
                        shards[shardsIndex].fileSplits.push_back(aFileShard);
                        bytesToReadInThisShard=bytesToReadInThisShard-bytesLeft;
                        filesIndex++;
                        infile.close();
                        infile.clear();
                        if(filesIndex<files.size())
                        {
                            infile.open((files[filesIndex]).c_str(),ifstream::binary);
                            file_size=getSize(files[filesIndex]);
                        }
                    }
                else
                    {
                        infile.seekg(currentPosition+bytesToReadInThisShard);
                        infile.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                        AFileShard aFileShard;
                        aFileShard.filePath=files[filesIndex];
                        aFileShard.offset=currentPosition;
                        aFileShard.end=infile.tellg();
                        shards[shardsIndex].fileSplits.push_back(aFileShard);
                        bytesToReadInThisShard=maxBytes;
                        shardsIndex++;
                    }
            }
    }



/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
    getFileShards(fileShards,mr_spec.inputFiles,mr_spec.mapSize);
	return true;
}
