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

using std::ofstream;
using std::map;


typedef struct MapFileOutPutStruct{
    std::string fileName;
    int hashId;
}MapFileOutPut;


int hashCode (const std::string key,int sizeOfTable)
    {
       unsigned long hash = 0;
       for(int i = 0; i < key.length(); i++)
       {
          hash = (hash * 131) + key[i];
       }
       return hash % sizeOfTable;
    }


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

        ofstream* getStream(const std::string key)
            {
              int hashId=hashCode(key,sizeOfHashTable);
              map<int,ofstream*>::iterator it = streamsToWrite.find(hashId);
              ofstream* stream;
              if(it != streamsToWrite.end())
                  {
                     //element found;
                     stream = it->second;
                     *stream<<'\n';
                     flushPos++;
                     //FLush After a numbe rof lines
                     if(flushPos==flushThreshold)
                         {
                            stream->flush();
                            flushPos=0;
                         }
                  }
              else
                  {
                     std::ostringstream oss;
                     oss << filePathPrefix << '_'<<hashId;
                     std::string newFileName=oss.str();
                     stream= new  ofstream(newFileName, std::ios::binary);
                     streamsToWrite[hashId]=stream;
                     MapFileOutPut mapFileOutPut;
                     mapFileOutPut.fileName=newFileName;
                     mapFileOutPut.hashId=hashId;
                     results.push_back(mapFileOutPut);
                  }
              return stream;
            }
         void  init(int limit,std::string filePathAppender, int flushLimit)
             {
                sizeOfHashTable=limit;
                filePathPrefix=filePathAppender;
                flushThreshold=flushLimit;
             }
        std::vector<MapFileOutPut> handleCompletion(){
            int i;
            for(i=0;i<results.size();i++)
            {
                (streamsToWrite[results[i].hashId])->flush();
                (streamsToWrite[results[i].hashId])->close();
                delete((streamsToWrite[results[i].hashId]));
            }
            return results;
        }

		private:
		std::vector<MapFileOutPut> results;
		std::map<int,ofstream*> streamsToWrite;
		int sizeOfHashTable;
		std::string filePathPrefix;
		int flushThreshold;
		int flushPos=0;

};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
//Do Nothing
}



/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
    *getStream(key)<<key<<','<<val;
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

        void  init(std::string filePath, int flushLimit){
            finalFilePath=filePath;
            flushThreshold=flushLimit;
            stream.open(finalFilePath, std::ios::binary);
        }

        std::string  handleCompletion(){
            stream.flush();
            stream.close();
            return finalFilePath;
        }

        private:
		std::string finalFilePath;
		int flushThreshold;
        int flushPos=0;
        ofstream stream;

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {

}



/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
    if(stream.tellp()>0)
        {
          stream<<'\n';
        }
    stream<<key<<','<<val;
    flushPos++;
    if(flushPos==flushThreshold)
    {
       stream.flush();
       flushPos=0;
    }

}
