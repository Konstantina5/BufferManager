/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb {

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/*
 Write all the modified pages to disk and free the memory
 allocated for buf description and the hashtable.
*/
BufMgr::~BufMgr() {
  for(FrameId i =0; i<numBufs;i++){
    if(bufDescTable[i].dirty == true)
      bufDescTable[i].file->writePage(bufPool[i]);
    }
  free(bufDescTable);
  delete[] bufPool;
  free(hashTable);
}

/*
 Increment the clockhand within the circular buffer pool .
*/
void BufMgr::advanceClock() {
    clockHand++;
    clockHand=clockHand%numBufs;
}

/*
 This function allocates a new frame in the buffer pool
 for the page to be read. The method used to allocate
 a new frame is the clock algorithm.
*/
void BufMgr::allocBuf(FrameId & frame) {



   std::uint32_t buffsChecked=0;


   while(buffsChecked<=2*numBufs)
   {
        advanceClock();
        if(bufDescTable[clockHand].valid==true)
        {
            if(bufDescTable[clockHand].refbit==false)
            {
                if(bufDescTable[clockHand].pinCnt==0)
                {
                    if(bufDescTable[clockHand].dirty==true){

                        //writes the page to the file

                        bufDescTable[clockHand].file->writePage(bufPool[clockHand]);

                        bufDescTable[clockHand].dirty=false;

                        //removes the page from the hash table and makes the returned frame equal to that one from the clockHand
                        hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);

                        frame=bufDescTable[clockHand].frameNo;
                        break;

                    }else{
                        //if the page is not dirty it delets it from the hash table and makes the return frame equal to that one from the clockHand
                        hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);

                        frame=bufDescTable[clockHand].frameNo;
                        break;
                    }


                }
            }
            else{
             //if the reference bit is true it sets it false
                bufDescTable[clockHand].refbit=false;
            }
        }else{

            //hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
            //bufDescTable[clockHand].Clear();
            //if the page is not valid makes the returned frame equal to that one from the clockHand
            frame=bufDescTable[clockHand].frameNo;
            break;
        }
       // std::cout<<"Current buffsChecked: "<<buffsChecked<<std::endl;
        buffsChecked++;
    }

    //if the buffer frames checked are larger than the number of frames in the buffer pool then
    //throws a buffer exceeded exception
    if(buffsChecked>2*numBufs){
        throw BufferExceededException();
    }

    //clears the returned frame in the bufDescTabel so it canbe used
    bufDescTable[clockHand].Clear();

}


/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page.
*/

void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {
    FrameId frameID;
    // looks for the page in the hashtable, if it exists it sets the page reference bit and
    //increases the pinCnt and also makes the variable page equals to the page thats in the
    //specific frame in the buffer
    try{

        hashTable->lookup(file,pageNo,frameID);
       // bufDescTable[frameID].refbit=true;
        bufDescTable[frameID].pinCnt++;
        page=&bufPool[frameID];
    }
    // if the page doesnt exist in the buffer pool, it reads the page from the file, allocates a frame
    //in the buffer and sets the specif frame in the buffer pool equals to the page that read from the file
    //also inserts the page in the hash table and sets the bufDescTable for the spesific frame
    catch (HashNotFoundException er){
        

        allocBuf(frameID);
        bufPool[frameID]=file->readPage(pageNo);
        page=&bufPool[frameID];
        hashTable->insert(file,pageNo,frameID);
        bufDescTable[frameID].Set(file,pageNo);
    }

}

/*
 This function decrements the pincount for a page from the buffer pool.
 Checks if the page is modified, then sets the dirty bit to true.
 If the page is already unpinned throws a PageNotPinned exception.
*/
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) {
    FrameId frame;
    //looks for the given page in the hash table
    hashTable->lookup(file,pageNo,frame);
    //if the page is already unpinned then throws a page not pinned exception
    if (bufDescTable[frame].pinCnt==0){
        throw PageNotPinnedException(file->filename(),pageNo,frame);
    }
    //else if the page is pinned, increases the pinCnt of the page in the bufDescTable
    //using the frame that found from the hash table and it sets the dirty bit if the given variable dirty is true
    //if the page does not exist in the hash table it throws a hash not found exception
    try {
        bufDescTable[frame].pinCnt=bufDescTable[frame].pinCnt-1;
        if(dirty==true)
        {
            bufDescTable[frame].dirty=true;
        }
    }catch (HashNotFoundException e){
        throw HashNotFoundException(file->filename(),pageNo);
    }

}

/*
 Checks for all the pages which belong to the file in the buffer pool.
 If the page is modified, then writes the file to disk and clears it
 from the Buffer manager. Else, if its being referenced by other
 services, then throws a pagePinnedException.
 Else if the frame is not valid then throws a BadBufferException.
*/
void BufMgr::flushFile(const File* file){
        //for every frame in the buffer
        for(FrameId i=0;i<numBufs;i++)
        {
            //if the file of the page that is stored in i spot of the buffer equals to the given file
            //and the page is dirty
            //then writes that page to corresponding the file, removes that page from the hashtable and clears the bufDescTable that is was stored
            if (bufDescTable[i].file==file){
                    //if the page is pinned it throws a pin exception
                if (bufDescTable[i].pinCnt>0){
                    throw PagePinnedException(file->filename(),bufDescTable[i].pageNo,bufDescTable[i].frameNo);
                }
                 //if the page is not valid it throws a bad buffer exception
                if (!bufDescTable[i].valid){
                    throw BadBufferException(i,bufDescTable[i].dirty,bufDescTable[i].valid,bufDescTable[i].refbit);
                }
                if(bufDescTable[i].dirty==true)
                {
                    bufDescTable[i].file->writePage(bufPool[i]);
                    hashTable->remove(file,bufDescTable[i].pageNo);
                    bufDescTable[i].Clear();
                }
            }
        }
}

/*
 This function allocates a new page and reads it into the buffer pool.
*/
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) {

	FrameId frameid;
	//allocates a frame of the buffer for the page to be stored
	//if it cant find any it throws a buffer exceeded exception
	try{
        allocBuf(frameid);
	}catch (BufferExceededException e){
        throw BufferExceededException();
	}


    //page=file->allocatePage();
    //allocates the page from the file that is stored and puts it in the specific frame to the buffer pool
	bufPool[frameid]=file->allocatePage();

	//makes the variable page equals to the page that was allocated earlier from the file
       page=&bufPool[frameid];
    //makes the variable pageNo equals to the page number that was allocated earlier from the file
	pageNo=page->page_number();

	//inserts the page in the hash table and if it cants throws a hash not found exception

	try{
        hashTable->insert(file,pageNo,frameid);
	}catch (HashNotFoundException e){
        throw HashNotFoundException(file->filename(),pageNo);
	}

	//also sets the corresponding frame in the bubDescTable with the specific file and page
    bufDescTable[frameid].Set(file,pageNo);

}

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
void BufMgr::disposePage(File* file, const PageId PageNo) {
    FrameId frameid;
    //it looks for the page in the hash table, if it exists it return the frame that is stored
    //else it goes to the HashNotFoundException and then deletes the page from the file in both cases
    try{

        hashTable->lookup(file,PageNo,frameid);
        //deletes the page from the hashtable
        hashTable->remove(file,PageNo);
        //it clears the frame that the deleted page was stored
        bufDescTable[frameid].Clear();
    }
    catch(HashNotFoundException e){
        //throw HashNotFoundException(file->filename(),PageNo);
    }
    //deletes the page from the corresponding file
    file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
  BufDesc* tmpbuf;
	int validFrames = 0;

  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
