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

	//variable that shows us if the buffer is full or has empty position
    FrameId filledFrames=0;
    //access of every position of the buffer
    for(FrameId i=0; i<numBufs;i++)
    {
        //if the position is filled increases the filledFrames variable and increases the page's counter
        if(bufDescTable[i].valid==true)
        {
            bufDescTable[i].counter++;
            filledFrames++;
        }
    }

    //variable that shows us the counter of the least recently used page in the buffer
    int max=-1;
    //contains the clockHand of the least recently used page
    FrameId maxFrame=0;
    //variable that will be used to access the buffer when it is filled
    FrameId bufCount=0;

    //checking whether the buffer is filled or not
    if (filledFrames<numBufs){
        //if the buffer has empty positions it accesses every buffer position
        //until it find the first empty position and assigns it to the frame variable

        //increase filledFrames because we will fill another buffer position
        filledFrames++;
        //variable that will show us the first empty position
        FrameId z=0;
        bool flag=false;
        //accessing all of the positions to find the first one, if it does not find any
        //increases the z variable to search further into the buffer
        while (!flag){
            if (bufDescTable[z].valid==false){
                frame=z;
                flag=true;
            }
            else
                z++;
        }
    }
    else {
        //if the buffer does not have any empty positions we use the LRU algorithm to
        //find a possible candidate

        //access of all the buffer positions
        while ((bufCount<2*numBufs)&&(max==-1))
        {
            //using the advanceClock to go to the next buffer frame
            advanceClock();

            //checking the requirements to find a possible candidate for replacement
            if(bufDescTable[clockHand].valid==true)
            {
                if(bufDescTable[clockHand].refbit==false)
                {
                    if(bufDescTable[clockHand].pinCnt==0)
                    {
                        //if the page is modified it is written in the disk
                        if(bufDescTable[clockHand].dirty==true){

                            bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
                            bufDescTable[clockHand].dirty=false;

                            //if the page currently being checked has not been used more
                            //recently than the current max page it replaces it as the
                            //least recently used page
                            if (bufDescTable[clockHand].counter>max){
                                max=bufDescTable[clockHand].counter;
                                maxFrame=clockHand;
                            }
                        }else{
                            //same as before
                            if (bufDescTable[clockHand].counter>max){
                                max=bufDescTable[clockHand].counter;
                                maxFrame=clockHand;
                            }
                        }
                    }
                }
                else{
                    //changes the refbit from true to false
                    bufDescTable[clockHand].refbit=false;
                }
            }

            //increase the bufCount after the current page has been checked
            bufCount++;

            //if no page for replacement has been found a BufferExceededException is thrown
            if (bufCount==2*numBufs && max==-1){
               throw BufferExceededException();
            }
        }

        //in case a page has been found its frame is assigned to the frame variable
        //and it is removed from the hash table and its bufDescTable position is cleared

        frame=maxFrame;
        hashTable->remove(bufDescTable[maxFrame].file,bufDescTable[maxFrame].pageNo);
        bufDescTable[maxFrame].Clear();

   }

}

/*
 This function reads a page of a file from the buffer pool
 if it exists. Else, fetches the page from disk, allocates
 a frame in the bufpool by calling allocBuf function and
 returns the Page.
*/
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page) {

	FrameId frameID;
    // looks for the page in the hash table, if it exists, sets the page reference bit and
    //increases the pinCnt and also makes the variable page equal to the page that's in the
    //specific frame in the buffer
    try{
        hashTable->lookup(file,pageNo,frameID);

        bufDescTable[frameID].refbit=true;
        bufDescTable[frameID].pinCnt++;
        page=&bufPool[frameID];

    }

 // if the page doesn't exist in the buffer pool, it reads the page from the file, allocates a frame
    //in the buffer and sets the specific frame in the buffer pool equal to the page that was read from the file
    //also inserts the page in the hash table and sets the bufDescTable for the specific frame
    catch (HashNotFoundException er){

        allocBuf(frameID);

        bufPool[frameID]=file->readPage(pageNo);
        page=&bufPool[frameID];
        hashTable->insert(file,pageNo,frameID);
        bufDescTable[frameID].Set(file,pageNo);

    }
    //increases the counter for every page in the buffer
    for(FrameId i=0; i<numBufs;i++)
    {
       if(bufDescTable[i].valid==true)
       {
           bufDescTable[i].counter++;
       }
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
    //if the page is already unpinned then throws a PageNotPinned exception
    if (bufDescTable[frame].pinCnt==0){
        throw PageNotPinnedException(file->filename(),pageNo,frame);
    }
    //else if the page is pinned, decreases the pinCnt of the page in the bufDescTable
    //using the frame that found in the hash table and sets the dirty bit if the given variable dirty is true
    //if the page does not exist in the hash table it throws a HashNotFoundException
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
void BufMgr::flushFile(const File* file) {

	//for every frame in the buffer
        for(FrameId i=0;i<numBufs;i++)
        {
            //if the file of the page that is stored in i spot of the buffer equals to the given file
            //and the page is dirty
            //then writes that page to the corresponding file, removes that page from the hash table
			//and clears the spot of the bufDescTable that it was stored
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
	//if it can't find any it throws a buffer exceeded exception
	try{
        allocBuf(frameid);
	}catch (BufferExceededException e){
        throw BufferExceededException();
	}

    //allocates the page from the file that is stored and puts it in the specific frame to the buffer pool
	bufPool[frameid]=file->allocatePage();

	//makes the variable page equal to the page that was allocated earlier from the file
    page=&bufPool[frameid];
    //makes the variable pageNo equal to the page number that was allocated earlier from the file
	pageNo=page->page_number();

	//inserts the page in the hash table and if it can't throws a hash not found exception
	try{
        hashTable->insert(file,pageNo,frameid);
	}catch (HashNotFoundException e){
        throw HashNotFoundException(file->filename(),pageNo);
	}

	//also sets the corresponding frame in the bufDescTable with the specific file and page
    bufDescTable[frameid].Set(file,pageNo);
}

/* This function is used for disposing a page from the buffer pool
   and deleting it from the corresponding file
*/
void BufMgr::disposePage(File* file, const PageId PageNo) {

	FrameId frameid;
    //it looks for the page in the hash table, if it exists it returns the frame that is stored inside
    //else it goes to the HashNotFoundException and then deletes the page from the file in both cases
    try{
        hashTable->lookup(file,PageNo,frameid);
        //deletes the page from the hash table
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
