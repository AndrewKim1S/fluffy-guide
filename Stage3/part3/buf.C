#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
  // advance clock until suitable page is found
  int i = 0;
  while(1) {
    if(i > numBufs) { return BUFFEREXCEEDED; }
    // advance the clock hand
    advanceClock();
    // check if the page is valid
    if(!bufTable[clockHand].valid) { break; }
    // check the reference bit
    if(bufTable[clockHand].refbit) {
      bufTable[clockHand].refbit = false;
      continue;
    }
    // check if the page is pinned
    if(bufTable[clockHand].pinCnt > 0) { 
      i++;
      continue; 
    }
    break;
  }
  // Check if the chosen page is dirty
  BufDesc* evict = &(bufTable[clockHand]);
  if(evict->dirty) { 
    if(flushFile(evict->file) != OK) { return UNIXERR; }
  }
  
  // Remove from hashtable
  hashTable->remove(evict->file, evict->pageNo); 
    
  // Clear
  evict->Clear();

  // Set the frame to the newly acquired frame
  frame = clockHand;

  return OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
  int frame;
  // Page is not in the buffer pool
  if(hashTable->lookup(file, PageNo, frame) == HASHNOTFOUND) {
    // allocate a buffer frame
    Status s = allocBuf(frame);
    if(s != OK) { return s; }

    // read page from disk into the buffer pool frame
    file->readPage(PageNo, &bufPool[frame]);
    
    // insert page into the hashtable
    hashTable->insert(file, PageNo, frame);

    // Set up the frame correctly
    bufTable[frame].Set(file, PageNo);
  } 
  // Page is in the buffer pool
  else {
    // Set the refbit
    bufTable[frame].refbit = true;
    
    // increment the pin count
    bufTable[frame].pinCnt++;
  }
  // return pointer to the frame via page parameter
  page = &bufPool[frame];
  return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, const bool dirty) 
{
  int frameNo;
  if(hashTable->lookup(file, PageNo, frameNo) == HASHNOTFOUND) {
    return HASHNOTFOUND;
  }
  BufDesc* pd = &bufTable[frameNo];
  if(pd->pinCnt > 0) { 
    pd->pinCnt--;
    pd->dirty = (dirty) ? true : pd->dirty;
  } else { return PAGENOTPINNED; }
  return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
  // allocate a page in file
  if(file->allocatePage(pageNo) != OK) { return UNIXERR; }

  // obtain a buffer pool frame
  int frame;
  Status s = allocBuf(frame); // Error??
  if(s != OK) { return s; }

  // insert page into hashtable
  if(hashTable->insert(file, pageNo, frame) != OK) { return HASHTBLERROR; }

  // set the frame
  bufTable[frame].Set(file, pageNo);

  // Set the page
  page = &bufPool[frame];

  return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


