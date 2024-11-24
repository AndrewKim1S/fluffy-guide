#include "heapfile.h"
#include "error.h"
#include "page.h"
#include "db.h"
#include <cstring>
#include <stdio.h>
#include <string.h>

// routine to create a heapfile
const Status createHeapFile(const string fileName) {
    File* file;
    Status status;
    FileHdrPage* hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page* newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK) {
        // file doesn't exist. First create it and allocate
        // an empty header page and data page.

        status = db.createFile(fileName);
        if (status != OK) {
            return status;
        }
        status = db.openFile(fileName, file);
        if (status != OK) {
            return status;
        }

        Page* page;
        status = bufMgr->allocPage(file, hdrPageNo, page);
        if (status != OK) {
            return status;
        }
        hdrPage = (FileHdrPage*)page;

        status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK) {
            return status;
        }

        // initialize the values in the header page
        std::strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;

        // initialize the new page contents
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1);

        // unpin both pages and mark them as dirty
        status = bufMgr->unPinPage(file, newPageNo, true);
        if (status != OK) {
            return status;
        }

        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if (status != OK) {
            return status;
        }
        status = db.closeFile(file);
        if(status != OK) { return status; }

        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName) {
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string& fileName, Status& returnStatus) {
    Status status;
    Page* pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK) {
        // header page
        status = filePtr->getFirstPage(headerPageNo);
        if (status != OK) {
            cerr << "error getting first page of file\n";
            returnStatus = status;
            return;
        }
        Page* page;
        status = bufMgr->readPage(filePtr, headerPageNo, page);
        if (status != OK) {
            cerr << "error reading header page of file\n";
            returnStatus = status;
            return;
        }
        headerPage = (FileHdrPage*)page;
        hdrDirtyFlag = false;

        // first page of heap file
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) {
            cerr << "error reading first data page of file\n";
            returnStatus = status;
            return;
        }
        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = status;
    } else {
        cerr << "open of heap file failed\n";
        returnStatus = status;
        return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile() {
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName
         << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK) cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file
    // are flushed to disk if (status != OK) cerr << "error in flushFile
    // call\n"; before close the file
    status = db.closeFile(filePtr);
    if (status != OK) {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const { return headerPage->recCnt; }

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID& rid, Record& rec) {
    Status status;

    if (rid.pageNo == curPageNo && curPage != NULL) {
        status = curPage->getRecord(rid, rec);
        if(status != OK) { return status; }
    }

    else {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if(status != OK) { return status; }

        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if(status != OK) { return status; }

        // Bookkeeping stuff
        status = curPage->getRecord(rid, rec);
        if(status != OK) { return status; }
        curDirtyFlag = false;
    }
    curPageNo = rid.pageNo;
    curRec = rid;

    return OK;
}

HeapFileScan::HeapFileScan(const string& name, Status& status)
    : HeapFile(name, status) {
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_, const int length_,
                                     const Datatype type_, const char* filter_,
                                     const Operator op_) {
    if (!filter_) {  // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) ||
         type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT &&
         op_ != NE)) {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan() {
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan() { endScan(); }

const Status HeapFileScan::markScan() {
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan() {
    Status status;
    if (markedPageNo != curPageNo) {
        if (curPage != NULL) {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK) return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) return status;
        curDirtyFlag = false;  // it will be clean
    } else
        curRec = markedRec;
    return OK;
}

const Status HeapFileScan::scanNext(RID& outRid) {
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    Record rec;

    // curPage is null then go from start
    // This will pin a page, a set curRec to the first element 
    if(curPage == NULL) {
        // get curPage to first page
        curPageNo = headerPage->firstPage;
        if(curPageNo == -1) { return FILEEOF; }

        // read page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if(status != OK) { return status; }

        // Get the first record of the start
        status = curPage->firstRecord(curRec);
        status = curPage->getRecord(curRec, rec);
        if(status != OK) { return status; }
        if(matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }

    // when a page is pinned in buffer
    while (true) {
        // Get the next record
        status = curPage->nextRecord(curRec, nextRid);
        
        // if there is no record read in next page
        if (status == ENDOFPAGE || status == NORECORDS) {
            
            // failed debugging
            //if(headerPage->lastPage == curPageNo) { return FILEEOF; }

            // Get the next page
            int nextPageNo;
            status = curPage->getNextPage(nextPageNo);
            
            // We have reached end of file
            if(nextPageNo == -1) { return FILEEOF; }

            // unpin the old page 
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if(status != OK) { return status; }

            // Read in new page
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if(status != OK) { return status; }

            //if(curPage->slotCnt == 0) { return FILEEOF; }

            // Start at the first record of new page
            status = curPage->firstRecord(curRec);
            if(status != OK) { return FILEEOF; }
        } 
        // else there is record, set curRec to nextRid
        else { curRec = nextRid; }

        status = curPage->getRecord(curRec, rec);
        if(status != OK) { return status; }
        if(matchRec(rec)) {
            outRid = curRec;
            return OK;
        }
    }
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record& rec) {
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord() {
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty() {
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record& rec) const {
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length) return false;

    float diff = 0;  // < 0 if attr < fltr
    switch (type) {
        case INTEGER:
            int iattr, ifltr;  // word-alignment problem possible
            memcpy(&iattr, (char*)rec.data + offset, length);
            memcpy(&ifltr, filter, length);
            diff = iattr - ifltr;
            break;

        case FLOAT:
            float fattr, ffltr;  // word-alignment problem possible
            memcpy(&fattr, (char*)rec.data + offset, length);
            memcpy(&ffltr, filter, length);
            diff = fattr - ffltr;
            break;

        case STRING:
            diff = strncmp((char*)rec.data + offset, filter, length);
            break;
    }

    switch (op) {
        case LT:
            if (diff < 0.0) return true;
            break;
        case LTE:
            if (diff <= 0.0) return true;
            break;
        case EQ:
            if (diff == 0.0) return true;
            break;
        case GTE:
            if (diff >= 0.0) return true;
            break;
        case GT:
            if (diff > 0.0) return true;
            break;
        case NE:
            if (diff != 0.0) return true;
            break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string& name, Status& status)
    : HeapFile(name, status) {
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan() {
    Status status;
    // unpin last page of the scan
    if (curPage != NULL) {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record& rec, RID& outRid) {
    Page* newPage;
    int newPageNo;
    Status status;  //, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED) {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    if(curPage == NULL) {
      curPageNo = headerPage->lastPage;
      status = bufMgr->readPage(filePtr, curPageNo, curPage);
      if(status != OK) { return status; }
    }

    // check if curPage is too small
    status = curPage->insertRecord(rec, rid);

    // current page was full
    if (status == NOSPACE) {

        // allocate a new page
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) { return status; }

        // initialize the new page
        newPage->init(newPageNo);
        status = newPage->setNextPage(-1);
        if (status != OK) { return status; }
       
        // modify header page
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // link new page by setting forward pointer 
        status = curPage->setNextPage(newPageNo);
        if (status != OK) { return status; }

        // unpin the current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) { return status; }

        // Set curpage to newPage
        curPage = newPage;
        curPageNo = newPageNo;

        // insert the record into the new page
        status = curPage->insertRecord(rec, rid);
        if (status != OK) { return status; }
    }

    // Bookkeeping
    headerPage->recCnt++;
    hdrDirtyFlag = true; 
    curDirtyFlag = true;
    outRid = rid;

    return OK;
}
