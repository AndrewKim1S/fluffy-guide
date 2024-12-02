#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
    Status status;

    // Get the total number of attributes
    int attrCntParameter = 0;
    AttrDesc *attrDescArray;
    status = attrCat->getRelInfo(relation, attrCntParameter, attrDescArray);
    if(status != OK) { return status; }

    // Check if the attributes number are equal - minirel rejects NULLs
    if(attrCntParameter != attrCnt) { return ATTRNOTFOUND; }

    // Find reclen
    int reclen = 0;
    for(int i = 0; i < attrCnt; i++) {
        reclen += attrDescArray[i].attrLen;
    }

    // Create record to insert
    Record outputRec;
    char *outputData[reclen];
    outputRec.data = (void*) outputData;
    outputRec.length = reclen;

    // tmp vars for type issues
    int int_tmp;
    float float_tmp;

    // n^2 search to match attrList arguments to attrDescArray given by RelInfo - (sort them)
    int outputOffset = 0;
    for(int i = 0; i < attrCnt; i++) {
        bool found = false;
        for(int j = 0; j < attrCnt; j++) {
            
            // attributes match
            if(strcmp(attrDescArray[i].attrName, attrList[j].attrName) == 0) {
                found = true;
                outputOffset = attrDescArray[i].attrOffset;

                // Check the the type cause else it cries and segfaults
                // attrList len is -1??
                switch(attrDescArray[i].attrType) {
                    // copy char* to data
                    // TODO There is an error in how I am copying the string 
                    case 0:
                        // attrList[j].attrValue is a void ptr so cast to char*
                        memcpy((char*)(outputData) + outputOffset, (char*)attrList[j].attrValue, attrDescArray[i].attrLen);
                        break;
                    // copy integer to data
                    case 1:
                        int_tmp = atoi((char*)attrList[j].attrValue);
                        memcpy((char*)(outputData) + outputOffset, (char*)&int_tmp, attrDescArray[i].attrLen);
                        break;
                    // copy float to data
                    case 2:
                        float_tmp = atof((char*)attrList[j].attrValue);
                        memcpy((char*)(outputData) + outputOffset, (char*)&float_tmp, attrDescArray[i].attrLen);
                        break;
                }
                break;
            }
        }
        if(!found) { return ATTRNOTFOUND; } 
    }

    // open the result table 
    InsertFileScan resultTable(relation, status);
    if(status != OK) { return status; }

    // insert record to table
    RID outputRid;
    status = resultTable.insertRecord(outputRec, outputRid);
    if(status != OK) { return status; }

    return OK;
}

