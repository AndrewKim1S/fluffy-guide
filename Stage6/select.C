#include "catalog.h"
#include "query.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;

    // Construct the attr desc array
    AttrDesc attrDescArray[projCnt];
    for(int i = 0; i < projCnt; i++) {
      status = attrCat->getInfo(projNames[i].relName,
                                projNames[i].attrName,
                                attrDescArray[i]);
      if(status != OK) { return status; }
    }

    // Setup attrDesc
    AttrDesc attrDesc;
    AttrDesc *attrDescArg; // - what we pass in
    // when attr is NULL, there is no WHERE condition
    if(attr != NULL) { 
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        /*switch(attr->attrType) {
            case 1:
                int attrValue = atoi (attrValue);
                break;
            case 2:
                attrValue = atof(attrValue);
                break;
        }*/
        if(status != OK) { return status; }
        attrDescArg = &attrDesc;
    } else {
        attrDescArg = NULL;
        memcpy(attrDescArg->relName, projNames[0].relName, MAXNAME);

        attrDescArg->attrType = 0;
        attrDescArg->attrLen = 0;
        attrDescArg->attrOffset = 0;
    }

    // Get the output record length
    int reclen = 0;
    for(int i = 0; i < projCnt; i++) {
        reclen += attrDescArray[i].attrLen;
    }

    // Call ScanSelect for heapfilescan selection
    status = ScanSelect(result, projCnt, attrDescArray, attrDescArg, op, attrValue, reclen);
    if(status != OK) { return status; }
}


const Status ScanSelect(const string & result, 
#include "stdio.h"
#include "stdlib.h"
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;

    // open the result table
    InsertFileScan resultRel(result, status);
    if(status != OK) { return status; }

    // Buffer for output record
    char outputData[reclen];
    Record outputRec;
    outputRec.data = (void*) outputData;
    outputRec.length = reclen;

    // start scan on relation 
    HeapFileScan scan(string(attrDesc->relName), status);
    if(status != OK) { return status; }
    status = scan.startScan(attrDesc->attrOffset, 
                            attrDesc->attrLen,
                            (Datatype) attrDesc->attrType,
                            filter,
                            op);
    if(status != OK) { return status; }

    RID record_RID;
    Record record_rec;
    while(scan.scanNext(record_RID) == OK) {
        status = scan.getRecord(record_rec);
        if(status != OK) { return status; }

        int outputOffset = 0;
        // Add data into output record
        for(int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset,
                (char *)record_rec.data + projNames[i].attrOffset, 
                projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }

        // add record to the output relation
        RID outputRid;
        status = resultRel.insertRecord(outputRec, outputRid);
        if(status != OK) { return status; }
    }
}
