#include "catalog.h"
#include "query.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
    Status status;

    AttrDesc attrdesc_obj;

    // Check if the attrName is null. Then no where so everything goes
    if(!attrName.empty()) {
        status = attrCat->getInfo(relation, attrName, attrdesc_obj);
        if(status != OK) { return status; }
    }

    int tmp_i;
    int tmp_f;

    // Start scan on relation
    HeapFileScan scan(relation, status);
    if(status != OK) { return status; }

    if(attrName.empty()) {
        status = scan.startScan(0, 0, type, NULL, op);
        if(status != OK) { return status; }
    } else {
        switch(type) {
            case STRING:  
                // Lol this only works for string??
                status = scan.startScan(attrdesc_obj.attrOffset, attrdesc_obj.attrLen, type, attrValue, op);
                if(status != OK) { return status; }
                break;
            case INTEGER:
                tmp_i = atoi(attrValue);
                status = scan.startScan(attrdesc_obj.attrOffset, attrdesc_obj.attrLen, type, (char*)&tmp_i, op);
                if(status != OK) { return status; }
                break;
            case FLOAT:
                tmp_f = atof(attrValue);
                status = scan.startScan(attrdesc_obj.attrOffset, attrdesc_obj.attrLen, type, (char*)&tmp_f, op);
                if(status != OK) { return status; }
                break;
        }
    }

    // Scan next through the relation
    RID record_RID;
    while(scan.scanNext(record_RID) == OK) {
        // Remove the record
        status = scan.deleteRecord();
        if(status != OK) { return status; }
        scan.markDirty();
        if(status != OK) { return status; }
    }

    return OK;
}


