
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"
#include <sstream>

using namespace std;

struct pQueueComparison {
	MyDB_RecordPtr lhs;
	MyDB_RecordPtr rhs;
	function<bool(const MyDB_RecordPtr&, const MyDB_RecordPtr&)> compareRecords;

	pQueueComparison(MyDB_RecordPtr lhsIn,
                 MyDB_RecordPtr rhsIn,
                 std::function<bool(const MyDB_RecordPtr&, const MyDB_RecordPtr&)> compIn)
    : lhs(lhsIn), rhs(rhsIn), compareRecords(std::move(compIn)) {}

	bool operator() (const MyDB_RecordIteratorAltPtr &a, const MyDB_RecordIteratorAltPtr &b) const {
		a->getCurrent(lhs);
		b->getCurrent(rhs);

		return !compareRecords(lhs,rhs);
	}
};

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs, function <bool ()>comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	auto compRecs = [](const MyDB_RecordPtr &lhs, const MyDB_RecordPtr &rhs) {
		auto function = buildRecordComparator(lhs, rhs, "");
		return function(); // Double check later if a proper string is necessary
	};

	pQueueComparison compare(lhs, rhs, compRecs);

	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, pQueueComparison> pQueue(compare);
	
	// Queues all the record iterators
	for (int i = 0; i < mergeUs.size(); i++) {
		pQueue.push(mergeUs.at(i));
	}

	// Runs until mergeUs is empty
	while (pQueue.size() > 0) {
		MyDB_RecordIteratorAltPtr temp = pQueue.top();
		pQueue.pop();
		MyDB_RecordPtr tempPtr;
		temp->getCurrent(tempPtr);
		// TODO: write tempPtr to file
		sortIntoMe.append(tempPtr);
		if (temp->advance()) {
			pQueue.push(temp);
		}
	}
}

vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr, MyDB_RecordIteratorAltPtr, MyDB_RecordIteratorAltPtr, function <bool ()>, 
	MyDB_RecordPtr, MyDB_RecordPtr) {return vector <MyDB_PageReaderWriter> (); } 
	
void sort (int, MyDB_TableReaderWriter &, MyDB_TableReaderWriter &, function <bool ()>, MyDB_RecordPtr, MyDB_RecordPtr) {} 

#endif
