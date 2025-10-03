
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
	

// performs a TPMMS of the table sortMe.  The results are written to sortIntoMe.  The run 
// size for the first phase of the TPMMS is given by runSize.  Comparisons are performed 
// using comparator, lhs, rhs
void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// Vector to hold list of sorted pages. Initially, each list is just one page.
	vector<vector<MyDB_PageReaderWriter>> runPages;
	
	// 1. Load a run of pages into RAM
	for (int i = 0; i < runSize; i++) {
		// Get page
		MyDB_PageReaderWriter currPage(sortMe[i]);

		// Sort the records in the page
		currPage.sort(comparator, lhs, rhs);

		// Add this single, sorted page as a new run.
		runPages.push_back({currPage});
	}

	// 2. Merging runs in memory
	while (runPages.size() > 1) {
		vector<vector<MyDB_PageReaderWriter>> nextDepthRuns;
		// Iterate through the current runs, merging adjacent pairs
		for (size_t i = 0; i < runSize; i += 2) {
			// If there's an odd run out at the end, move it to the next level. Checks if runPages[i+1] exists
			if (i + 1 >= runPages.size()) {
				nextDepthRuns.push_back(runPages[i]);
			} else {
				vector<MyDB_PageReaderWriter> mergedRun = mergeIntoList(sortMe.getBufferMgr(), runPages[i][0].getIteratorAlt(), runPages[i+1][0].getIteratorAlt(), comparator, lhs, rhs);
				nextDepthRuns.push_back(mergedRun);
			}
		}

		// The next depth level becomes the current depth level for the next iteration.
		runPages = nextDepthRuns;
	}

	// runPages now contains a single element, vector<MyDB_PageReaderWriter> thar represents
	// the completed, sorted run of runSize pages.
	vector<MyDB_PageReaderWriter> &lastRun = runPages[0]; //reference instead of object-by-object copy

	// 3.  Write the sorted run to SortIntoMe
	for (MyDB_PageReaderWriter page: lastRun) {
		
		// Get an iterator for the current page.
		MyDB_RecordIteratorPtr pageIter = page.getIterator(lhs); // could also be rhs: lhs/rhs are MyDB_RecordPtr scratchpads
		
		// Append all records from this page to the output table
		while (pageIter->hasNext()) {
			pageIter->getNext();
			sortIntoMe.append(lhs);
		}
	}
}

#endif
