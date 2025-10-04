
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"
#include <sstream>

using namespace std;

// Structure to handle comparisons in the priority queue and other places
struct pQueueComparison {
	MyDB_RecordPtr lhs;  // left hand side pointer
	MyDB_RecordPtr rhs;  // right hand side pointer
	function<bool()> compareRecords;  // Function to compare left and right hand sides

	// Constructor
	pQueueComparison(MyDB_RecordPtr lhsIn,
                 	 MyDB_RecordPtr rhsIn,
                 	 function<bool()> compIn)
    : lhs(lhsIn), rhs(rhsIn), compareRecords(compIn) {}

	bool operator() (const MyDB_RecordIteratorAltPtr &a, const MyDB_RecordIteratorAltPtr &b) {  // Comparison operator. Must not be const().
		a->getCurrent(lhs);  // Loads a's current record into lhs
		b->getCurrent(rhs);  // Loads b's current record into rhs

		// Returns !(a < b) to make the priority queue max-heap behave like a min-heap.
		return !compareRecords();  // Compares the lhs and rhs using the comparison function
	}
};

// Function that generates a comparison between the data in lhs and rhs
// auto compRecs = [](const MyDB_RecordPtr &lhs, const MyDB_RecordPtr &rhs) {
// 		auto function = buildRecordComparator(lhs, rhs, "");
// 		return function(); // Double check later if a proper string is necessary
// };

void mergeIntoFile (MyDB_TableReaderWriter &sortIntoMe, vector <MyDB_RecordIteratorAltPtr> &mergeUs, function <bool ()>comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	// Creates comparator for the priority queue	
	pQueueComparison compare(lhs, rhs, comparator);

	// Creates priority queue for RecordIteratorAltPtrs, using a vector of RecordIteratorAltPtrs, and using an 
	// inputted pQueueComparison as the comparator function
	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, pQueueComparison> pQueue(compare);
	
	// Queues all the record iterators
	for (auto &iter: mergeUs) {
		// Advance each iterator to its first record.
		if (iter->advance()) {
			pQueue.push(iter);
		}
		
	}

	// Runs until mergeUs is empty
	while (pQueue.size() > 0) {
		// Gets the iterator that is pointing to the smallest record.
		MyDB_RecordIteratorAltPtr smallestIter = pQueue.top();
		pQueue.pop();

		// Use lhs as the temporary scratchpad to get the current record
		smallestIter->getCurrent(lhs);

		// Appends the record that just loaded into lhs to the table
		sortIntoMe.append(lhs);

		// Advance the iterator. If it still has records, push it back into the priority queue. 
		if (smallestIter->advance()) {
			pQueue.push(smallestIter);
		}
	}
}

// helper function.  Gets two iterators, leftIter and rightIter.  It is assumed that these are iterators over
// sorted lists of records.  This function then merges all of those records into a list of anonymous pages,
// and returns the list of anonymous pages to the caller.  The resulting list of anonymous pages is sorted.
// Comparisons are performed using comparator, lhs, rhs
vector <MyDB_PageReaderWriter> mergeIntoList (MyDB_BufferManagerPtr parent, MyDB_RecordIteratorAltPtr leftIter,
        MyDB_RecordIteratorAltPtr rightIter, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

			vector<MyDB_PageReaderWriter> returnVector;

			// Get the first anonymous page to start writing to
			MyDB_PageReaderWriter currPage(*parent);
			currPage.clear();
			returnVector.push_back(currPage);

			// Move to first record (priming iterator)
			bool leftHasNext = leftIter->advance();
			bool rightHasNext = rightIter->advance();

			// Merge loop: continue as long as either iterators has records
			while (leftHasNext || rightHasNext) {
				
				// We append the left record if:
				//	1. The left iterator has a record, and
				//	2. Either the right iterator is empty or the left record is smaller
				if (leftHasNext && (!rightHasNext || (leftIter->getCurrent(lhs), rightIter->getCurrent(rhs), comparator()))) { // If comparator() is true, lhs < rhs
					
					leftIter->getCurrent(lhs);

					// Append lhs and advance left iterator.
					if (!currPage.append(lhs)) {
						// Could not append to currPage. Page is full. Get a new one. Append to returnVector.
						currPage = MyDB_PageReaderWriter(*parent);
						currPage.clear();
						currPage.append(lhs);
						returnVector.push_back(currPage);
					}
					leftHasNext = leftIter->advance(); // move to next leftIter record
				} else { //Otherwise append the right record

					rightIter->getCurrent(rhs);

					// Append rhs and advance the right iterator. 
					if (!currPage.append(rhs)) {
						// Could not append to currPage. Page is full. Get a new one. Append to returnVector.
						currPage = MyDB_PageReaderWriter(*parent);
						currPage.clear();
						currPage.append(rhs);
						returnVector.push_back(currPage);
					}
					rightHasNext = rightIter->advance(); // move to next rightIter record
				}

			}

			return returnVector;
		}
	

// performs a TPMMS of the table sortMe.  The results are written to sortIntoMe.  The run 
// size for the first phase of the TPMMS is given by runSize.  Comparisons are performed 
// using comparator, lhs, rhs
void sort (int runSize, MyDB_TableReaderWriter &sortMe, MyDB_TableReaderWriter &sortIntoMe, function <bool ()> comparator, MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {
	
	/* PHASE 1: GENERATE ALL SORTED RUNS */

	int numPages = sortMe.getNumPages();
	vector<MyDB_TableReaderWriterPtr> tempRuns; // Stores temporary run tables

	for (int i = 0; i < numPages; i+= runSize) {
		
		// 1.1 Create a new temporary table for this run. 
		string runName = "temp_run_" + to_string(i / runSize);
		MyDB_TablePtr runTablePtr = make_shared<MyDB_Table>(runName, runName + ".bin", sortMe.getTable()->getSchema());
		MyDB_TableReaderWriterPtr runTableSortIntoMe = make_shared<MyDB_TableReaderWriter>(runTablePtr, sortMe.getBufferMgr());
		tempRuns.push_back(runTableSortIntoMe);

		// 1.2 Perform in-memory sort for the current run of pages

		// Vector to hold list of sorted pages. Initially, each list is just one page.
		vector<vector<MyDB_PageReaderWriter>> inMemoryRunPages;

		// Determine the total number of pages for this run. Handles the last, smaller run of pages.
		int pagesInThisRun = min(runSize, numPages - i);

		// 1.2.1.  Load a run of pages into RAM
		for (int j = 0; j < pagesInThisRun; j++) {
			// Get page
			MyDB_PageReaderWriter currPage(sortMe[i + j]);

			// Sort the records in the page in RAM
			currPage.sortInPlace(comparator, lhs, rhs);

			// Add this single, sorted page as a new run.
			inMemoryRunPages.push_back({currPage});

		}

		// 1.2.3. Merging runs in memory
		while (inMemoryRunPages.size() > 1) {
			vector<vector<MyDB_PageReaderWriter>> nextDepthRuns;
			// Iterate through the current runs, merging adjacent pairs
			for (size_t j = 0; j < inMemoryRunPages.size(); j += 2) {
				// If there's an odd run out at the end, move it to the next level. Checks if runPages[i+1] exists
				if (j + 1 >= inMemoryRunPages.size()) {
					nextDepthRuns.push_back(inMemoryRunPages[j]);
				} else {
					vector<MyDB_PageReaderWriter> mergedRun = mergeIntoList(sortMe.getBufferMgr(), inMemoryRunPages[j][0].getIteratorAlt(), inMemoryRunPages[j+1][0].getIteratorAlt(), comparator, lhs, rhs);
					nextDepthRuns.push_back(mergedRun);
				}
			}

			// The next depth level becomes the current depth level for the next iteration.
			inMemoryRunPages = nextDepthRuns;
		}


		// 1.3. Write the sorted in-memory tun to its temporary table. 

		if (!inMemoryRunPages.empty()) {

			// inMemoryRunPages now contains a single element, vector<MyDB_PageReaderWriter> that represents
			// the completed, sorted run of pagesInThisRun pages.
			vector<MyDB_PageReaderWriter> &lastRun = inMemoryRunPages[0]; //reference instead of object-by-object copy

			// 3.  Write the sorted run to SortIntoMe
			for (MyDB_PageReaderWriter page: lastRun) {
				
				// Get an iterator for the current page.
				MyDB_RecordIteratorPtr pageIter = page.getIterator(lhs); // could also be rhs: lhs/rhs are MyDB_RecordPtr scratchpads
				
				// Append all records from this page to the output table
				while (pageIter->hasNext()) {
					pageIter->getNext();
					runTableSortIntoMe->append(lhs);
				}
			}
		}

	}
	
	/* Phase 2: Merge all runs into the final output */
	
	// Get iterators for all the temporary run tables created
	vector<MyDB_RecordIteratorAltPtr> runIterators;
	for (auto &run: tempRuns) {
		runIterators.push_back(run->getIteratorAlt());
	}

	// Call mergeIntoFile to perform the final merge all temp files into sortIntoMe
	mergeIntoFile(sortIntoMe, runIterators, comparator, lhs, rhs);

	// Clean up temp files
	for (auto &run: tempRuns) {
		remove(run->getTable()->getStorageLoc().c_str());
	}
	
	

	
}

#endif
