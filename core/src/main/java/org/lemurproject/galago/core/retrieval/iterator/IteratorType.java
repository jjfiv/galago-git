package org.lemurproject.galago.core.retrieval.iterator;

/**
 * Start encoding Galago's retrieval type system a little better.
 * @author jfoley.
 */
enum IteratorType {
	INDICATOR, // Has a boolean for every document.

	COUNT,  // Has an integer for every document
	        // all COUNT iterators are also INDICATOR iterators

	EXTENT, // Has an ExtentArray for every document
	        // all EXTENT iterators are also COUNT iterators
	SCORE   // Has a double score for every document, and a maxScore and minScore predictions
}
