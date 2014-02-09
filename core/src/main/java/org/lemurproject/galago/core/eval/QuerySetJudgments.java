//  BSD License (http://www.galagosearch.org/license)
package org.lemurproject.galago.core.eval;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.tupleflow.util.WrappedMap;

/**
 * This class store a relevance judgments for a set of queries
 *  - the judgments for each query are stored in a QueryJudgments object
 * 
 * Relevance is represented by an integer
 *  - positive indicates that the document is relevant
 *  - zero indicates that the document is not relevant
 *  - negative indicates that the document is detrimental to results (irrelevant)
 * 
 * @author sjh, jfoley
 */
public class QuerySetJudgments extends WrappedMap<String, QueryJudgments> {


  /**
   * Loads a TREC judgments file.
   *
   * @param filename The filename of the judgments file to load.
   * @param makeBinary whether to force the judgments into a binary form
   * @param makePositive whether to force the judgments into a positive form.
   * @throws java.io.IOException file issues
   */
  public QuerySetJudgments(String filename, boolean makeBinary, boolean makePositive) throws IOException {
    this(loadJudgments(filename, makeBinary, makePositive));
  }
  
  /**
   * Creates a galago evaluation object from your home-grown judgments.
   *
   * @param data something like a Map<String,Map<String,Int>>
   */
  public QuerySetJudgments(Map<String, QueryJudgments> data) {
    super(data);
  }

  /**
   * 
   * @return
   * @deprecated Use keySet() instead
   */
  @Deprecated
  public Iterable<String> getQueryIterator() {
    return keySet();
  }

  public QueryJudgments get(String query) {
    return wrapped.get(query);
  }

  // LOADING FUNCTIONS
  /**
   * Loads a TREC judgments file.
   *
   * @param filename The filename of the judgments file to load.
   * @param makeBinary whether to force the judgments into a binary form
   * @param makePositive whether to force the judgments into a positive form.
   * @return Maps from query numbers to lists of judgments for each query.
   * @throws java.io.IOException file issues
   */
  public static Map<String, QueryJudgments> loadJudgments(String filename, boolean makeBinary, boolean makePositive) throws IOException {
    BufferedReader in = null;
    Map<String, QueryJudgments> judgments = new TreeMap();

    try {
      in = new BufferedReader(new FileReader(filename));

      while (true) {
        String line = in.readLine();
        if (line == null) {
          break;
        }
        
        String[] cols = line.split("\\s+");
        assert(cols.length == 4);

        String queryNumber = cols[0];
        String unused = cols[1];
        String docno = cols[2];
        String judgment = cols[3];

        // ensure the query is stored
        if (!judgments.containsKey(queryNumber)) {
          judgments.put(queryNumber, new QueryJudgments(queryNumber));
        }

        // add this judgment to the query
        int j = Integer.parseInt(judgment);
        if (makeBinary) {
          j = (j > 0) ? 1 : 0;
        } else if (makePositive) {
          j = (j > 0) ? j : 0;
        }
        judgments.get(queryNumber).add(docno, j);
      }
    } finally {
      if (in != null) {
        in.close();
      }
    }

    if(judgments.isEmpty()) {
      throw new RuntimeException("Error: no judgments found in file: "+filename);
    }
    return judgments;
  }
}
