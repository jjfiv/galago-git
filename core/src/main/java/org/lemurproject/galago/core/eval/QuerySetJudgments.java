/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.eval;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class store a relevance judgments for a set of queries
 *  - the judgments for each query are stored in a QueryJudgments object
 * 
 * Relevance is represented by an integer
 *  - positive indicates that the document is relevant
 *  - zero indicates that the document is not relevant
 *  - negative indicates that the document is detrimental to results (irrelevant)
 * 
 * @author sjh
 */
public class QuerySetJudgments {

  private Map<String, QueryJudgments> querySetJudgments;
  private boolean binary;
  private boolean positive;
          

  public QuerySetJudgments(String filename, boolean binary, boolean positive) throws IOException {
    querySetJudgments = loadJudgments(filename);
    this.binary = binary;
    this.positive = positive;
  }

  public Iterable<String> getQueryIterator() {
    return querySetJudgments.keySet();
  }

  public QueryJudgments get(String query) {
    return querySetJudgments.get(query);
  }


  // LOADING FUNCTIONS
  /**
   * Loads a TREC judgments file.
   *
   * @param filename The filename of the judgments file to load.
   * @return Maps from query numbers to lists of judgments for each query.
   */
  private TreeMap<String, QueryJudgments> loadJudgments(String filename) throws IOException {
    // open file
    BufferedReader in = new BufferedReader(new FileReader(filename));
    String line = null;
    TreeMap<String, QueryJudgments> judgments = new TreeMap();

    while ((line = in.readLine()) != null) {
      int[] columns = splits(line, 4);

      String queryNumber = line.substring(columns[0], columns[1]);
      String unused = line.substring(columns[2], columns[3]);
      String docno = line.substring(columns[4], columns[5]);
      String judgment = line.substring(columns[6]);

      // ensure the query is stored
      if (!judgments.containsKey(queryNumber)) {
        judgments.put(queryNumber, new QueryJudgments(queryNumber));
      }

      // add this judgment to the query
      int j = Integer.parseInt(judgment);
      if(binary){
        j = (j > 0)? 1 : 0;
      } else if(positive){
        j = (j > 0)? j : 0;
      }
      judgments.get(queryNumber).add(docno, j);
    }

    in.close();
    return judgments;
  }

  /**
   * Finds characters to split a line 
   *  of a ranking file or a judgment file
   */
  private int[] splits(String s, int columns) {
    int[] result = new int[2 * columns];
    boolean lastWs = true;
    int column = 0;
    result[0] = 0;

    for (int i = 0; i < s.length() && column < columns; i++) {
      char c = s.charAt(i);
      boolean isWs = (c == ' ') || (c == '\t');

      if (!isWs && lastWs) {
        result[2 * column] = i;
      } else if (isWs && !lastWs) {
        result[2 * column + 1] = i;
        column++;
      }

      lastWs = isWs;
    }

    return result;
  }
}
