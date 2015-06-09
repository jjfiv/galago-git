/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;

/** 
 * Data class contains information about a set of queries
 *
 * @author sjh
 */
public class QuerySet {

  private Logger logger;
  protected TreeSet<String> queryIdentifiers;
  protected TreeMap<String, String> queryTexts;
  protected TreeMap<String, Node> queryNodes;
  protected TreeMap<String, Parameters> queryParams;

  public QuerySet(List<Parameters> queries, Parameters globalBackoff) {
    queryIdentifiers = new TreeSet<>();
    queryTexts = new TreeMap<>();
    queryNodes = new TreeMap<>();
    queryParams = new TreeMap<>();

    logger = Logger.getLogger(this.getClass().getName());

    for (Parameters q : queries) {
      String qnum = q.getString("number");
      if (queryIdentifiers.contains(qnum)) {
        logger.info("Ingoring duplicated query: number: " + qnum + "\t" + q.toString());
        continue;
      }
      queryIdentifiers.add(qnum);

      // clone the defaultQParams + overwrite any settings
      Parameters qparams = q.clone();
      qparams.setBackoff(globalBackoff);
      queryParams.put(qnum, qparams);

      String qtext = q.getString("text");
      queryTexts.put(qnum, qtext);

      Node qnode = StructuredQuery.parse(qtext);
      queryNodes.put(qnum, qnode);

    }
  }

  public boolean isEmpty() {
    return queryIdentifiers.isEmpty();
  }

  public Iterable<String> getQueryNumbers() {
    return queryIdentifiers;
  }

  public Node getNode(String number) {
    if (queryIdentifiers.contains(number)) {
      return this.queryNodes.get(number);
    } else {
      logger.warning("Tried to get non-existant query: " + number);
      return null;
    }
  }

  public Parameters getParameters(String number) {
    if (queryIdentifiers.contains(number)) {
      return this.queryParams.get(number);
    } else {
      logger.warning("Tried to get non-existant query parameters: " + number);
      return null;
    }
  }

  public List<Parameters> getParametersSubset(List<String> numbers) {
    ArrayList<Parameters> sublist = new ArrayList<>();
    for (String request : numbers) {
      sublist.add(this.queryParams.get(request).clone());
    }
    return sublist;
  }

  public List<Parameters> getQueryParameters() {
    ArrayList<Parameters> list = new ArrayList<>();
    for (String num : this.queryIdentifiers) {
      list.add(this.queryParams.get(num));
    }
    return list;
  }
}
