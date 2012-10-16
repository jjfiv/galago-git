/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.contrib.learning;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/** 
 * Data class contains information about a set of queries
 *
 * @author sjh
 */
public class Queries {
  
  private Logger logger;
  
  protected TreeSet<String> queryNumbers;
  protected TreeMap<String,String> queryTexts;
  protected TreeMap<String,Node> queryNodes;
  protected TreeMap<String,Parameters> queryParams;
  
  public Queries(List<Parameters> queries, Parameters defaultQParams){
    queryNumbers = new TreeSet<String>();
    queryTexts = new TreeMap<String,String>();
    queryNodes = new TreeMap<String,Node>();
    queryParams = new TreeMap<String,Parameters>();
    
    logger = Logger.getLogger(this.getClass().getName());
    
    for(Parameters q : queries){
      String qnum = q.getString("number");
      if(queryNumbers.contains(qnum)){
        logger.info("Ingoring duplicated query: number: " + qnum + "\t" + q.toString());
        continue;
      }
      queryNumbers.add(qnum);

      // clone the defaultQParams + overwrite any settings
      Parameters qparams = defaultQParams.clone();
      qparams.copyFrom(q);
      queryParams.put(qnum, qparams);
      
      String qtext = q.getString("text");
      queryTexts.put(qnum,qtext);

      Node qnode = StructuredQuery.parse(qtext);
      queryNodes.put(qnum,qnode);
      
    }
  }

  public boolean isEmpty() {
    return queryNumbers.isEmpty();
  }

  public Iterable<String> getQueryNumbers() {
    return queryNumbers;
  }

  public Node getNode(String number) {
    if(queryNumbers.contains(number)){
      return this.queryNodes.get(number);
    } else {
      logger.warning("Tried to get non-existant query: " + number);
      return null;
    }
  }
  
  public List<Parameters> getParametersSubset(List<String> numbers){
    ArrayList<Parameters> sublist = new ArrayList();
    for(String request : numbers){
      sublist.add(this.queryParams.get(request));
    }
    return sublist;
  }
}
