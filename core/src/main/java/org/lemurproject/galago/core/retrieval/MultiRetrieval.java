// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.lang.String;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * This class allows searching over a set of Retrievals.
 *
 * Although it is possible to list such objects as GroupRetrievals or
 * other MultiRetrievals under a MultiRetrieval, it is not recommended,
 * as this behavior has not been tested and is currently undefined.
 *
 * @author sjh
 */
public class MultiRetrieval implements Retrieval {

  protected ArrayList<Retrieval> retrievals;
  protected FeatureFactory features;
  protected Parameters globalParameters;
  protected HashMap<String, CollectionStatistics> retrievalStatistics;
  protected Parameters retrievalParts;

  public MultiRetrieval(ArrayList<Retrieval> indexes, Parameters p) throws Exception {
    this.retrievals = indexes;
    this.globalParameters = p;
    initRetrieval();
    this.features = new FeatureFactory(this.globalParameters);
  }

  public void close() throws IOException {
    for (Retrieval r : retrievals) {
      r.close();
    }
  }

  public CollectionStatistics getRetrievalStatistics() throws IOException {
    return this.retrievalStatistics.get("postings");
  }

  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException {
    return this.retrievalStatistics.get(partName);
  }

  public Parameters getAvailableParts() throws IOException {
    return this.retrievalParts;
  }

  public Parameters getGlobalParameters() {
    return this.globalParameters;
  }

  @Override
  public Document getDocument(String identifier) throws IOException{
    for(Retrieval r : this.retrievals){
      Document d = r.getDocument(identifier);
      if(d != null){
        return d;
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifiers) throws IOException{
    HashMap<String,Document> results = new HashMap();
    for(Retrieval r : this.retrievals){
      results.putAll(r.getDocuments(identifiers));
    }
    return results;
  }

  /**
   *
   * Runs a query across all retrieval objects
   *
   * @param query
   * @param parameters
   * @return
   * @throws Exception
   */
  public ScoredDocument[] runQuery(Node root) throws Exception {
    return runQuery(root, new Parameters());
  }

  // Based on the root of the tree, that dictates how we execute.
  public ScoredDocument[] runQuery(Node queryTree, Parameters p) throws Exception {
    ScoredDocument[] results = null;
    switch (this.getQueryType(queryTree)) {
      case RANKED:
        results = runRankedQuery(queryTree, p);
        break;
      case BOOLEAN:
        results = runBooleanQuery(queryTree, p);
        break;
    }
    return results;
  }

  private ScoredDocument[] runBooleanQuery(Node root, Parameters parameters) throws Exception {
    throw new UnsupportedOperationException();
  }

  private ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception {
    // Asynchronously run retrieval
    ArrayList<Thread> threads = new ArrayList();
    final List<ScoredDocument> queryResultCollector = Collections.synchronizedList(new ArrayList());
    final List<String> errorCollector = Collections.synchronizedList(new ArrayList());
    final Node queryTree = root;

    for (int i = 0; i < retrievals.size(); i++) {
      final Parameters shardParams = parameters.clone();
      final Retrieval r = retrievals.get(i);
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            ScoredDocument[] results = r.runQuery(queryTree, shardParams);
            queryResultCollector.addAll(Arrays.asList(results));
          } catch (Exception e) {
            errorCollector.add(e.getMessage());
          }
        }
      };
      threads.add(t);
      t.start();
    }

    // Wait for a finished list
    for (Thread t : threads) {
      t.join();
    }

    if (errorCollector.size() > 0) {
      System.err.println("Failed to run: " + root.toString());
      for (String e : errorCollector) {
        System.err.println(e);
      }
      // we do not want to return partial or erroneous results.
      return new ScoredDocument[0];
    }


    // sort the results and invert (sort is inverted)
    Collections.sort(queryResultCollector, Collections.reverseOrder());

    // get the best {requested} results
    int requested = (int) parameters.get("requested", 1000);

    return queryResultCollector.subList(0, Math.min(queryResultCollector.size(), requested)).toArray(new ScoredDocument[0]);
  }

  public Node transformQuery(Node root) throws Exception {
    return transformQuery(features.getTraversals(this, root), root);
  }

  // private functions
  private Node transformQuery(List<Traversal> traversals, Node queryTree) throws Exception {
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  private void initRetrieval() throws IOException {

    ArrayList<Parameters> parts = new ArrayList();
    for (Retrieval r : retrievals) {
      Parameters partSet = r.getAvailableParts();
      parts.add(partSet);
    }
    this.retrievalParts = mergeParts(parts);
    retrievalStatistics = new HashMap();
    for (String part : getAvailableParts().getKeys()) {
      CollectionStatistics mergedStats = null;
      for (Retrieval r : this.retrievals) {
        if (mergedStats == null) {
          mergedStats = r.getRetrievalStatistics(part);
        } else {
          mergedStats.add(r.getRetrievalStatistics(part));
        }
      }
      this.retrievalStatistics.put(part, mergedStats);
    }
  }

  // This takes the intersection of parts from constituent retrievals, and determines which
  // part/operator pairs are ok to search on given the current retrievalGroup. We assume that
  // a part is valid if it has at least one usable operator, and an operator is usable if the
  // iteratorClass that implements it is the same across all constituents under a given part.
  private Parameters mergeParts(List<Parameters> ps) {
    Parameters unifiedParts = new Parameters();
    HashSet<String> operators = new HashSet<String>();

    // Get *all* parts
    HashSet<String> allParts = new HashSet<String>();
    for (Parameters j : ps) {
      allParts.addAll(j.getKeys());
    }

    // Now iterate over the keys, looking for matches
    for (String part : allParts) {
      Parameters unifiedPart = new Parameters();
      // If one of the constituents doesn't have a part of this name, we skip
      // further processing of it
      boolean hasPart = true;
      operators.clear();
      for (Parameters retrievalParams : ps) {
        if (!retrievalParams.getKeys().contains(part)) {
          hasPart = false;
          break;
        } else {
          operators.addAll(retrievalParams.getMap(part).getKeys());
        }
      }
      if (!hasPart) {
        continue;
      }

      // All operators discovered for a given part. Go over those.
      for (String op : operators) {
        String iteratorClassName = null;
        boolean sharesIterator = true;
        for (Parameters retrievalParams : ps) {
          String partIterator = retrievalParams.getMap(part).getString(op);
          if (iteratorClassName == null) {
            iteratorClassName = partIterator;
          } else {
            if (!iteratorClassName.equals(partIterator)) {
              sharesIterator = false;
              break;
            }
          }
        }
        // If not all had the same iterator, skip adding it to that part's available operators
        if (!sharesIterator) {
          continue;
        }
        unifiedPart.set(op, iteratorClassName);
      }
      // the unified part is not empty, we have at least one viable operator for that part, so add it.
      if (!unifiedPart.isEmpty()) {
        unifiedParts.set(part, unifiedPart);
      }
    }

    return unifiedParts;
  }

  /**
   * Note that this assumes the retrieval objects involved in the group
   * contain mutually exclusive subcollections. If you're doing PAC-search
   * or another non-disjoint subset retrieval model, look out.
   */
  public NodeStatistics nodeStatistics(String nodeString) throws Exception {
    Node root = StructuredQuery.parse(nodeString);
    root.getNodeParameters().set("queryType", "count");
    root = transformQuery(root);
    return nodeStatistics(root);
  }

  public NodeStatistics nodeStatistics(Node node) throws Exception {

    ArrayList<Thread> threads = new ArrayList();
    final Node root = node;
    final List<NodeStatistics> stats = Collections.synchronizedList(new ArrayList());
    final List<String> errors = Collections.synchronizedList(new ArrayList());

    for (int i = 0; i < this.retrievals.size(); i++) {
      final Retrieval r = this.retrievals.get(i);
      Thread t = new Thread() {

        @Override
        public void run() {
          try {
            NodeStatistics ns = r.nodeStatistics(root);
            stats.add(ns);
          } catch (Exception ex) {
            errors.add(ex.getMessage());
          }
        }
      };
      threads.add(t);
      t.start();
    }

    for (Thread t : threads) {
      t.join();
    }

    if (errors.size() > 0) {
      System.err.println("Failed to count: " + root.toString());
      for (String e : errors) {
        System.err.println(e);
      }
      throw new IOException("Unable to count " + node.toString());
    }

    NodeStatistics output = stats.remove(0);
    for (NodeStatistics s : stats) {
      output.add(s);
    }
    return output;
  }

  public NodeType getNodeType(Node node) throws Exception {
    NodeType nodeType = getIndexNodeType(node);
    if (nodeType == null) {
      nodeType = features.getNodeType(node);
    }
    return nodeType;
  }

  private NodeType getIndexNodeType(Node node) throws Exception {
    if (node.getNodeParameters().containsKey("part")) {
      Parameters parts = getAvailableParts();
      String partName = node.getNodeParameters().getString("part");

      if (!parts.containsKey(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
      String operator = node.getOperator();
      Parameters partParams = parts.getMap(partName);
      if (!partParams.containsKey(operator)) {
        throw new IOException("The index has part called  iterator for the operator '" + operator + "'");
      }
      String iteratorClass = partParams.getString(operator);

      // may need to do some checking here...
      return new NodeType((Class<? extends StructuredIterator>) Class.forName(iteratorClass));
    }
    return null;
  }

  public QueryType getQueryType(Node node) throws Exception {
    NodeType nodeType = getNodeType(node);
    Class outputClass = nodeType.getIteratorClass();
    if (ScoreIterator.class.isAssignableFrom(outputClass)
            || ScoringFunctionIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.RANKED;
    } else if (IndicatorIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.BOOLEAN;
    } else {
      return QueryType.RANKED;
    }
  }

  @Override
  public int getDocumentLength(int docid) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getDocumentLength(String docname) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getDocumentName(int docid) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
