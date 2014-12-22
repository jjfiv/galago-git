// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.*;

/**
 * This class allows searching over a set of Retrievals.
 *
 * Although it is possible to list such objects as GroupRetrievals or other
 * MultiRetrievals under a MultiRetrieval, it is not recommended, as this
 * behavior has not been tested and is currently undefined.
 *
 * @author sjh
 */
public class MultiRetrieval implements Retrieval {

  protected ArrayList<Retrieval> retrievals;
  protected FeatureFactory features;
  protected List<Traversal> defaultTraversals;
  protected Parameters globalParameters;
  protected Parameters retrievalParts;
  protected HashMap<String, String> defaultIndexOperators = new HashMap<>();
  protected HashSet<String> knownIndexOperators = new HashSet<>();

  public MultiRetrieval(ArrayList<Retrieval> indexes, Parameters p) throws Exception {
    this.retrievals = indexes;
    this.globalParameters = p;
    initRetrieval();
    this.features = new FeatureFactory(this.globalParameters);
    defaultTraversals = features.getTraversals(this);
  }

  @Override
  public void close() throws IOException {
    for (Retrieval r : retrievals) {
      r.close();
    }
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
    IndexPartStatistics aggregate = null;
    for (Retrieval r : retrievals) {
      IndexPartStatistics stats = r.getIndexPartStatistics(partName);
      if (aggregate == null) {
        aggregate = stats;
      } else {
        aggregate.add(stats);
      }
    }
    return aggregate;
  }

  @Override
  public Parameters getAvailableParts() throws IOException {
    return this.retrievalParts;
  }

  @Override
  public Parameters getGlobalParameters() {
    return this.globalParameters;
  }

  @Override
  public Document getDocument(String identifier, DocumentComponents p) throws IOException {
    for (Retrieval r : this.retrievals) {
      Document d = r.getDocument(identifier, p);
      if (d != null) {
        return d;
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifiers, DocumentComponents p) throws IOException {
    HashMap<String, Document> results = new HashMap<String,Document>();
    for (Retrieval r : this.retrievals) {
      results.putAll(r.getDocuments(identifiers, p));
    }
    return results;
  }

  /**
   * Runs a query across all retrieval objects
   *
   * @throws Exception
   */
  @Override
  public Results executeQuery(Node root) throws Exception {
    return executeQuery(root, Parameters.create());
  }

  // Based on the root of the tree, that dictates how we execute.
  @Override
  public Results executeQuery(Node queryTree, Parameters p) throws Exception {
    ScoredDocument[] rankedList = runRankedQuery(queryTree, p);
    Results results = new Results();
    results.inputQuery = queryTree;
    results.scoredDocuments = Arrays.asList(rankedList);
    return results;
  }

  private ScoredDocument[] runRankedQuery(Node root, Parameters parameters) throws Exception {
    // Asynchronously run retrieval
    ArrayList<Thread> threads = new ArrayList<>();
    final List<ScoredDocument> queryResultCollector = Collections.synchronizedList(new ArrayList<ScoredDocument>());
    final List<String> errorCollector = Collections.synchronizedList(new ArrayList<String>());
    final Node queryTree = root;

    for (Retrieval retrieval : retrievals) {
      final Parameters shardParams = parameters.clone();
      final Retrieval r = retrieval;
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            List<ScoredDocument> results = r.executeQuery(queryTree, shardParams).scoredDocuments;
            if (results != null) {
              queryResultCollector.addAll(results);
            }
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

    // fix ranks
    List<ScoredDocument> scoredDocuments = queryResultCollector.subList(0, Math.min(queryResultCollector.size(), requested));
    ScoredDocument[] results = scoredDocuments.toArray(new ScoredDocument[scoredDocuments.size()]);
    int rank = 1;
    for (ScoredDocument r : results) {
      r.rank = rank;
      rank += 1;
    }

    return results;
  }

  @Override
  public Node transformQuery(Node root, Parameters qp) throws Exception {
    return transformQuery(defaultTraversals, root, qp);
  }

  // private functions
  private Node transformQuery(List<Traversal> traversals, Node queryTree, Parameters queryParams) throws Exception {
    for (Traversal traversal : traversals) {
      queryTree = traversal.traverse(queryTree, queryParams);
    }
    return queryTree;
  }

  private void initRetrieval() throws IOException {

    ArrayList<Parameters> parts = new ArrayList<Parameters>();
    for (Retrieval r : retrievals) {
      Parameters partSet = r.getAvailableParts();
      parts.add(partSet);
    }
    this.retrievalParts = mergeParts(parts);
  }

  // This takes the intersection of parts from constituent retrievals, and determines which
  // part/operator pairs are ok to search on given the current retrievalGroup. We assume that
  // a part is valid if it has at least one usable operator, and an operator is usable if the
  // iteratorClass that implements it is the same across all constituents under a given part.
  private Parameters mergeParts(List<Parameters> ps) {
    Parameters unifiedParts = Parameters.create();
    HashSet<String> operators = new HashSet<>();

    // Get *all* parts
    HashSet<String> allParts = new HashSet<>();
    for (Parameters j : ps) {
      //System.out.println("*** Printing parameters: " + j.toPrettyString());
      allParts.addAll(j.getKeys());
    }

    // Now iterate over the keys, looking for matches
    for (String part : allParts) {

      Parameters unifiedPart = Parameters.create();
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
    //System.out.println("Unified parts: ***: " + unifiedParts.toPrettyString());
    return unifiedParts;
  }

  @Override
  public FieldStatistics getCollectionStatistics(String nodeString) throws Exception {
    Node root = StructuredQuery.parse(nodeString);
    return getCollectionStatistics(root);
  }

  @Override
  public FieldStatistics getCollectionStatistics(Node node) throws Exception {

    ArrayList<Thread> threads = new ArrayList<Thread>();
    final Node root = node;
    final List<FieldStatistics> stats = Collections.synchronizedList(new ArrayList<FieldStatistics>());
    final List<String> errors = Collections.synchronizedList(new ArrayList<String>());

    for (final Retrieval r : this.retrievals) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            FieldStatistics ns = r.getCollectionStatistics(root);
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

    FieldStatistics output = stats.remove(0);
    for (FieldStatistics s : stats) {
      output.add(s);
    }
    return output;
  }

  /**
   * Note that this assumes the retrieval objects involved in the group contain
   * mutually exclusive subcollections. If you're doing PAC-search or another
   * non-disjoint subset retrieval model, look out.
   */
  @Override
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
    Node root = StructuredQuery.parse(nodeString);
    return getNodeStatistics(root);
  }

  @Override
  public NodeStatistics getNodeStatistics(Node node) throws Exception {

    ArrayList<Thread> threads = new ArrayList<Thread>();
    final Node root = node;
    final List<NodeStatistics> stats = Collections.synchronizedList(new ArrayList<NodeStatistics>());
    final List<String> errors = Collections.synchronizedList(new ArrayList<String>());

    for (final Retrieval r : this.retrievals) {
      Thread t = new Thread() {
        @Override
        public void run() {
          try {
            NodeStatistics ns = r.getNodeStatistics(root);
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

  @Override
  public NodeType getNodeType(Node node) throws Exception {
    NodeType nodeType = getIndexNodeType(node);
    if (nodeType == null) {
      nodeType = features.getNodeType(node);
    }
    return nodeType;
  }

  private NodeType getIndexNodeType(Node node) throws Exception {
    if (node.getNodeParameters().containsKey("part") || node.getOperator().equals("field")) {
      //   System.out.println("Trying to get operators for node... got part parameter");
      Parameters parts = getAvailableParts();
      String partName = getPartName(node);
      if (node.getOperator().equals("field")) {
        partName = "fields";
      }

      // System.out.println("Fetching part : " + partName  + " in parts: " + parts.toPrettyString() );
      if (!parts.containsKey(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
      String operator = node.getOperator();
      // System.out.println("Trying to look up part: " + partName + " and operator: " + operator);
      Parameters partParams = parts.getMap(partName);
      if (!partParams.containsKey(operator)) {
        throw new IOException("The index has part called  iterator for the operator '" + operator + "'");
      }
      String iteratorClass = partParams.getString(operator);
      //System.out.println("Got part for node.");
      // may need to do some checking here...
      return new NodeType((Class<? extends BaseIterator>) Class.forName(iteratorClass));
    } else {
      // System.out.println("No part in node parameters. for node: " + node.toPrettyString() + " Not returning index part.");
    }
    return null;
  }

  public String getPartName(Node node) throws IOException {
    String operator = node.getOperator();
    String partName = null;
    Parameters parts = getAvailableParts();

    if (node.getNodeParameters().containsKey("part")) {
      partName = node.getNodeParameters().getString("part");
      if (!parts.containsKey(partName)) {
        throw new IOException("The index has no part named '" + partName + "'");
      }
    } else if (knownIndexOperators.contains(operator)) {
      if (!defaultIndexOperators.containsKey(operator)) {
        throw new IOException("More than one index part supplies the operator '"
                + operator + "', but no part name was specified.");
      } else {
        partName = defaultIndexOperators.get(operator);
      }
    }
    return partName;
  }

  @Override
  public QueryType getQueryType(Node node) throws Exception {
    if (node.getOperator().equals("text")) {
      return QueryType.UNKNOWN;
    }
    NodeType nodeType = getNodeType(node);
    Class outputClass = nodeType.getIteratorClass();
    if (ScoreIterator.class.isAssignableFrom(outputClass)
            || ScoringFunctionIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.RANKED;
    } else if (IndicatorIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.BOOLEAN;
    } else if (CountIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.COUNT;
    } else {
      return QueryType.RANKED;
    }
  }

  @Override
  public Integer getDocumentLength(Integer docid) throws IOException {
    throw new UnsupportedOperationException("Not supported.");
  }

  @Override
  public Integer getDocumentLength(String docname) throws IOException {
    for (Retrieval r : this.retrievals) {
      Integer l = r.getDocumentLength(docname);
      if (l > 0) {
        return l;
      }
    }
    return 0;
  }

  @Override
  public String getDocumentName(Integer docid) throws IOException {
    for (Retrieval r: this.retrievals) {
      String id = r.getDocumentName(docid);
      if(id != null)
        return id;
    }
    return null;
  }

  @Override
  public Long getDocumentId(String docname) throws IOException {
    for (Retrieval r: this.retrievals) {
      Long id = r.getDocumentId(docname);
      if(id != null)
        return id;
    }
    return null;
  }

  @Override
  public void addNodeToCache(Node node) throws Exception {
    for (Retrieval r : this.retrievals) {
      r.addNodeToCache(node);
    }
  }

  @Override
  public void addAllNodesToCache(Node node) throws Exception {
    for (Retrieval r : this.retrievals) {
      r.addAllNodesToCache(node);
    }
  }
}
