// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.NamesReader.NamesIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.iterator.ContextualIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * The responsibility of the LocalRetrieval object is to provide a simpler
 * interface on top of the DiskIndex. Therefore, given a query or text string
 * representing a query, this object will perform the necessary transformations
 * to make it an executable object.
 *
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 */
public class LocalRetrieval implements Retrieval {
  
  protected final Logger logger = Logger.getLogger(this.getClass().getName());
  protected Index index;
  protected FeatureFactory features;
  protected Parameters globalParameters;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public LocalRetrieval(Index index) throws IOException {
    this(index, new Parameters());
  }
  
  public LocalRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException, Exception {
    this(new DiskIndex(filename), parameters);
  }
  
  public LocalRetrieval(Index index, Parameters parameters) throws IOException {
    this.globalParameters = parameters;
    setIndex(index);
  }
  
  protected void setIndex(Index indx) throws IOException {
    this.index = indx;
    features = new FeatureFactory(globalParameters);
  }

  /**
   * Closes the underlying index
   */
  @Override
  public void close() throws IOException {
    index.close();
  }

  /**
   * Returns the collectionLength and documentCount of a given index, contained
   * in a Parameters object. Additional statistics may be provided, but are not
   * expected.
   */
  @Override
  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException {
    return index.getCollectionStatistics(partName);
  }
  
  @Override
  public CollectionStatistics getRetrievalStatistics() throws IOException {
    return index.getCollectionStatistics();
  }
  
  @Override
  public Parameters getGlobalParameters() {
    return this.globalParameters;
  }

  /*
   * {
   * <partName> : { <nodeName> : <iteratorClass>, stemming : false, ... },
   * <partName> : { <nodeName> : <iteratorClass>, ... }, ... }
   */
  @Override
  public Parameters getAvailableParts() throws IOException {
    Parameters p = new Parameters();
    for (String partName : index.getPartNames()) {
      Parameters inner = new Parameters();
      Map<String, NodeType> nodeTypes = index.getPartNodeTypes(partName);
      for (String nodeName : nodeTypes.keySet()) {
        inner.set(nodeName, nodeTypes.get(nodeName).getIteratorClass().getName());
      }
      p.set(partName, inner);
    }
    return p;
  }
  
  public Index getIndex() {
    return index;
  }
  
  @Override
  public Document getDocument(String identifier, Parameters p) throws IOException {
    return this.index.getDocument(identifier, p);
  }
  
  @Override
  public Map<String, Document> getDocuments(List<String> identifier, Parameters p) throws IOException {
    return this.index.getDocuments(identifier, p);
  }

  /**
   * Accepts a transformed query, constructs the iterator tree from the node
   * tree, then iterates over the iterator tree, and returns the results.
   */
  public ScoredDocument[] runQuery(String query, Parameters p) throws Exception {
    Node root = StructuredQuery.parse(query);
    root = transformQuery(root, p);
    return runQuery(root, p);
  }
  
  @Override
  public ScoredDocument[] runQuery(Node queryTree) throws Exception {
    return runQuery(queryTree, new Parameters());
  }

  // Based on the root of the tree, that dictates how we execute.
  @Override
  public ScoredDocument[] runQuery(Node queryTree, Parameters queryParams) throws Exception {
    ScoredDocument[] results = null;
    ProcessingModel pm = ProcessingModel.instance(this, queryTree, queryParams);
    
    // Figure out if there's a working set to deal with
    int[] workingSet = null;
    
    if (queryParams.containsKey("working")) {
      workingSet = this.getDocumentIds(queryParams.getList("working"));
    }
    
    if (workingSet != null) {
      pm.defineWorkingSet(workingSet);
    }

    // get some results
    results = pm.execute(queryTree, queryParams);
    if (results == null) {
      results = new ScoredDocument[0];
    }

    // Format and get names
    String indexId = this.globalParameters.get("indexId", "0");
    return getArrayResults(results, indexId);
  }

  /*
   * getArrayResults annotates a queue of scored documents returns an array
   *
   */
  protected <T extends ScoredDocument> T[] getArrayResults(T[] results, String indexId) throws IOException {
    if (results == null || results.length == 0) {
      return null;
    }
    
    for (int i = 0; i < results.length; i++) {
      results[i].source = indexId;
      results[i].rank = i + 1;
    }

    // this is to assign proper document names
    T[] byID = Arrays.copyOf(results, results.length);
    
    Arrays.sort(byID, new Comparator<T>() {
      
      @Override
      public int compare(T o1, T o2) {
        return Utility.compare(o1.document, o2.document);
      }
    });

    NamesIterator namesIterator = index.getNamesIterator();
    ScoringContext sc = new ScoringContext();
    namesIterator.setContext(sc);

    for (T doc : byID) {
      namesIterator.moveTo(doc.document);
      sc.document = doc.document;

      if (doc.document == namesIterator.getCurrentIdentifier()) {
        doc.documentName = namesIterator.getCurrentName();
      } else {
        System.err.println("NAMES ITERATOR FAILED TO FIND DOCUMENT " + doc.document);
        // now throw an error.
        doc.documentName = index.getName(doc.document);
      }
    }
    
    return results;
  }
  
  public StructuredIterator createIterator(Parameters queryParameters, Node node, ScoringContext context) throws Exception {
    if (globalParameters.get("shareNodes", true)) {
      if (queryParameters.get("shareNodes", true)) {
        return createNodeMergedIterator(node, context, new HashMap());
      }
    }
    
    return createNodeMergedIterator(node, context, null);
  }
  
  protected StructuredIterator createNodeMergedIterator(Node node, ScoringContext context,
          HashMap<String, StructuredIterator> queryIteratorCache)
          throws Exception {
    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();
    StructuredIterator iterator;

    // first check if the cache contains this node
    if (queryIteratorCache != null && queryIteratorCache.containsKey(node.toString())) {
      return queryIteratorCache.get(node.toString());
    }
    
    for (Node internalNode : node.getInternalNodes()) {
      StructuredIterator internalIterator = createNodeMergedIterator(internalNode, context, queryIteratorCache);
      internalIterators.add(internalIterator);
    }
    
    iterator = index.getIterator(node);
    if (iterator == null) {
      iterator = features.getIterator(node, internalIterators);
    }
    
    if (ContextualIterator.class.isInstance(iterator) && (context != null)) {
      ((ContextualIterator) iterator).setContext(context);
    }

    // we've created a new iterator - add to the cache for future nodes
    if (queryIteratorCache != null) {
      queryIteratorCache.put(node.toString(), iterator);
    }
    return iterator;
  }
  
  @Override
  public Node transformQuery(Node queryTree, Parameters queryParams) throws Exception {
    return transformQuery(features.getTraversals(this, queryTree, queryParams), queryTree);
  }
  
  protected Node transformQuery(List<Traversal> traversals, Node queryTree) throws Exception {
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.walk(traversal, queryTree);
    }
    return queryTree;
  }
  
  @Override
  public NodeStatistics nodeStatistics(String nodeString) throws Exception {
    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    root.getNodeParameters().set("queryType", "count");
    root = transformQuery(root, new Parameters());
    return nodeStatistics(root);
  }
  
  @Override
  public NodeStatistics nodeStatistics(Node root) throws Exception {
    NodeStatistics stats = new NodeStatistics();
    // set up initial values
    stats.node = root.toString();
    stats.nodeDocumentCount = 0;
    stats.nodeFrequency = 0;
    stats.collectionLength = getRetrievalStatistics().collectionLength;
    stats.documentCount = getRetrievalStatistics().documentCount;
    
    ScoringContext sc = new ScoringContext();
    StructuredIterator structIterator = createIterator(new Parameters(), root, sc);
    if (AggregateIterator.class.isInstance(structIterator)) {
      stats = ((AggregateIterator) structIterator).getStatistics();
      
    } else if (structIterator instanceof MovableCountIterator) {
      MovableCountIterator iterator = (MovableCountIterator) structIterator;
      while (!iterator.isDone()) {
        sc.document = iterator.currentCandidate();
        if (iterator.hasMatch(iterator.currentCandidate())) {
          stats.nodeFrequency += iterator.count();
          stats.nodeDocumentCount++;
        }
        iterator.movePast(iterator.currentCandidate());
      }
      
    } else {
      throw new IllegalArgumentException("Node " + root.toString() + " did not return a counting iterator.");
    }
    return stats;
  }
  
  @Override
  public NodeType getNodeType(Node node) throws Exception {
    NodeType nodeType = index.getNodeType(node);
    if (nodeType == null) {
      nodeType = features.getNodeType(node);
    }
    return nodeType;
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
  public int getDocumentLength(int docid) throws IOException {
    return index.getLength(docid);
  }
  
  @Override
  public int getDocumentLength(String docname) throws IOException {
    return index.getLength(index.getIdentifier(docname));
  }
  
  @Override
  public String getDocumentName(int docid) throws IOException {
    return index.getName(docid);
  }
  
  public int[] getDocumentIds(List<String> docnames) throws IOException {
    int[] ids = new int[docnames.size()];
    int i = 0;
    for (String name : docnames) {
      ids[i] = index.getIdentifier(name);
      i++;
    }
    return ids;
  }
}
