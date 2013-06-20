// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.NamesReader.NamesIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.stats.AggregateStatistic;
import org.lemurproject.galago.core.index.stats.CollectionAggregateIterator;
import org.lemurproject.galago.core.index.stats.CollectionStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeAggregateIterator;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.iterator.IndicatorIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.processing.ActiveContext;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.structured.ContextFactory;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
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
  protected CachedRetrieval cache;
  protected List<Traversal> defaultTraversals;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public LocalRetrieval(Index index) throws Exception {
    this(index, new Parameters());
  }

  public LocalRetrieval(String filename, Parameters parameters) throws Exception {
    this(new DiskIndex(filename), parameters);
  }

  public LocalRetrieval(Index index, Parameters parameters) throws Exception {
    this.globalParameters = parameters;
    setIndex(index);
  }

  protected void setIndex(Index indx) throws Exception {
    this.index = indx;
    features = new FeatureFactory(globalParameters);
    defaultTraversals = features.getTraversals(this);
    cache = null;
    if (this.globalParameters.get("cache", false)) {
      cache = new CachedRetrieval(this.globalParameters);
    }
  }

  /**
   * Closes the underlying index
   */
  @Override
  public void close() throws IOException {
    index.close();
  }

  /**
   * Returns some statistics about a particular index part
   *  -- vocab size, number of entries, maximumDocCount of any indexed term, etc
   */
  @Override
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
    return index.getIndexPartStatistics(partName);
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
  public Document getDocument(String identifier, DocumentComponents p) throws IOException {
    return this.index.getDocument(identifier, p);
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException {
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
    if (globalParameters.containsKey("processingModel")) {
      queryParams.set("processingModel", globalParameters.getString("processingModel"));
    }
    ProcessingModel pm = ProcessingModel.instance(this, queryTree, queryParams);

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
      namesIterator.syncTo(doc.document);
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

  public BaseIterator createIterator(Parameters queryParameters, Node node, ScoringContext context) throws Exception {
    if (globalParameters.get("shareNodes", true)) {
      if (queryParameters.get("shareNodes", true)) {
        return createNodeMergedIterator(node, context, new HashMap());
      }
    }
    return createNodeMergedIterator(node, context, null);
  }

  protected BaseIterator createNodeMergedIterator(Node node, ScoringContext context,
          HashMap<String, BaseIterator> queryIteratorCache)
          throws Exception {
    ArrayList<BaseIterator> internalIterators = new ArrayList<BaseIterator>();
    BaseIterator iterator;

    // first check if this is a repeated node in this tree:
    if (queryIteratorCache != null && queryIteratorCache.containsKey(node.toString())) {
      iterator = queryIteratorCache.get(node.toString());
      context.toNodes.put(iterator, node);
      return iterator;
    }

    // second check if this node is cached
    if (cache != null && cache.isCached(node)) {
      iterator = cache.getCachedIterator(node);
    } else {

      // otherwise we need to create a new iterator
      // start by recursively creating children
      for (Node internalNode : node.getInternalNodes()) {
        BaseIterator internalIterator = createNodeMergedIterator(internalNode, context, queryIteratorCache);
        internalIterators.add(internalIterator);
      }

      iterator = index.getIterator(node);
      if (iterator == null) {
        iterator = features.getIterator(node, internalIterators);
      }
    }

    // we have now constructed the iterator from the cache or from the index:
    //  --> deal with the context
    if (context != null && BaseIterator.class.isAssignableFrom(iterator.getClass())) {
      ((BaseIterator) iterator).setContext(context);
    }

    if (context != null && ActiveContext.class.isAssignableFrom(context.getClass())) {
      ((ActiveContext) context).checkIterator(node, iterator);
    }

    // we've created a new iterator - add to the cache for future nodes
    if (queryIteratorCache != null) {
      queryIteratorCache.put(node.toString(), iterator);
    }

    context.toNodes.put(iterator, node);
    return iterator;
  }

  @Override
  public Node transformQuery(Node queryTree, Parameters queryParams) throws Exception {
    return transformQuery(defaultTraversals, queryTree, queryParams);
  }

  private Node transformQuery(List<Traversal> traversals, Node queryTree, Parameters queryParams) throws Exception {
    for (Traversal traversal : traversals) {
      traversal.beforeTreeRoot(queryTree, queryParams);
      queryTree = StructuredQuery.walk(traversal, queryTree, queryParams);
      queryTree = traversal.afterTreeRoot(queryTree, queryParams);
    }
    return queryTree;
  }

  @Override
  public CollectionStatistics getCollectionStatistics(String nodeString) throws Exception {
    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    return getCollectionStatistics(root);
  }

  @Override
  public CollectionStatistics getCollectionStatistics(Node root) throws Exception {

    String rootString = root.toString();
    if (cache!= null && cache.cacheStats) {
      AggregateStatistic stat = cache.getCachedStatistic(rootString);
      if (stat != null && stat instanceof CollectionStatistics) {
        return (CollectionStatistics) stat;
      }
    }

    CollectionStatistics s;
    ScoringContext sc = ContextFactory.createContext(globalParameters);
    BaseIterator structIterator = createIterator(new Parameters(), root, sc);

    // first check if this iterator is an aggregate iterator (has direct access to stats)
    if (CollectionAggregateIterator.class.isInstance(structIterator)) {
      s = ((CollectionAggregateIterator) structIterator).getStatistics();

    } else if (structIterator instanceof LengthsIterator) {
      LengthsIterator iterator = (LengthsIterator) structIterator;
      s = new CollectionStatistics();
      s.fieldName = root.toString();
      s.minLength = Integer.MAX_VALUE;

      while (!iterator.isDone()) {
        sc.document = iterator.currentCandidate();
        if (iterator.hasMatch(iterator.currentCandidate())) {
          int len = iterator.getCurrentLength();
          s.collectionLength += len;
          s.documentCount += 1;
          s.nonZeroLenDocCount += (len > 0) ? 1 : 0;
          s.maxLength = Math.max(s.maxLength, len);
          s.minLength = Math.min(s.minLength, len);
        }
        iterator.movePast(sc.document);
      }

      s.avgLength = (s.documentCount > 0) ? (double) s.collectionLength / (double) s.documentCount : 0;
      s.minLength = (s.documentCount > 0) ? s.minLength : 0;
      return s;
    } else {
      throw new IllegalArgumentException("Node " + root.toString() + " is not a lengths iterator.");
    }

    if (cache!= null && cache.cacheStats) {
      cache.addToCache(rootString, s);
    }

    return s;
  }

  @Override
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    NodeStatistics ns = getNodeStatistics(root);
    return ns;
  }

  @Override
  public NodeStatistics getNodeStatistics(Node root) throws Exception {

    String rootString = root.toString();
    if (cache!= null && cache.cacheStats) {
      AggregateStatistic stat = cache.getCachedStatistic(rootString);
      if (stat != null && stat instanceof NodeStatistics) {
        return (NodeStatistics) stat;
      }
    }


    NodeStatistics s;
    ScoringContext sc = ContextFactory.createContext(globalParameters);
    BaseIterator structIterator = createIterator(new Parameters(), root, sc);
    if (NodeAggregateIterator.class.isInstance(structIterator)) {
      s = ((NodeAggregateIterator) structIterator).getStatistics();

    } else if (structIterator instanceof CountIterator) {

      s = new NodeStatistics();
      // set up initial values
      s.node = root.toString();
      s.nodeDocumentCount = 0;
      s.nodeFrequency = 0;
      s.maximumCount = 0;

      CountIterator iterator = (CountIterator) structIterator;

      while (!iterator.isDone()) {
        sc.document = iterator.currentCandidate();
        if (iterator.hasMatch(iterator.currentCandidate())) {
          s.nodeFrequency += iterator.count();
          s.maximumCount = Math.max(iterator.count(), s.maximumCount);
          s.nodeDocumentCount++;
        }
        iterator.movePast(iterator.currentCandidate());
      }

      return s;
    } else {
      // otherwise :
      throw new IllegalArgumentException("Node " + root.toString() + " is not a count iterator.");
    }

    if (cache!= null && cache.cacheStats) {
      cache.addToCache(rootString, s);
    }

    return s;
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
//    } else if (LengthsIterator.class.isAssignableFrom(outputClass)) {
//      return QueryType.LENGTH;
    } else {
      return QueryType.RANKED;
    }
  }

  @Override
  public Integer getDocumentLength(Integer docid) throws IOException {
    return index.getLength(docid);
  }

  @Override
  public Integer getDocumentLength(String docname) throws IOException {
    return index.getLength(index.getIdentifier(docname));
  }

  @Override
  public String getDocumentName(Integer docid) throws IOException {
    return index.getName(docid);
  }

  public Integer getDocumentId(String docname) throws IOException {
    return index.getIdentifier(docname);
  }

  public List<Integer> getDocumentIds(List<String> docnames) throws IOException {
    ArrayList<Integer> internalDocBuffer = new ArrayList<Integer>();

    for (String name : docnames) {
      try {
        internalDocBuffer.add(index.getIdentifier(name));
      } catch (Exception e) {
        // ignore missing document-names (they could be from other index shards)
      }
    }
    return internalDocBuffer;
  }

  @Override
  public void addNodeToCache(Node node) throws Exception {
    if (cache != null) {
      cache.addToCache(node, this.createIterator(new Parameters(), node, new ScoringContext()));
    }
  }

  @Override
  public void addAllNodesToCache(Node node) throws Exception {
    if (cache != null) {
      // recursivly add all nodes
      for (Node child : node.getInternalNodes()) {
        addAllNodesToCache(child);
      }

      cache.addToCache(node, this.createIterator(new Parameters(), node, new ScoringContext()));
    }
  }
}
