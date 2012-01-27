// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import gnu.trove.list.array.TIntArrayList;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.NamesReader.Iterator;
import org.lemurproject.galago.core.index.disk.CachedDiskIndex;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.iterator.AbstractIndicator;
import org.lemurproject.galago.core.retrieval.iterator.ContextualIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.iterator.CountValueIterator;
import org.lemurproject.galago.core.retrieval.structured.ScoringContext;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoringFunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.structured.ContextFactory;
import org.lemurproject.galago.core.retrieval.structured.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.structured.WorkingSetContext;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * The responsibility of the LocalRetrieval object is to
 * provide a simpler interface on top of the DiskIndex.
 * Therefore, given a query or text string representing a query,
 * this object will perform the necessary transformations to make
 * it an executable object.
 *
 * 10/7/2010 - Modified for asynchronous execution
 *
 * @author trevor
 * @author irmarc
 */
public class LocalRetrieval implements Retrieval {

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

  public LocalRetrieval(Index index, Parameters parameters) throws IOException {
    this.globalParameters = parameters;
    setIndex(index);
  }

  /** 
   * For this constructor, being sent a filename path to the indicies, 
   * we first list out all the directories in the path. If there are none, then 
   * we can safely assume that the filename specifies a single index (the files 
   * listed are all parts), otherwise we will treat each subdirectory as a 
   * separate logical index.
   */
  public LocalRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException, Exception {
    this.globalParameters = parameters;
    if (globalParameters.containsKey("cacheQueries")) {
      CachedDiskIndex cachedIndex = new CachedDiskIndex(filename);
      setIndex(cachedIndex);

      if (globalParameters.isList("cacheQueries", Type.STRING)) {
        List<String> queries = globalParameters.getAsList("cacheQueries");
        for (String q : queries) {
          Node queryTree = StructuredQuery.parse(q);
          queryTree = transformQuery(queryTree);
          cachedIndex.cacheQueryData(queryTree);
        }
      } else if (globalParameters.isList("cacheQueries", Type.MAP)) {
        List<Parameters> queries = globalParameters.getAsList("cacheQueries");
        for (Parameters q : queries) {
          Node queryTree = StructuredQuery.parse(q.getString("text"));
          queryTree = transformQuery(queryTree);
          cachedIndex.cacheQueryData(queryTree);
        }
      } else {
        Logger.getLogger(this.getClass().getName()).info("Could not process cachedQueries list. No posting list data cached.");
      }
    } else {
      setIndex(new DiskIndex(filename));
    }
  }

  private void setIndex(Index indx) throws IOException {
    this.index = indx;

    // Handle parameters for this index (since some of these can be different)
    features = new FeatureFactory(globalParameters);
  }

  /**
   * Closes the underlying index
   */
  public void close() throws IOException {
    index.close();
  }

  /**
   * Returns the collectionLength and documentCount of a given index, contained
   * in a Parameters object. Additional statistics may be provided, but are not
   * expected.
   */
  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException {
    return index.getCollectionStatistics(partName);
  }

  public CollectionStatistics getRetrievalStatistics() throws IOException {
    return index.getCollectionStatistics();
  }

  public Parameters getGlobalParameters() {
    return this.globalParameters;
  }

  /*
   * {
   *  <partName> : { <nodeName> : <iteratorClass>, stemming : false, ... },
   *  <partName> : { <nodeName> : <iteratorClass>, ... },
   *  ...
   * }
   */
  public Parameters getAvailableParts() throws IOException {
    Parameters p = new Parameters();
    ArrayList<String> parts = new ArrayList<String>();
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

  @Override
  public Document getDocument(String identifier) throws IOException {
    return this.index.getDocument(identifier);
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifier) throws IOException {
    return this.index.getDocuments(identifier);
  }

  /**
   * Accepts a transformed query, constructs the iterator tree from the node tree,
   * then iterates over the iterator tree, and returns the results.
   */
  public ScoredDocument[] runQuery(String query, Parameters p) throws Exception {
    Node root = StructuredQuery.parse(query);
    root = transformQuery(root);
    return runQuery(root, p);
  }

  public ScoredDocument[] runQuery(Node queryTree) throws Exception {
    return runQuery(queryTree, new Parameters());
  }

  // Based on the root of the tree, that dictates how we execute.
  public ScoredDocument[] runQuery(Node queryTree, Parameters p) throws Exception {
    ScoredDocument[] results = null;
    switch (this.getQueryType(queryTree)) {
      case RANKED:
        results = runRankedQuery(queryTree, p);
        break;
      case BOOLEAN:
        results = runBooleanQuery(queryTree, p, new ScoringContext());
        break;
    }
    if (results == null) {
      results = new ScoredDocument[0];
    }
    return results;
  }

  public ScoredDocument[] runQuery(Node queryTree, Parameters p, TIntArrayList workingSet) throws Exception {
    ScoredDocument[] results = null;
    WorkingSetContext wsc = new WorkingSetContext();
    wsc.workingSet = workingSet;
    switch (this.getQueryType(queryTree)) {
      case RANKED:
        results = runRankedQuery(queryTree, p, wsc);
        break;
      case BOOLEAN:
        results =
                runBooleanQuery(queryTree, p, wsc);
        break;
    }

    if (results == null) {
      results = new ScoredDocument[0];
    }

    return results;
  }

  private ScoredDocument[] runBooleanQuery(Node queryTree, Parameters parameters, ScoringContext ctx) throws Exception {

    // construct the query iterators
    AbstractIndicator iterator = (AbstractIndicator) createIterator(queryTree, ctx);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();
    while (!iterator.isDone()) {
      if (iterator.hasMatch(iterator.currentCandidate())) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
      iterator.next();
    }
    return list.toArray(new ScoredDocument[0]);
  }

  private ScoredDocument[] runBooleanQuery(Node queryTree, Parameters parameters, WorkingSetContext ctx) throws Exception {
    TIntArrayList workingSet = ctx.workingSet;

    if (workingSet == null) {
      return runBooleanQuery(queryTree, parameters, new ScoringContext());
    }

    // have to be sure
    workingSet.sort();

    // construct the query iterators
    AbstractIndicator iterator = (AbstractIndicator) createIterator(queryTree, ctx);
    ArrayList<ScoredDocument> list = new ArrayList<ScoredDocument>();

    for (int i = 0; i < workingSet.size(); i++) {
      int document = workingSet.get(i);
      iterator.moveTo(document);
      if (iterator.hasMatch(document)) {
        list.add(new ScoredDocument(iterator.currentCandidate(), 1.0));
      }
    }
    return list.toArray(new ScoredDocument[0]);
  }

  /**
   * Evaluates a probabilistic query using document-at-a-time evaluation, producing ranked passages instead of documents.
   * @param query A query tree that has been already transformed with LocalRetrieval.transformRankedQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return The top k results
   * @throws java.lang.Exception
   */
  private ScoredDocument[] runRankedPassageQuery(Node queryTree, Parameters parameters, ScoringContext ctx) throws Exception {

    // Following operations are all just setup
    int requested = (int) parameters.get("requested", 1000);
    int passageSize = (int) parameters.getLong("passageSize");
    int passageShift = (int) parameters.getLong("passageShift");
    PassageScoringContext context = (PassageScoringContext) ctx;
    ScoreValueIterator iterator = (ScoreValueIterator) createIterator(queryTree, context);
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.skipToKey(document);
      int length = lengthsIterator.getCurrentLength();

      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      context.begin = 0;
      context.end = passageSize;

      // Keep iterating over the same doc, but incrementing the begin/end fields of the
      // context until the next one
      while (context.end <= length) {
        if (iterator.hasMatch(document)) {
          double score = iterator.score();
          if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
            ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
            queue.add(scored);
            if (requested > 0 && queue.size() > requested) {
              queue.poll();
            }
          }
        }

        // Move the window forward
        context.begin += passageShift;
        context.end += passageShift;
      }
      iterator.next();
    }
    String indexId = parameters.get("indexId", "0");
    return getArrayResults(queue, indexId);
  }

  /**
   * Evaluates a probabilistic query using document-at-a-time evaluation.
   *
   * @param query A query tree that has been already transformed with LocalRetrieval.transformRankedQuery.
   * @param parameters - query parameters (indexId, # requested, query type, transform)
   * @return The top k results
   * @throws java.lang.Exception
   */
  private ScoredDocument[] runRankedQuery(Node queryTree, Parameters parameters) throws Exception {
    ScoringContext ctx = ContextFactory.createContext(parameters);
    if (PassageScoringContext.class.isAssignableFrom(ctx.getClass())) {
      return runRankedPassageQuery(queryTree, parameters, ctx);
    } else {
      return runRankedDocumentQuery(queryTree, parameters, ctx);
    }
  }

  private ScoredDocument[] runRankedDocumentQuery(Node queryTree, Parameters parameters, ScoringContext context) throws Exception {

    // Following operations are all just setup
    int requested = (int) parameters.get("requested", 1000);

    ScoreValueIterator iterator = (ScoreValueIterator) createIterator(queryTree, context);

    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      lengthsIterator.skipToKey(document);
      int length = lengthsIterator.getCurrentLength();
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      if (iterator.hasMatch(document)) {
        double score = iterator.score();
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          queue.add(scoredDocument);
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
          }
        }
      }
      iterator.next();
    }
    String indexId = parameters.get("indexId", "0");
    return getArrayResults(queue, indexId);
  }

  private ScoredDocument[] runRankedQuery(Node queryTree, Parameters parameters, WorkingSetContext context) throws Exception {

    TIntArrayList workingSet = context.workingSet;

    if (workingSet == null) {
      return runRankedQuery(queryTree, parameters);
    }

    // have to be sure
    workingSet.sort();

    // construct the query iterators
    ScoreValueIterator iterator = (ScoreValueIterator) createIterator(queryTree, context);
    int requested = (int) parameters.get("requested", 1000);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
    LengthsReader.Iterator lengthsIterator = index.getLengthsIterator();

    for (int i = 0; i < workingSet.size(); i++) {
      int document = workingSet.get(i);
      iterator.moveTo(document);
      lengthsIterator.skipToKey(document);
      int length = lengthsIterator.getCurrentLength();
      // This context is shared among all scorers
      context.document = document;
      context.length = length;
      double score = iterator.score();
      if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);
        queue.add(scoredDocument);
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
        }
      }
    }
    String indexId = parameters.get("indexId", "0");
    return getArrayResults(queue, indexId);
  }

  /*
   * getArrayResults annotates a queue of scored documents
   * returns an array
   *
   */
  protected <T extends ScoredDocument> T[] getArrayResults(PriorityQueue<T> scores, String indexId) throws IOException {
    if (scores.size() == 0) {
      return null;
    }
    T[] results = (T[]) Array.newInstance(scores.peek().getClass(), scores.size());

    for (int i = scores.size() - 1; i >= 0; i--) {
      results[i] = scores.poll();
      results[i].source = indexId;
      results[i].rank = i + 1;
    }

    // create a new list of scored documents
    ArrayList<T> docIds = new ArrayList(Arrays.asList(results));
    // sort the scoreddocuments into docid order
    Collections.sort(docIds, new Comparator<T>() {

      @Override
      public int compare(T o1, T o2) {
        return Utility.compare(o1.document, o2.document);
      }
    });
    // TODO: fix this to use an iterator.
    Iterator namesIterator = index.getNamesIterator();

    for (T doc : docIds) {
      namesIterator.skipToKey(doc.document);
      if (doc.document == namesIterator.getCurrentIdentifier()) {
        doc.documentName = namesIterator.getCurrentName();
      } else {
        System.err.println("NAMES ITERATOR FAILED TO FIND DOCUMENT " + doc.document);
        doc.documentName = index.getName(doc.document);
      }
    }

    return results;
  }

  public StructuredIterator createIterator(Node node, ScoringContext context) throws Exception {
    HashMap<String, StructuredIterator> iteratorCache = new HashMap();
    return createNodeMergedIterator(node, context, iteratorCache);
  }

  protected StructuredIterator createNodeMergedIterator(Node node, ScoringContext context,
          HashMap<String, StructuredIterator> iteratorCache)
          throws Exception {
    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();
    StructuredIterator iterator;

    // first check if the cache contains this node
    if (globalParameters.get("shareNodes", false)
            && iteratorCache.containsKey(node.toString())) {
      return iteratorCache.get(node.toString());
    }

    for (Node internalNode : node.getInternalNodes()) {
      StructuredIterator internalIterator = createNodeMergedIterator(internalNode, context, iteratorCache);
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
    if (globalParameters.get("shareNodes", false)) {
      iteratorCache.put(node.toString(), iterator);
    }
    return iterator;
  }

  @Override
  public Node transformQuery(Node queryTree) throws Exception {
    return transformQuery(features.getTraversals(this), queryTree);
  }

  private Node transformQuery(List<Traversal> traversals, Node queryTree) throws Exception {
    for (Traversal traversal : traversals) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
    }
    return queryTree;
  }

  public NodeStatistics nodeStatistics(String nodeString) throws Exception {
    // first parse the node
    Node root = StructuredQuery.parse(nodeString);
    root.getNodeParameters().set("queryType", "count");
    root = transformQuery(root);
    return nodeStatistics(root);
  }

  public NodeStatistics nodeStatistics(Node root) throws Exception {
    NodeStatistics stats = new NodeStatistics();
    // set up initial values
    stats.node = root.toString();
    stats.nodeDocumentCount = 0;
    stats.nodeFrequency = 0;
    stats.collectionLength = getRetrievalStatistics().collectionLength;
    stats.documentCount = getRetrievalStatistics().documentCount;

    StructuredIterator structIterator = createIterator(root, null);
    if (AggregateIterator.class.isInstance(structIterator)) {
      stats = ((AggregateIterator) structIterator).getStatistics();
    } else if (structIterator instanceof CountIterator) {
      CountValueIterator iterator = (CountValueIterator) structIterator;
      while (!iterator.isDone()) {
        if (iterator.hasMatch(iterator.currentCandidate())) {
          stats.nodeFrequency += iterator.count();
          stats.nodeDocumentCount++;
        }
        iterator.next();
      }
    } else {
      throw new IllegalArgumentException("Node " + root.toString() + " did not return a counting iterator.");
    }
    return stats;
  }

  public NodeType getNodeType(Node node) throws Exception {
    NodeType nodeType = index.getNodeType(node);
    if (nodeType == null) {
      nodeType = features.getNodeType(node);
    }
    return nodeType;
  }

  public QueryType getQueryType(Node node) throws Exception {
    if (node.getOperator().equals("text")) {
      return QueryType.UNKNOWN;
    }
    NodeType nodeType = getNodeType(node);
    Class outputClass = nodeType.getIteratorClass();
    if (ScoreIterator.class.isAssignableFrom(outputClass)
            || ScoringFunctionIterator.class.isAssignableFrom(outputClass)) {
      return QueryType.RANKED;
    } else if (AbstractIndicator.class.isAssignableFrom(outputClass)) {
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
}
