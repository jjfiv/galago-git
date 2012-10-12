/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.mem.*;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Parameters.Type;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * The CacbedRetrieval object wraps a local retrieval object
 *
 * Data produced by iterators created by the retrieval may be cached
 *
 * @author sjh
 */
public class CachedRetrieval extends LocalRetrieval {

  // scores are risky to cache -> dirichlet smoothed scores depend on the length of the document.
  protected boolean cacheScores;
  protected HashMap<String, MemoryIndexPart> cacheParts;
  protected HashMap<String, String> cachedNodes;
  protected HashMap<String, NodeStatistics> cachedStats;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public CachedRetrieval(Index index) throws Exception {
    this(index, new Parameters());
  }

  public CachedRetrieval(String filename, Parameters parameters)
          throws FileNotFoundException, IOException, Exception {
    super(filename, parameters);

    init();
  }

  public CachedRetrieval(Index index, Parameters parameters) throws Exception {
    super(index, parameters);

    init();
  }

  private void init() throws Exception {
    
    // default behaviour is not to cache scores - as mentioned above dirichlet scores carry some risk
    this.cacheScores = this.globalParameters.get("cacheScores", false);

    this.cachedNodes = new HashMap();
    this.cachedStats = new HashMap();
    this.cacheParts = new HashMap();

    this.cacheParts.put("score", new MemorySparseDoubleIndex(new Parameters()));
    this.cacheParts.put("extent", new MemoryWindowIndex(new Parameters()));
    this.cacheParts.put("count", new MemoryCountIndex(new Parameters()));
    // this.cacheParts.put("names", new MemoryDocumentNames(new Parameters()));
    this.cacheParts.put("lengths", new MemoryDocumentLengths(new Parameters()));

    if (globalParameters.containsKey("cacheQueries")) {
      if (globalParameters.isList("cacheQueries", Type.STRING)) {
        List<String> queries = globalParameters.getAsList("cacheQueries");
        for (String q : queries) {
          Node queryTree = StructuredQuery.parse(q);
          queryTree = transformQuery(queryTree, new Parameters());
          addAllToCache(queryTree);
        }
      } else if (globalParameters.isList("cacheQueries", Type.MAP)) {
        List<Parameters> queries = globalParameters.getAsList("cacheQueries");
        for (Parameters q : queries) {
          Node queryTree = StructuredQuery.parse(q.getString("text"));
          queryTree = transformQuery(queryTree, new Parameters());
          addAllToCache(queryTree);
        }
      } else {
        logger.info("Could not process cachedQueries as a list<String> or list<Parameters>. No data cached.");
      }
    }
  }

  @Override
  protected StructuredIterator createNodeMergedIterator(Node node, ScoringContext context,
          HashMap<String, StructuredIterator> queryIteratorCache)
          throws Exception {

    ArrayList<StructuredIterator> internalIterators = new ArrayList<StructuredIterator>();
    StructuredIterator iterator;

    // first check if the query cache already contains this iterator
    if (queryIteratorCache != null && queryIteratorCache.containsKey(node.toString())) {
      return queryIteratorCache.get(node.toString());
    }

    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      // new behaviour - check cache for this node.
      //logger.info("Getting cached iterator cache for node : " + nodeString);
      iterator = cacheParts.get(cachedNodes.get(nodeString)).getIterator(Utility.fromString(nodeString));
    } else {
      //logger.info("Failed to get cached iterator cache for node : " + nodeString);
      // otherwise create iterator
      for (Node internalNode : node.getInternalNodes()) {
        StructuredIterator internalIterator = createNodeMergedIterator(internalNode, context, queryIteratorCache);
        internalIterators.add(internalIterator);
      }

      iterator = index.getIterator(node);
      if (iterator == null) {
        iterator = features.getIterator(node, internalIterators);
      }
    }

    // add a context if necessary
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
  public NodeStatistics getNodeStatistics(Node node) throws Exception {
    // check the node cache first - this will avoid zeros.
    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      //logger.info("Getting stats from cache for node : " + nodeString);
      return this.cachedStats.get(nodeString);
    }
    return super.getNodeStatistics(node);
  }

  // caching functions
  /*
   * Checks if a particular node is cached or not.
   */
  public boolean isCached(Node node) {
    String nodeString = node.toString();
    return cachedNodes.containsKey(nodeString);
  }

  /*
   * Recurses through a query tree to cache all nodes present
   */
  public void addAllToCache(Node queryTree) throws Exception {
    for (Node child : queryTree.getInternalNodes()) {
      addAllToCache(child);
    }
    addToCache(queryTree);
  }

  /**
   * caches an arbitrary query node currently can store only count, extent, and
   * score iterators.
   */
  public void addToCache(Node node) throws Exception {
    ScoringContext sc = new ScoringContext();
    StructuredIterator iterator = super.createIterator(new Parameters(), node, sc);
    ProcessingModel.initializeLengths(this, sc);


    String nodeString = node.toString();
    if (!cachedNodes.containsKey(nodeString)) {
      if (iterator instanceof MovableScoreIterator) {
        if(this.cacheScores){
          cachedNodes.put(nodeString, "score");
          cacheParts.get("score").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
          // logger.info("Cached scoring node : " + nodeString);
        } else {
          // logger.info("Scoring node are not cachable : " + nodeString);
        }
        
      } else if (iterator instanceof MovableLengthsIterator) {
        cacheParts.get("lengths").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
      
      } else if (iterator instanceof MovableExtentIterator) {
        NodeStatistics ns = super.getNodeStatistics(node);
        cachedStats.put(nodeString, ns);
        cachedNodes.put(nodeString, "extent");
        cacheParts.get("extent").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
        // logger.info("Cached extent node : " + nodeString);
        
      } else if (iterator instanceof MovableCountIterator) {
        NodeStatistics ns = super.getNodeStatistics(node);
        cachedStats.put(nodeString, ns);
        cachedNodes.put(nodeString, "count");
        cacheParts.get("count").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
        // logger.info("Cached count node : " + nodeString);

      } else {
        // logger.info("Unable to cache node : " + nodeString);
      }
    } else {
      // logger.info("Already cached node : " + nodeString);
    }
  }

  public void removeFromCache(Node node) throws Exception {
    ScoringContext sc = new ScoringContext();

    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      if (cachedNodes.get(nodeString).equals("score")) {
        cachedNodes.remove(nodeString);
        cacheParts.get("score").removeIteratorData(Utility.fromString(nodeString));
        // logger.info("Deleted cached scoring node : " + nodeString);
      } else if (cachedNodes.get(nodeString).equals("count")) {
        NodeStatistics ns = super.getNodeStatistics(node);
        cachedNodes.remove(nodeString);
        cachedStats.remove(nodeString);
        cacheParts.get("extent").removeIteratorData(Utility.fromString(nodeString));
        // logger.info("Deleted cached extent node : " + nodeString);

      } else if (cachedNodes.get(nodeString).equals("extent")) {
        NodeStatistics ns = super.getNodeStatistics(node);
        cachedNodes.remove(nodeString);
        cachedStats.remove(nodeString);
        cacheParts.get("count").removeIteratorData(Utility.fromString(nodeString));
        // logger.info("Deleted cached count node : " + nodeString);

      } else {
        // logger.info("Unable to delete cached node : " + nodeString);
      }
    } else {
      // logger.info("Ignoring non-cached node : " + nodeString);
    }
  }
}
