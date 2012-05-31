/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.mem.*;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * The CacbedRetrieval object wraps a local retrieval object
 *
 * Data produced by iterators created by the retrieval may be cached
 *
 * @author sjh
 */
public class CachedRetrieval extends LocalRetrieval {

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

    this.cachedNodes = new HashMap();
    this.cachedStats = new HashMap();

    this.cacheParts = new HashMap();
    this.cacheParts.put("score", new MemorySparseDoubleIndex(new Parameters()));
    this.cacheParts.put("extent", new MemoryWindowIndex(new Parameters()));
    this.cacheParts.put("count", new MemoryCountIndex(new Parameters()));
    // this.cacheParts.put("names", new MemoryDocumentNames(new Parameters()));
    // this.cacheParts.put("lengths", new MemoryDocumentLengths(new Parameters()));
  }

  public CachedRetrieval(Index index, Parameters parameters) throws Exception {
    super(index, parameters);

    this.cachedNodes = new HashMap();
    this.cachedStats = new HashMap();

    this.cacheParts = new HashMap();
    this.cacheParts.put("score", new MemorySparseDoubleIndex(new Parameters()));
    this.cacheParts.put("extent", new MemoryWindowIndex(index.getIndexPart("postings").getManifest()));
    this.cacheParts.put("count", new MemoryCountIndex(index.getIndexPart("postings").getManifest()));
    // this.cacheParts.put("names", new MemoryDocumentNames(new Parameters()));
    // this.cacheParts.put("lengths", new MemoryDocumentLengths(new Parameters()));
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
      //Logger.getLogger(this.getClass().getName()).info("Getting cached iterator cache for node : " + nodeString);
      iterator = cacheParts.get(cachedNodes.get(nodeString)).getIterator(Utility.fromString(nodeString));
    } else {
      //Logger.getLogger(this.getClass().getName()).info("Failed to get cached iterator cache for node : " + nodeString);
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

  /*
   * Checks if a particular node is cached or not.
   */
  public boolean isCached(Node node) {
    String nodeString = node.toString();
    return cachedNodes.containsKey(nodeString);
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
        cachedNodes.put(nodeString, "score");
        cacheParts.get("score").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
        // Logger.getLogger(this.getClass().getName()).info("Cached scoring node : " + nodeString);

      } else if (iterator instanceof MovableExtentIterator) {
        NodeStatistics ns = super.nodeStatistics(node);
        cachedStats.put(nodeString, ns);
        cachedNodes.put(nodeString, "extent");
        cacheParts.get("extent").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
        // Logger.getLogger(this.getClass().getName()).info("Cached extent node : " + nodeString);

      } else if (iterator instanceof MovableCountIterator) {
        NodeStatistics ns = super.nodeStatistics(node);
        cachedStats.put(nodeString, ns);
        cachedNodes.put(nodeString, "count");
        cacheParts.get("count").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
        // Logger.getLogger(this.getClass().getName()).info("Cached count node : " + nodeString);

      } else {
        // Logger.getLogger(this.getClass().getName()).info("Unable to cache node : " + nodeString);
      }
    } else {
      // Logger.getLogger(this.getClass().getName()).info("Already cached node : " + nodeString);
    }
  }

  @Override
  public NodeStatistics nodeStatistics(Node node) throws Exception {
    // check the node cache first - this will avoid zeros.
    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      //Logger.getLogger(this.getClass().getName()).info("Getting stats from cache for node : " + nodeString);
      return this.cachedStats.get(nodeString);
    }
    return super.nodeStatistics(node);
  }
}
