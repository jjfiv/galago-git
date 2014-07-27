/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.mem.*;
import org.lemurproject.galago.core.index.stats.AggregateStatistic;
import org.lemurproject.galago.core.retrieval.iterator.*;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The CacbedRetrieval object exists in a retrieval - it allows in-memory
 * caching of node iterators - particularly useful for caching complex nodes for
 * repeated querying, as in parameter tuning
 *
 * @author sjh
 */
public class CachedRetrieval {

  protected Parameters parameters;
  // scores are risky to cache -> dirichlet smoothed scores depend on the length of the document.
  protected boolean cacheScores;
  protected boolean cacheLeafNodes;
  protected boolean cacheStats;
  protected ConcurrentHashMap<String, MemoryIndexPart> cacheParts;
  protected ConcurrentHashMap<String, String> cachedNodes;
  protected ConcurrentHashMap<String, AggregateStatistic> cachedStats;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public CachedRetrieval(Parameters p) throws Exception {
    this.parameters = p;
    init();
  }

  private void init() throws Exception {

    // default behaviour is not to cache scores - as mentioned above dirichlet scores carry some risk
    this.cacheScores = this.parameters.get("cacheScores", false);
    this.cacheLeafNodes = this.parameters.get("cacheLeafNodes", true);
    this.cacheStats = this.parameters.get("cacheStats", false); // useful when we just need lots of stats, no real iterators

    this.cachedNodes = new ConcurrentHashMap();
    this.cachedStats = new ConcurrentHashMap();
    this.cacheParts = new ConcurrentHashMap();

    this.cacheParts.put("score", new MemorySparseDoubleIndex(Parameters.instance()));
    this.cacheParts.put("extent", new MemoryWindowIndex(Parameters.instance()));
    this.cacheParts.put("count", new MemoryCountIndex(Parameters.instance()));
    // this.cacheParts.put("names", new MemoryDocumentNames(Parameters.instance()));
    this.cacheParts.put("lengths", new MemoryDocumentLengths(Parameters.instance()));

  }

  public BaseIterator getCachedIterator(Node node) throws IOException {
    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      // new behaviour - check cache for this node.
      //logger.info("Getting cached iterator cache for node : " + nodeString);
      return cacheParts.get(cachedNodes.get(nodeString)).getIterator(ByteUtil.fromString(nodeString));
    } else {
      return null;
    }
  }

  // caching functions
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
  public void addToCache(Node node, BaseIterator iterator) throws Exception {

    String nodeString = node.toString();
    if (!cachedNodes.containsKey(nodeString)) {
      if (this.cacheLeafNodes || node.numChildren() > 0) {
        if (iterator instanceof ScoreIterator) {
          if (this.cacheScores) {
            cachedNodes.put(nodeString, "score");
            cacheParts.get("score").addIteratorData(ByteUtil.fromString(nodeString), iterator);
            // logger.info("Cached scoring node : " + nodeString);
          } else {
            // logger.info("Scoring node are not cachable : " + nodeString);
          }

        } else if (iterator instanceof LengthsIterator) {
          cachedNodes.put(nodeString, "lengths");
          cacheParts.get("lengths").addIteratorData(ByteUtil.fromString(nodeString), iterator);

        } else if (iterator instanceof ExtentIterator) {
          cachedNodes.put(nodeString, "extent");
          cacheParts.get("extent").addIteratorData(ByteUtil.fromString(nodeString), iterator);
          // logger.info("Cached extent node : " + nodeString);

        } else if (iterator instanceof CountIterator) {
          cachedNodes.put(nodeString, "count");
          cacheParts.get("count").addIteratorData(ByteUtil.fromString(nodeString), iterator);
          // logger.info("Cached count node : " + nodeString);

        } else {
          // logger.info("Unable to cache node : " + nodeString);
        }
      } else {
        // logger.info("Already cached node : " + nodeString);
      }
    }
  }

  public void removeFromCache(Node node) throws Exception {
    String nodeString = node.toString();
    if (cachedNodes.containsKey(nodeString)) {
      if (cachedNodes.get(nodeString).equals("score")) {
        cachedNodes.remove(nodeString);
        cacheParts.get("score").removeIteratorData(ByteUtil.fromString(nodeString));
        // logger.info("Deleted cached scoring node : " + nodeString);
      } else if (cachedNodes.get(nodeString).equals("count")) {
//        NodeStatistics ns = super.getNodeStatistics(node);
//        cachedStats.remove(nodeString);
        cachedNodes.remove(nodeString);
        cacheParts.get("extent").removeIteratorData(ByteUtil.fromString(nodeString));
        // logger.info("Deleted cached extent node : " + nodeString);

      } else if (cachedNodes.get(nodeString).equals("extent")) {
//        NodeStatistics ns = super.getNodeStatistics(node);
//        cachedStats.remove(nodeString);
        cachedNodes.remove(nodeString);
        cacheParts.get("count").removeIteratorData(ByteUtil.fromString(nodeString));
        // logger.info("Deleted cached count node : " + nodeString);

      } else {
        // logger.info("Unable to delete cached node : " + nodeString);
      }
    } else {
      // logger.info("Ignoring non-cached node : " + nodeString);
    }
  }

  /**
   * Stores several types of statistics:
   *  partName -> IndexPartStatistics
   *  lengthNode -> CollectionStatistics
   *  countNode -> NodeStatistics
   */
  public void addToCache(String key, AggregateStatistic stat) {
    if (!cachedNodes.containsKey(key)) {
      cachedStats.put(key, stat);
    } else {
      // logger.info(Ignoring non-cached node : " + key);
    }
  }

  public AggregateStatistic getCachedStatistic(String key) throws Exception {
    // check the node cache first - this will avoid zeros.
    if (cachedStats.containsKey(key)) {
      //logger.info("Getting stats from cache for node : " + nodeString);
      return this.cachedStats.get(key);
    }
    return null;
  }

}
