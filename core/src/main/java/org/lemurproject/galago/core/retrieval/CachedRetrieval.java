/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.util.HashMap;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.index.mem.*;
import org.lemurproject.galago.core.retrieval.iterator.*;
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
  protected HashMap<String, String> nodeCache;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public CachedRetrieval(Index index) throws Exception {
    this(index, new Parameters());
  }

  public CachedRetrieval(Index index, Parameters parameters) throws Exception {
    super(index, parameters);

    this.nodeCache = new HashMap();

    this.cacheParts = new HashMap();
    this.cacheParts.put("score", new MemorySparseFloatIndex(new Parameters()));
    this.cacheParts.put("extent", new MemoryWindowIndex(new Parameters()));
    this.cacheParts.put("count", new MemoryCountIndex(new Parameters()));
    // this.cacheParts.put("names", new MemoryDocumentNames(new Parameters()));
    // this.cacheParts.put("lengths", new MemoryDocumentLengths(new Parameters()));
  }

  @Override
  public StructuredIterator createIterator(Parameters queryParameters, Node node, ScoringContext context) throws Exception {
    String nodeString = node.toString();
    if (nodeCache.containsKey(nodeString)) {
      ValueIterator iterator = cacheParts.get(nodeCache.get(nodeString)).getIterator(Utility.fromString(nodeString));
      if (iterator instanceof ContextualIterator) {
        ((ContextualIterator) iterator).setContext(context);
      }
      return iterator;
    }
    return super.createIterator(queryParameters, node, context);
  }

  /**
   * caches an arbitrary query node currently can store only count, extent, and
   * score iterators.
   */
  public void cacheIterator(Node node) throws Exception {
    StructuredIterator iterator = super.createIterator(new Parameters(), node, new ScoringContext());

    String nodeString = node.toString();
    if (!nodeCache.containsKey(nodeString)) {
      if (iterator instanceof MovableScoreIterator) {
        nodeCache.put(nodeString, "score");
        cacheParts.get("score").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
      } else if (iterator instanceof MovableExtentIterator) {
        nodeCache.put(nodeString, "extent");
        cacheParts.get("extent").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
      } else if (iterator instanceof MovableCountIterator) {
        nodeCache.put(nodeString, "count");
        cacheParts.get("count").addIteratorData(Utility.fromString(nodeString), (MovableIterator) iterator);
      } else {
        Logger.getLogger(this.getClass().getName()).info("Unable to cache node : " + nodeString);
      }
    }
  }
}
