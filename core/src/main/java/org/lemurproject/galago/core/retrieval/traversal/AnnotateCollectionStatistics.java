// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.HashSet;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;

/**
 * When performing distributed retrieval it is necessary to collect
 * correct collection statistics.
 *
 * @author sjh
 */
public class AnnotateCollectionStatistics implements Traversal {

  HashSet<String> availiableStatistics;
  Retrieval retrieval;

  // featurefactory is necessary to get the correct class
  public AnnotateCollectionStatistics(Retrieval retrieval) throws IOException {
    this.retrieval = retrieval;
    this.availiableStatistics = new HashSet();
    this.availiableStatistics.add("collectionLength");
    this.availiableStatistics.add("documentCount");
    this.availiableStatistics.add("vocabCount");
    this.availiableStatistics.add("nodeFrequency");
    this.availiableStatistics.add("nodeDocumentCount");
    this.availiableStatistics.add("collectionProbability");
  }

  public void beforeNode(Node node) {
  }

  public Node afterNode(Node node) throws Exception {
    // need to get list of required statistics
    RequiredStatistics required = null;
    Class<? extends StructuredIterator> c = retrieval.getNodeType(node).getIteratorClass();
    required = c.getAnnotation(RequiredStatistics.class);

    // then annotate the node with any of:
    // -- nodeFreq, nodeDocCount, collLen, docCount, collProb
    if (required != null) {
      HashSet<String> reqStats = new HashSet();
      for (String stat : required.statistics()) {
        if (availiableStatistics.contains(stat)) {
          reqStats.add(stat);
        }
      }
      if (!reqStats.isEmpty()) {
        annotate(node, reqStats);
      }
    }
    return node;
  }

  private void annotate(Node node, HashSet<String> reqStats) throws Exception {
    NodeParameters nodeParams = node.getNodeParameters();
    if (reqStats.contains("nodeFrequency")
            || reqStats.contains("nodeDocumentCount")
            || reqStats.contains("collectionProbability")) {
      Node countable = getCountableNode(node);
      if (countable == null) {
        return;
      }
      NodeStatistics stats = retrieval.nodeStatistics(countable);
      if (reqStats.contains("nodeFrequency")
              && !nodeParams.containsKey("nodeFrequency")) {
        nodeParams.set("nodeFrequency", stats.nodeFrequency);
      }
      if (reqStats.contains("nodeDocumentCount")
              && !nodeParams.containsKey("nodeDocumentCount")) {
        nodeParams.set("nodeDocumentCount", stats.nodeDocumentCount);
      }
      if (reqStats.contains("collectionProbability")
              && !nodeParams.containsKey("collectionProbability")) {
        nodeParams.set("collectionProbability", computeCollectionProbability(stats.nodeFrequency, stats.collectionLength));
      }
      if (reqStats.contains("collectionLength")
              && !nodeParams.containsKey("collectionLength")) {
        nodeParams.set("collectionLength", stats.collectionLength);
      }
      if (reqStats.contains("documentCount")
              && !nodeParams.containsKey("documentCount")) {
        nodeParams.set("documentCount", stats.documentCount);
      }
    } else {
      // this should be for the correct index part: if possible.
      CollectionStatistics stats = retrieval.getRetrievalStatistics();
      if (reqStats.contains("collectionLength")
              && !nodeParams.containsKey("collectionLength")) {
        nodeParams.set("collectionLength", stats.collectionLength);
      }
      if (reqStats.contains("documentCount")
              && !nodeParams.containsKey("documentCount")) {
        nodeParams.set("documentCount", stats.documentCount);
      }
      if (reqStats.contains("vocabCount")
              && !nodeParams.containsKey("vocabCount")) {
        nodeParams.set("vocabCount", stats.vocabCount);
      }
    }
  }

  private Node getCountableNode(Node node) throws Exception {
    if (isCountNode(node)) {
      return node;
    }
    // search through chain of single child nodes looking for a countable node:
    while (node.getInternalNodes().size() == 1) {
      Node child = node.getInternalNodes().get(0);
      if (isCountNode(child)) {
        return checkBackgroundLM(child.clone());
      }
      node = child;
    }
    return null;
  }

  private boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  private double computeCollectionProbability(long collectionCount, double collectionLength) {
    if (collectionCount > 0) {
      return ((double) collectionCount / collectionLength);
    } else {
      return (0.5 / (double) collectionLength);
    }
  }

  private Node checkBackgroundLM(Node countNode) {
    for(Node child : countNode.getInternalNodes()){
      checkBackgroundLM(child);
    }
    NodeParameters np = countNode.getNodeParameters();
    if(np.isString("lm")){
      np.set("part", np.getString("lm"));
    }
    return countNode;
  }
}
