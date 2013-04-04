// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.HashSet;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.retrieval.GroupRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.iterator.CountIterator;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.structured.RequiredStatistics;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Class collects collections statistics:
 *  - collectionLength : number of terms in index part / collection
 *  - documentCount : number of documents in index part / collection
 *  - vocabCount : number of unique terms in index part
 *  - nodeFrequency : number of matching instances of node in index part / collection
 *  - nodeDocumentCount : number of matching documents for node in index part / collection
 *
 * @author sjh
 */
public class AnnotateCollectionStatistics extends Traversal {

  HashSet<String> availableStatistics;
  Parameters globalParameters;
  Retrieval retrieval;

  // featurefactory is necessary to get the correct class
  public AnnotateCollectionStatistics(Retrieval retrieval) throws IOException {
    this.globalParameters = retrieval.getGlobalParameters();
    this.retrieval = retrieval;

    this.availableStatistics = new HashSet();
    // field or document region statistics
    this.availableStatistics.add("collectionLength");
    this.availableStatistics.add("documentCount");
    this.availableStatistics.add("maxLength");
    this.availableStatistics.add("minLength");
    this.availableStatistics.add("avgLength");

    // countable-node statistics
    this.availableStatistics.add("nodeFrequency");
    this.availableStatistics.add("nodeDocumentCount");
  }

  public void beforeNode(Node node) {
  }

  public Node afterNode(Node node) throws Exception {
    // need to get list of required statistics
    RequiredStatistics required = null;
    Class<? extends MovableIterator> c = retrieval.getNodeType(node).getIteratorClass();
    required = c.getAnnotation(RequiredStatistics.class);

    // then annotate the node with any of:
    // -- nodeFreq, nodeDocCount, collLen, docCount, collProb
    if (required != null) {
      HashSet<String> reqStats = new HashSet();
      for (String stat : required.statistics()) {
        if (availableStatistics.contains(stat)) {
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

    if (reqStats.contains("collectionLength")
            || reqStats.contains("documentCount")
            || reqStats.contains("maxLength")
            || reqStats.contains("minLength")
            || reqStats.contains("avgLength")) {

      // extract field if possible:
      // use 'document' as the default context
      String field = node.getNodeParameters().get("lengths", "document");
      CollectionStatistics stats = getCollectionStatistics(field);

      if (reqStats.contains("collectionLength")
              && !nodeParams.containsKey("collectionLength")) {
        nodeParams.set("collectionLength", stats.collectionLength);
      }
      if (reqStats.contains("documentCount")
              && !nodeParams.containsKey("documentCount")) {
        nodeParams.set("documentCount", stats.documentCount);
      }
      if (reqStats.contains("maxLength")
              && !nodeParams.containsKey("maxLength")) {
        nodeParams.set("maxLength", stats.maxLength);
      }
      if (reqStats.contains("minLength")
              && !nodeParams.containsKey("minLength")) {
        nodeParams.set("minLength", stats.minLength);
      }
      if (reqStats.contains("avgLength")
              && !nodeParams.containsKey("avgLength")) {
        nodeParams.set("avgLength", stats.avgLength);
      }
    }

    if (reqStats.contains("nodeFrequency")
            || reqStats.contains("nodeDocumentCount")) {

      NodeStatistics stats = getNodeStatistics(node);
      if (stats == null) {
        return;
      }

      if (reqStats.contains("nodeFrequency")
              && !nodeParams.containsKey("nodeFrequency")) {
        nodeParams.set("nodeFrequency", stats.nodeFrequency);
      }
      if (reqStats.contains("nodeDocumentCount")
              && !nodeParams.containsKey("nodeDocumentCount")) {
        nodeParams.set("nodeDocumentCount", stats.nodeDocumentCount);
      }
    }
  }

  private CollectionStatistics getCollectionStatistics(String field) throws Exception {
    if (globalParameters.isString("backgroundIndex")) {
      assert (GroupRetrieval.class.isAssignableFrom(retrieval.getClass())) : "Retrieval object must be a GroupRetrieval to use the backgroundIndex parameter.";
      return ((GroupRetrieval) retrieval).getCollectionStatistics("#lengths:"+field+":part=lengths()", globalParameters.getString("backgroundIndex"));

    } else {
      return retrieval.getCollectionStatistics("#lengths:"+field+":part=lengths()");
    }
  }

  private NodeStatistics getNodeStatistics(Node node) throws Exception {
    // recurses down a stick (single children nodes only)
    if (isCountNode(node)) {
      return collectStatistics(node);

    } else if (node.numChildren() == 1) {
      return getNodeStatistics(node.getInternalNodes().get(0));

    } else if (node.numChildren() == 2) {
      return getNodeStatistics(node.getInternalNodes().get(1));
    }
    return null;
  }

  private NodeStatistics collectStatistics(Node countNode) throws Exception {
    // recursively check if any child nodes use a specific background part
    Node n = assignParts(countNode.clone());

    if (globalParameters.isString("backgroundIndex")) {
      assert (GroupRetrieval.class.isAssignableFrom(retrieval.getClass())) : "Retrieval object must be a GroupRetrieval to use the backgroundIndex parameter.";
      return ((GroupRetrieval) retrieval).getNodeStatistics(n, globalParameters.getString("backgroundIndex"));

    } else {
      return retrieval.getNodeStatistics(n);
    }
  }

  private boolean isCountNode(Node node) throws Exception {
    NodeType nodeType = retrieval.getNodeType(node);
    if (nodeType == null) {
      return false;
    }
    Class outputClass = nodeType.getIteratorClass();
    return CountIterator.class.isAssignableFrom(outputClass);
  }

  private Node assignParts(Node n) {
    if (n.getInternalNodes().isEmpty()) {

      // we should have a part by now.
      String part = n.getNodeParameters().getString("part");

      // check if there is a new background part to assign
      if (n.getNodeParameters().isString("backgroundPart")) {
        n.getNodeParameters().set("part", n.getNodeParameters().getString("backgroundPart"));
        return n;
      } else if (globalParameters.isMap("backgroundPartMap")
              && globalParameters.getMap("backgroundPartMap").isString(part)) {
        n.getNodeParameters().set("part", globalParameters.getMap("backgroundPartMap").getString(part));
        return n;
      }
      // otherwise no change.
      return n;

    } else { // has a child: assign parts to children:
      for (Node c : n.getInternalNodes()) {
        assignParts(c);
      }
      return n;
    }
  }
}