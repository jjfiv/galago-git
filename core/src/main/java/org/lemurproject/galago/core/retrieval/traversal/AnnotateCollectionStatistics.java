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
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
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
 *  - collectionProbability : nodeFrequency / collectionLength
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
    this.availableStatistics.add("collectionLength");
    this.availableStatistics.add("documentCount");
    this.availableStatistics.add("vocabCount");
    this.availableStatistics.add("nodeFrequency");
    this.availableStatistics.add("nodeDocumentCount");
    this.availableStatistics.add("collectionProbability");
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
    if (reqStats.contains("nodeFrequency")
            || reqStats.contains("nodeDocumentCount")
            || reqStats.contains("collectionProbability")) {

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

  private NodeStatistics getNodeStatistics(Node node) throws Exception {
    // recurses down a stick (single children nodes only)
    if (isCountNode(node)) {
      return collectStatistics(node);
    
    } else if (node.getInternalNodes().size() == 1) {
      return getNodeStatistics(node.getInternalNodes().get(0));

    } else if (node.getInternalNodes().size() == 2) {
      return getNodeStatistics(node.getInternalNodes().get(1));
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

  private NodeStatistics collectStatistics(Node countNode) throws Exception {
    // recursively check if any child nodes use a specific background part
    Node n = assignParts(countNode.clone());
    
    if(globalParameters.isString("backgroundIndex")){
      assert( GroupRetrieval.class.isAssignableFrom( retrieval.getClass())): "Retrieval object must be a GroupRetrieval to use the backgroundIndex parameter.";
      return ((GroupRetrieval) retrieval).nodeStatistics(n, globalParameters.getString("backgroundIndex"));

    } else {
      return retrieval.nodeStatistics(n);
    }
  }
  
  private Node assignParts(Node n){
    if(n.getInternalNodes().isEmpty()){

      // we should have a part by now.
      String part = n.getNodeParameters().getString("part");
      
      // check if there is a new background part to assign
      if(n.getNodeParameters().isString("backgroundPart")){
        n.getNodeParameters().set("part", n.getNodeParameters().getString("backgroundPart"));
        return n;
      } else if(globalParameters.isMap("backgroundPartMap")
              && globalParameters.getMap("backgroundPartMap").isString(part)){
        n.getNodeParameters().set("part", globalParameters.getMap("backgroundPartMap").getString(part));
        return n;
      }
      // otherwise no change.
      return n;

    } else { // has a child: assign parts to children:
      for(Node c : n.getInternalNodes()){
        assignParts(c);
      }
      return n;
    }
  }

  private double computeCollectionProbability(long collectionCount, double collectionLength) {
    System.err.printf("Calculating collProb=(%d / %f) = %f\n",
            collectionCount, collectionLength, ((double) collectionCount / collectionLength));
    if (collectionCount > 0) {
      return ((double) collectionCount / collectionLength);
    } else {
      return (0.5 / (double) collectionLength);
    }
  }
}