/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.core.retrieval.traversal.Traversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Provides a facility to map labels to an abstract retrieval. Therefore, if you
 * want a set of retrievals to be grouped under the "civil war" label, you can
 * tie them together using a MultiRetrieval, and list it here under "civil war".
 *
 * Note that the GroupRetrieval implementation has no facility to explicitly
 * group retrieval objects - that has been left to the MultiRetrieval class.
 *
 * @author sjh
 */
public class GroupRetrieval implements Retrieval {

  protected FeatureFactory features;
  protected Parameters globalParameters;
  protected String defGroup;
  protected Map<String, Retrieval> groups;
  protected List<Traversal> defaultTraversals;

  public GroupRetrieval(Map<String, Retrieval> groups, Parameters parameters,
          String defGroup) throws Exception {
    this.groups = groups;
    this.globalParameters = parameters;
    this.defGroup = defGroup;
    this.features = new FeatureFactory(globalParameters);
    defaultTraversals = features.getTraversals(this);
  }

  // IMPLEMENTED FUNCTIONS - Traversals use group-retrievals to collect aggregate stats
  public Collection<String> getGroups() {
    return groups.keySet();
  }

  @Override
  public Node transformQuery(Node queryTree, Parameters queryParams) throws Exception {
    for (Traversal traversal : defaultTraversals) {
      traversal.beforeTreeRoot(queryTree, queryParams);
      queryTree = StructuredQuery.walk(traversal, queryTree, queryParams);
      queryTree = traversal.afterTreeRoot(queryTree, queryParams);
    }
    return queryTree;
  }

  @Override
  public void close() throws IOException {
    for (Retrieval r : groups.values()) {
      r.close();
    }
  }

  // DEFAULT FORWARDED FUNCTIONS
  @Override
  public Parameters getGlobalParameters() {
    return groups.get(defGroup).getGlobalParameters();
  }

  @Override
  public Parameters getAvailableParts() throws IOException {
    return groups.get(defGroup).getAvailableParts();
  }

  @Override
  public Document getDocument(String identifier, DocumentComponents p) throws IOException {
    return groups.get(defGroup).getDocument(identifier, p);
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException {
    return groups.get(defGroup).getDocuments(identifier, p);
  }

  @Override
  public NodeType getNodeType(Node node) throws Exception {
    return groups.get(defGroup).getNodeType(node);
  }

  @Override
  public QueryType getQueryType(Node node) throws Exception {
    return groups.get(defGroup).getQueryType(node);
  }

  @Override
  public ScoredDocument[] runQuery(Node root) throws Exception {
    return groups.get(defGroup).runQuery(root);
  }

  @Override
  public ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception {
    if (parameters.isString("group")) {
      return groups.get(parameters.getString("group")).runQuery(root, parameters);
    }
    return groups.get(defGroup).runQuery(root, parameters);
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
    return groups.get(defGroup).getIndexPartStatistics(partName);
  }

  @Override
  public FieldStatistics getCollectionStatistics(String nodeString) throws Exception {
    return groups.get(defGroup).getCollectionStatistics(nodeString);
  }

  @Override
  public FieldStatistics getCollectionStatistics(Node node) throws Exception {
    return groups.get(defGroup).getCollectionStatistics(node);
  }

  @Override
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
    return groups.get(defGroup).getNodeStatistics(nodeString);
  }

  @Override
  public NodeStatistics getNodeStatistics(Node node) throws Exception {
    return groups.get(defGroup).getNodeStatistics(node);
  }

  @Override
  public Integer getDocumentLength(Integer docid) throws IOException {
    return groups.get(defGroup).getDocumentLength(docid);
  }

  @Override
  public Integer getDocumentLength(String docname) throws IOException {
    return groups.get(defGroup).getDocumentLength(docname);
  }

  @Override
  public String getDocumentName(Integer docid) throws IOException {
    return groups.get(defGroup).getDocumentName(docid);
  }

  @Override
  public void addNodeToCache(Node node) throws Exception {
    groups.get(defGroup).addNodeToCache(node);
  }

  @Override
  public void addAllNodesToCache(Node node) throws Exception {
    groups.get(defGroup).addAllNodesToCache(node);
  }

  // IDENTICAL FUNCTIONS THAT USE PARTICULAR GROUPS //
  public Parameters getGlobalParameters(String group) {
    return groups.get(group).getGlobalParameters();
  }

  public Parameters getAvailableParts(String group) throws IOException {
    return groups.get(group).getAvailableParts();
  }

  public Document getDocument(String identifier, DocumentComponents p, String group) throws IOException {
    return groups.get(group).getDocument(identifier, p);
  }

  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p, String group) throws IOException {
    return groups.get(group).getDocuments(identifier, p);
  }

  public NodeType getNodeType(Node node, String group) throws Exception {
    return groups.get(group).getNodeType(node);
  }

  public QueryType getQueryType(Node node, String group) throws Exception {
    return groups.get(group).getQueryType(node);
  }

  public Node transformQuery(Node queryTree, Parameters qp, String group) throws Exception {
    return groups.get(group).transformQuery(queryTree, qp);
  }

  public ScoredDocument[] runQuery(Node root, String group) throws Exception {
    return groups.get(group).runQuery(root);
  }

  public ScoredDocument[] runQuery(Node root, Parameters parameters, String group) throws Exception {
    return groups.get(group).runQuery(root, parameters);
  }

  public IndexPartStatistics getRetrievalStatistics(String partName, String group) throws IOException {
    return groups.get(group).getIndexPartStatistics(partName);
  }

  public FieldStatistics getCollectionStatistics(String nodeString, String group) throws Exception {
    return groups.get(group).getCollectionStatistics(nodeString);
  }

  public FieldStatistics getCollectionStatistics(Node node, String group) throws Exception {
    return groups.get(group).getCollectionStatistics(node);
  }

  public NodeStatistics getNodeStatistics(String nodeString, String group) throws Exception {
    return groups.get(group).getNodeStatistics(nodeString);
  }

  public NodeStatistics getNodeStatistics(Node node, String group) throws Exception {
    return groups.get(group).getNodeStatistics(node);
  }

  public Integer getDocumentLength(Integer docid, String group) throws IOException {
    return groups.get(group).getDocumentLength(docid);
  }

  public Integer getDocumentLength(String docname, String group) throws IOException {
    return groups.get(group).getDocumentLength(docname);
  }

  public String getDocumentName(Integer docid, String group) throws IOException {
    return groups.get(group).getDocumentName(docid);
  }

  public void addNodeToCache(Node node, String group) throws Exception {
    groups.get(group).addNodeToCache(node);
  }

  public void addAllNodesToCache(Node node, String group) throws Exception {
    groups.get(group).addAllNodesToCache(node);
  }
}
