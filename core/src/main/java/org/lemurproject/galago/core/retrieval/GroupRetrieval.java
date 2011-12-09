/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import gnu.trove.TIntArrayList;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
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
 * Note that the GroupRetrieval implementation has no facility to explicitly group
 * retrieval objects - that has been left to the MultiRetrieval class.
 *
 * @author sjh
 */
public class GroupRetrieval implements Retrieval {

  protected FeatureFactory features;
  protected Parameters globalParameters;
  protected String defGroup;
  protected HashMap<String, Retrieval> groups;

  public GroupRetrieval(HashMap<String, Retrieval> groups, Parameters parameters,
          String defGroup) {
    this.groups = groups;
    this.globalParameters = parameters;
    this.defGroup = defGroup;
    this.features = new FeatureFactory(globalParameters);
  }

  // IMPLEMENTED FUNCTIONS - Traversals use group-retrievals to collect aggregate stats
  public Collection<String> getGroups() {
    return groups.keySet();
  }

  @Override
  public Node transformQuery(Node queryTree) throws Exception {
    for (Traversal traversal : this.features.getTraversals(this)) {
      queryTree = StructuredQuery.copy(traversal, queryTree);
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
  public Document getDocument(String identifier) throws IOException{
    return groups.get(defGroup).getDocument(identifier);
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifier) throws IOException{
    return groups.get(defGroup).getDocuments(identifier);
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
    return groups.get(defGroup).runQuery(root, parameters);
  }

  @Override
  public ScoredDocument[] runQuery(Node root, Parameters parameters, TIntArrayList workingSet) throws Exception {
    return groups.get(defGroup).runQuery(root, parameters, workingSet);
  }

  @Override
  public CollectionStatistics getRetrievalStatistics() throws IOException {
    return groups.get(defGroup).getRetrievalStatistics();
  }

  @Override
  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException {
    return groups.get(defGroup).getRetrievalStatistics(partName);
  }

  @Override
  public NodeStatistics nodeStatistics(String nodeString) throws Exception {
    return groups.get(defGroup).nodeStatistics(nodeString);
  }

  @Override
  public NodeStatistics nodeStatistics(Node node) throws Exception {
    return groups.get(defGroup).nodeStatistics(node);
  }

  // IDENTICAL FUNCTIONS THAT USE PARTICULAR GROUPS //
  public Parameters getGlobalParameters(String group) {
    return groups.get(group).getGlobalParameters();
  }

  public Parameters getAvailableParts(String group) throws IOException {
    return groups.get(group).getAvailableParts();
  }

  public Document getDocument(String identifier, String group) throws IOException{
    return groups.get(group).getDocument(identifier);
  }

  public Map<String, Document> getDocuments(List<String> identifier, String group) throws IOException{
    return groups.get(group).getDocuments(identifier);
  }

  public NodeType getNodeType(Node node, String group) throws Exception {
    return groups.get(group).getNodeType(node);
  }

  public QueryType getQueryType(Node node, String group) throws Exception {
    return groups.get(group).getQueryType(node);
  }

  public Node transformQuery(Node queryTree, String group) throws Exception {
    return groups.get(group).transformQuery(queryTree);
  }

  public ScoredDocument[] runQuery(Node root, String group) throws Exception {
    return groups.get(group).runQuery(root);
  }

  public ScoredDocument[] runQuery(Node root, Parameters parameters, String group) throws Exception {
    return groups.get(group).runQuery(root, parameters);
  }

  public ScoredDocument[] runQuery(Node root, Parameters parameters, TIntArrayList workingSet, String group) throws Exception {
    return groups.get(group).runQuery(root, parameters, workingSet);
  }

  public CollectionStatistics getRetrievalStatistics(String partName, String group) throws IOException {
    return groups.get(group).getRetrievalStatistics(partName);
  }

  public NodeStatistics nodeStatistics(String nodeString, String group) throws Exception {
    return groups.get(group).nodeStatistics(nodeString);
  }

  public NodeStatistics nodeStatistics(Node node, String group) throws Exception {
    return groups.get(group).nodeStatistics(node);
  }

  public int getDocumentLength(int docid, String group) throws IOException {
    return groups.get(group).getDocumentLength(docid);
  }

  @Override
  public int getDocumentLength(int docid) throws IOException {
    return groups.get(defGroup).getDocumentLength(docid);
  }

  public int getDocumentLength(String docname, String group) throws IOException {
    return groups.get(group).getDocumentLength(docname);
  }

  @Override
  public int getDocumentLength(String docname) throws IOException {
    return groups.get(defGroup).getDocumentLength(docname);
  }

  @Override
  public String getDocumentName(int docid) throws IOException {
    return groups.get(defGroup).getDocumentName(docid);
  }

  public String getDocumentName(int docid, String group) throws IOException {
    return groups.get(group).getDocumentName(docid);
  }
}
