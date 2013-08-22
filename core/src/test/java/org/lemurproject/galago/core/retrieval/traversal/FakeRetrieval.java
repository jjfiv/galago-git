/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.traversal;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class FakeRetrieval implements Retrieval {

  @Override
  public void close() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Parameters getGlobalParameters() {
    return new Parameters();
  }

  @Override
  public Parameters getAvailableParts() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Document getDocument(String identifier, DocumentComponents p) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NodeType getNodeType(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public QueryType getQueryType(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Node transformQuery(Node root, Parameters queryParams) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public FieldStatistics getCollectionStatistics(String nodeString) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public FieldStatistics getCollectionStatistics(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public NodeStatistics getNodeStatistics(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Integer getDocumentLength(Integer docid) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Integer getDocumentLength(String docname) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String getDocumentName(Integer docid) throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void addNodeToCache(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void addAllNodesToCache(Node node) throws Exception {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Results executeQuery(Node root) throws Exception {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public Results executeQuery(Node root, Parameters parameters) throws Exception {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
