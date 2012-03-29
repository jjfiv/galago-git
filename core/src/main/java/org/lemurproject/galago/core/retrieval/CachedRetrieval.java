/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.core.retrieval.structured.FeatureFactory;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * The CacbedRetrieval object wraps a local retrieval object
 *
 * Data produced by iterators created by the retrieval may be cached
 *
 * @author sjh
 */
public class CachedRetrieval implements Retrieval {

  protected MemoryIndex cacheIndex;
  protected LocalRetrieval retrieval;
  protected FeatureFactory features;
  protected Parameters globalParameters;

  /**
   * One retrieval interacts with one index. Parameters dictate the behavior
   * during retrieval time, and selection of the appropriate feature factory.
   * Additionally, the supplied parameters will be passed forward to the chosen
   * feature factory.
   */
  public CachedRetrieval(LocalRetrieval retrieval) throws IOException {
    this(retrieval, new Parameters());
  }

  public CachedRetrieval(LocalRetrieval retrieval, Parameters parameters) throws IOException {
    this.globalParameters = parameters;
    this.retrieval = retrieval;
    this.features = new FeatureFactory(globalParameters);
  }

  @Override
  public void close() throws IOException {
    this.cacheIndex.close();
    this.retrieval.close();
  }

  @Override
  public Parameters getGlobalParameters() {
    return globalParameters;
  }

  @Override
  public Parameters getAvailableParts() throws IOException {
    return retrieval.getAvailableParts();
  }

  @Override
  public Document getDocument(String identifier) throws IOException {
    return retrieval.getDocument(identifier);
  }

  @Override
  public Map<String, Document> getDocuments(List<String> identifiers) throws IOException {
    return retrieval.getDocuments(identifiers);
  }

  @Override
  public NodeType getNodeType(Node node) throws Exception {
    return retrieval.getNodeType(node);
  }

  @Override
  public QueryType getQueryType(Node node) throws Exception {
    return retrieval.getQueryType(node);
  }

  @Override
  public Node transformQuery(Node root, Parameters queryParams) throws Exception {
    // this should do something
    return retrieval.transformQuery(root, queryParams);
  }

  @Override
  public ScoredDocument[] runQuery(Node root) throws Exception {
    // this should do something
    return retrieval.runQuery(root);
  }

  @Override
  public ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception {
    // this should do something
    return retrieval.runQuery(root, parameters);
  }

  @Override
  public CollectionStatistics getRetrievalStatistics() throws IOException {
    return retrieval.getRetrievalStatistics();
  }

  @Override
  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException {
    return retrieval.getRetrievalStatistics(partName);
  }

  @Override
  public NodeStatistics nodeStatistics(String nodeString) throws Exception {
    // this should do something
    return retrieval.nodeStatistics(nodeString);
  }

  @Override
  public NodeStatistics nodeStatistics(Node node) throws Exception {
    // this should do something
    return retrieval.nodeStatistics(node);
  }

  @Override
  public int getDocumentLength(int docid) throws IOException {
    // this should do something
    return retrieval.getDocumentLength(docid);
  }

  @Override
  public int getDocumentLength(String docname) throws IOException {
    // this should do something
    return retrieval.getDocumentLength(docname);
  }

  @Override
  public String getDocumentName(int docid) throws IOException {
    // this should do something
    return retrieval.getDocumentName(docid);
  }
}
