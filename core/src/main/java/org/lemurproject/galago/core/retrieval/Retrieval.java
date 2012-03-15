// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import gnu.trove.list.array.TIntArrayList;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * <p>This is a base interface for all kinds of retrieval classes.  Historically this was
 * used to support binned indexes in addition to structured indexes.</p>
 *
 * This interface now defines the basic functionality every Retrieval implementation should have.
 *
 * @author trevor
 * @author irmarc
 */
public interface Retrieval {

  /**
   * Should close the Retrieval and release any underlying resources.
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Returns the Parameters object that parameterize the retrieval object, if one exists.
   */
  public Parameters getGlobalParameters();

  /**
   * Returns the index parts available under this retrieval. The parts are returned as a Parameters
   * object, which acts as a map between parts and the node types they support.
   * @return
   * @throws IOException
   */
  public Parameters getAvailableParts() throws IOException;

  /**
   * Returns the requested Document, if found.
   * 
   * @param identifier The external name of the document to locate.
   * @return If found, the Document object. Null otherwise.
   * @throws IOException
   */
  public Document getDocument(String identifier) throws IOException;


  /**
   * Returns a Map of Document objects that have been found, given the list of
   * identifiers provided.
   * 
   * @param identifier
   * @return
   * @throws IOException
   */
  public Map<String, Document> getDocuments(List<String> identifier) throws IOException;

  /**
   * Attempts to return a NodeType object for the supplied Node.
   * @param node
   * @return
   * @throws Exception
   */
  public NodeType getNodeType(Node node) throws Exception;

  /**
   * Attempts to return a QueryType object for the supplied Node.
   * This is typically called on root nodes of query trees.
   * @param node
   * @return
   * @throws Exception
   */
  public QueryType getQueryType(Node node) throws Exception;

  /**
   * Performs any additional transformations necessary to prepare the query for execution.
   * @param root
   * @param queryParams a Parameters object that may further populated by the transformations applied
   * @return
   * @throws Exception
   */
  public Node transformQuery(Node root, Parameters queryParams) throws Exception;

  /**
   * Runs the query against the retrieval. Assumes the query has been properly annotated.
   * An example is the query produced from transformQuery.
   * 
   * @param root
   * @return
   * @throws Exception
   */
  public ScoredDocument[] runQuery(Node root) throws Exception;

  public ScoredDocument[] runQuery(Node root, Parameters parameters) throws Exception;

  // term and collection statistics gatherers
  public CollectionStatistics getRetrievalStatistics() throws IOException;

  public CollectionStatistics getRetrievalStatistics(String partName) throws IOException;

  public NodeStatistics nodeStatistics(String nodeString) throws Exception;

  public NodeStatistics nodeStatistics(Node node) throws Exception;

  public int getDocumentLength(int docid) throws IOException;

  public int getDocumentLength(String docname) throws IOException;

  public String getDocumentName(int docid) throws IOException;
}
