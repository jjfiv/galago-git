// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.retrieval.query.QueryType;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * <p>This is a base interface for all kinds of retrieval classes. Historically
 * this was used to support binned indexes in addition to structured
 * indexes.</p>
 *
 * This interface now defines the basic functionality every Retrieval
 * implementation should have.
 *
 * @author trevor
 * @author irmarc
 */
public interface Retrieval {

  /**
   * Should close the Retrieval and release any underlying resources.
   *
   * @throws IOException
   */
  public void close() throws IOException;

  /**
   * Returns the Parameters object that parameterize the retrieval object, if
   * one exists.
   */
  public Parameters getGlobalParameters();

  /**
   * Returns the index parts available under this retrieval. The parts are
   * returned as a Parameters object, which acts as a map between parts and the
   * node types they support.
   *
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
  public Document getDocument(String identifier, DocumentComponents p) throws IOException;

  /**
   * Returns the requested Document Id, if found.
   *
   * @param docname The external name of the document to locate.
   * @return If found, the Document identifier, null otherwise.
   * @throws IOException
   */
  public Long getDocumentId(String docname) throws IOException;

  /**
   * Returns a Map of Document objects that have been found, given the list of
   * identifiers provided.
   *
   * @throws IOException
   */
  public Map<String, Document> getDocuments(List<String> identifier, DocumentComponents p) throws IOException;

  /**
   * Attempts to return a NodeType object for the supplied Node.
   *
   * @throws Exception
   */
  public NodeType getNodeType(Node node) throws Exception;

  /**
   * Attempts to return a QueryType object for the supplied Node. This is
   * typically called on root nodes of query trees.
   *
   * @throws Exception
   */
  public QueryType getQueryType(Node node) throws Exception;

  /**
   * Performs any additional transformations necessary to prepare the query for
   * execution.
   *
   * @param queryParams a Parameters object that may further populated by the
   * transformations applied
   * @throws Exception
   */
  public Node transformQuery(Node root, Parameters queryParams) throws Exception;

  /**
   * Runs the query against the retrieval. Assumes the query has been properly
   * annotated. An example is the query produced from transformQuery.
   *
   * @return Results (contains a list of scored documents)
   * @throws Exception
   */
  public Results executeQuery(Node root) throws Exception;

  /**
   * Runs the query against the retrieval. Assumes the query has been properly
   * annotated. An example is the query produced from transformQuery.
   * Parameters object allows any global execution parameters or default values
   * to be overridden.
   *
   * @param root, parameters
   * @return Results (contains a list of scored documents)
   * @throws Exception
   */
  public Results executeQuery(Node root, Parameters parameters) throws Exception;

  
  /**
   * Returns IndexPartStatistics for the named postings part.
   * 
   * Data includes statistics for vocabulary size, 
   * total number of postings stored and longest posting list.
   * 
   * @return IndexPartStatistics
   * @throws IOException
   */
  public IndexPartStatistics getIndexPartStatistics(String partName) throws IOException;

  /**
   * Returns statistics for a string representation of a lengths node.
   * See collectionStatistics(Node node).
   * 
   * Data returned includes collectionLength, document count, longest document,
   * shortest document, average document. 
   *
   * @return FieldStatistics
   * @throws Exception
   */
  public FieldStatistics getCollectionStatistics(String nodeString) throws Exception;

  /**
   * Returns statistics for a lengths node. This data is commonly used
   * in probabilistic smoothing functions. 
   * 
   * The root-node must implement LengthsIterator.
   * 
   * Data returned includes collectionLength, document count, longest document,
   * shortest document, average document. Where 'document' may be a 'field' or other
   * specified region of indexed documents.
   * 
   *
   * @return FieldStatistics
   * @throws Exception
   */
  public FieldStatistics getCollectionStatistics(Node node) throws Exception;

  /**
   * Returns collection statistics for a count node. This data is commonly used
   * as a feature in a retrieval model. 
   * See nodeStatistics(Node node).
   * 
   * Data returned includes the frequency of the node in the collection, 
   * the number of documents that return a non-zero count for the node, and
   * the maximum frequency of the node in any single document.
   * 
   * @return NodeStatistics
   * @throws Exception
   */
  public NodeStatistics getNodeStatistics(String nodeString) throws Exception;

  /**
   * Returns collection statistics for a count node. This data is commonly used
   * as a feature in a retrieval model. 
   * 
   * The root-node must implement a 'CountIterator'.
   * 
   * Data returned includes the frequency of the node in the collection, 
   * the number of documents that return a non-zero count for the node, and
   * the maximum frequency of the node in any single document.
   * 
   * @return NodeStatistics
   * @throws Exception
   */
  public NodeStatistics getNodeStatistics(Node node) throws Exception;

  /**
   * Returns the length of a particular document. Where docid
   * is the internal identifier of the document.
   *
   * @return document length
   * @throws IOException
   */
  public Integer getDocumentLength(Integer docid) throws IOException;

  /**
   * Returns the length of a particular document. Where docname
   * is the internally stored name of the document.
   *
   * @return document length
   * @throws IOException
   */
  public Integer getDocumentLength(String docname) throws IOException;

  /**
   * Returns the internally stored name of a particular document. 
   * Where docid is the internal identifier of the document.
   * 
   * @return document length
   * @throws IOException
   */
  public String getDocumentName(Integer docid) throws IOException;

  /**
   * adds a node to the cache
   *  -- can improve efficiency for repeated queries
   *  -- if no cache is present, function does nothing
   */
  public void addNodeToCache(Node node) throws Exception;

  /**
   * recursively adds nodes to the cache
   *  -- all children nodes in the tree are added to the cache
   *  -- can improve efficiency for repeated queries
   *  -- if no cache is present, function does nothing
   */
  public void addAllNodesToCache(Node node) throws Exception;
}
