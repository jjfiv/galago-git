// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.prf;

import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

/**
 * Expansion Model;
 *  Generally, this is meant to support various RM models
 *  -- runs a query
 *  -- collects top documents
 *  -- extracts some terms
 *  -- generates a new query using some expansion terms
 *  
 *  But can support other methods
 * 
 * @author sjh
 */
public interface ExpansionModel {

  public Node expand(Node root, Parameters queryParameters) throws Exception;
  
}
