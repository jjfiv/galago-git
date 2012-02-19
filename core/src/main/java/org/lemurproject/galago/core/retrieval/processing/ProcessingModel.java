// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * An interface that defines the contract for processing a query.
 * There's one method : execute, which takes a fully annotated query
 * tree, and somehow produces a result list.
 *
 *
 * @author irmarc
 */
public interface ProcessingModel {
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception;
  public void defineWorkingSet(int[] docs);
}
