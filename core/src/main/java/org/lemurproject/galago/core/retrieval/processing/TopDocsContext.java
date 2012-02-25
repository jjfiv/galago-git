/*
 * BSD License (http://lemurproject.org/galago-license)

 */

package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.iterator.ScoreValueIterator;
import java.util.ArrayList;
import java.util.HashMap;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;

/**
 * Extension to the ScoringContext to support passing around topdocs, which is used
 * during iterator construction for the MaxscoreIterator to perform more efficient pruning.
 *
 * @author irmarc
 */
public class TopDocsContext extends ScoringContext {
  public HashMap<ScoreValueIterator, ArrayList<TopDocument>> topdocs;
  public ArrayList<TopDocument> hold;

  public TopDocsContext() {
    super();
    topdocs = new HashMap<ScoreValueIterator, ArrayList<TopDocument>>();
  }
}
