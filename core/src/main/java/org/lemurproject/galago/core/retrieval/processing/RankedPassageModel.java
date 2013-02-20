// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs passage-level retrieval scoring. Passage windows are currently
 * generated the same as Indri: if we hit the end of the document prematurely,
 * we generate a shortened last passage (i.e. window slides are constant). 
 *
 * @author irmarc
 */
public class RankedPassageModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;

  public RankedPassageModel(LocalRetrieval lr) {
    this.retrieval = lr;
    this.index = lr.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    PassageScoringContext context = new PassageScoringContext();
    context.cachable = false;
    
    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);
    int passageSize = (int) queryParams.getLong("passageSize");
    int passageShift = (int) queryParams.getLong("passageShift");
    MovableScoreIterator iterator = 
            (MovableScoreIterator) retrieval.createIterator(queryParams, 
                      queryTree, 
                      context);
    MovableLengthsIterator documentLengths = 
            (MovableLengthsIterator) retrieval.createIterator(new Parameters(), 
            StructuredQuery.parse("#lengths:part=lengths()"), context);
    
    PriorityQueue<ScoredPassage> queue = new PriorityQueue<ScoredPassage>(requested);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      
      // This context is shared among all scorers
      context.document = document;
      documentLengths.syncTo(document);
      int length = documentLengths.getCurrentLength();

      // set the parameters for the first passage
      context.begin = 0;
      context.end = Math.min(passageSize, length);
      
      // ensure we are at the document we wish to score
      // -- this function will move ALL iterators, 
      //     not just the ones that do not have all candidates
      iterator.syncTo(document);

      // Keep iterating over the same doc, but incrementing the begin/end fields of the
      // context until the next one
      boolean lastIteration = false;
      while (context.begin < length && !lastIteration) {
        if (context.end >= length) lastIteration = true;

        if (iterator.hasMatch(document)) {
          double score = iterator.score();
          if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
            ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
            queue.add(scored);
            if (requested > 0 && queue.size() > requested) {
              queue.poll();
            }
          }
        }

        // Move the window forward
        context.begin += passageShift;
        // end must be bigger or equal to the begin, and less than the length of the document
        context.end = Math.max(context.begin, Math.min(passageSize+context.begin, length));
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
