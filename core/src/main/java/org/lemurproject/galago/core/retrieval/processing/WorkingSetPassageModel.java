// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs passage-level retrieval scoring. Passage windows are currently
 * generated the same as Indri: if we hit the end of the document prematurely,
 * we generate a shortened last passage (i.e. window slides are constant).
 *
 * @author irmarc
 */
public class WorkingSetPassageModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;

  public WorkingSetPassageModel(LocalRetrieval lr) {
    this.retrieval = lr;
    this.index = lr.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    PassageScoringContext context = new PassageScoringContext();
    context.cachable = false;

    // There should be a whitelist to deal with
    List l = queryParams.getList("working");
    if (l == null) {
      throw new IllegalArgumentException("Parameters must contain a 'working' parameter specifying the working set");
    }

    Class containedType = l.get(0).getClass();
    List<Long> whitelist;
    if (Long.class.isAssignableFrom(containedType)) {
      whitelist = (List<Long>) l;
    } else if (String.class.isAssignableFrom(containedType)) {
      whitelist = retrieval.getDocumentIds((List<String>) l);
    } else {
      throw new IllegalArgumentException(
              String.format("Parameter 'working' must be a list of longs or a list of strings. Found type %s\n.",
              containedType.toString()));
    }
    Collections.sort(whitelist);

    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);
    int passageSize = (int) queryParams.getLong("passageSize");
    int passageShift = (int) queryParams.getLong("passageShift");
    boolean annotate = queryParams.get("annotate", false);

    if (passageSize <= 0 || passageShift <= 0) {
      throw new IllegalArgumentException("passageSize/passageShift must be specified as positive integers.");
    }

    ScoreIterator iterator =
            (ScoreIterator) retrieval.createIterator(queryParams,
            queryTree,
            context);
    LengthsIterator documentLengths =
            (LengthsIterator) retrieval.createIterator(new Parameters(),
            StructuredQuery.parse("#lengths:part=lengths()"),
            context);

    FixedSizeMinHeap<ScoredPassage> queue = new FixedSizeMinHeap(ScoredPassage.class, requested, new ScoredPassage.ScoredPassageComparator());

    // now there should be an iterator at the root of this tree
    for (int i = 0; i < whitelist.size(); i++) {
      long document = whitelist.get(i);
      iterator.syncTo(document);
      context.document = document;
      documentLengths.syncTo(document);
      int length = documentLengths.length();

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
        if (context.end >= length) {
          lastIteration = true;
        }

        if (iterator.hasMatch(document)) {
          double score = iterator.score();
          if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
            ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
            if (annotate) {
              scored.annotation = iterator.getAnnotatedNode();
            }
            queue.offer(scored);
          }
        }

        // Move the window forward
        context.begin += passageShift;
        // end must be bigger or equal to the begin, and less than the length of the document
        context.end = Math.max(context.begin, Math.min(passageSize + context.begin, length));
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
