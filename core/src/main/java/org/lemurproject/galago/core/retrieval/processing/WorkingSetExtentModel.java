// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.MovableExtentIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs extent-level retrieval scoring. Scores each named 
 *  extent in the document. Useful for scoring sentences or paragraphs.
 * 
 * Can also score seqential sets of extents.
 *  (but it assumes no break in extents) -- any gaps will be scored.
 * 
 * 
 * to use:
 *  --extentQuery=true
 *  --extent=name
 *  --working=[names/numbers]
 * 
 * @author sjh
 */
public class WorkingSetExtentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;

  public WorkingSetExtentModel(LocalRetrieval lr) {
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
    List<Integer> whitelist;
    if (Integer.class.isAssignableFrom(containedType)) {
      whitelist = (List<Integer>) l;
    } else if (Long.class.isAssignableFrom(containedType)) {
      // Sadly this will not directly translate for now - maybe when/if we move
      // Galago to using longs instead of ints...
      whitelist = new ArrayList<Integer>();
      for (Long docid : (List<Long>) l) {
        whitelist.add(docid.intValue());
      }
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
    // passageSize and shift can be used to cover a set of extents, instead of just one
    int extentSetSize = (int) queryParams.get("extentCount", 1);
    int extentShift = (int) queryParams.get("extentShift", 1);

    // scoring iterator
    MovableScoreIterator iterator =
            (MovableScoreIterator) retrieval.createIterator(queryParams,
            queryTree,
            context);

    // get the extent iterator
    String extent = queryParams.getString("extent");
    MovableExtentIterator extentIterator =
            (MovableExtentIterator) retrieval.createIterator(new Parameters(),
            StructuredQuery.parse("#extents:" + extent + ":part=extents()"),
            context);


    PriorityQueue<ScoredPassage> queue = new PriorityQueue<ScoredPassage>(requested);

    // now there should be an iterator at the root of this tree
    for (int i = 0; i < whitelist.size(); i++) {
      int document = whitelist.get(i);
      context.document = document;

      extentIterator.syncTo(document);

      ExtentArray extents = extentIterator.extents();
      if (extents.size() == 0) {
        // nothing to score, skip to next document
        continue;
      }

      // otherwise we have something to score, shift the scorer
      iterator.syncTo(document);

      // passageSize, passageShift defaults to 1: all extents are scored individually.

      for (int e = 0; e < extents.size(); e += extentShift) {
        context.begin = extents.begin(e);
        
        // if the window extends past the end of the array:
        if ((e + extentSetSize - 1) >= extents.size()) {
          context.end = extents.end(extents.size() - 1);
        } else {
          context.end = extents.end(e + extentSetSize - 1);
        }

        // we know that extents is non-empty.

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

        // if we're done - break
        if (context.end == extents.end(extents.size() -1)) {
          break;
        }
      }
    }
    return toReversedArray(queue);
  }
}
