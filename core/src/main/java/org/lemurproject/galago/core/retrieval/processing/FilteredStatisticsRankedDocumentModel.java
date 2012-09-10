// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.MovableCountIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.AdjustAnnotationsTraversal;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author irmarc
 */
public class FilteredStatisticsRankedDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;

  public FilteredStatisticsRankedDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
    whitelist = null;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    if (whitelist == null) {
      return executeWholeCollection(queryTree, queryParams);
    } else {
      return executeWorkingSet(queryTree, queryParams);
    }
  }

  @Override
  public void defineWorkingSet(int[] docs) {
    whitelist = docs;
  }

  private ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // have to be sure
    Arrays.sort(whitelist);

    // construct the query iterators
    MovableScoreIterator iterator = (MovableScoreIterator) retrieval.createIterator(queryParams, queryTree, context);
    int requested = (int) queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // now there should be an iterator at the root of this tree
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>();
    ProcessingModel.initializeLengths(retrieval, context);

    for (int i = 0; i < whitelist.length; i++) {
      int document = whitelist[i];
      iterator.moveTo(document);
      context.moveLengths(document);

      // This context is shared among all scorers
      context.document = document;
      double score = iterator.score();
      if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
        ScoredDocument scoredDocument = new ScoredDocument(document, score);

        if (annotate) {
          scoredDocument.annotation = iterator.getAnnotatedNode();
        }

        queue.add(scoredDocument);
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
        }
      }
    }
    return toReversedArray(queue);
  }

  private ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
          throws Exception {

    // FIRST PASS -- used to only gather statistics for the second pass
    FilteredStatisticsScoringContext fssContext = new FilteredStatisticsScoringContext();

    // construct the iterators -- we use tree processing
    MovableScoreIterator iterator =
            (MovableScoreIterator) retrieval.createIterator(queryParams, queryTree, fssContext);
    ProcessingModel.initializeLengths(retrieval, fssContext);

    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();

      // This context is shared among all scorers
      fssContext.document = document;
      fssContext.moveLengths(document);

      if (iterator.hasMatch(document)) {

        // update global statistics
        ++fssContext.documentCount;
        fssContext.collectionLength += fssContext.getLength();

        // update per-term statistics
        for (String term : fssContext.trackedIterators.keySet()) {
          MovableCountIterator ci = fssContext.trackedIterators.get(term);
          if (ci.hasMatch(document)) {
            int count = ci.count();
            if (count > 0) {
              fssContext.tfs.adjustOrPutValue(term, count, count);
              fssContext.dfs.adjustOrPutValue(term, 1, 1);
            }
          }
        }
      }
      iterator.next();
    }

    // SECOND PASS -- should look like a normal run, except we run one more traversal
    // over the query tree to ''correct'' statistics, then instantiate the iterators.
    // We use a copy to make sure we don't perturb the original tree, in case there are
    // references outside this method.
    AdjustAnnotationsTraversal traversal = new AdjustAnnotationsTraversal(fssContext);
    queryTree = StructuredQuery.copy(traversal, queryTree);

    // Nothing special needed here
    ScoringContext context = new ScoringContext();
    // Number of documents requested.
    int requested = (int) queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // Maintain a queue of candidates
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // construct the iterators -- we use tree processing
    iterator = (MovableScoreIterator) retrieval.createIterator(queryParams, queryTree, context);
    ProcessingModel.initializeLengths(retrieval, context);
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();
      context.document = document;
      context.moveLengths(document);
      if (iterator.hasMatch(document)) {
        double score = iterator.score();
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          if (annotate) {
            scoredDocument.annotation = iterator.getAnnotatedNode();
          }
          queue.add(scoredDocument);
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
          }
        }
      }
      iterator.next();
    }
    return toReversedArray(queue);
  }
}
