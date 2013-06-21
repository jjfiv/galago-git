// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Performs straightforward document-at-a-time (daat) processing of a fully
 * annotated query, processing scores over documents.
 *
 * @author irmarc
 */
public class RankedDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  List<Integer> whitelist;

  public RankedDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // Number of documents requested.
    int requested = (int) queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // Maintain a queue of candidates
    //PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());


    // construct the iterators -- we use tree processing
    ScoreIterator iterator = (ScoreIterator) retrieval.createIterator(queryParams, queryTree, context);

    // now there should be an iterator at the root of this tree
    while (!iterator.isDone()) {
      int document = iterator.currentCandidate();

      // This context is shared among all scorers
      context.document = document;
      iterator.syncTo(document);
      if (iterator.hasMatch(document)) {
        double score = iterator.score();
        if (requested < 0 || queue.size() <= requested || queue.peek().score < score) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          if (annotate) {
            scoredDocument.annotation = iterator.getAnnotatedNode();
          }
          queue.offer(scoredDocument);
        }
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
