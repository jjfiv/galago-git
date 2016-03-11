// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

/**
 * Same as RankedDocumentModel except it ignores zero length documents.
 *
 * @author MichaelZ - copy of RankedDocumentModel
 */
public class RankedNonZeroLengthDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;

  public RankedNonZeroLengthDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    // This model uses the simplest ScoringContext
    ScoringContext context = new ScoringContext();

    // Number of documents requested.
    int requested = queryParams.get("requested", 1000);
    boolean annotate = queryParams.get("annotate", false);

    // Maintain a queue of candidates
    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap<>(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    // construct the iterators -- we use tree processing
    ScoreIterator iterator = (ScoreIterator) retrieval.createIterator(queryParams, queryTree);

    while (!iterator.isDone()) {

      long document = iterator.currentCandidate();

      context.document = document;
      int length = retrieval.getDocumentLength((int) document);

      iterator.syncTo(document);
      if (iterator.hasMatch(context)) {
        double score = iterator.score(context);

        if (length > 0 && (queue.size() < requested || queue.peek().score < score)) {
          ScoredDocument scoredDocument = new ScoredDocument(document, score);
          if (annotate) {
            scoredDocument.annotation = iterator.getAnnotatedNode(context);
          }

          queue.offer(scoredDocument);

        }
      }
      iterator.movePast(document);
    }
    return toReversedArray(queue);
  }
}
