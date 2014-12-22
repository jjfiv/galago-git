// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.FixedSizeMinHeap;
import org.lemurproject.galago.utility.Parameters;

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

        if (!queryParams.get("passageQuery", false)) {
            throw new IllegalArgumentException("passageQuery must be true for passage retrieval to work!");
        }

        // Following operations are all just setup
        int requested = (int) queryParams.get("requested", 1000);
        int passageSize = (int) queryParams.getLong("passageSize");
        int passageShift = (int) queryParams.getLong("passageShift");

        if (passageSize <= 0 || passageShift <= 0) {
            throw new IllegalArgumentException("passageSize/passageShift must be specified as positive integers.");
        }

        ScoreIterator iterator
                = (ScoreIterator) retrieval.createIterator(queryParams,
                        queryTree);
        LengthsIterator documentLengths = retrieval.getDocumentLengthsIterator();

        FixedSizeMinHeap<ScoredPassage> queue = new FixedSizeMinHeap(ScoredPassage.class, requested, new ScoredPassage.ScoredPassageComparator());

        // now there should be an iterator at the root of this tree
        while (!iterator.isDone()) {
            long document = iterator.currentCandidate();

            // This context is shared among all scorers
            context.document = document;
            documentLengths.syncTo(document);
            int length = documentLengths.length(context);

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
            // short-circut the while loop if there is no match. Don't do
            // it in the while condtion check - no need to re-evaluate every time.
            if (!iterator.hasMatch(document)) {
                lastIteration = true;
            }

            while (context.begin < length && !lastIteration) {
                if (context.end >= length) {
                    lastIteration = true;
                }

                double score = iterator.score(context);
                if (requested < 0 || queue.size() < requested || queue.peek().score < score) {
                    ScoredPassage scored = new ScoredPassage(document, score, context.begin, context.end);
                    queue.offer(scored);
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
