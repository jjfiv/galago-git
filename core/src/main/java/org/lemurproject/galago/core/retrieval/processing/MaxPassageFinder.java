package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.ScoredPassage;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a processing model that takes a list of document ids or document
 * names and returns the max passage matching a query in each.
 */
public class MaxPassageFinder extends ProcessingModel {

    private final LocalRetrieval retrieval;
    private final Index index;

    public MaxPassageFinder(LocalRetrieval lr) {
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

        List<Long> docIds;
        List untyped = queryParams.getList("working");
        if (untyped.get(0) instanceof String) {
            List<String> working = (List<String>) untyped;
            docIds = retrieval.getDocumentIds(working);
        } else if (untyped.get(0) instanceof Long) {
            docIds = (List<Long>) untyped;
        } else {
            throw new IllegalArgumentException("Bad form with your annotateMe list");
        }
        Collections.sort(docIds);

        int requested = docIds.size();
        int passageSize = (int) queryParams.getLong("passageSize");
        int passageShift = (int) queryParams.getLong("passageShift");
        boolean annotate = queryParams.get("annotate", false);

        if (passageSize <= 0 || passageShift <= 0) {
            throw new IllegalArgumentException("passageSize/passageShift must be specified as positive integers.");
        }

        ScoreIterator iterator
                = (ScoreIterator) retrieval.createIterator(queryParams,
                        queryTree);
        LengthsIterator documentLengths = retrieval.getDocumentLengthsIterator();

        ArrayList<ScoredPassage> perDocument = new ArrayList<ScoredPassage>(requested);

        // now there should be an iterator at the root of this tree
        for (Long docId : docIds) {
            if (docId < 0) {
                continue;
            }

            iterator.syncTo(docId);
            context.document = docId;
            documentLengths.syncTo(docId);
            int length = documentLengths.length(context);

            context.begin = 0;
            context.end = Math.min(passageSize, length);

            ScoredPassage documentBest = null;
            boolean lastIteration = false;

            // short-circut the while loop if there is no match. Don't do
            // it in the while condtion check - no need to re-evaluate every time.
            if (!iterator.hasMatch(context)) {
                lastIteration = true;
            }
            while (context.begin < length && !lastIteration) {
                if (context.end >= length) {
                    lastIteration = true;
                }

                double score = iterator.score(context);
                if (documentBest == null || documentBest.score < score) {
                    documentBest = new ScoredPassage(docId, score, context.begin, context.end);
                    if (annotate) {
                        documentBest.annotation = iterator.getAnnotatedNode(context);
                    }
                }

                context.begin += passageShift;
                context.end = Math.max(context.begin, Math.min(passageSize + context.begin, length));
            }
            if (documentBest != null) {
                perDocument.add(documentBest);
            }
            iterator.movePast(docId);
        }

        if (perDocument.size() == 0) {
            return perDocument.toArray(new ScoredPassage[perDocument.size()]);
        }
        // sort by scoredPassageComparator
        Collections.sort(perDocument, new ScoredPassage.ScoredPassageComparator());
        Collections.reverse(perDocument);
        return perDocument.toArray(new ScoredPassage[perDocument.size()]);
    }

}
