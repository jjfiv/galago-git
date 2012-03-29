/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using Maxscore.
 * Generally this causes a massive speedup in processing time.
 *
 * @author irmarc
 */
public class DeltaScoreDocumentModel extends ProcessingModel {

    LocalRetrieval retrieval;
    Index index;
    int[] whitelist;

    public DeltaScoreDocumentModel(LocalRetrieval lr) {
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

    public ScoredDocument[] executeWholeCollection(Node queryTree, Parameters queryParams)
            throws Exception {
        DeltaScoringContext context = new DeltaScoringContext();

        int requested = (int) queryParams.get("requested", 1000);

        context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.getLong("numberOfTerms"))];
        context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.getLong("numberOfTerms"))];
        Arrays.fill(context.startingPotentials, 0);
        
        StructuredIterator iterator = retrieval.createIterator(queryTree, context);

        PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
        ProcessingModel.initializeLengths(retrieval, context);
        context.minCandidateScore = Double.NEGATIVE_INFINITY;
        context.quorumIndex = context.scorers.size();

        // Compute the starting potential
        context.scorers.get(0).aggregatePotentials(context);

        /*
        System.err.printf("starting PHI: %f\n", context.startingPotential);
        for (int i = 0; i < context.startingPotentials.length; i++) {
            System.err.printf("starting phi[%d]: %f\n", i, context.startingPotentials[i]);
        }
        */
        
        // Make sure the scorers are sorted properly
        Collections.sort(context.scorers, new Comparator<DeltaScoringIterator>() {

            @Override
            public int compare(DeltaScoringIterator t, DeltaScoringIterator t1) {
                return (int) (t.totalEntries() - t1.totalEntries());
            }
        });

        // Routine is as follows:
        // 1) Find the next candidate from the quorum
        // 2) Move quorum and field length readers to candidate
        // 3) Score quorum unconditionally
        // 4) while (runningScore > R)
        //      move iterator to candidate
        //      score candidate w/ iterator
        while (true) {
            int candidate = Integer.MAX_VALUE;
            for (int i = 0; i < context.quorumIndex; i++) {
                if (!context.scorers.get(i).isDone()) {
                    candidate = Math.min(candidate, context.scorers.get(i).currentCandidate());
                }
            }

            // Means quorum is done, we can quit
            if (candidate == Integer.MAX_VALUE) {
                break;
            }

            // Otherwise move lengths
            context.document = candidate;
            context.moveLengths(candidate);

            // Setup to score
            context.runningScore = context.startingPotential;
            System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
                    context.startingPotentials.length);

            // now score quorum w/out question
            int i;
            for (i = 0; i < context.quorumIndex; i++) {
                context.scorers.get(i).deltaScore();
            }

            // Now score the rest, but keep checking
            while (context.runningScore > context.minCandidateScore && i < context.scorers.size()) {
                context.scorers.get(i).moveTo(context.document);
                context.scorers.get(i).deltaScore();
                i++;
            }

            // Fully scored it
            if (i == context.scorers.size()) {
                if (requested < 0 || queue.size() <= requested) {
                    ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
                    queue.add(scoredDocument);
                    if (requested > 0 && queue.size() > requested) {
                        queue.poll();
                        if (context.minCandidateScore < queue.peek().score) {
                            context.minCandidateScore = queue.peek().score;
                            computeQuorum(context);
                        }
                    }
                }
            }

            // Now move all matching quorum members forward, and repeat
            for (i = 0; i < context.quorumIndex; i++) {
                if (context.scorers.get(i).atCandidate(candidate)) {
                    context.scorers.get(i).next();
                }
            }
        }
        return toReversedArray(queue);
    }

    public ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
            throws Exception {
        DeltaScoringContext context = new DeltaScoringContext();

        // Following operations are all just setup
        int requested = (int) queryParams.get("requested", 1000);
        StructuredIterator iterator = retrieval.createIterator(queryTree, context);

        PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
        ProcessingModel.initializeLengths(retrieval, context);
        context.minCandidateScore = Double.NEGATIVE_INFINITY;
        context.quorumIndex = context.scorers.size();

        // Compute the starting potential
        context.startingPotential = 0.0;
        for (int i = 0; i < context.startingPotentials.length; i++) {
            context.startingPotential += Math.log(context.startingPotentials[i]);
        }

        // Make sure the scorers are sorted properly
        Collections.sort(context.scorers, new Comparator<DeltaScoringIterator>() {

            @Override
            public int compare(DeltaScoringIterator t, DeltaScoringIterator t1) {
                return (int) (t.totalEntries() - t1.totalEntries());
            }
        });

        // Routine is as follows:
        // 1) Find the next candidate from the quorum
        // 2) Move quorum and field length readers to candidate
        // 3) Score quorum unconditionally
        // 4) while (runningScore > R)
        //      move iterator to candidate
        //      score candidate w/ iterator
        for (int i = 0; i < whitelist.length; i++) {
            int candidate = whitelist[i];
            for (int j = 0; j < context.quorumIndex; j++) {
                if (!context.scorers.get(j).isDone()) {
                    context.scorers.get(j).moveTo(candidate);
                }
            }

            // Otherwise move lengths
            context.document = candidate;
            context.moveLengths(candidate);

            // Setup to score
            context.runningScore = context.startingPotential;
            System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
                    context.startingPotentials.length);

            // now score quorum w/out question
            int j;
            for (j = 0; j < context.quorumIndex; j++) {
                context.scorers.get(j).deltaScore();
            }

            // Now score the rest, but keep checking
            while (context.runningScore > context.minCandidateScore && j < context.scorers.size()) {
                context.scorers.get(j).moveTo(context.document);
                context.scorers.get(j).deltaScore();
                j++;
            }

            // Fully scored it
            if (j == context.scorers.size()) {
                if (requested < 0 || queue.size() <= requested) {
                    ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
                    queue.add(scoredDocument);
                    if (requested > 0 && queue.size() > requested) {
                        queue.poll();
                        if (context.minCandidateScore < queue.peek().score) {
                            context.minCandidateScore = queue.peek().score;
                            computeQuorum(context);
                        }
                    }
                }
            }
            // The quorum is moved at the top, so we don't do it here.
        }
        return toReversedArray(queue);
    }

    private void computeQuorum(DeltaScoringContext ctx) {
        // Now we try to find our quorum
        ctx.runningScore = ctx.startingPotential;
        System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
                ctx.startingPotentials.length);
        int i;
        for (i = 0; i < ctx.scorers.size() && ctx.runningScore > ctx.minCandidateScore; i++) {
            ((DeltaScoringIterator) ctx.scorers.get(i)).maximumDifference();
        }

        ctx.quorumIndex = i;
    }

    @Override
    public void defineWorkingSet(int[] docs) {
        whitelist = docs;
    }
}
