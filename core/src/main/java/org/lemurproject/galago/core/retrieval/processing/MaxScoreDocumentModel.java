// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.Arrays;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using Maxscore.
 * Generally this causes a substantial speedup in processing time.
 *
 * @author irmarc
 */
public class MaxScoreDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  private ArrayList<Sentinel> sortedSentinels = null;

  public MaxScoreDocumentModel(LocalRetrieval lr) {
    this.retrieval = lr;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    EarlyTerminationScoringContext context = new EarlyTerminationScoringContext();

    int requested = (int) queryParams.get("requested", 1000);

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    ScoreIterator rootIterator =
            (ScoreIterator) retrieval.createIterator(queryParams, queryTree, context);

    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();
    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);

    // Make sure the scorers are sorted properly
    buildSentinels(context, queryParams);
    determineSentinelIndex(context);

    // Routine is as follows:
    // 1) Find the next candidate from the sentinels
    // 2) Move sentinels and field length readers to candidate
    // 3) Score sentinels unconditionally
    // 4) while (runningScore > R)
    //      move iterator to candidate
    //      score candidate w/ iterator
    while (true) {
      int candidate = Integer.MAX_VALUE;
      for (int i = 0; i < context.sentinelIndex; i++) {
        if (!sortedSentinels.get(i).iterator.isDone()) {
          candidate = Math.min(candidate, sortedSentinels.get(i).iterator.currentCandidate());
        }
      }

      // Means sentinels are done, we can quit
      if (candidate == Integer.MAX_VALUE) {
        break;
      }

      context.document = candidate;
      // Due to different semantics between "currentCandidate" and 
      // "hasMatch", we need to see if the candidate given actually matches
      // properly before scoring.
      if (rootIterator.hasMatch(candidate)) {
        // Otherwise move lengths
        context.moveLengths(candidate);

        // Setup to score
        context.runningScore = context.startingPotential;
        System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
                context.startingPotentials.length);

        // now score sentinels w/out question
        int i;
        for (i = 0; i < context.sentinelIndex; i++) {
          DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
          dsi.syncTo(context.document);
          dsi.deltaScore();
        }

        // Now score the rest, but keep checking
        while (context.runningScore > context.minCandidateScore && i < sortedSentinels.size()) {
          DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
          dsi.syncTo(context.document);
          dsi.deltaScore();
          ++i;
        }

        // Fully scored it
        if (i == context.scorers.size()) {
          if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
            queue.offer(scoredDocument);

            if (requested > 0 && queue.size() >= requested) {
              if (context.minCandidateScore < queue.peek().score) {
                context.minCandidateScore = queue.peek().score;
                determineSentinelIndex(context);
              }
            }
          }
        }
      }

      // Now move all matching sentinels members past the current doc, and repeat
      for (int i = 0; i < context.sentinelIndex; i++) {
        DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
        dsi.movePast(context.document);
      }
    }

    return toReversedArray(queue);
  }

  private void determineSentinelIndex(EarlyTerminationScoringContext ctx) {
    // Now we try to find our sentinel set
    ctx.runningScore = ctx.startingPotential;

    int i;
    for (i = 0; i < sortedSentinels.size() && ctx.runningScore > ctx.minCandidateScore; i++) {
      ctx.runningScore -= sortedSentinels.get(i).score;
    }

    if (ctx.sentinelIndex != i) {
      ////CallTable.increment("sentinel_change");
    }

    ctx.sentinelIndex = i;
  }

  private void buildSentinels(EarlyTerminationScoringContext ctx, Parameters qp) {
    String type = retrieval.getGlobalParameters().get("sort", "length");
    sortedSentinels = SortStrategies.populateIndependentSentinels(ctx);

    if (qp.get("seqdep", true)) {
      if (type.equals("length")) {
        SortStrategies.fullLengthSort(sortedSentinels);
      } else if (type.equals("score")) {
        SortStrategies.fullScoreSort(sortedSentinels);
      } else if (type.equals("length-split")) {
        SortStrategies.splitLengthSort(sortedSentinels);
      } else if (type.equals("score-split")) {
        SortStrategies.splitScoreSort(sortedSentinels);
      } else if (type.equals("mixed-ls")) {
        SortStrategies.mixedLSSort(sortedSentinels);
      } else if (type.equals("mixed-sl")) {
        SortStrategies.mixedSLSort(sortedSentinels);
      } else {
        throw new IllegalArgumentException(String.format("What the hell is %s?", type));
      }
    } else {
      throw new IllegalArgumentException("Unimplemented score-split outside Seqdep.");
    }

  }
}
