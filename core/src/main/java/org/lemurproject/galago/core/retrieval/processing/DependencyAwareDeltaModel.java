// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.*;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.StructuredIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using Maxscore.
 * Generally this causes a substantial speedup in processing time.
 *
 * @author irmarc
 */
public class DependencyAwareDeltaModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  int[] whitelist;

  public DependencyAwareDeltaModel(LocalRetrieval lr) {
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

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    StructuredIterator iterator = retrieval.createIterator(queryParams, queryTree, context);

    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();
    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);

    // But ignore them now and use actual sentinels
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
        DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
        if (!dsi.isDone()) {
          candidate = Math.min(candidate, dsi.currentCandidate());
        }
      }

      // Means sentinels are done, we can quit
      if (candidate == Integer.MAX_VALUE) {
        break;
      }

      // Otherwise move lengths
      context.document = candidate;
      context.moveLengths(candidate);
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

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
        ////CallTable.increment("scops");
      }

      // Now score the rest, but keep checking
      while (context.runningScore > context.minCandidateScore && i < sortedSentinels.size()) {
        DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
        dsi.syncTo(context.document);
        dsi.deltaScore();
        ////CallTable.increment("scops");
        i++;
      }

      // Fully scored it
      if (i == sortedSentinels.size()) {
        ////CallTable.increment("doc_finish");
        if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
          ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
          queue.add(scoredDocument);
          ////CallTable.increment("heap_insert");
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
            ////CallTable.increment("heap_eject");
            if (context.minCandidateScore < queue.peek().score) {
              ////CallTable.increment("threshold_change");
              context.minCandidateScore = queue.peek().score;
              determineSentinelIndex(context);
            }
          }
        } else {
          ////CallTable.increment("heap_miss");
        }
        ////CallTable.max("heap_max_size", queue.size());
      } else {
        ////CallTable.increment("doc_truncated");
        ////CallTable.increment("scores_skipped", sortedSentinels.size() - i);
      }

      // Now move all matching sentinels members forward, and repeat
      for (i = 0; i < context.sentinelIndex; i++) {
        sortedSentinels.get(i).iterator.movePast(context.document);
      }
    }

    ////CallTable.increment("heap_end_size", queue.size());
    return toReversedArray(queue);
  }

  public ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    DeltaScoringContext context = new DeltaScoringContext();

    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    StructuredIterator iterator = retrieval.createIterator(queryParams, queryTree, context);

    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);
    // Make sure the scorers are sorted properly
    /*
     *
     * Collections.sort(context.scorers, new Comparator<DeltaScoringIterator>()
     * {
     *
     * @Override public int compare(DeltaScoringIterator t, DeltaScoringIterator
     * t1) { return (int) (t.totalEntries() - t1.totalEntries()); } });
     */

    // Routine is as follows:
    // 1) Find the next candidate from the sentinels
    // 2) Move sentinels and field length readers to candidate
    // 3) Score sentinels unconditionally
    // 4) while (runningScore > R)
    //      move iterator to candidate
    //      score candidate w/ iterator
    for (int i = 0; i < whitelist.length; i++) {
      int candidate = whitelist[i];
      for (int j = 0; j < context.sentinelIndex; j++) {
        if (!context.scorers.get(j).isDone()) {
          context.scorers.get(j).syncTo(candidate);
        }
      }

      // Otherwise move lengths
      context.document = candidate;
      context.moveLengths(candidate);

      // Setup to score
      context.runningScore = context.startingPotential;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // now score sentinels w/out question
      int j;
      for (j = 0; j < context.sentinelIndex; j++) {
        context.scorers.get(j).deltaScore();
      }

      // Now score the rest, but keep checking
      while (context.runningScore > context.minCandidateScore && j < context.scorers.size()) {
        context.scorers.get(j).syncTo(context.document);
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
              determineSentinelIndex(context);
            }
          }
        }
      }
      // The sentinels are moved at the top, so we don't do it here.
    }
    return toReversedArray(queue);
  }
  ArrayList<Sentinel> sortedSentinels = null;

  private void buildSentinels(DeltaScoringContext ctx, Parameters qp) {
    // If we expanded using SDM, we have dependencies
    if (qp.get("seqdep", false)) {
      // Check for degenerate case
      if (ctx.scorers.size() == 1) {
        sortedSentinels = new ArrayList<Sentinel>(ctx.scorers.size());
        ctx.runningScore = ctx.startingPotential;
        System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
                ctx.startingPotentials.length);
        ctx.scorers.get(0).maximumDifference();
        sortedSentinels.add(new Sentinel(ctx.scorers.get(0), ctx.startingPotential - ctx.runningScore));
      } else {
        // Stack scores based on dependencies
        sortedSentinels = SortStrategies.populateDependentSentinels(ctx);
      }
    } else {
      // Don't back out - throw a shit fit if we're not in the top.
      throw new IllegalArgumentException("Did not expand with SDM but using dependencies.");
    }

    // Now we figure out the sorting scheme.
    String type = retrieval.getGlobalParameters().get("sort", "length");
    if (type.equals("length")) {
      SortStrategies.fullLengthSort(sortedSentinels);
    } else if (type.equals("length-split")) {
      SortStrategies.splitLengthSort(sortedSentinels);
    } else if (type.equals("score-split")) {
      SortStrategies.splitScoreSort(sortedSentinels);
    } else if (type.equals("mixed-ls")) {
      SortStrategies.mixedLSSort(sortedSentinels);
    } else if (type.equals("mixed-sl")) {
      SortStrategies.mixedSLSort(sortedSentinels);
    } else {
      throw new IllegalArgumentException(String.format("What the hell is %s? %s doesn't do that.",
              type, this.getClass().getName()));
    }
  }

  private void determineSentinelIndex(DeltaScoringContext ctx) {
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

  @Override
  public void defineWorkingSet(int[] docs) {
    whitelist = docs;
  }
}
