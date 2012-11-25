// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.StagedLocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.scoring.Estimator;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Fully evaluates all "stored" terms, but delays processing on unordered and
 * ordered windows until we have a better sense of what *needs* to be scored in
 * order to create a final ranked list.
 *
 * @author irmarc
 */
public class DelayedScoringModel extends AbstractPartialProcessor {

  Index index;
  int[] whitelist;

  public DelayedScoringModel(LocalRetrieval lr) {
    retrieval = (StagedLocalRetrieval) lr;
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
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    int requested = (int) queryParams.get("requested", 1000);

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);

    // Creates our nodes
    MovableIterator iterator = retrieval.createIterator(queryParams, queryTree, context);

    // Split-heap
    PriorityQueue<EstimatedDocument> bottom = new PriorityQueue<EstimatedDocument>(requested,
            new EstimatedDocument.MaxComparator());
    PriorityQueue<EstimatedDocument> top = new PriorityQueue<EstimatedDocument>(requested,
            new EstimatedDocument.MinComparator());
    EstimatedDocument thresholdDoc = null;

    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);

    // Make sure the scorers are sorted properly
    // We set sentinels to be all non-estimators (since the estimators are repetitive
    Collections.sort(context.scorers, new Comparator<DeltaScoringIterator>() {

      @Override
      public int compare(DeltaScoringIterator t, DeltaScoringIterator t1) {
        // Push non-estimators first
        if (Estimator.class.isAssignableFrom(t.getClass())
                && !Estimator.class.isAssignableFrom(t1.getClass())) {
          return 1;
        }

        if (Estimator.class.isAssignableFrom(t1.getClass())
                && !Estimator.class.isAssignableFrom(t.getClass())) {
          return -1;
        }

        // Then base it on number of entries (convention)
        return (int) (t.totalEntries() - t1.totalEntries());
      }
    });
    computeNonEstimatorIndex(context);
    

    // Scoring loop - note sentinels == non-estimators for this one. Maybe something
    // cleverer on the horizon, but let's get this working first.
    //
    // while (candidates)
    //  1) Find a candidate (only needs to be from the non-estimators)
    //  2) Hard scoring
    //  3) Soft scoring
    //  4) Determine if it should enter the queue (kth entry minimum is cutoff)
    // 
    // Can prune based on that score as well.
    while (true) {
      int candidate = Integer.MAX_VALUE;
      for (int i = 0; i < context.sentinelIndex; i++) {
        if (!context.scorers.get(i).isDone()) {
          candidate = Math.min(candidate, context.scorers.get(i).currentCandidate());
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
      context.min = context.max = 0;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // now score sentinels w/out question
      int i;
      for (i = 0; i < context.sentinelIndex; ++i) {
        DeltaScoringIterator dsi = context.scorers.get(i);
	dsi.syncTo(context.document);
        dsi.deltaScore();
        ////CallTable.increment("scops");
      }

      // Soft scoring - this is a little more work because we need to completely
      // remove the potential of the iterator from the main score and add the 
      // min/maxes of the soft scorer.
      while ((context.runningScore + context.max) > context.minCandidateScore
              && i < context.scorers.size()) {
        context.scorers.get(i).syncTo(context.document);

        // remove the score from the runningScore
        DeltaScoringIterator dsi = context.scorers.get(i);
        Estimator e = (Estimator) dsi;
        context.runningScore -= (dsi.getWeight() * dsi.maximumScore());

        // Now update the mins and maxes
        double[] range = e.estimate(context);
        context.min += range[0];
        context.max += range[1];
        ////CallTable.increment("scops");
        i++;
      }

      // Fully scored it - should go in the candidates
      if (i == context.scorers.size()) {
        ////CallTable.increment("doc_finish");
        if (requested < 0 || top.size() < requested) {
          // Just throw it in there
          EstimatedDocument estimate =
                  new EstimatedDocument(context.document, context.runningScore,
                  context.min, context.max, context.getLength());
          top.add(estimate);
          ////CallTable.increment("heap_top_insert");
          if (top.size() == requested) {
            // Now it's big enough
            thresholdDoc = top.peek();
            context.minCandidateScore = thresholdDoc.score + thresholdDoc.min;
            ////CallTable.increment("threshold_change");
          }
        } else {
          // Determine if we're adding to the top heap
          if (context.runningScore + context.min > context.minCandidateScore) {
            // It's going in the top heap, have a threshold change
            EstimatedDocument estimate =
                    new EstimatedDocument(context.document, context.runningScore,
                    context.min, context.max, context.getLength());
            // shuffle things around
            top.add(estimate);
            ////CallTable.increment("heap_top_insert");
            bottom.add(top.poll());
            ////CallTable.increment("heap_top_eject");
            ////CallTable.increment("heap_bottom_insert");

            // now reset the threshold
            thresholdDoc = top.peek();
            context.minCandidateScore = thresholdDoc.score + thresholdDoc.min;
            ////CallTable.increment("threshold_change");

            // For now, try to pull stuff off the end
            // This is really the only chance we get to shorten.
            EstimatedDocument tail = bottom.peek();
            while (tail != null && tail.score + tail.max < context.minCandidateScore) {
              bottom.poll();
              tail = bottom.peek();
              ////CallTable.increment("heap_bottom_eject");
            }
          } else if (context.runningScore + context.max > context.minCandidateScore) {
            // It at least made it into the bottom heap
            EstimatedDocument estimate =
                    new EstimatedDocument(context.document, context.runningScore,
                    context.min, context.max, context.getLength());
            bottom.add(estimate);
            ////CallTable.increment("heap_bottom_insert");
          } else {
            // Not put anywhere.
            ////CallTable.increment("heap_miss");
          }
        }
      } else {
        ////CallTable.increment("doc_truncated");
        ////CallTable.increment("scores_skipped", context.scorers.size() - i);
      }

      // Done with all changes. Update max counters
      ////CallTable.max("heap_max_size", bottom.size() + top.size());

      // Now move all matching sentinel members forward, and repeat
      for (i = 0; i < context.sentinelIndex; i++) {
        context.scorers.get(i).movePast(context.document);
      }
    }

    ////CallTable.set("heap_end_size", bottom.size() + top.size());

    if (retrieval.getGlobalParameters().containsKey("completion")) {
      return toReversedArray(completeScoring(top, bottom, queryTree, queryParams));
    }

    return toReversedArray(top);
  }

  public ScoredDocument[] executeWorkingSet(Node queryTree, Parameters queryParams)
          throws Exception {
    DeltaScoringContext context = new DeltaScoringContext();

    // Following operations are all just setup
    int requested = (int) queryParams.get("requested", 1000);

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    MovableIterator iterator = retrieval.createIterator(queryParams, queryTree, context);

    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);
    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);
    // Make sure the scorers are sorted properly
    Collections.sort(context.scorers, new Comparator<DeltaScoringIterator>() {

      @Override
      public int compare(DeltaScoringIterator t, DeltaScoringIterator t1) {
        return (int) (t.totalEntries() - t1.totalEntries());
      }
    });

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
        ////CallTable.increment("hard_score");
      }

      // Now score the rest, but keep checking
      while (context.runningScore > context.minCandidateScore && j < context.scorers.size()) {
        context.scorers.get(j).syncTo(context.document);
        context.scorers.get(j).deltaScore();
        j++;
        ////CallTable.increment("soft_score");
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
            }
          }
        }
      }
      // The sentinels are moved at the top, so we don't do it here.
    }


    return toReversedArray(queue);
  }

  private void computeNonEstimatorIndex(DeltaScoringContext ctx) {
    int i = 0;

    while (i < ctx.scorers.size()
            && !Estimator.class.isAssignableFrom(ctx.scorers.get(i).getClass())) {
      i++;
    }
    ctx.sentinelIndex = i;
  }

  @Override
  public void defineWorkingSet(int[] docs) {
    whitelist = docs;
  }
}
