// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.retrieval.processing;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.*;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.processing.DeltaScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ProcessingModel;
import org.lemurproject.galago.core.retrieval.processing.SoftDeltaScoringContext;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.optimize.ReplaceEstimatedIteratorTraversal;
import org.lemurproject.galago.core.scoring.Estimator;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Fully evaluates all "stored" terms, but delays processing on unordered and
 * ordered windows until we have a better sense of what *needs* to be scored in
 * order to create a final ranked list.
 *
 * @author irmarc
 */
public class AdaptiveEveryDelayedModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  List<Integer> whitelist;

  public AdaptiveEveryDelayedModel(LocalRetrieval lr) {
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
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    int requested = (int) queryParams.get("requested", 1000);

    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);

    // Creates our nodes
    MovableIterator iterator = retrieval.createIterator(queryParams, queryTree, context);
    computeNonEstimatorIndex(context);
    context.counts = new short[context.scorers.size() - context.sentinelIndex];


    // Split-heap
    PriorityQueue<EstimatedDocument> bottom = new PriorityQueue<EstimatedDocument>(requested,
            new EstimatedDocument.MaxComparator());
    PriorityQueue<EstimatedDocument> top = new PriorityQueue<EstimatedDocument>(requested);
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
      for (i = 0; i < context.sentinelIndex; i++) {
        DeltaScoringIterator dsi = context.scorers.get(i);
	dsi.syncTo(context.document);
        dsi.deltaScore();
        ////CallTable.increment("scops");
      }

      // Soft scoring - this is a little more work because we need to completely remove the 
      // potential of the iterator from the main score and add the min/maxes of the soft scorer.
      while ((context.runningScore + context.max) > context.minCandidateScore
              && i < context.scorers.size()) {
        context.scorers.get(i).syncTo(context.document);

        // remove the score from the runningScore
        DeltaScoringIterator dsi = context.scorers.get(i);
        Estimator e = (Estimator) dsi;
        context.runningScore -= (dsi.getWeight() * dsi.maximumScore());

        // Now update the mins and maxes
        double[] range = e.estimateWithUpdate(context, i - context.sentinelIndex);
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
                  context.min, context.max, context.getLength(), context.counts);
          estimate.length = (short) context.getLength();
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
                    context.min, context.max, context.getLength(), context.counts);
            estimate.length = (short) context.getLength();
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

            // Adjust the heaps
            adjustTopHeap(top, context);
            adjustBottomHeap(top, bottom, context);

          } else if (context.runningScore + context.max > context.minCandidateScore) {
            // It at least made it into the bottom heap
            EstimatedDocument estimate =
                    new EstimatedDocument(context.document, context.runningScore,
                    context.min, context.max, context.getLength(), context.counts);
            estimate.length = (short) context.getLength();
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
        context.scorers.get(i).movePast(candidate);
      }
    }

    adjustTopHeap(top, context);
    adjustBottomHeap(top, bottom, context);

    ////CallTable.set("heap_end_size", bottom.size() + top.size());
    // Want to use the top sort order
    top.addAll(bottom);

    if (retrieval.getGlobalParameters().get("twopass", false)) {
      return toReversedArray(completeScoring(top, queryTree, queryParams));
    }

    return toReversedArray(top);
  }

  private PriorityQueue<ScoredDocument> completeScoring(PriorityQueue<EstimatedDocument> heap,
          Node queryTree, Parameters queryParams) throws Exception {

    int requested = (int) queryParams.get("requested", 1000);
    TIntArrayList docids = new TIntArrayList();
    TIntObjectHashMap<EstimatedDocument> map = new TIntObjectHashMap<EstimatedDocument>();
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Rebuild query tree and scorers
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    ReplaceEstimatedIteratorTraversal traversal = 
            new ReplaceEstimatedIteratorTraversal(retrieval, queryParams);
    traversal.context = context;
    Node newroot = StructuredQuery.walk(traversal, queryTree);

    // Short-circuit if there are no scorers to use (no uncertainty)
    if (context.scorers.isEmpty()) {
      while (!heap.isEmpty()) {
        queue.add(heap.poll());
      }

      while (queue.size() > requested) {
        queue.poll();
      }
      return queue;
    }

    // Otherwise unload the heap, prep for iteration
    while (!heap.isEmpty()) {
      EstimatedDocument ed = heap.poll();
      docids.add(ed.document);
      map.put(ed.document, ed);
    }

    // Scorers should be ready
    ProcessingModel.initializeLengths(retrieval, context);
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);
    docids.sort();
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

    for (int i = 0; i < docids.size(); i++) {
      int candidate = docids.get(i);
      for (int j = 0; j < context.sentinelIndex; j++) {
        if (!context.scorers.get(j).isDone()) {
          context.scorers.get(j).syncTo(candidate);
        }
      }

      // Otherwise move lengths
      context.document = candidate;
      context.moveLengths(candidate);
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

      // Setup to score
      context.runningScore = context.startingPotential + map.get(candidate).score;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // now score sentinels w/out question
      int k;
      for (k = 0; k < context.sentinelIndex; k++) {
        DeltaScoringIterator dsi = context.scorers.get(k);
        dsi.deltaScore();
        ////CallTable.increment("scops");
      }

      // Now score the rest, but keep checking
      while (context.runningScore > context.minCandidateScore && k < context.scorers.size()) {
        DeltaScoringIterator dsi = context.scorers.get(k);
        dsi.syncTo(context.document);
        dsi.deltaScore();
        ////CallTable.increment("scops");
        k++;
      }

      // Fully scored it
      if (k == context.scorers.size()) {
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
              //computeSentinels(context);  <---- need to think on this a bit first
            }
          }
        } else {
          ////CallTable.increment("heap_miss");
        }
        ////CallTable.max("heap_max_size", queue.size());
      } else {
        ////CallTable.increment("doc_truncated");
        ////CallTable.increment("scores_skipped", context.scorers.size() - i);
      }

      // Now move all matching sentinels members forward, and repeat
      for (k = 0; k < context.sentinelIndex; k++) {
          context.scorers.get(k).movePast(candidate);
      }
    }


    return queue;
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
    for (int i = 0; i < whitelist.size(); i++) {
      int candidate = whitelist.get(i);
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

  private void adjustTopHeap(PriorityQueue<EstimatedDocument> top, SoftDeltaScoringContext context) {
    EstimatedDocument ed;
    do {
      ed = top.poll();
      for (int i = context.sentinelIndex; i < context.scorers.size(); i++) {
        Estimator e = (Estimator) context.scorers.get(i);
        e.adjustEstimate(context, ed, i - context.sentinelIndex);
      }
      top.add(ed);
    } while (ed != top.peek());

    context.minCandidateScore = ed.score + ed.min;
  }

  private void adjustBottomHeap(PriorityQueue<EstimatedDocument> top, PriorityQueue<EstimatedDocument> bottom,
          SoftDeltaScoringContext context) {
    // Whittle off what we can
    EstimatedDocument ed;
    if (bottom.size() > 0) {
      do {
        ed = bottom.poll();
        for (int i = context.sentinelIndex; i < context.scorers.size(); i++) {
          Estimator e = (Estimator) context.scorers.get(i);
          e.adjustEstimate(context, ed, i - context.sentinelIndex);
        }
        if (ed.score + ed.max > context.minCandidateScore) {
          bottom.add(ed);
        } else {
          ////CallTable.increment("heap_bottom_eject");
        }
      } while (ed != bottom.peek() && !bottom.isEmpty());
    }
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
  public void defineWorkingSet(List<Integer> docs) {
    Collections.sort(docs);
    whitelist = docs;
  }
}
