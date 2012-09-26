/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.retrieval.EstimatedDocument;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.StagedLocalRetrieval;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.retrieval.traversal.optimize.ReplaceEstimatedIteratorTraversal;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Contains algorithms to complete scoring from a partially evaluated list.
 *
 *
 * @author irmarc
 */
public abstract class AbstractPartialProcessor extends ProcessingModel {

  protected StagedLocalRetrieval retrieval;
  // Need this for sentinel checking
  double maxPartialScore;

  /*
   * Method used to route the completion step
   */
  protected PriorityQueue<ScoredDocument> completeScoring(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {
    String completer = retrieval.getGlobalParameters().getString("completion");

    if (completer.equals("naive")) {
      return naive(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("delta")) {
      return delta(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("tear")) {
      return tear(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("onepass")) {
      return onepass(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("sampled")) {
      return sampled(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("hi")) {
      return hi_estimate(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("lo")) {
      return lo_estimate(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("avg")) {
      return avg_estimate(topHeap, bottomHeap, queryTree, queryParams);
    } else if (completer.equals("top")) {
      return topOnly(topHeap, bottomHeap, queryTree, queryParams);
    } else {
      throw new IllegalArgumentException(String.format("Don't know completer %s\n", completer));
    }
  }

  private void computeSentinels(DeltaScoringContext ctx) {
    // Now we try to find our sentinel set - have to use the best "partial" to make
    // it comparable to the finish doc scores
    ctx.runningScore = ctx.startingPotential + maxPartialScore;
    System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
            ctx.startingPotentials.length);
    int i;
    for (i = 0; i < ctx.scorers.size() && ctx.runningScore > ctx.minCandidateScore;
            i++) {
      ((DeltaScoringIterator) ctx.scorers.get(i)).maximumDifference();
    }

    if (ctx.sentinelIndex != i) {
      ////CallTable.increment("sentinel_change");
    }

    ctx.sentinelIndex = i;
  }

  protected PriorityQueue<ScoredDocument> naive(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    topHeap.addAll(bottomHeap);
    int requested = (int) queryParams.get("requested", 1000);
    boolean rereadLengths = !retrieval.getGlobalParameters().get("cacheLengths", false);
    TIntArrayList docids = new TIntArrayList();
    TIntObjectHashMap<EstimatedDocument> map = new TIntObjectHashMap<EstimatedDocument>();
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Rebuild query tree and scorers
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    ReplaceEstimatedIteratorTraversal traversal = new ReplaceEstimatedIteratorTraversal(retrieval, queryParams);
    traversal.context = context;

    Node newroot = StructuredQuery.walk(traversal, queryTree); // materializes scorers

    // Short-circuit if there are no scorers to use (no uncertainty)
    if (context.scorers.isEmpty()) {
      while (!topHeap.isEmpty()) {
        queue.add(topHeap.poll());
      }

      while (queue.size() > requested) {
        queue.poll();
      }
      return queue;
    }

    // Otherwise unload the heap, prep for iteration
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      docids.add(ed.document);
      map.put(ed.document, ed);
    }

    // Scorers should be ready - just straight score all of them.
    if (rereadLengths) {
      ProcessingModel.initializeLengths(retrieval, context);
    }
    context.minCandidateScore = Double.NEGATIVE_INFINITY;

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);
    docids.sort();

    for (int i = 0; i < docids.size(); i++) {
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

      // Set up to score current candidate
      context.document = docids.get(i);
      if (rereadLengths) {
        context.moveLengths(context.document);
      }
      context.runningScore = context.startingPotential + map.get(context.document).score;
      System.err.printf("Completing %d: startingPot=%f, partial=%f\n",
              context.document, context.startingPotential, 
              map.get(context.document).score);
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // Score it fully (no checking)
      for (DeltaScoringIterator dsi : context.scorers) {
        dsi.syncTo(context.document);
        if (rereadLengths) {
          dsi.deltaScore();
        } else {
          dsi.deltaScore(map.get(context.document).length);
        }
        ////CallTable.increment("scops");
      }

      ////CallTable.increment("doc_finish");
      if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
        ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
        queue.add(scoredDocument);
        ////CallTable.increment("heap_insert");
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
          ////CallTable.increment("heap_eject");
        }
      } else {
        ////CallTable.increment("heap_miss");
      }
      ////CallTable.max("heap_max_size", queue.size());

      // Move iterators when scoring (see top of algorithm)
    }
    return queue;
  }

  protected PriorityQueue<ScoredDocument> delta(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    topHeap.addAll(bottomHeap);
    int requested = (int) queryParams.get("requested", 1000);
    boolean rereadLengths = !retrieval.getGlobalParameters().get("cacheLengths", false);
    TIntArrayList docids = new TIntArrayList();
    TIntObjectHashMap<EstimatedDocument> map = new TIntObjectHashMap<EstimatedDocument>();
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Rebuild query tree and scorers
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    ReplaceEstimatedIteratorTraversal traversal = new ReplaceEstimatedIteratorTraversal(retrieval, queryParams);
    traversal.context = context;
    Node newroot = StructuredQuery.walk(traversal, queryTree);

    // Short-circuit if there are no scorers to use (no uncertainty)
    if (context.scorers.isEmpty()) {
      while (!topHeap.isEmpty()) {
        queue.add(topHeap.poll());
      }

      while (queue.size() > requested) {
        queue.poll();
      }
      return queue;
    }

    // Otherwise unload the heap, prep for iteration
    maxPartialScore = 0.0;
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      docids.add(ed.document);
      map.put(ed.document, ed);
      maxPartialScore = Math.max(maxPartialScore, ed.score);
    }

    // Scorers should be ready
    if (rereadLengths) {
      ProcessingModel.initializeLengths(retrieval, context);
    }
    context.minCandidateScore = Double.NEGATIVE_INFINITY;
    context.sentinelIndex = context.scorers.size();

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);
    docids.sort();
    Collections.sort(context.scorers, new IteratorLengthComparator());

    for (int i = 0; i < docids.size(); i++) {
      int candidate = docids.get(i);
      for (int j = 0; j < context.sentinelIndex; j++) {
        if (!context.scorers.get(j).isDone()) {
          context.scorers.get(j).syncTo(candidate);
        }
      }

      // Otherwise move lengths
      context.document = candidate;
      if (rereadLengths) {
        context.moveLengths(candidate);
      }
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

      // Setup to score - add in the existing score from the initial run
      context.runningScore = context.startingPotential + map.get(candidate).score;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // now score sentinels w/out question
      int k;
      for (k = 0; k < context.sentinelIndex; k++) {
        DeltaScoringIterator dsi = context.scorers.get(k);
        if (rereadLengths) {
          dsi.deltaScore();
        } else {
          dsi.deltaScore(map.get(context.document).length);
        }
        ////CallTable.increment("scops");
      }

      // Now score the rest, but keep checking
      while (context.runningScore > context.minCandidateScore && k < context.scorers.size()) {
        DeltaScoringIterator dsi = context.scorers.get(k);
        dsi.syncTo(context.document);
        if (rereadLengths) {
          dsi.deltaScore();
        } else {
          dsi.deltaScore(map.get(context.document).length);
        }
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
              computeSentinels(context);
            }
          }
        } else {
          ////CallTable.increment("heap_miss");
        }
        ////CallTable.max("heap_max_size", queue.size());
      } else {
        ////CallTable.increment("doc_truncated");
        ////CallTable.increment("scores_skipped", context.scorers.size() - k);
      }

      // Now move all matching sentinels members forward, and repeat
      for (k = 0; k < context.sentinelIndex; k++) {
        if (context.scorers.get(k).hasMatch(candidate)) {
          context.scorers.get(k).movePast(candidate);
        }
      }
    }
    return queue;
  }

  protected PriorityQueue<ScoredDocument> onepass(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {
    if (retrieval.getGlobalParameters().containsKey("syntheticCounts")) {
      throw new IllegalArgumentException("Can't correctly run onepass with stored synthetic collection stats.");
    }

    topHeap.addAll(bottomHeap);
    int requested = (int) queryParams.get("requested", 1000);
    boolean rereadLengths = !retrieval.getGlobalParameters().get("cacheLengths", false);
    TIntArrayList docids = new TIntArrayList();
    TIntObjectHashMap<EstimatedDocument> map = new TIntObjectHashMap<EstimatedDocument>();
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Rebuild query tree and scorers
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);

    ReplaceEstimatedIteratorTraversal traversal = new ReplaceEstimatedIteratorTraversal(retrieval, queryParams);

    traversal.context = context;

    // Also need to unload the heap now, b/c we need the list of docids for annotation
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      docids.add(ed.document);
      map.put(ed.document, ed);
    }

    docids.sort();
    retrieval.candidates = new TIntHashSet(docids);
    Node newroot = StructuredQuery.walk(traversal, queryTree); // materializes scorers

    // Short-circuit if there are no scorers to use (no uncertainty)
    if (context.scorers.isEmpty()) {
      for (int i = 0; i < docids.size(); i++) {
        EstimatedDocument ed = map.get(docids.get(i));
        queue.add(new ScoredDocument(ed.document, ed.score));
      }

      while (queue.size() > requested) {
        queue.poll();
      }
      return queue;
    }


    // Scorers should be ready - just straight score all of them.
    if (rereadLengths) {
      ProcessingModel.initializeLengths(retrieval, context);
    }
    context.minCandidateScore = Double.NEGATIVE_INFINITY;

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);

    for (int i = 0; i < docids.size(); i++) {
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

      // Set up to score current candidate
      context.document = docids.get(i);
      if (rereadLengths) {
        context.moveLengths(context.document);
      }
      context.runningScore = context.startingPotential + map.get(context.document).score;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // Score it fully using the cached counts
      for (DeltaScoringIterator dsi : context.scorers) {
        int count = 0;
        PriorityQueue<Pair> list = retrieval.occurrenceCache.get(Utility.toString(dsi.key()));
        assert (list.peek().doc >= context.document);
        if (!list.isEmpty() && list.peek().doc == context.document) {
          Pair top = list.poll();
          count = top.count;
        }
        if (rereadLengths) {
          dsi.deltaScore(count, context.getLength());
        } else {
          dsi.deltaScore(count, map.get(context.document).length);
        }
        ////CallTable.increment("scops");
      }

      ////CallTable.increment("doc_finish");
      if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
        ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
        queue.add(scoredDocument);
        ////CallTable.increment("heap_insert");
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
          ////CallTable.increment("heap_eject");
        }
      } else {
        ////CallTable.increment("heap_miss");
      }
      ////CallTable.max("heap_max_size", queue.size());

      // Move iterators when scoring (see top of algorithm)
    }
    return queue;
  }

  protected PriorityQueue<ScoredDocument> sampled(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {
    if (retrieval.getGlobalParameters().containsKey("syntheticCounts")) {
      throw new IllegalArgumentException("Can't correctly run sampled with stored synthetic collection stats.");
    }
    topHeap.addAll(bottomHeap);
    int requested = (int) queryParams.get("requested", 1000);
    boolean rereadLengths = !retrieval.getGlobalParameters().get("cacheLengths", false);
    TIntArrayList docids = new TIntArrayList();
    TIntObjectHashMap<EstimatedDocument> map = new TIntObjectHashMap<EstimatedDocument>();
    PriorityQueue<ScoredDocument> queue = new PriorityQueue<ScoredDocument>(requested);

    // Rebuild query tree and scorers
    SoftDeltaScoringContext context = new SoftDeltaScoringContext();
    context.potentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    context.startingPotentials = new double[(int) queryParams.get("numPotentials", queryParams.get("numberOfTerms", 0))];
    Arrays.fill(context.startingPotentials, 0);
    ReplaceEstimatedIteratorTraversal traversal = new ReplaceEstimatedIteratorTraversal(retrieval, queryParams);
    traversal.context = context;

    // Also need to unload the heap now, b/c we need the list of docids for annotation
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      docids.add(ed.document);
      map.put(ed.document, ed);
    }
    docids.sort();
    retrieval.sortedCandidates = docids;
    Node newroot = StructuredQuery.walk(traversal, queryTree); // materializes scorers

    // Short-circuit if there are no scorers to use (no uncertainty)
    if (context.scorers.isEmpty()) {
      for (int i = 0; i < docids.size(); i++) {
        EstimatedDocument ed = map.get(docids.get(i));
        queue.add(new ScoredDocument(ed.document, ed.score));
      }

      while (queue.size() > requested) {
        queue.poll();
      }
      return queue;
    }

    // Scorers should be ready - just straight score all of them.
    if (rereadLengths) {
      ProcessingModel.initializeLengths(retrieval, context);
    }
    context.minCandidateScore = Double.NEGATIVE_INFINITY;

    // Compute the starting potential
    context.scorers.get(0).aggregatePotentials(context);

    for (int i = 0; i < docids.size(); i++) {
      ////CallTable.increment("doc_begin");
      ////CallTable.increment("score_possible", context.scorers.size());

      // Set up to score current candidate
      context.document = docids.get(i);
      if (rereadLengths) {
        context.moveLengths(context.document);
      }
      context.runningScore = context.startingPotential + map.get(context.document).score;
      System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
              context.startingPotentials.length);

      // Score it fully using the cached counts
      for (DeltaScoringIterator dsi : context.scorers) {
        int count = 0;
        PriorityQueue<Pair> list = retrieval.occurrenceCache.get(Utility.toString(dsi.key()));
        assert (list.peek().doc >= context.document);
        if (!list.isEmpty() && list.peek().doc == context.document) {
          Pair top = list.poll();
          count = top.count;
        }
        if (rereadLengths) {
          dsi.deltaScore(count, context.getLength());
        } else {
          dsi.deltaScore(count, map.get(context.document).length);
        }
        ////CallTable.increment("scops");
      }

      ////CallTable.increment("doc_finish");
      if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
        ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
        queue.add(scoredDocument);
        ////CallTable.increment("heap_insert");
        if (requested > 0 && queue.size() > requested) {
          queue.poll();
          ////CallTable.increment("heap_eject");
        }
      } else {
        ////CallTable.increment("heap_miss");
      }
      ////CallTable.max("heap_max_size", queue.size());

      // Move iterators when scoring (see top of algorithm)
    }
    return queue;
  }

  protected PriorityQueue<ScoredDocument> tear(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    return null;
  }

  protected PriorityQueue<ScoredDocument> topOnly(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {
    PriorityQueue<ScoredDocument> finalHeap = new PriorityQueue<ScoredDocument>(topHeap.size());
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
    }
    return finalHeap;
  }

  protected PriorityQueue<ScoredDocument> hi_estimate(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    PriorityQueue<ScoredDocument> finalHeap = new PriorityQueue<ScoredDocument>(topHeap.size());
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      ed.score += ed.max;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
    }

    while (!bottomHeap.isEmpty()) {
      EstimatedDocument ed = bottomHeap.poll();
      ed.score += ed.max;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
      finalHeap.poll();
    }
    return finalHeap;
  }

  protected PriorityQueue<ScoredDocument> lo_estimate(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    PriorityQueue<ScoredDocument> finalHeap = new PriorityQueue<ScoredDocument>(topHeap.size());
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      ed.score += ed.min;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
    }

    while (!bottomHeap.isEmpty()) {
      EstimatedDocument ed = bottomHeap.poll();
      ed.score += ed.min;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
      finalHeap.poll();
    }
    return finalHeap;
  }

  protected PriorityQueue<ScoredDocument> avg_estimate(PriorityQueue<EstimatedDocument> topHeap,
          PriorityQueue<EstimatedDocument> bottomHeap,
          Node queryTree, Parameters queryParams) throws Exception {

    PriorityQueue<ScoredDocument> finalHeap = new PriorityQueue<ScoredDocument>(topHeap.size());
    while (!topHeap.isEmpty()) {
      EstimatedDocument ed = topHeap.poll();
      ed.score += (ed.max + ed.min) / 2;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
    }

    while (!bottomHeap.isEmpty()) {
      EstimatedDocument ed = bottomHeap.poll();
      ed.score += (ed.max + ed.min) / 2;
      finalHeap.add(new ScoredDocument(ed.document, ed.score));
      finalHeap.poll();
    }
    return finalHeap;
  }

  public static String makeNodeKey(Node n) {
    String[] children = new String[n.numChildren()];
    for (int i = 0; i < n.numChildren(); i++) {
      children[i] = n.getChild(i).getDefaultParameter();
    }
    String key = String.format("%s%d:%s", n.getOperator().substring(0, 1).toUpperCase(),
            n.getNodeParameters().getLong("default"), Utility.join(children, "~"));
    return key;
  }
}
