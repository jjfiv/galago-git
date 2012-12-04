// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.MovableIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using WAND.
 * Generally this causes a substantial speedup in processing time.
 * 
 * You may get small perturbations in rank order at some ranks. As far as I have been able
 * to ascertain, it's from rounding errors b/c the scorers are sometimes not executed in the 
 * same order, causing differences in the 13th decimal place or below (it causes a rank inversion,
 * but not based on a meaningful number).
 *
 * @author irmarc
 */
public class WANDScoreDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;
  Index index;
  Comparator<Sentinel> comp;
  double scoreMinimums;
  Sentinel[] sortedSentinels = null;
  
  public WANDScoreDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
    this.index = retrieval.getIndex();
    comp = new SentinelPositionComparator();
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    EarlyTerminationScoringContext context = new EarlyTerminationScoringContext();
    double factor = retrieval.getGlobalParameters().get("thresholdFactor", 1.0);
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
    buildSentinels(context, queryParams);

    // Going to need the minimum set
    scoreMinimums = 0.0;
    for (Sentinel s : sortedSentinels) {
      scoreMinimums += s.iterator.getWeight() * s.score;
      s.score = s.iterator.getWeight() * (s.iterator.maximumScore() - s.iterator.minimumScore());
    }

    context.document = -1;
    int advancePosition;
    fullSort(sortedSentinels);
    while (true) {
      advancePosition = -1;
      int pivotPosition = findPivot(context.minCandidateScore);

      if (pivotPosition == -1) {
        break;
      }

      if (sortedSentinels[pivotPosition].iterator.isDone()) {
        break;
      }

      int pivot = sortedSentinels[pivotPosition].iterator.currentCandidate();

      if (pivot <= context.document) {
        advancePosition = pickAdvancingSentinel(pivotPosition, pivot);
        sortedSentinels[advancePosition].iterator.movePast(context.document);

      } else {
        if (sortedSentinels[0].iterator.currentCandidate() == pivot) {
          // We're in business. Let's score.
          context.document = pivot;
          score(context);

          if (requested < 0 || queue.size() <= requested || context.runningScore > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(context.document, context.runningScore);
            queue.add(scoredDocument);
          }
          if (requested > 0 && queue.size() > requested) {
            queue.poll();
            context.minCandidateScore = factor * queue.peek().score;
          }
        } else {
          advancePosition = pickAdvancingSentinel(pivotPosition, pivot);
          sortedSentinels[advancePosition].iterator.syncTo(pivot);
        }
      }

      // We only moved one iterator, so we only need to worry about putting that one in the right place
      if (advancePosition != -1) {
        shuffleDown(sortedSentinels, advancePosition);
      }
    }

    return toReversedArray(queue);
  }



  private void fullSort(Sentinel[] s) {
    Arrays.sort(s, comp);
  }

  // Premise here is that the 'start' iterator is the one that moved forward, but it was already behind
  // any other iterator at position n where 0 <= n < start. So we don't even look at those. Makes the sort
  // linear at worst.
  private void shuffleDown(Sentinel[] s, int start) {
    for (int i = start; i < s.length - 1; i++) {
      int result = comp.compare(s[i], s[i + 1]);
      if (result <= 0) {
        break;
      } else {
        Sentinel tmp = s[i];
        s[i] = s[i + 1];
        s[i + 1] = tmp;
      }
    }
  }

  private void score(EarlyTerminationScoringContext context) throws IOException {
    context.moveLengths(context.document);

    // Setup to score
    context.runningScore = context.startingPotential;
    System.arraycopy(context.startingPotentials, 0, context.potentials, 0,
            context.startingPotentials.length);

    // now score all scorers
    int i;
    for (i = 0; i < sortedSentinels.length; i++) {
      DeltaScoringIterator dsi = sortedSentinels[i].iterator;
      dsi.syncTo(context.document); // just in case
      dsi.deltaScore();
    }
  }

  private void buildSentinels(EarlyTerminationScoringContext ctx, Parameters qp) {
    String type = retrieval.getGlobalParameters().get("sort", "length");
    ArrayList<Sentinel> tmp = SortStrategies.populateIndependentSentinels(ctx, false);

    if (qp.get("seqdep", true)) {
      if (type.equals("length")) {
        SortStrategies.fullLengthSort(tmp);
      } else if (type.equals("score")) {
        SortStrategies.fullScoreSort(tmp);
      } else if (type.equals("length-split")) {
        SortStrategies.splitLengthSort(tmp);
      } else if (type.equals("score-split")) {
        SortStrategies.splitScoreSort(tmp);
      } else if (type.equals("mixed-ls")) {
        SortStrategies.mixedLSSort(tmp);
      } else if (type.equals("mixed-sl")) {
        SortStrategies.mixedSLSort(tmp);
      } else {
        throw new IllegalArgumentException(String.format("What the hell is %s?", type));
      }
    }
    sortedSentinels = tmp.toArray(new Sentinel[0]);
  }

  private int findPivot(double threshold) {
    if (threshold == Double.NEGATIVE_INFINITY) {
      return 0;
    }

    double sum = scoreMinimums;

    for (int i = 0; i < sortedSentinels.length; i++) {
      DeltaScoringIterator dsi = sortedSentinels[i].iterator;
      if (!dsi.isDone()) {
        sum += sortedSentinels[i].score;
      }

      if (sum > threshold) {
        return i;
      }
    }

    return -1; // couldn't exceed threshold
  }

  /**
   * Returns the iterator that should be advanced. The current selection
   * strategy involves using the iterator w/ the lowest df (translates to
   * highest idf), under the assumption that the lowest df will have the
   * largest skips in doc ids in its list. Candidates are from sorted(0..limit),
   * inclusive.
   *
   * @return The iterator that should be advanced next
   */
  private int pickAdvancingSentinel(int limit, int limitDoc) {
    long minDF = Long.MAX_VALUE;
    int minPos = 0;
    for (int i = 0; i < limit; i++) {
      DeltaScoringIterator dsi = sortedSentinels[i].iterator;
      if (dsi.currentCandidate() < limitDoc && dsi.totalEntries() < minDF) {
        minDF = sortedSentinels[i].iterator.totalEntries();
        minPos = i;
      }
    }
    return minPos;
  }
}
