/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import static org.lemurproject.galago.core.retrieval.processing.ProcessingModel.toReversedArray;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.FixedSizeSortedArray;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author sjh
 */
public class HeuristicMaxScoreDocumentModel extends ProcessingModel {

  private final LocalRetrieval retrieval;
  private final FieldStatistics colStats;

  public HeuristicMaxScoreDocumentModel(LocalRetrieval lr) throws Exception {
    this.retrieval = lr;
    this.colStats = lr.getCollectionStatistics("#lengths:document:part=lengths()");
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();
    int requested = (int) queryParams.get("requested", 1000);

    // step one: find the set of deltaScoringNodes in the tree
    List<Node> scoringNodes = new ArrayList();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    // step two: create an iterator for each node
    boolean shareNodes = queryParams.get("shareNodes", retrieval.getGlobalParameters().get("shareNodes", true));
    List<DeltaScoringIterator> scoringIterators = createScoringIterators(scoringNodes, retrieval, shareNodes);

    FixedSizeSortedArray<ScoredDocument> queue = new FixedSizeSortedArray(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    // step three: determine the collection segments
    // System.err.println(colStats.toString());
    long[] collectionSegments = determineCollectionSegments(queryParams.getDouble("mxerror"), requested, colStats.lastDocId);
    int currentSegment = 0;

//    for (int i = 0; i < collectionSegments.length; i++) {
//      System.err.println(i + "\t" + collectionSegments[i]);
//    }


    double maximumPossibleScore = 0.0;
    for (DeltaScoringIterator scorer : scoringIterators) {
      maximumPossibleScore += scorer.maximumWeightedScore();
    }

    // sentinel scores are set to collectionFrequency (sort function ensures decreasing order)
    Collections.sort(scoringIterators, new DeltaScoringIteratorMaxDiffComparator());

    // precompute statistics that allow us to update the quorum index
    double runningMaxScore = maximumPossibleScore;
    double[] maxScoreOfRemainingIterators = new double[scoringIterators.size()];
    for (int i = 0; i < scoringIterators.size(); i++) {
      // before scoring this iterator, the max possible score is:
      maxScoreOfRemainingIterators[i] = runningMaxScore;
      // after scoring this iterator (assuming min), the max possible score is:
      runningMaxScore -= scoringIterators.get(i).maximumDifference();
    }

    // all scorers are scored until the minheap is full
    int quorumIndex = scoringIterators.size();
    double minHeapThresholdScore = Double.NEGATIVE_INFINITY;

    // Routine is as follows:
    // 1) Find the next candidate from the sentinels
    // 2) Move iterators and length readers to candidate
    // 3) Score sentinels unconditionally
    // 4) while (runningScore > R)
    //      move iterator to candidate
    //      score candidate w/ iterator
    while (true) {
      long candidate = Long.MAX_VALUE;
      for (int i = 0; i < quorumIndex; i++) {
        if (!scoringIterators.get(i).isDone()) {
          long itrCandidate = scoringIterators.get(i).currentCandidate();
          candidate = (candidate < itrCandidate) ? candidate : itrCandidate;
        }
      }

      // if we are done, we can quit
      if (candidate == Long.MAX_VALUE) {
        break;
      }

      context.document = candidate;
      // Due to slightly different semantics between "currentCandidate" and 
      // "hasMatch", we need to see if the candidate given actually matches
      // properly before scoring.
      boolean match = false;
      for (ScoreIterator si : scoringIterators) {
        match |= si.hasMatch(candidate);
      }

      if (match) {
        // Setup to score
        double runningScore = maximumPossibleScore;

        // now score quorum sentinels w/out question
        int i = 0;
        for (i = 0; i < quorumIndex; i++) {
          DeltaScoringIterator dsi = scoringIterators.get(i);
          dsi.syncTo(candidate);
          runningScore -= dsi.deltaScore(context);
        }

        // Now score the rest, but keep checking
        while (runningScore > minHeapThresholdScore && i < scoringIterators.size()) {
          DeltaScoringIterator dsi = scoringIterators.get(i);
          dsi.syncTo(candidate);
          runningScore -= dsi.deltaScore(context);
          ++i;
        }

        // Fully scored it
        if (i == scoringIterators.size()) {

          while (collectionSegments[currentSegment] < candidate) {
            currentSegment += 1;
            quorumIndex = scoringIterators.size();
          }

          if (queue.size() < requested || runningScore > queue.getFinal().score) {
            ScoredDocument scoredDocument = new ScoredDocument(candidate, runningScore);
            queue.offer(scoredDocument);

            if (queue.size() > currentSegment && minHeapThresholdScore < queue.get(currentSegment).score) {
              minHeapThresholdScore = queue.get(currentSegment).score;

              while (quorumIndex > 0 && maxScoreOfRemainingIterators[(quorumIndex - 1)] < minHeapThresholdScore) {
                quorumIndex--;
              }
            }
          }
        }
      }

      // Now move all matching sentinels members past the current doc, and repeat
      for (int i = 0; i < quorumIndex; i++) {
        DeltaScoringIterator dsi = scoringIterators.get(i);
        dsi.movePast(candidate);
//        if (!shareNodes) {
//          long dsiCandidate = dsi.currentCandidate();
//          while (!dsi.isDone() && !dsi.hasMatch(dsiCandidate)) {
//            dsi.movePast(dsiCandidate);
//            dsiCandidate = dsi.currentCandidate();
//          }
//        }
      }
    }

    return toReversedArray2(queue);
  }

  public static <T extends ScoredDocument> T[] toReversedArray2(FixedSizeSortedArray<T> queue) {
    if (queue.size() == 0) {
      return null;
    }

    T[] items = queue.getSortedArray();
    int r = 1;
    for (T i : items) {
      i.rank = r;
      r++;
    }

    return items;
  }

  private boolean findDeltaNodes(Node n, List<Node> scorers, LocalRetrieval ret) throws Exception {
    // throw exception if we can't determine the class of each node.
    NodeType nt = ret.getNodeType(n);
    Class<? extends BaseIterator> iteratorClass = nt.getIteratorClass();



    if (DeltaScoringIterator.class
            .isAssignableFrom(iteratorClass)) {
      // we have a delta scoring class
      scorers.add(n);

      return true;

    } else if (DisjunctionIterator.class
            .isAssignableFrom(iteratorClass) && ScoreIterator.class
            .isAssignableFrom(iteratorClass)) {
      // we have a disjoint score combination node (e.g. #combine)
      boolean r = true;
      for (Node c
              : n.getInternalNodes()) {
        r &= findDeltaNodes(c, scorers, ret);
      }
      return r;
    } else {
      return false;
    }
  }

  private List<DeltaScoringIterator> createScoringIterators(List<Node> scoringNodes, LocalRetrieval ret, boolean shareNodes) throws Exception {
    List<DeltaScoringIterator> scoringIterators = new ArrayList();

    // the cache allows low level iterators to be shared
    Map<String, BaseIterator> queryIteratorCache;
    if (shareNodes) {
      queryIteratorCache = new HashMap();
    } else {
      queryIteratorCache = null;
    }
    for (int i = 0; i < scoringNodes.size(); i++) {
      DeltaScoringIterator scorer = (DeltaScoringIterator) ret.createNodeMergedIterator(scoringNodes.get(i), queryIteratorCache);
      scoringIterators.add(scorer);
    }
    return scoringIterators;
  }

  /**
   * Returns the document segments
   */
  private long[] determineCollectionSegments(double error, int requested, long documentCount) {
    long[] segments = new long[requested + 1];

    for (int i = 0; i < segments.length; i++) {
      segments[i] = 0;
    }

    double maxError = error / ((requested - 1));
    double confidence = 1.0 - maxError;
    double prob = (double) requested / (double) documentCount;

    // natural log.
    double low_i = (-1.0 * Math.log(confidence)) / prob;

    // need to ensure that the first segment is >= 1;
    if (Math.floor(low_i) <= 0.0) {
      segments[1] = 1;
    } else {
      segments[1] = (long) Math.floor(low_i);
    }

    for (int j = 1; j <= (requested - 2); j++) {
      low_i = low_i + 1.0;

      double old_hi = documentCount - 1.0;

      while ((old_hi - low_i) > 2.0) {
        double high_i = ((old_hi + low_i - 1.0) / 2.0) + 1.0;

        double lambda_i = high_i * prob;
        double term = Math.exp(-1.0 * lambda_i);
        double sum = term;

        for (int i = 1; i <= j; i++) {
          term = term * (lambda_i / (double) i);
          sum = sum + term;
        }

        if (sum > confidence) {
          low_i = high_i;
        } else {
          old_hi = high_i;
        }
      }
      segments[j + 1] = (long) Math.floor(low_i);
    }

    segments[requested] = documentCount;
    return segments;
  }
}
