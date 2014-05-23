// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.tupleflow.Parameters;

import java.util.*;

/**
 * Assumes the use of delta functions for scoring, then prunes using Maxscore.
 * Generally this causes a substantial speedup in processing time.
 *
 * @author irmarc, sjh
 */
public class MaxScoreDocumentModel extends ProcessingModel {

  LocalRetrieval retrieval;

  public MaxScoreDocumentModel(LocalRetrieval lr) {
    this.retrieval = lr;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();
    int requested = (int) queryParams.get("requested", 1000);

    // step one: find the set of deltaScoringNodes in the tree
    List<Node> scoringNodes = new ArrayList<Node>();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    // step two: create an iterator for each node
    boolean shareNodes = queryParams.get("shareNodes", retrieval.getGlobalParameters().get("shareNodes", true));
    List<DeltaScoringIterator> scoringIterators = createScoringIterators(scoringNodes, retrieval, shareNodes);

    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap<ScoredDocument>(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

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
          candidate = Math.min(candidate, scoringIterators.get(i).currentCandidate());
        }
      }

      // Means sentinels are done, we can quit
      if (candidate == Long.MAX_VALUE) {
        break;
      }

      context.document = candidate;
      // Due to different semantics between "currentCandidate" and 
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
        int i;
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
          if (queue.size() < requested || runningScore > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(candidate, runningScore);
            queue.offer(scoredDocument);

            if (queue.size() >= requested && minHeapThresholdScore < queue.peek().score) {
              minHeapThresholdScore = queue.peek().score;
              // check if this update will allow us to discard an iterator from consideration : 
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

    return toReversedArray(queue);
  }

  private boolean findDeltaNodes(Node n, List<Node> scorers, LocalRetrieval ret) throws Exception {
    // throw exception if we can't determine the class of each node.
    NodeType nt = ret.getNodeType(n);
    Class<? extends BaseIterator> iteratorClass = nt.getIteratorClass();

    if (DeltaScoringIterator.class.isAssignableFrom(iteratorClass)) {
      // we have a delta scoring class
      scorers.add(n);
      return true;

    } else if (DisjunctionIterator.class.isAssignableFrom(iteratorClass) && ScoreIterator.class.isAssignableFrom(iteratorClass)) {
      // we have a disjoint score combination node (e.g. #combine)
      boolean r = true;
      for (Node c : n.getInternalNodes()) {
        r &= findDeltaNodes(c, scorers, ret);
      }
      return r;

    } else {
      return false;
    }
  }

  private List<DeltaScoringIterator> createScoringIterators(List<Node> scoringNodes, LocalRetrieval ret, boolean shareNodes) throws Exception {
    List<DeltaScoringIterator> scoringIterators = new ArrayList<DeltaScoringIterator>();

    // the cache allows low level iterators to be shared
    Map<String, BaseIterator> queryIteratorCache;
    if (shareNodes) {
      queryIteratorCache = new HashMap<String,BaseIterator>();
    } else {
      queryIteratorCache = null;
    }
    for (Node scoringNode : scoringNodes) {
      DeltaScoringIterator scorer = (DeltaScoringIterator) ret.createNodeMergedIterator(scoringNode, queryIteratorCache);
      scoringIterators.add(scorer);
    }
    return scoringIterators;
  }
}
