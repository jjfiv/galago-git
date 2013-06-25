// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    List<Node> scoringNodes = new ArrayList();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    // step two: create an iterator for each node
    List<DeltaScoringIterator> scoringIterators = createScoringIterators(scoringNodes, retrieval);

    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    // score of the min document in minheap
    double maximumPossibleScore = 0.0;
    for (DeltaScoringIterator scorer : scoringIterators) {
      maximumPossibleScore += scorer.startingPotential();
    }

    // Make sure the scorers are sorted properly
    List<Sentinel> sortedSentinels = buildSentinels(scoringIterators, maximumPossibleScore, queryParams);

    // all scorers are scored until the minheap is full
    double sentinelIndex = scoringIterators.size();
    double minCandidateScore = Double.NEGATIVE_INFINITY;

    // Routine is as follows:
    // 1) Find the next candidate from the sentinels
    // 2) Move sentinels and field length readers to candidate
    // 3) Score sentinels unconditionally
    // 4) while (runningScore > R)
    //      move iterator to candidate
    //      score candidate w/ iterator
    while (true) {
      long candidate = Long.MAX_VALUE;
      for (int i = 0; i < sentinelIndex; i++) {
        if (!sortedSentinels.get(i).iterator.isDone()) {
          candidate = Math.min(candidate, sortedSentinels.get(i).iterator.currentCandidate());
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
        for (i = 0; i < sentinelIndex; i++) {
          DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
          dsi.syncTo(candidate);
          runningScore += dsi.deltaScore(context);
        }

        // Now score the rest, but keep checking
        while (runningScore > minCandidateScore && i < sortedSentinels.size()) {
          DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
          dsi.syncTo(candidate);
          runningScore += dsi.deltaScore(context);
          ++i;
        }

        // Fully scored it
        if (i == sortedSentinels.size()) {
          if (requested < 0 || queue.size() <= requested || runningScore > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(candidate, runningScore);
            queue.offer(scoredDocument);

            if (requested > 0 && queue.size() >= requested) {
              if (minCandidateScore < queue.peek().score) {
                minCandidateScore = queue.peek().score;
                sentinelIndex = determineSentinelIndex(sortedSentinels, maximumPossibleScore, minCandidateScore);
              }
            }
          }
        }
      }

      // Now move all matching sentinels members past the current doc, and repeat
      for (int i = 0; i < sentinelIndex; i++) {
        DeltaScoringIterator dsi = sortedSentinels.get(i).iterator;
        dsi.movePast(candidate);
      }
    }

    return toReversedArray(queue);
  }

  private int determineSentinelIndex(List<Sentinel> sortedSentinels, double startingPotential, double minCandidateScore) {
    // Now we try to find our sentinel set
    double runningScore = startingPotential;

    int idx;
    for (idx = 0; idx < sortedSentinels.size() && runningScore > minCandidateScore; idx++) {
      runningScore -= sortedSentinels.get(idx).score;
    }

    return idx;
  }

  private List<Sentinel> buildSentinels(List<DeltaScoringIterator> scorers, double startingPotential, Parameters qp) {
    String type = retrieval.getGlobalParameters().get("sort", "length");
    List<Sentinel> sortedSentinels = SortStrategies.populateIndependentSentinels(scorers, startingPotential);

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
    return sortedSentinels;
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

  private List<DeltaScoringIterator> createScoringIterators(List<Node> scoringNodes, LocalRetrieval ret) throws Exception {
    List<DeltaScoringIterator> scoringIterators = new ArrayList();

    // the cache allows low level iterators to be shared
    Map<String, BaseIterator> queryIteratorCache = new HashMap();
    for (int i = 0; i < scoringNodes.size(); i++) {
      DeltaScoringIterator scorer = (DeltaScoringIterator) ret.createNodeMergedIterator(scoringNodes.get(i), queryIteratorCache);
      scoringIterators.add(scorer);
    }

    return scoringIterators;
  }
}
