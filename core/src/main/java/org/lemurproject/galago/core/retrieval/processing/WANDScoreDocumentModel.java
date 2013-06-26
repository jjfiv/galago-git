// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.processing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.core.retrieval.iterator.BaseIterator;
import org.lemurproject.galago.core.retrieval.iterator.DisjunctionIterator;
import org.lemurproject.galago.core.retrieval.iterator.ScoreIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeType;
import org.lemurproject.galago.core.util.FixedSizeMinHeap;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Assumes the use of delta functions for scoring, then prunes using WAND.
 * Generally this causes a substantial speedup in processing time.
 *
 * You may get small perturbations in rank order at some ranks. As far as I have
 * been able to ascertain, it's from rounding errors b/c the scorers are
 * sometimes not executed in the same order, causing differences in the 13th
 * decimal place or below (it causes a rank inversion, but not based on a
 * meaningful number).
 *
 * @author irmarc
 */
public class WANDScoreDocumentModel extends ProcessingModel {

  Comparator<Sentinel> comp = new SentinelPositionComparator();
  LocalRetrieval retrieval;

  public WANDScoreDocumentModel(LocalRetrieval lr) {
    retrieval = lr;
  }

  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();

    int requested = (int) queryParams.get("requested", 1000);
    // default threshol is 1.0 (equivalent to a disjunction (OR))
    double factor = retrieval.getGlobalParameters().get("thresholdFactor", 1.0);

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
      maximumPossibleScore += scorer.maximumWeightedScore();
    }

    // Make sure the scorers are sorted properly
//    List<Sentinel> sortedSentinels = buildSentinels(scoringIterators, maximumPossibleScore, queryParams);

    // --- OLD CODE --- //    
    double minCandidateScore = Double.NEGATIVE_INFINITY;

    Sentinel[] sortedSentinels = buildSentinels(scoringIterators, maximumPossibleScore, queryParams);

    // Going to need the minimum set
    double scoreMinimums = 0.0;
    for (Sentinel s : sortedSentinels) {
      scoreMinimums += s.iterator.getWeight() * s.score;
      // similar to iterator.maxDifference (but we need the negative value) 
      s.score = -1 * s.iterator.maximumDifference();
    }

    context.document = -1;
    int advancePosition;
    fullSort(sortedSentinels);

    int x = 0;
    while (true) {
      advancePosition = -1;
      
      int pivotPosition = findPivot(sortedSentinels, scoreMinimums, minCandidateScore);

      if (pivotPosition == -1) {
        break;
      }

      if (sortedSentinels[pivotPosition].iterator.isDone()) {
        break;
      }

      x += 1;
      
      long pivot = sortedSentinels[pivotPosition].iterator.currentCandidate();

      if (pivot <= context.document) {
        advancePosition = pickAdvancingSentinel(sortedSentinels, pivotPosition, pivot);
        sortedSentinels[advancePosition].iterator.movePast(context.document);

      } else {
        if (sortedSentinels[0].iterator.currentCandidate() == pivot) {
          // We're in business. Let's score.
          context.document = pivot;
          double score = score(sortedSentinels, context, maximumPossibleScore);

          if (requested < 0 || queue.size() <= requested || score > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(context.document, score);
            queue.offer(scoredDocument);
            minCandidateScore = factor * queue.peek().score;
          }
        } else {
          advancePosition = pickAdvancingSentinel(sortedSentinels, pivotPosition, pivot);
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

  private double score(Sentinel[] sortedSentinels, ScoringContext context, double startingPotential) throws IOException {

    // Setup to score
    double runningScore = startingPotential;

    // now score all scorers
    int i;
    for (i = 0; i < sortedSentinels.length; i++) {
      DeltaScoringIterator dsi = sortedSentinels[i].iterator;
      dsi.syncTo(context.document); // just in case
      runningScore += dsi.deltaScore(context);
    }
    return runningScore;
  }

  private Sentinel[] buildSentinels(List<DeltaScoringIterator> scorers, double startingPotential, Parameters qp) {

    String type = retrieval.getGlobalParameters().get("sort", "length");
    ArrayList<Sentinel> tmp = SortStrategies.populateIndependentSentinels(scorers, startingPotential, false);

    return tmp.toArray(new Sentinel[tmp.size()]);
  }

  private int findPivot(Sentinel[] sortedSentinels, double scoreMinimum, double threshold) {
    if (threshold == Double.NEGATIVE_INFINITY) {
      return 0;
    }

    double sum = scoreMinimum;

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
   * highest idf), under the assumption that the lowest df will have the largest
   * skips in doc ids in its list. Candidates are from sorted(0..limit),
   * inclusive.
   *
   * @return The iterator that should be advanced next
   */
  private int pickAdvancingSentinel(Sentinel[] sortedSentinels, int limit, long limitDoc) {
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
