/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
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
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Implements Weak-And processing model (Broder et al. 2003)
 * 
 * This processing model CAN NOT share nodes.
 *   -- See WeakAndDocumentModel2 for a shared-node version
 * 
 * @author irmarc, sjh
 */
public class WeakAndDocumentModel extends ProcessingModel {
  
  LocalRetrieval retrieval;
  boolean annotate;
  
  public WeakAndDocumentModel(LocalRetrieval lr) {
    this.retrieval = lr;
  }
  
  @Override
  public ScoredDocument[] execute(Node queryTree, Parameters queryParams) throws Exception {
    ScoringContext context = new ScoringContext();
    int requested = (int) queryParams.get("requested", 1000);
    annotate = queryParams.get("annotate", false);

    // 1.0 is rank-k-safe, higher values are not.
    double factor = queryParams.get("weakandfactor", 1.0);

    // step one: find the set of deltaScoringNodes in the tree
    List<Node> scoringNodes = new ArrayList<>();
    boolean canScore = findDeltaNodes(queryTree, scoringNodes, retrieval);
    if (!canScore) {
      throw new IllegalArgumentException("Query tree does not support delta scoring interface.\n" + queryTree.toPrettyString());
    }

    // step two: create an iterator for each node
    DeltaScoringIteratorWrapper[] sortedIterators = createScoringIterators(scoringNodes, retrieval);
    Arrays.sort(sortedIterators);
    FixedSizeMinHeap<ScoredDocument> queue = new FixedSizeMinHeap<>(ScoredDocument.class, requested, new ScoredDocument.ScoredDocumentComparator());

    // NOTE that the min scores here are OVER-ESTIMATES of the actual minimum scores
    double minimumPossibleScore = 0.0;
    double maximumPossibleScore = 0.0;
    for (DeltaScoringIteratorWrapper scorer : sortedIterators) {
      minimumPossibleScore += scorer.itr.minimumWeightedScore();
      maximumPossibleScore += scorer.itr.maximumWeightedScore();
    }

    context.document = -1;
    double minDocScore = Double.NEGATIVE_INFINITY;
    int advancePosition;
    while (true) {
      // if advance position is set, then an iterator has moved.
      advancePosition = -1;
      
      int pivotPosition = findPivot(sortedIterators, minimumPossibleScore, minDocScore);
      if (pivotPosition == -1) {
        break;
      }
      
      if (sortedIterators[pivotPosition].itr.isDone()) {
        break;
      }
      
      long pivot = sortedIterators[pivotPosition].currentCandidate;

      // if the pivot is less than or equal to the last scored document, move on.
      if (pivot <= context.document) {
        advancePosition = pickAdvancingSentinel(sortedIterators, context.document);
        sortedIterators[advancePosition].next(context.document + 1);
        
        
      } else {
        //if (sortedIterators[0].currentCandidate == pivot && hasMatch(sortedIterators, pivot)) {
        if (sortedIterators[0].currentCandidate == pivot) {
          // score the document.
          context.document = pivot;
          double score = score(sortedIterators, context, maximumPossibleScore);
          
          if (queue.size() < requested || score > queue.peek().score) {
            ScoredDocument scoredDocument = new ScoredDocument(context.document, score);
            queue.offer(scoredDocument);
            
            if (queue.size() == requested) {
              minDocScore = factor * queue.peek().score;
            }
          }
        } else {
          advancePosition = pickAdvancingSentinel(sortedIterators, pivot);
          sortedIterators[advancePosition].next(pivot);
        }
      }

      // We only moved one iterator, so we only need to worry about putting that one in the right place
      if (advancePosition != -1) {
        shuffleDown(sortedIterators, advancePosition);
      }
    }
    
    return toReversedArray(queue);
  }
  
//  private boolean hasMatch(DeltaScoringIteratorWrapper[] s, long doc) {
//    for (int i = 0; i < s.length; i++) {
//      if (s[i].currentCandidate <= doc) {
//        if (s[i].itr.hasMatch(doc)) {
//          return true;
//        }
//      } else {
//        return false;
//      }
//    }
//    return false;
//  }

  // Premise here is that the 'start' iterator is the one that moved forward, but it was already behind
  // any other iterator at position n where 0 <= n < start. So we don't even look at those. Makes the sort
  // linear at worst.
  private void shuffleDown(DeltaScoringIteratorWrapper[] s, int start) {
    for (int i = start; i < s.length - 1; i++) {
      int result = s[i].compareTo(s[i + 1]);
      if (result <= 0) {
        break;
      } else {
        DeltaScoringIteratorWrapper tmp = s[i];
        s[i] = s[i + 1];
        s[i + 1] = tmp;
      }
    }
  }
  
  private double score(DeltaScoringIteratorWrapper[] sortedIterators, ScoringContext context, double maximumPossibleScore) throws IOException {

    // Setup to score
    double runningScore = maximumPossibleScore;
    
//    if (annotate) {
//      System.err.println("Scoring " + context.document);
//    }
//
    // now score all scorers
    int i;
    for (i = 0; i < sortedIterators.length; i++) {
      DeltaScoringIterator dsi = sortedIterators[i].itr;
      dsi.syncTo(context.document);
      runningScore -= dsi.deltaScore(context);
      
//      if (annotate) {
//        System.err.println(dsi.getAnnotatedNode(context));
//      }
    }
    
//    if (annotate) {
//      System.err.println("Final Score " + runningScore);
//    }
    
    return runningScore;
  }
  
  private int findPivot(DeltaScoringIteratorWrapper[] sortedIterators, double scoreMinimum, double threshold) {
    if (threshold == Double.NEGATIVE_INFINITY) {
      // score the first document
      return 0;
    }
    
    double sum = scoreMinimum;
    
    for (int i = 0; i < sortedIterators.length; i++) {
      DeltaScoringIterator dsi = sortedIterators[i].itr;
      if (!dsi.isDone()) {
        sum += sortedIterators[i].itr.maximumDifference();
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
  private int pickAdvancingSentinel(DeltaScoringIteratorWrapper[] sortedIterators, long limitDoc) {
    long minEntries = Long.MAX_VALUE;
    int minPos = 0;
    for (int i = 0; i < sortedIterators.length; i++) {
      DeltaScoringIteratorWrapper dsi = sortedIterators[i];
      if (dsi.currentCandidate < limitDoc) {
        if (dsi.entries < minEntries) {
          minEntries = dsi.entries;
          minPos = i;
        }
      } else {
        return minPos;
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
  
  private DeltaScoringIteratorWrapper[] createScoringIterators(List<Node> scoringNodes, LocalRetrieval ret) throws Exception {
    DeltaScoringIteratorWrapper[] scoringIterators = new DeltaScoringIteratorWrapper[scoringNodes.size()];

    // NO Node sharing is permitted.
    for (int i = 0; i < scoringNodes.size(); i++) {
      DeltaScoringIterator scorer = (DeltaScoringIterator) ret.createNodeMergedIterator(scoringNodes.get(i), null);
      scoringIterators[i] = new DeltaScoringIteratorWrapper(scorer, scoringNodes.get(i));
    }
    
    return scoringIterators;
  }
  
  public class DeltaScoringIteratorWrapper implements Comparable<DeltaScoringIteratorWrapper> {
    
    public DeltaScoringIterator itr;
    public long currentCandidate;
    private long entries;
    
    private DeltaScoringIteratorWrapper(DeltaScoringIterator itr, Node node) throws IOException {
      this.itr = itr;

      if (node.getNodeParameters().containsKey("nodeDocumentCount")) {
        this.entries = node.getNodeParameters().getLong("nodeDocumentCount");
      } else if (node.getNodeParameters().containsKey("nodeFrequency")) {
        this.entries = node.getNodeParameters().getLong("nodeFrequency");
      } else {
        // otherwise all nodes are considered equal
        this.entries = 1;
      }

      // find the first document that has a match
      this.currentCandidate = -1;
      next();
    }
    
    @Override
    public int compareTo(DeltaScoringIteratorWrapper t) {
      return CmpUtil.compare(currentCandidate, t.currentCandidate);
    }
    
    public void updateCC() {
      currentCandidate = itr.currentCandidate();
    }
    
    public void next() throws IOException {
      do {
        itr.movePast(currentCandidate);
        currentCandidate = itr.currentCandidate();
      } while (!itr.isDone() && !itr.hasMatch(currentCandidate));
    }
    
    public void next(long doc) throws IOException {
      // want to move past currentCandidate, to at least doc
      currentCandidate = (doc <= currentCandidate) ? currentCandidate : (doc - 1);
      next();
    }
  }
}
