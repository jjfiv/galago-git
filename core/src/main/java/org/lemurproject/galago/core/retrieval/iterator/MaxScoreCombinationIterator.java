// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.hash.TIntDoubleHashMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.set.hash.TIntHashSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.disk.TopDocsReader.TopDocument;
import org.lemurproject.galago.core.index.ValueIterator;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.processing.TopDocsContext;
import org.lemurproject.galago.core.scoring.ScoringFunction;
import org.lemurproject.galago.core.util.CallTable;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Galago's implementation of the max_score algorithm for document-at-a-time processing.
 * The implementation is as follows:
 *
 * Init) Scan children for topdocs. If they exist, iterate over those first to bootstrap bounds.
 *       Otherwise while the number of scored docs from this iterator is under requested, make no
 *       adjustments.
 *
 * For each candidate document:
 *
 * If scored is less than requested, fully score the document, add it to the best seen queue, and return
 * score.
 * Otherwise,
 * While we have not found a candidate with a score above max_score:
 *  For all iterators:
 *      1) Choose the iterator with the shortest estimated list of candidates
 *      2) Determine if the score + remaining upper bounds is less than max_score. If so, stop scoring and move to the next candidate.
 *         Else move to next iterator.
 *
 *      3) If all iterators are used, mark that document as the scored document, and update max_score if needed.
 *
 * In order to utilize the optimization properly, we need to lazy-move the iterators involved in scoring.
 * Therefore, moveTo and movePast will only set the "nextDocumentToScore" variable to indicate where the first
 * iterator should move to if we're required to score.
 *
 * Things are a bit trickier when information is requested of the iterator. The semantics are:
 *  atCandidate(doc): Does the iterator have 'doc' in its candidate list? In order to answer this accurately, we need to
 *                 attempt to score. Given that this is usually called after a move call, we score forward until we reach
 *                 nextDocumentToScore. If lastDocumentScored == doc, return true, meaning it made the cut.
 *  identifier(): What is the next candidate according to this iterator? Once again, we need to attempt to score.
 *                 however in this case we need to continue iterating until we find a winner. There is no external bound.
 *  score(doc, length): We need to score. If doc == 		   
 * @author irmarc
 */
public class MaxScoreCombinationIterator extends ScoreCombinationIterator {

  public static class TopDocsIterator implements Comparable<TopDocsIterator> {

    int index;
    ArrayList<TopDocument> docs;

    public TopDocsIterator(ArrayList<TopDocument> a) {
      docs = a;
      index = 0;
    }

    public boolean isDone() {
      return (index >= docs.size());
    }

    public boolean next() {
      if (index < docs.size()) {
        index++;
      }
      return (!isDone());
    }

    public boolean hasMatch(int document) {
      return (!isDone() && docs.get(index).document == document);
    }

    public TopDocument getCurrentTopDoc() {
      if (isDone()) {
        return null;
      }
      return docs.get(index);
    }

    public int compareTo(TopDocsIterator tdi) {
      // Check for doneness
      if (this.isDone() && tdi.isDone()) {
        return 0;
      }
      if (this.isDone()) {
        return 1;
      }
      if (tdi.isDone()) {
        return -1;
      }

      // Now compare topdocs
      TopDocument td1 = this.getCurrentTopDoc();
      TopDocument td2 = this.getCurrentTopDoc();
      if (td1 == null && td2 == null) {
        return 0;
      }
      if (td1 == null) {
        return 1;
      }
      if (td2 == null) {
        return -1;
      }
      return td1.document - td2.document;
    }
  }

  public static class Placeholder implements Comparable<Placeholder> {

    int document;
    double score;

    public Placeholder() {
    }

    public Placeholder(int d, double s) {
      document = d;
      score = s;
    }

    public int compareTo(Placeholder that) {
      return this.score > that.score ? -1 : this.score < that.score ? 1 : 0;
    }

    @Override
    public String toString() {
      return String.format("<%d,%f>", document, score);
    }
  }

  private static class DocumentOrderComparator implements Comparator<ScoreValueIterator> {

    public int compare(ScoreValueIterator a, ScoreValueIterator b) {
      return (a.currentCandidate() - b.currentCandidate());
    }
  }

  private class WeightComparator implements Comparator<ScoreValueIterator> {

    public int compare(ScoreValueIterator a, ScoreValueIterator b) {
      if (!a.isDone() && b.isDone()) {
        return -1;
      }
      if (a.isDone() && !b.isDone()) {
        return 1;
      }
      double w1 = weightLookup.get(a);
      double w2 = weightLookup.get(b);
      return (w1 > w2 ? -1 : (w1 < w2 ? 1 : 0));
    }
  }

  private static class TotalCandidateComparator implements Comparator<ScoreValueIterator> {

    public int compare(ScoreValueIterator a, ScoreValueIterator b) {
      if (!a.isDone() && b.isDone()) {
        return -1;
      }
      if (a.isDone() && !b.isDone()) {
        return 1;
      }
      return (a.totalEntries() < b.totalEntries() ? -1 : a.totalEntries() > b.totalEntries() ? 1 : 0);
    }
  }
  int requested;
  double threshold = 0;
  double potential;
  double minimum;
  ScoringContext backgroundContext;
  TObjectDoubleHashMap weightLookup;
  TIntArrayList topdocsCandidates;
  int lastReportedCandidate = 0;
  int candidatesIndex;
  int quorumIndex;
  ArrayList<ScoreValueIterator> scoreList;
  ArrayList<Placeholder> R;
  TIntHashSet ids;
  ScoringContext context = null;
  boolean dirty = false;

  public MaxScoreCombinationIterator(Parameters globalParams, NodeParameters parameters,
          ScoreValueIterator[] childIterators) throws IOException {
    super(globalParams, parameters, childIterators);
    requested = (int) parameters.get("requested", 100);
    R = new ArrayList<Placeholder>();
    ids = new TIntHashSet();
    backgroundContext = new ScoringContext();
    topdocsCandidates = new TIntArrayList();
    scoreList = new ArrayList<ScoreValueIterator>(iterators.length);
    weightLookup = new TObjectDoubleHashMap();
    for (int i = 0; i < iterators.length; i++) {
      ScoreValueIterator si = iterators[i];
      weightLookup.put(si, weights[i]);
    }
    scoreList.addAll(Arrays.asList(iterators));
    String sort = parameters.get("sort", "length");
    if (sort.equals("weight")) {
      Collections.sort(scoreList, new WeightComparator());
    } else {
      Collections.sort(scoreList, new TotalCandidateComparator());
    }
  }

  @Override
  public double maximumScore() {
    double s = 0;
    for (ScoreValueIterator it : iterators) {
      s = Math.max(it.maximumScore(), s);
    }
    if (s == Double.MAX_VALUE || s == Double.POSITIVE_INFINITY) {
      return s;
    }

    // Not infinity/max
    s = 0;
    for (int i = 0; i < iterators.length; i++) {
      s += weights[i] * iterators[i].maximumScore();
    }
    return s;
  }

  @Override
  public double minimumScore() {
    double s = 0;
    for (int i = 0; i < iterators.length; i++) {
      s += weights[i] * iterators[i].minimumScore();
    }
    return s;
  }

  /**
   * Resets all iterators, does not perform caching again
   * @throws IOException
   */
  @Override
  public void reset() throws IOException {
    for (ScoreValueIterator si : iterators) {
      si.reset();
    }
    candidatesIndex = 0;
    computeQuorum();
  }

  /**
   * Done means
   * 1) No more open iterators, or
   * 2) if scored > requested, none of the open iterators can contribute
   *      enough to get over the limit we've seen so far
   * @return
   */
  @Override
  public boolean isDone() {
    return (currentCandidate() == Integer.MAX_VALUE);
  }

  public boolean atCandidate(int identifier) {
    if ((!isDone()) && (currentCandidate() == identifier)) {
      return true;
    }
    return false;
  }

  public long totalEntries() {
    long max = 0;
    for (ValueIterator iterator : iterators) {
      max = Math.max(max, iterator.totalEntries());
    }
    return max;
  }

  @Override
  public int currentCandidate() {
    int candidate = Integer.MAX_VALUE;

    if (!dirty) {
      return lastReportedCandidate;
    }

    // first check the topdocs
    if (candidatesIndex < topdocsCandidates.size()) {
      candidate = topdocsCandidates.get(candidatesIndex);
    }

    // Now look among the quorum iterators
    for (int i = 0; i < quorumIndex; i++) {
      if (!scoreList.get(i).isDone()) {
        int c = scoreList.get(i).currentCandidate();
        candidate = Math.min(candidate, c);
      }
    }
    lastReportedCandidate = candidate;
    dirty = false;
    return candidate;
  }

  /**
   *  *** BE VERY CAREFUL IN CALLING THIS FUNCTION ***
   */
  public boolean next() throws IOException {
    movePast(currentCandidate());
    return (!isDone());
  }

  @Override
  public boolean moveTo(int document) throws IOException {
    dirty = true;
    // Move topdocs candidate list forward
    while (candidatesIndex < topdocsCandidates.size()
            && document > topdocsCandidates.get(candidatesIndex)) {
      candidatesIndex++;
    }

    // Now only skip the quorum forward
    for (int i = 0; i < quorumIndex; i++) {
      scoreList.get(i).moveTo(document);
    }

    return atCandidate(document);
  }

  /**
   * Just sets context for scoring
   */
  public double score() {
    int document = context.document;
    int length = context.length;

    // Dumps TONS O SHIT
    /*
    if (document == 460408) {
    System.err.printf("Starting to score %d\n", document);
    // R
    System.err.printf("Contents of R:\n");
    for (int i = 0; i < R.size(); i++) System.err.printf("%d: %f %d\n", i, R.get(i).score / weightSum, R.get(i).document);
    System.err.printf("Quorum index=%d\n", quorumIndex);
    System.err.printf("threshold=%f, potential=%f\n", threshold, potential);
    System.err.printf("candidatesIndex = %d, current topdoc: %d\n", candidatesIndex, topdocsCandidates.get(candidatesIndex));
    }
     */
    try {
      if ((candidatesIndex < topdocsCandidates.size()
              && document == topdocsCandidates.get(candidatesIndex))
              || (R.size() < requested)) {
        // Make sure the iterators are lined up
        for (ScoreValueIterator it : iterators) {
          it.moveTo(lastReportedCandidate);
        }
        double score = fullyScore();
        adjustThreshold(document, score);
        //if (document == 460408) System.err.printf("Fully scored. Normalized score = %f\n", score / weightSum);
        return score;
      } else {
        // First score the quorum, then as we score the rest, skip it forward.
        double adjustedPotential = potential;
        double inc;
        int i;
        for (i = 0; i < quorumIndex; i++) {
          ScoreValueIterator it = scoreList.get(i);
          inc = weightLookup.get(it) * (it.score() - it.maximumScore());
          //System.err.printf("it %s score= %f, weight = %f\n", it.toString(), it.score(), weightLookup.get(it));
          adjustedPotential += inc;
        }

        while (adjustedPotential > threshold && i < scoreList.size()) {
          ScoreValueIterator it = scoreList.get(i);
          it.moveTo(lastReportedCandidate);
          inc = weightLookup.get(it) * (it.score() - it.maximumScore());
          //System.err.printf("it %s score= %f, weight = %f\n", it.toString(), it.score(), weightLookup.get(it));
          adjustedPotential += inc;
          i++;
        }

        /*
        if (document == 460408) {
        System.err.printf("adjPot = %f, threshold = %f, numScored = %d, total = %d\n", adjustedPotential,
        threshold, i, scoreList.size());
        }
         */

        // fully scored!
        if (i == scoreList.size()) {
          adjustThreshold(document, adjustedPotential);
          //System.err.printf("Normalized score = %f\n", adjustedPotential / weightSum);
          return adjustedPotential;
        } else {
          // didn't make the cut
          CallTable.increment("iterator_pruned", (scoreList.size() - i));
          return minimum;
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private double fullyScore() {
    double total = 0;
    for (int i = 0; i < iterators.length; i++) {
      double score = iterators[i].score();
      //System.err.printf("it %s score= %f, weight = %f\n", iterators[i].toString(), score, weights[i]);
      total += weights[i] * score;
    }
    return total;
  }

  /**
   * Determines if the top candidates list has changed. 
   * Note that R is just a queue of doubles - if you add
   * a score of a single doc multiple times, it won't know any better.
   * @param newscore
   */
  protected void adjustThreshold(int document, double newscore) {
    if (ids.contains(document)) {
      for (Placeholder p : R) {
        if (p.document == document) {
          p.score = newscore;
          break;
        }
      }
    } else {
      if (R.size() < requested) {
        R.add(new Placeholder(document, newscore));
        ids.add(document);
      } else {
        if (newscore > R.get(requested - 1).score) {
          Placeholder old = R.remove(requested - 1);
          R.add(new Placeholder(document, newscore));
          ids.remove(old.document);
          ids.add(document);
        }
      }
    }
    if (R.size() == requested) {
      Collections.sort(R);
      threshold = R.get(requested - 1).score;
      computeQuorum();
    }
  }

  /**
   * Determines which iterators we need to worry about to produce a valid
   * candidate score. If scored < requested, it defaults to all of them.
   */
  protected void computeQuorum() {
    double adjustedPotential = 0;
    if (R.size() < requested) {
      quorumIndex = scoreList.size();
    } else {
      //if (threshold > -21.0 && threshold > -23.0)  System.err.printf("setting quorum for threshold %f, pot %f.\n", threshold, potential);
      // Now we try
      adjustedPotential = potential;
      int i;
      for (i = 0; i < scoreList.size() && adjustedPotential > threshold; i++) {
        ScoreValueIterator it = scoreList.get(i);
        double inc = weightLookup.get(it) * (it.minimumScore() - it.maximumScore());
        //if (threshold > -21.0 && threshold > -23.0) System.err.printf("idx %d: adding inc of %f\n", i, inc);
        adjustedPotential += inc;
      }
      quorumIndex = i;
      //if (threshold > -21.0 && threshold > -23.0) System.err.printf("Quorum idx is now %d\n", quorumIndex);
    }
  }

  /**
   * We pre-evaluate the the topdocs lists by scoring them against all iterators
   * that have topdocs, giving us a partial evaluation, and therefore a better
   * threshold bound - we also move the scored count up in order to activate the
   * threshold sooner.
   *
   * Note that this makes the assumption that all scoring function formulae are
   * identical, and they have been identically parameterized (i.e. using equal smoothing parameters).
   */
  protected void cacheScores(HashMap<ScoreValueIterator, ArrayList<TopDocument>> topdocs) throws IOException {
    // Iterate over the lists, scoring as we go - they are doc-ordered after all
    TObjectIntHashMap loweredCount = new TObjectIntHashMap();
    PriorityQueue<TopDocsIterator> toScore = new PriorityQueue<TopDocsIterator>();
    TIntDoubleHashMap scores = new TIntDoubleHashMap();
    HashMap<TopDocsIterator, ScoringFunctionIterator> lookup =
            new HashMap<TopDocsIterator, ScoringFunctionIterator>();
    for (Map.Entry<ScoreValueIterator, ArrayList<TopDocument>> td : topdocs.entrySet()) {
      TopDocsIterator it = new TopDocsIterator(td.getValue());
      if (it != null) {
        toScore.add(it);
        lookup.put(it, (ScoringFunctionIterator) td.getKey());
      }
    }

    if (toScore.size() == 0) {
      return;
    }

    double score;
    backgroundContext.document = 0;

    // Now we can simply iterate until done
    while (!toScore.peek().isDone()) {
      TopDocsIterator it = toScore.poll();
      TopDocument td = it.getCurrentTopDoc();

      // If it's not the scores table already, add it w/ background
      if (!scores.containsKey(td.document)) {
        // Need the background score first
        score = 0;
        for (ScoreValueIterator si : iterators) {
          backgroundContext.length = td.length;
          score += weightLookup.get(si) * si.score(backgroundContext);
        }
        //System.err.printf("Doc %d has bkgrnd = %f\n", td.document, score);
        scores.put(td.document, score);
      }

      // Score it using this iterator
      ScoringFunction fn = lookup.get(it).getScoringFunction();
      score = weightLookup.get(lookup.get(it))
              * (fn.score(td.count, td.length) - fn.score(0, td.length));
      scores.adjustValue(td.document, score);
      it.next();
      toScore.add(it); // put it back in, do it again
      //System.err.printf("Adding topdoc candidate %d\n", td.document);
      topdocsCandidates.add(td.document);
    }

    double[] values = scores.values();
    int[] keys = scores.keys();
    for (int i = 0; i < keys.length; i++) {
      R.add(new Placeholder(keys[i], scores.get(keys[i])));
    }
    Collections.sort(R);
    topdocsCandidates.sort();
    while (R.size() > requested) {
      R.remove(R.size() - 1);
    }
    threshold = R.get(R.size() - 1).score;
    R.trimToSize();
    //System.err.printf("R at init (threshold=%f)\n", threshold);
    for (int i = 0; i < R.size(); i++) {
      //System.err.printf("%d: %f %d\n", i, R.get(i).score / weightSum, R.get(i).document);
      ids.add(R.get(i).document);
    }
    candidatesIndex = 0;
  }

  public void setContext(ScoringContext c) {

    // look for topdocs - but only the first time we set the context
    if (c != null) {
      if (TopDocsContext.class.isAssignableFrom(c.getClass())) {
        try {
          cacheScores(((TopDocsContext) c).topdocs);
        } catch (IOException ioe) {
          // Do nothing?
          throw new RuntimeException(ioe);
        }
        ((TopDocsContext) c).topdocs.clear(); // all done!
        for (int i = 0; i < iterators.length; i++) {
          ScoreValueIterator si = iterators[i];
          potential += weights[i] * si.maximumScore();
          minimum += weights[i] * si.minimumScore();
        }
      }

      // This needs to be called regardless
      computeQuorum();
    }
    this.context = c;
  }

  public ScoringContext getContext() {
    return context;
  }
}
