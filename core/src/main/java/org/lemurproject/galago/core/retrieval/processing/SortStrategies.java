/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.*;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;

/**
 *
 * @author irmarc
 */
public class SortStrategies {
  // Never instantiate

  private SortStrategies() {
  }

  // Sorts all scorers by iterator length, ignores weights
  public static void fullLengthSort(ArrayList<Sentinel> s) {
    Collections.sort(s, new SentinelLengthComparator());
  }

  // Sorts all scorers by weight, ignoring length
  public static void fullScoreSort(ArrayList<Sentinel> s) {
    Collections.sort(s, new SentinelScoreComparator());
  }

  public static void currentCandidateSort(ArrayList<Sentinel> s) {
    Collections.sort(s, new Comparator<Sentinel>() {
      public int compare(Sentinel s1, Sentinel s2) {
        if (s2.iterator.isDone()) return -1;
        if (s1.iterator.isDone()) return 1;
        return (s1.iterator.currentCandidate() - s2.iterator.currentCandidate());
      }
    });
  }

  // Sorts all unigrams by length, then all ngrams by length.
  // However unigrams are always before ngrams
  public static void splitLengthSort(ArrayList<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelLengthComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelLengthComparator());
  }

  public static void splitScoreSort(ArrayList<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelScoreComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelScoreComparator());
  }

  public static void mixedLSSort(ArrayList<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelLengthComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelScoreComparator());
  }

  public static void mixedSLSort(ArrayList<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelScoreComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelLengthComparator());
  }

  public static ArrayList<Sentinel> populateIndependentSentinels(EarlyTerminationScoringContext ctx, boolean useMaximums) {
    ArrayList<Sentinel> s = new ArrayList<Sentinel>(ctx.scorers.size());
    double max = ctx.startingPotential;
    for (DeltaScoringIterator dsi : ctx.scorers) {
	if (useMaximums) {
	    ctx.runningScore = ctx.startingPotential;
	    System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
			     ctx.startingPotentials.length);
	    dsi.maximumDifference();
	    s.add(new Sentinel(dsi, max - ctx.runningScore));
	} else {
	    s.add(new Sentinel(dsi, dsi.minimumScore()));
	}
    }
    return s;
  }

  public static ArrayList<Sentinel> populateIndependentSentinels(EarlyTerminationScoringContext ctx) {
      return populateIndependentSentinels(ctx, true);
  }

  public static ArrayList<Sentinel> populateDependentSentinels(EarlyTerminationScoringContext ctx) {
    ArrayList<Sentinel> s = new ArrayList<Sentinel>(ctx.scorers.size());
    Comparator<Sentinel> comp = new SentinelScoreComparator();
    int numUnigrams = (ctx.scorers.size() + 2) / 3;
    // Build our dumb dependency map (more intelligent will take some work)
    HashMap<DeltaScoringIterator, ArrayList<DeltaScoringIterator>> deps =
            new HashMap<DeltaScoringIterator, ArrayList<DeltaScoringIterator>>();
    for (int i = 0; i < numUnigrams; i++) {
      DeltaScoringIterator dsi = ctx.scorers.get(i);
      deps.put(dsi, new ArrayList<DeltaScoringIterator>());
      if (i == 0) {
        // two other iterators involved
        deps.get(dsi).add(ctx.scorers.get(numUnigrams));
        deps.get(dsi).add(ctx.scorers.get((2 * numUnigrams) - 1));
      } else if (i == numUnigrams - 1) {
        // also two other iterators involved
        ctx.scorers.get(numUnigrams).maximumDifference();
        deps.get(dsi).add(ctx.scorers.get(2 * (numUnigrams - 1)));
        deps.get(dsi).add(ctx.scorers.get(3 * (numUnigrams - 1)));
      } else if (i > 0 && i < numUnigrams) {
        dsi.maximumDifference();
        // there are actually 4 other iterators to deal with
        deps.get(dsi).add(ctx.scorers.get(numUnigrams + i - 1));
        deps.get(dsi).add(ctx.scorers.get(numUnigrams + i));
        deps.get(dsi).add(ctx.scorers.get((2 * numUnigrams) + i - 2));
        deps.get(dsi).add(ctx.scorers.get((2 * numUnigrams) + i - 1));
      }
    }

    // Look at this as a shortest path problem, and let's try greedy. We build the
    // order of the sentinel set based on most lost first.
    ArrayList<Sentinel> horizon = new ArrayList<Sentinel>();
    HashSet<DeltaScoringIterator> candidates = new HashSet<DeltaScoringIterator>();
    HashSet<DeltaScoringIterator> available = new HashSet<DeltaScoringIterator>();
    candidates.addAll(ctx.scorers.subList(0, numUnigrams));
    available.addAll(ctx.scorers);
    while (!candidates.isEmpty()) {
      for (DeltaScoringIterator cand : candidates) {
        // setup to score
        ctx.runningScore = ctx.startingPotential;
        System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
                ctx.startingPotentials.length);
        // score with what's left
        cand.maximumDifference();
        for (DeltaScoringIterator dsi : deps.get(cand)) {
          if (available.contains(dsi)) {
            dsi.maximumDifference();
          }
        }
        horizon.add(new Sentinel(cand, ctx.startingPotential - ctx.runningScore));
      }
      // Sort and add the winner and clean up
      Collections.sort(horizon, comp);
      Sentinel winner = horizon.get(0);
      s.add(winner);
      candidates.remove(winner.iterator);
      available.remove(winner.iterator);
      // remove dependencies
      for (DeltaScoringIterator dsi : deps.get(winner.iterator)) {
        available.remove(dsi);
      }
      horizon.clear();
    }

    // Now add in the list of n-grams
    List<DeltaScoringIterator> ngrams = ctx.scorers.subList(numUnigrams, ctx.scorers.size());
    ArrayList<Sentinel> remaining = new ArrayList<Sentinel>();
    for (DeltaScoringIterator dsi : ngrams) {
      ctx.runningScore = ctx.startingPotential;
      System.arraycopy(ctx.startingPotentials, 0, ctx.potentials, 0,
              ctx.startingPotentials.length);
      dsi.maximumDifference();
      remaining.add(new Sentinel(dsi, ctx.startingPotential - ctx.runningScore));
    }
    s.addAll(remaining);
    return s;
  }

}
