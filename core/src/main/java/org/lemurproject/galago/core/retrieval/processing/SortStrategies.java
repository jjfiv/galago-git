/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import java.util.*;
import org.lemurproject.galago.core.retrieval.iterator.DeltaScoringIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author irmarc
 */
public class SortStrategies {
  // Never instantiate

  private SortStrategies() {
  }

  // Sorts all scorers by iterator length, ignores weights
  public static void fullLengthSort(List<Sentinel> s) {
    Collections.sort(s, new SentinelLengthComparator());
  }

  // Sorts all scorers by weight, ignoring length
  public static void fullScoreSort(List<Sentinel> s) {
    Collections.sort(s, new SentinelScoreComparator());
  }

  public static void currentCandidateSort(List<Sentinel> s) {
    Collections.sort(s, new Comparator<Sentinel>() {
      public int compare(Sentinel s1, Sentinel s2) {
        if (s2.iterator.isDone()) {
          return -1;
        }
        if (s1.iterator.isDone()) {
          return 1;
        }
        return Utility.compare(s1.iterator.currentCandidate(), s2.iterator.currentCandidate());
      }
    });
  }

  // Sorts all unigrams by length, then all ngrams by length.
  // However unigrams are always before ngrams
  public static void splitLengthSort(List<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelLengthComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelLengthComparator());
  }

  public static void splitScoreSort(List<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelScoreComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelScoreComparator());
  }

  public static void mixedLSSort(List<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelLengthComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelScoreComparator());
  }

  public static void mixedSLSort(List<Sentinel> s) {
    int numUnigrams = (s.size() + 2) / 3;
    Collections.sort(s.subList(0, numUnigrams),
            new SentinelScoreComparator());
    Collections.sort(s.subList(numUnigrams, s.size()),
            new SentinelLengthComparator());
  }

  public static ArrayList<Sentinel> populateIndependentSentinels(List<DeltaScoringIterator> scorers, double startingPotential, boolean useMaximums) {
    ArrayList<Sentinel> s = new ArrayList<Sentinel>(scorers.size());
    double max = startingPotential;
    for (DeltaScoringIterator dsi : scorers) {
      if (useMaximums) {
        double runningScore = startingPotential;
        runningScore += dsi.maximumDifference();
        s.add(new Sentinel(dsi, max - runningScore));
      } else {
        s.add(new Sentinel(dsi, dsi.minimumScore()));
      }
    }
    return s;
  }

  public static ArrayList<Sentinel> populateIndependentSentinels(List<DeltaScoringIterator> scorers, double startingPotential) {
    return populateIndependentSentinels(scorers, startingPotential, true);
  }
}
