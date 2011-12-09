/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.lang.reflect.Method;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.types.Adjacency;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Not the most efficient ever 
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
@OutputClass(className = "org.lemurproject.galago.core.types.Adjacency")
public class DistanceCalculator extends StandardStep<KeyValuePair, Adjacency> {

  double maxdistance;
  KeyIterator iterator;
  Counter counter;
  Method m;
  boolean isSymmetric;
  Adjacency a;

  public DistanceCalculator(TupleFlowParameters parameters) throws Exception {
    maxdistance = (double) parameters.getJSON().get("distance", 1.0F);
    String indexLocation = parameters.getJSON().getString("directory");
    String partName = parameters.getJSON().getString("part");
    isSymmetric = parameters.getJSON().get("symmetric", true);
    IndexPartReader partReader = new PositionIndexReader(DiskIndex.getPartPath(indexLocation, partName));
    counter = parameters.getCounter("pairs calculated");
    iterator = partReader.getIterator();
    String method = parameters.getJSON().get("method", "levenshtein");
    m = this.getClass().getMethod(method, String.class, String.class);
  }

  @Override
  public void process(KeyValuePair object) throws IOException {
    iterator.reset();

    String source = Utility.toString(object.key);
    // Now move to the key indicated by the input
    if (iterator.skipToKey(object.key)) {
      int start = Utility.toInt(object.value);
      int index = start;
      while (iterator.nextKey()) {
        index++;
        String target = Utility.toString(iterator.getKeyBytes());
        try {
          Double result = (Double) m.invoke(this, source, target);
          if (result <= maxdistance) {
            a = new Adjacency();
            a.source = object.key;
            a.destination = iterator.getKeyBytes();
            a.weight = result;
            processor.process(a);
            if (counter != null) {
              counter.increment();
            }
          }

          if (!isSymmetric) {
            result = (Double) m.invoke(this, target, source);
          }

          if (result <= maxdistance) {
            a = new Adjacency();
            a.source = iterator.getKeyBytes();
            a.destination = object.key;
            a.weight = result;
            processor.process(a);
            if (counter != null) {
              counter.increment();
            }
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  // DISTANCE FUNCTIONS
  /**
   * This method calculates the string edit distance (Levenshtein distance) between two strings. It originates from
   * http://en.wikibooks.org/wiki/Algorithm_implementation/Strings/Levenshtein_distance .
   */
  private static int minimum(int a, int b, int c) { return Math.min(Math.min(a, b), c); }

  public double levenshtein(String str1, String str2) {
    int[][] distance = new int[str1.length() + 1][str2.length() + 1];

    for (int i = 0; i <= str1.length(); i++) {
      distance[i][0] = i;
    }
    for (int j = 0; j <= str2.length(); j++) {
      distance[0][j] = j;
    }

    for (int i = 1; i <= str1.length(); i++) {
      for (int j = 1; j <= str2.length(); j++) {
        distance[i][j] = minimum(
                distance[i - 1][j] + 1,
                distance[i][j - 1] + 1,
                distance[i - 1][j - 1]
                + ((str1.charAt(i - 1) == str2.charAt(j - 1)) ? 0
                : 1));
      }
    }

    return distance[str1.length()][str2.length()];
  }
}
