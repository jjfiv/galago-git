/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.util;

import org.lemurproject.galago.tupleflow.Utility;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * This is a utility class to hold word lists in memory.
 *  - This class is meant to ensure that traversals don't 
 *    need to read word lists from files each time a query is run.
 *
 * @author sjh
 */
public class WordLists {

  private static Map<String, Set<String>> wordLists;

  public static Set<String> getWordList(String name) throws IOException {
    if (wordLists == null) {
      wordLists = new HashMap<>();
    }
    if (!wordLists.containsKey(name)) {
      Set<String> list;
      File f = new File(name);
      if (f.exists()) {
        list = Utility.readStreamToStringSet(new BufferedInputStream(new FileInputStream(f)));
      } else {
        // try to find word list in "/stopwords/"
        InputStream resourceStream = WordLists.class.getResourceAsStream("/stopwords/" + name);
        if (resourceStream == null) {
          // try to find word list in specified folder...
          resourceStream = WordLists.class.getResourceAsStream(name);

          if (resourceStream == null) {
            // give up
            Logger.getLogger("WordList").warning(String.format("Unable to create resource file."));
            return null;
          }
        }
        // found a stream -- read it.
        list = Utility.readStreamToStringSet(resourceStream);
      }
      // ensure we keep the wordlist (also ensure unmodifiable).
      wordLists.put(name, Collections.unmodifiableSet(list));
    }

    // otherwise we've already read this list.
    return wordLists.get(name);
  }
}
