/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.*;
import java.util.*;
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

  public static HashSet<String> readStreamIgnoringComments(InputStream stream) throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
    HashSet<String> set = new HashSet<>();
    String line;

    while ((line = reader.readLine()) != null) {
      if(line.startsWith("#")) continue;
      set.add(line.trim());
    }

    reader.close();
    return set;
  }

  @Nullable
  public static Set<String> getWordList(String name) throws IOException {
    if (wordLists == null) {
      wordLists = new HashMap<>();
    }
    if (!wordLists.containsKey(name)) {
      Set<String> list;
      File f = new File(name);
      if (f.exists()) {
        list = readStreamIgnoringComments(new BufferedInputStream(new FileInputStream(f)));
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
        list = readStreamIgnoringComments(resourceStream);
      }
      // ensure we keep the wordlist (also ensure unmodifiable).
      wordLists.put(name, Collections.unmodifiableSet(list));
    }

    // otherwise we've already read this list.
    return wordLists.get(name);
  }

  @Nonnull
  public static Set<String> getWordListOrDie(String name) {
    try {
      Set<String> list = getWordList(name);
      if(list == null) {
        throw new IllegalArgumentException("Couldn't find wordlist: "+name);
      }
      return list;
    } catch (IOException e) {
      throw new IllegalArgumentException("Couldn't find wordlist: "+name, e);
    }
  }
}
