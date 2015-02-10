// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.tagtok.IntSpan;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.stem.KrovetzStemmer;

/**
 * This is a very simple snippet generator for generating small summaries of
 * returned documents.
 *
 * @author trevor
 */
public class SnippetGenerator {

  public static final int width = 5;

  public static class Match {

    public Match(String term, int index) {
      this(term, index, index + 1);
    }

    public Match(String term, int start, int end) {
      this.term = term;
      this.start = start;
      this.end = end;
    }
    String term;
    int start;
    int end;
  }

  public static class SnippetRegion {

    int start;
    int end;
    ArrayList<Match> matches;

    public SnippetRegion(String term, int index, int width, int maximum) {
      matches = new ArrayList<Match>();
      matches.add(new Match(term, index));
      start = Math.max(index - width, 0);
      end = Math.min(maximum, index + width);
    }

    public SnippetRegion(ArrayList<Match> m, int s, int e) {
      matches = m;
      start = s;
      end = e;
    }

    public boolean overlap(SnippetRegion o) {
      return (start <= o.start && end >= o.start)
              || (start <= o.end && end >= o.end);
    }

    public boolean within(SnippetRegion o, int distance) {
      if (overlap(o)) {
        return true;
      }
      if (Math.abs(start - o.end) <= distance) {
        return true;
      }
      if (Math.abs(end - o.start) <= distance) {
        return true;
      }
      return false;
    }

    public SnippetRegion merge(SnippetRegion o) {
      ArrayList<Match> m = new ArrayList<Match>();
      m.addAll(matches);
      m.addAll(o.matches);

      return new SnippetRegion(m, Math.min(start, o.start), Math.max(end, o.end));
    }

    public boolean equals(SnippetRegion o) {
      return start == o.start && end == o.end;
    }

    public int size() {
      return this.end - this.start;
    }

    public ArrayList<Match> getMatches() {
      return matches;
    }
  }

  public class Snippet {

    private ArrayList<SnippetRegion> regions;
    double score;

    public Snippet(ArrayList<SnippetRegion> regions) {
      this.regions = regions;
    }

    @Override
    public int hashCode() {
      int result = 0;

      for (SnippetRegion region : regions) {
        result += region.end * 3 + region.start;
        result *= 5;
        result += region.getMatches().size();
      }

      return result;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Snippet)) {
        return false;
      }
      Snippet other = (Snippet) o;

      if (other.regions.size() != regions.size()) {
        return false;
      }
      for (int i = 0; i < regions.size(); i++) {
        if (regions.get(i).equals(other.regions.get(i))) {
          continue;
        }
        return false;
      }

      return true;
    }

    public double score() {
      if (score == 0) {
        cacheScore();
      }
      return score;
    }

    public void cacheScore() {
      // Factors:  big snippets are discounted
      //           coverage is good
      //           proximity is good
      //           close to document start is good

      int wordLength = 0;
      int prox = 0;

      HashSet<String> words = new HashSet<String>();

      for (SnippetRegion region : regions) {
        wordLength += region.size();
        prox += Math.pow(2, region.getMatches().size());

        for (SnippetGenerator.Match m : region.getMatches()) {
          words.add(m.term);
        }
      }

      score = -Math.pow(1.2, Math.min(0, wordLength - 150)) + prox + Math.pow(words.size(), 2);
    }

    /**
     * <p>This is part of an aborted attempt to score many candidate snippets to
     * produce the best one. It returns many different candidates which can then
     * be scored using the score method. As coded, this method is too slow to be
     * useful.</p>
     */
    public ArrayList<Snippet> expand() {
      ArrayList<Snippet> results = new ArrayList<Snippet>();
      int size = 0;

      for (SnippetRegion region : regions) {
        size += region.size();
      }

      if (size > 150) {
        // try deletions
        for (int i = 0; i < regions.size(); i++) {
          ArrayList<SnippetRegion> newRegions = new ArrayList<SnippetRegion>();

          newRegions.addAll(regions.subList(0, i));
          newRegions.addAll(regions.subList(i + 1, regions.size()));

          results.add(new Snippet(newRegions));
        }
      }

      // try merges
      for (int i = 0; i < regions.size() - 1; i++) {
        if (regions.get(i + 1).start - regions.get(i).end > 100) {
          continue;
        }
        ArrayList<SnippetRegion> newRegions = new ArrayList<SnippetRegion>();

        newRegions.addAll(regions.subList(0, i));
        SnippetRegion merged = regions.get(i).merge(regions.get(i + 1));
        newRegions.add(merged);
        newRegions.addAll(regions.subList(i + 2, regions.size()));

        results.add(new Snippet(newRegions));
      }

      return results;
    }
  }
  private KrovetzStemmer stemmer = new KrovetzStemmer();
  private boolean stemming = true;

  public void setStemming(boolean stemming) {
    this.stemming = stemming;
  }

  private Document parseAsDocument(String text, ArrayList<IntSpan> positions) throws IOException {
    Document document = new Document();
    document.text = text;

    // Tokenize the document
    TagTokenizer tokenizer = new TagTokenizer();
    tokenizer.process(document);

    if (positions != null) {
      positions.addAll(tokenizer.getTokenPositions());
    }
    if (stemming) {
      document = stemmer.stem(document);
    }

    return document;
  }

  /**
   * <p>Highlights query terms in a string of document text. This is most useful
   * for highlighting query terms in document titles.</p>
   */
  public String highlight(String documentText, Set<String> queryTerms) throws IOException {
    ArrayList<IntSpan> positions = new ArrayList<IntSpan>();
    Document document = parseAsDocument(documentText, positions);

    SnippetRegion merged = findSingleRegion(document, queryTerms);
    Snippet best = new Snippet(new ArrayList<SnippetRegion>(Collections.singletonList(merged)));

    // sjh nasty hack to return something, rather than erroring.
    if (positions.isEmpty()) {
      return documentText;
    }

    return buildHtmlString(best, document, positions);
  }

  /**
   * <p>Produces a short query-dependent summary of a document with query terms
   * highlighted. The result is an HTML string.</p>
   */
  public String getSnippet(String documentText, Set<String> queryTerms) throws IOException {
    ArrayList<IntSpan> positions = new ArrayList<IntSpan>();
    Document document = parseAsDocument(documentText, positions);
    return generateSnippet(document, positions, queryTerms);
  }

  private String generateSnippet(
          final Document document,
          final ArrayList<IntSpan> positions,
          final Set<String> queryTerms) {
    ArrayList<SnippetRegion> regions = findMatches(document, queryTerms);
    ArrayList<SnippetRegion> finalRegions = combineRegions(regions);
    Snippet best = new Snippet(finalRegions);

    return buildHtmlString(best, document, positions);
  }

  private SnippetRegion findSingleRegion(final Document document, final Set<String> queryTerms) {
    // Make a snippet region object for each term occurrence in the document,
    // while also counting matches
    ArrayList<Match> matches = new ArrayList<Match>();

    for (int i = 0; i < document.terms.size(); i++) {
      String term = document.terms.get(i);
      if (queryTerms.contains(term)) {
        matches.add(new Match(term, i));
      }
    }

    return new SnippetRegion(matches, 0, document.terms.size());
  }

  private ArrayList<SnippetRegion> findMatches(final Document document, final Set<String> queryTerms) {
    // Make a snippet region object for each term occurrence in the document,
    // while also counting matches
    ArrayList<SnippetRegion> regions = new ArrayList<SnippetRegion>();

    for (int i = 0; i < document.terms.size(); i++) {
      String term = document.terms.get(i);
      if (queryTerms.contains(term)) {
        regions.add(new SnippetRegion(term, i, width, document.terms.size()));
      }
    }
    return regions;
  }

  public String buildHtmlString(Snippet best, Document document, ArrayList<IntSpan> positions) {
    StringBuilder builder = new StringBuilder();

    for (SnippetRegion region : best.regions) {
      if (region.start != 0) {
        builder.append("...");
      }
      int startChar = positions.get(region.start).start;
      int endChar = positions.get(region.end - 1).end;
      int start = 0;

      // section string
      String section = document.text.substring(startChar, endChar);

      for (Match m : region.matches) {
        int startMatchChar = positions.get(m.start).start - startChar;
        int endMatchChar = positions.get(m.end - 1).end - startChar;

        String intermediate = stripTags(section.substring(start, startMatchChar));
        builder.append(intermediate);
        builder.append("<strong>");
        builder.append(stripTags(section.substring(startMatchChar, endMatchChar)));
        builder.append("</strong>");
        start = endMatchChar;
      }

      if (start >= 0) {
        builder.append(stripTags(section.substring(start)));
      }

      // terminate matches once we reached a max length.
      int maxSnippetSize = 500;
      if (builder.length() > maxSnippetSize) {
        break;
      }
    }

    if (best.regions.size() > 1 && best.regions.get(best.regions.size() - 1).end != document.terms.
            size()) {
      builder.append("...");
    }
    return builder.toString();
  }

  public String stripTag(String tag, String input) {
    input = input.replaceAll("<" + tag.toLowerCase() + "[^>]*>.*?</" + tag.toLowerCase() + ">",
            "");
    input = input.replaceAll("<" + tag.toUpperCase() + "[^>]*>.*?</" + tag.toUpperCase() + ">",
            "");
    return input;
  }

  public String stripTags(String input) {
    input = stripTag("script", input);
    input = stripTag("style", input);
    input = input.replaceAll("<!--.*?-->", "");

    input = input.replaceAll("&nbsp;", " ");
    input = input.replaceAll("<[^>]*>", " ");
    input = input.replaceAll("\\s+", " ");

    return input;
  }
  // Goals:  1. find as many terms as possible
  //         2. find terms that are close together
  //         3. break on sentences when possible (?)
  // BUGBUG: might not have all the terms highlighted here

  public ArrayList<SnippetRegion> combineRegions(final ArrayList<SnippetRegion> regions) {
    ArrayList<SnippetRegion> finalRegions = new ArrayList<SnippetRegion>();
    SnippetRegion last = null;
    int snippetSize = 0;
    int maxSize = 40;

    for (SnippetRegion current : regions) {
      if (last == null) {
        last = current;
      } else if (last.overlap(current)) {
        SnippetRegion bigger = last.merge(current);

        if (bigger.size() + snippetSize > maxSize) {
          finalRegions.add(last);
          last = null;
        } else {
          last = bigger;
        }
      } else if (last.size() + snippetSize > maxSize) {
        break;
      } else {
        finalRegions.add(last);
        snippetSize += last.size();
        last = current;
      }
    }

    if (last != null && snippetSize + last.size() < maxSize) {
      finalRegions.add(last);
    }

    return finalRegions;
  }
}
