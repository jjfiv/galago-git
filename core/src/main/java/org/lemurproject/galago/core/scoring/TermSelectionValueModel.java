// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.scoring;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.index.AggregateReader.CollectionStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeAggregateIterator;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.util.TextPartAssigner;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *  * Implements the query expansion of BM25 as described in
 * "Technique for Efficient Query Expansion" by Billerbeck and Zobel.
 * The weighting model itself is called "Term Selection Value".
 *
 *
 * @author irmarc
 */
public class TermSelectionValueModel implements ExpansionModel {
  // This version of the gram contains all contextual information
  // for the modified scoring algorithm.

  public static class Gram implements WeightedTerm {

    public String term;
    public double score;
    public int R;
    public int rt;

    public Gram(String t) {
      term = t;
      score = 0.0;
    }

    public String getTerm() {
      return term;
    }

    public double getWeight() {
      return score;
    }

    public int compareTo(WeightedTerm other) {
      Gram that = (Gram) other;
      return (this.score > that.score ? -1 : (this.score < that.score ? 1 : 0));
    }
  }
  Retrieval retrieval;
  CollectionStatistics stats;
  Parameters parameters;
  DocumentReader cReader = null;
  TagTokenizer tokenizer = null;
  IndexPartReader reader = null;
  long N = 0;
  int fbDocs;

  public TermSelectionValueModel(Parameters parameters, Retrieval r, CollectionStatistics stats) {
    this.retrieval = r;
    this.parameters = parameters;
    this.stats = stats;
  }

  public void initialize() throws IOException {
    this.N = stats.documentCount;
    this.fbDocs = (int) parameters.get("fbDocs", 10);

    if (cReader == null) {
      // Let's make a corpus reader
	String corpusLocation = parameters.get("corpus", (String)null);
      if (corpusLocation == null) { // keep trying
        corpusLocation = parameters.getString("index") + File.separator + "corpus";
      }
      cReader = new CorpusReader(corpusLocation);
    }

    if (tokenizer == null) {
      tokenizer = new TagTokenizer();
    }

    // Finally, we need an iterator from the index for the doc. frequencies
    // For now we only take AggregateReader objects, which can report that number. Meaning we need
    // a dummy text node to get the part assignment
    Node dummy = TextPartAssigner.assignPart(new Node("extents", "dummy"),
            retrieval.getGlobalParameters(), retrieval.getAvailableParts());
    String indexPart = parameters.getString("index") + File.separator + dummy.getNodeParameters().getString("part");
    reader = DiskIndex.openIndexPart(indexPart);
  }

  public void cleanup() throws IOException {
    cReader.close();
    reader.close();
    cReader = null;
    reader = null;
  }

  public List<WeightedTerm> generateGrams(List<ScoredDocument> initialResults) throws IOException {
    // Count the dfs of the terms relative to the fb docs
    TObjectIntHashMap counts = countRFDF(initialResults);

    // now get collection-wide dfs, and calculate the TSVs.
    ArrayList<WeightedTerm> scored = scoreGrams(counts);
    Collections.sort(scored);
    return scored;
  }

  public Node generateExpansionQuery(List<ScoredDocument> initialResults, int fbTerms,
          Set<String> exclusionTerms) throws IOException {
    List<WeightedTerm> scored = (List<WeightedTerm>) generateGrams(initialResults);

    ArrayList<Node> children = new ArrayList<Node>();
    NodeParameters expParams = new NodeParameters();
    int expanded = 0;
    for (int i = 0; i < scored.size() && expanded < fbTerms; i++) {
      Gram g = (Gram) scored.get(i);
      if (exclusionTerms.contains(g.term)) {
        continue;
      }
      Node inner = TextPartAssigner.assignPart(new Node("extents", g.term), 
              retrieval.getGlobalParameters(), retrieval.getAvailableParts());
      ArrayList<Node> innerChild = new ArrayList<Node>();
      innerChild.add(inner);
      NodeParameters weightParameters = new NodeParameters();
      weightParameters.set("default", "bm25rf");
      weightParameters.set("rt", g.rt);
      weightParameters.set("R", g.R);
      children.add(new Node("feature", weightParameters, innerChild, 0));
      expanded++;
    }
    Node newRoot = new Node("combine", expParams, children, 0);
    return newRoot;
  }

  protected TObjectIntHashMap countRFDF(List<ScoredDocument> results) throws IOException {
    TObjectIntHashMap counts = new TObjectIntHashMap();
    Document doc;
    HashSet<String> seen = new HashSet<String>();
    for (ScoredDocument sd : results) {
      doc = cReader.getDocument(sd.document, new Parameters());
      tokenizer.tokenize(doc);
      seen.clear();
      for (String term : doc.terms) {
        seen.add(term);
      }
      for (String term : seen) {
        counts.adjustOrPutValue(term, 1, 1);
      }
    }
    return counts;
  }

  protected ArrayList<WeightedTerm> scoreGrams(TObjectIntHashMap counts) {
    ArrayList<WeightedTerm> grams = new ArrayList<WeightedTerm>();
    TObjectIntProcedure gramGenerator = new GramGenerator(grams, reader);
    counts.forEachEntry(gramGenerator);
    return grams;
  }

  public class GramGenerator implements TObjectIntProcedure {

    ArrayList<WeightedTerm> grams;
    int R;
    IndexPartReader reader;

    public GramGenerator(ArrayList<WeightedTerm> g, IndexPartReader reader) {
      this.grams = g;
      this.R = fbDocs;
      this.reader = reader;
    }

    // Variable naming is consistent w/ the formula for TSV in the paper.
    public boolean execute(Object a, int rt) {
      Gram g = new Gram((String) a);
      try {
        // Get df
        NodeAggregateIterator iterator = (NodeAggregateIterator) reader.getIterator(new Node("counts", g.term));
        long ft = iterator.getStatistics().nodeDocumentCount;
        double partone = java.lang.Math.pow(((ft + 0.0) / N), rt);
        double parttwo = org.lemurproject.galago.core.util.Math.binomialCoeff(R, rt);
        g.score = partone * parttwo;
        g.rt = rt;
        g.R = R;
        grams.add(g);
        return true;
      } catch (IOException ioe) {
        throw new RuntimeException("Unable to retrieval doc count for term: " + g.term, ioe);
      }
    }
  }
}
