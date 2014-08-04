package org.lemurproject.galago.core.retrieval.iterator.counts;

import org.junit.Test;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class LogCountIteratorTest {
  static Tokenizer tok = Tokenizer.instance(Parameters.instance());
  public static Document makeDocument(String name, String text) {
    Document doc = new Document();
    doc.name = name;
    doc.text = text;
    tok.tokenize(doc);
    return doc;
  }

  @Test
  public void testLogCountIter() throws Exception {
    MemoryIndex memIndex = new MemoryIndex();
    memIndex.process(makeDocument("1", "This is a sample document"));
    memIndex.process(makeDocument("2", "The cat jumped over the moon"));
    memIndex.process(makeDocument("3", "If the shoe fits, it's ugly"));
    memIndex.process(makeDocument("4", "Though a program be but three lines long, someday it will have to be maintained."));
    memIndex.process(makeDocument("5", "To be trusted is a greater compliment than to be loved"));
    memIndex.process(makeDocument("6", "Just because everything is different doesn't mean anything has changed."));
    memIndex.process(makeDocument("7", "everything everything jumped sample ugly"));
    memIndex.process(makeDocument("8", "though cat moon cat cat cat"));
    memIndex.process(makeDocument("9", "document document document document"));
    memIndex.process(makeDocument("10", "program fits"));

    LocalRetrieval ret = new LocalRetrieval(memIndex);

    Parameters qp = Parameters.instance();
    Node xq = ret.transformQuery(StructuredQuery.parse("#log-count(#count-sum( the cat ))"), qp);
    List<ScoredDocument> docs = ret.executeQuery(xq).scoredDocuments;

    /*for (ScoredDocument doc : docs) {
      double score = Math.exp(doc.score);
      System.out.printf("Doc%s: #(is the cat) = %d\n", doc.documentName, (int) score);
    }*/

    assertEquals("8", docs.get(0).documentName); // through cat! moon cat! cat! cat!
    assertEquals(Math.log(4), docs.get(0).score, CmpUtil.epsilon);
    assertEquals("2", docs.get(1).documentName); // the! cat! jumped over the! moon
    assertEquals(Math.log(3), docs.get(1).score, CmpUtil.epsilon);
  }

}
