/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse.stem;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.PositionIndexReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author sjh
 */
public class KrovetzStemmerTest {

  final static String text = "Call me Ishmael. Some years ago never mind how long precisely "
          + "having little or no money in my purse, and nothing particular to interest "
          + "me on shore, I thought I would sail about a little and see the watery part "
          + "of the world. It is a way I have of driving off the spleen and regulating "
          + "the circulation. Whenever I find myself growing grim about the mouth; "
          + "whenever it is a damp, drizzly November in my soul; whenever I find myself "
          + "involuntarily pausing before coffin warehouses, and bringing up the rear of "
          + "every funeral I meet; and especially whenever my hypos get such an upper "
          + "hand of me, that it requires a strong moral principle to prevent me from "
          + "deliberately stepping into the street, and methodically knocking people's "
          + "hats off then, I account it high time to get to sea as soon as I can. This "
          + "is my substitute for pistol and ball. With a philosophical flourish Cato "
          + "throws himself upon his sword; I quietly take to the ship. There is nothing "
          + "surprising in this. If they but knew it, almost all men in their degree, "
          + "some time or other, cherish very nearly the same feelings towards the ocean "
          + "with me.";

  @Test
  public void testStemming() {

    Document test = new Document("test", text);
    TagTokenizer tt = new TagTokenizer();
    tt.tokenize(test);

    Stemmer stemmer = new KrovetzStemmer();
    test = stemmer.stem(test);

    List<String> stemmedTerms = new ArrayList<>(test.terms);

    List<String> idealStemmedTerms = Arrays.asList(("call me ishmael some years ago never mind how "
            + "long precisely have little or no money in my "
            + "purse and nothing particular to interest me on shore i "
            + "thought i would sail about a little and see the "
            + "watery part of the world it is a way i "
            + "have of driving off the spleen and regulate the circulation "
            + "whenever i find myself grow grim about the mouth whenever "
            + "it is a damp drizzle november in my soul whenever "
            + "i find myself involuntary pause before coffin warehouse and bring "
            + "up the rear of every funeral i meet and especially "
            + "whenever my hypo get such an upper hand of me "
            + "that it require a strong moral principle to prevent me "
            + "from deliberate step into the street and methodical knock people "
            + "hat off then i account it high time to "
            + "get to sea as soon as i can this is "
            + "my substitute for pistol and ball with a philosophical flourish "
            + "cato throw himself upon his sword i quiet take to "
            + "the ship there is nothing surprising in this if they "
            + "but knew it almost all men in their degree some "
            + "time or other cherish very nearly the same feelings towards "
            + "the ocean with me").split(" "));

    for (int i = 0; i < stemmedTerms.size(); i++) {
      String s = stemmedTerms.get(i);
      String t = idealStemmedTerms.get(i);
      assertEquals(s, t);
    }
  }

  @Test
  public void testIndexStemming() throws Exception {
    File trecCorpusFile = null;
    File indexFile1 = null;
    File indexFile2 = null;

    try {
      // create a simple doc file, trec format:
      String trecCorpus = AppTest.trecDocument("1", text);
      trecCorpusFile = FileUtility.createTemporary();
      StreamUtil.copyStringToFile(trecCorpus, trecCorpusFile);

      // now, try to build an index from that
      indexFile1 = FileUtility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile1.getAbsolutePath(),
                "--inputPath+" + trecCorpusFile.getAbsolutePath(),
                "--stemmerClass/Porter2Stemmer=org.lemurproject.galago.core.parse.stem.Porter2Stemmer"});

      // now, try to build an index from that
      indexFile2 = FileUtility.createTemporaryDirectory();
      App.main(new String[]{"build", "--indexPath=" + indexFile2.getAbsolutePath(),
                "--inputPath+" + trecCorpusFile.getAbsolutePath(),
                "--stemmerClass/KrovetzStemmer=org.lemurproject.galago.core.parse.stem.KrovetzStemmer"});

      // make sure the indexes exists
      assertTrue(indexFile1.exists());
      assertTrue(indexFile2.exists());

      // open stemmedPostings and compare lengths with postings.
      PositionIndexReader porterPart = (PositionIndexReader) DiskIndex.openIndexPart(indexFile1 + "/postings.Porter2Stemmer");
      PositionIndexReader krovetzPart = (PositionIndexReader) DiskIndex.openIndexPart(indexFile2 + "/postings.KrovetzStemmer");

      // ensure nodes can be found
      assert (porterPart.getIterator(new Node("counts", "warehouse")) != null);
      assert (krovetzPart.getIterator(new Node("counts", "warehouse")) != null);

      // ensure a second term works
      assertEquals(porterPart.getIterator(new Node("counts", "having")).getKeyString(), "have");
      assertEquals(krovetzPart.getIterator(new Node("counts", "having")).getKeyString(), "have");

    } finally {
      if (trecCorpusFile != null) {
        trecCorpusFile.delete();
      }
      if (indexFile1 != null) {
        FSUtil.deleteDirectory(indexFile1);
      }
      if (indexFile2 != null) {
        FSUtil.deleteDirectory(indexFile2);
      }
    }
  }
}
