/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.stats.FieldStatistics;
import org.lemurproject.galago.core.index.stats.IndexPartStatistics;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.tools.apps.BuildIndex;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class UniversalParserTest extends TestCase {

  Random r = new Random();

  public UniversalParserTest(String name) {
    super(name);
  }

  public void createTxtDoc(File folder, String fn) throws IOException {

    StringBuilder sb = new StringBuilder();
    sb.append("Text document\n");
    for (int i = 0; i < 10; i++) {
      sb.append(i).append(" ").append(r.nextInt(100)).append("\n");
    }

    Utility.copyStringToFile(sb.toString(), new File(folder, fn));
  }

  public void createXMLDoc(File folder, String fn) throws IOException {

    StringBuilder sb = new StringBuilder();
    sb.append("<document>\n");
    sb.append("<title>XMLdocument</title>\n");
    for (int i = 0; i < 10; i++) {
      sb.append("<num>").append(i).append("</num>");
      sb.append("<num>").append(r.nextInt(100)).append("</num>\n");
    }
    sb.append("</document>\n");

    Utility.copyStringToFile(sb.toString(), new File(folder, fn));
  }

  public void createTrecTextDoc(File folder, String fn) throws IOException {

    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < 10; d++) {
      sb.append("<DOC>\n");
      sb.append("<DOCNO>tt-").append(d).append("</DOCNO>\n<TEXT>\n");
      for (int i = 0; i < 10; i++) {
        sb.append("<num>").append(i).append("</num>");
        sb.append("<num>").append(r.nextInt(100)).append("</num>\n");
      }
      sb.append("</TEXT>\n</DOC>\n");

    }
    Utility.copyStringToFile(sb.toString(), new File(folder, fn));
  }

  public void createTrecWebDoc(File folder, String fn) throws IOException {

    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < 10; d++) {
      sb.append("<DOC>\n");
      sb.append("<DOCNO>tw-").append(d).append("</DOCNO>\n");
      sb.append("<DOCHDR>\nURL\n</DOCHDR>\n");
      for (int i = 0; i < 10; i++) {
        sb.append("<num>").append(i).append("</num>");
        sb.append("<num>").append(r.nextInt(100)).append("</num>\n");
      }
      sb.append("</DOC>\n");
    }
    Utility.copyStringToFile(sb.toString(), new File(folder, fn));
  }

  public void createTwitterDoc(File folder, String fn) throws IOException {

    StringBuilder sb = new StringBuilder();
    for (int d = 0; d < 10; d++) {
      sb.append("uid-").append(d).append("\tnow\t");

      for (int i = 0; i < 10; i++) {
        sb.append(i).append(" ").append(r.nextInt(100));
      }
      sb.append("\tfaked\n");
    }

    Utility.copyStringToFile(sb.toString(), new File(folder, fn));
  }

  public void testDefaultyBehavior() throws Exception {
    File index = FileUtility.createTemporaryDirectory();
    File dataDir = FileUtility.createTemporaryDirectory();
    try {

      createTxtDoc(dataDir, "d1.txt"); // 1 doc
      createXMLDoc(dataDir, "d2.xml"); // 1 doc
      createTrecTextDoc(dataDir, "d3.trectext"); // 10 docs
      createTrecWebDoc(dataDir, "d4.trecweb"); // 10 docs
      createTwitterDoc(dataDir, "d5.twitter"); // 10 docs

      Parameters p = new Parameters();
      p.set("inputPath", Collections.singletonList(dataDir.getAbsolutePath()));
      p.set("indexPath", index.getAbsolutePath());

      BuildIndex bi = new BuildIndex();
      bi.run(p, System.err);

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());

      FieldStatistics cs = ret.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(cs.collectionLength, 553);
      assertEquals(cs.documentCount, 32);
      assertEquals(cs.maxLength, 22);
      assertEquals(cs.minLength, 11);

      IndexPartStatistics is1 = ret.getIndexPartStatistics("postings");
      assertEquals(is1.collectionLength, 553);

      IndexPartStatistics is2 = ret.getIndexPartStatistics("postings.krovetz");
      assertEquals(is2.collectionLength, 553);

      // should have about the same vocabs
      assertEquals(is1.vocabCount, is2.vocabCount);

    } finally {
      Utility.deleteDirectory(index);
      Utility.deleteDirectory(dataDir);
    }
  }

  public void testAllIsOneBehavior() throws Exception {
    File index = FileUtility.createTemporaryDirectory();
    File dataDir = FileUtility.createTemporaryDirectory();
    try {

      createTxtDoc(dataDir, "d1"); // 1 doc
      createXMLDoc(dataDir, "d2"); // 1 doc
      createTxtDoc(dataDir, "d3"); // 1 doc
      createXMLDoc(dataDir, "d4"); // 1 doc
      createTxtDoc(dataDir, "d5"); // 1 doc
      createXMLDoc(dataDir, "d6"); // 1 doc

      Parameters p = new Parameters();
      p.set("inputPath", Collections.singletonList(dataDir.getAbsolutePath()));
      p.set("indexPath", index.getAbsolutePath());
      p.set("filetype", "txt");

      BuildIndex bi = new BuildIndex();
      bi.run(p, System.err);

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());

      FieldStatistics cs = ret.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(cs.collectionLength, 129);
      assertEquals(cs.documentCount, 6);
      assertEquals(cs.maxLength, 22);
      assertEquals(cs.minLength, 21);

      IndexPartStatistics is1 = ret.getIndexPartStatistics("postings");
      assertEquals(is1.collectionLength, 129);

      IndexPartStatistics is2 = ret.getIndexPartStatistics("postings.krovetz");
      assertEquals(is2.collectionLength, 129);

      // should have about the same vocabs
      assertEquals(is1.vocabCount, is2.vocabCount);

    } finally {
      Utility.deleteDirectory(index);
      Utility.deleteDirectory(dataDir);
    }
  }

  public void testManualOverrideBehavior() throws Exception {
    File index = FileUtility.createTemporaryDirectory();
    File dataDir = FileUtility.createTemporaryDirectory();
    try {

      createTrecTextDoc(dataDir, "d1.qqe"); // 10 docs - trectext
      createTrecWebDoc(dataDir, "d2.qwe"); // 10 docs - trecweb
      createTrecTextDoc(dataDir, "d3.trectext"); // 10 docs - trectext
      createTrecTextDoc(dataDir, "d4.trecweb"); // 10 docs - trectext
      createTxtDoc(dataDir, "d5.txt"); // 1 docs - txt

      Parameters p = new Parameters();
      p.set("inputPath", Collections.singletonList(dataDir.getAbsolutePath()));
      p.set("indexPath", index.getAbsolutePath());
      p.set("parser", new Parameters());
      p.getMap("parser").set("externalParsers", new ArrayList<Parameters>());
      p.getMap("parser").getList("externalParsers").add(Parameters.parseString("{\"filetype\" : \"qqe\", \"class\" :\"" + TrecTextParser.class.getName() + "\"}"));
      p.getMap("parser").getList("externalParsers").add(Parameters.parseString("{\"filetype\" : \"qwe\", \"class\" :\"" + TrecWebParser.class.getName() + "\"}"));
      p.getMap("parser").getList("externalParsers").add(Parameters.parseString("{\"filetype\" : \"trecweb\", \"class\" :\"" + TrecTextParser.class.getName() + "\"}"));

      BuildIndex bi = new BuildIndex();
      bi.run(p, System.err);

      Retrieval ret = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());

      FieldStatistics cs = ret.getCollectionStatistics("#lengths:part=lengths()");
      assertEquals(cs.collectionLength, 822);
      assertEquals(cs.documentCount, 41); 
      assertEquals(cs.maxLength, 22);
      assertEquals(cs.minLength, 20);

      IndexPartStatistics is1 = ret.getIndexPartStatistics("postings");
      assertEquals(is1.collectionLength, 822);

      IndexPartStatistics is2 = ret.getIndexPartStatistics("postings.krovetz");
      assertEquals(is2.collectionLength, 822);

      // should have about the same vocabs
      assertEquals(is1.vocabCount, is2.vocabCount);

    } finally {
      Utility.deleteDirectory(index);
      Utility.deleteDirectory(dataDir);
    }
  }
}
