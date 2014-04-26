/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.web.WebServer;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;

/**
 *
 * @author sjh
 */
public class ProxyRetrievalTest {

  @Test
  public void testProxyRet() throws Exception {
    final int vocab = 1000;
    final int docCount = 1000;
    final int docLen = 100;
    final int port = 1111;

    File index = null;

    try {
      index = makeIndex(docCount, docLen, vocab);
      final Parameters retParams = new Parameters();
      retParams.set("index", index.getAbsolutePath());
      retParams.set("port", port);

      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
      final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      final PrintStream stream = new PrintStream(byteStream);

      Thread remoteRetThread = new Thread() {

        @Override
        public void run() {
          try {
            System.out.println("remoteRetThread.start()");
            App.run("search", retParams, stream);

          } catch (InterruptedException i) {
            System.err.println("INTERRUPTED!");

          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      };

      remoteRetThread.start();

      // look through output of server startup to ensure it contains this url
      String url = "http://"+WebServer.getHostName()+":" + port;
      boolean success = false;
      int i = 0;
      while (i < 10) {
        stream.flush();
        String s = byteStream.toString();
        System.out.println(s);
        if (s.contains(url)) {
          success = true;
          break;
        }

        Thread.sleep(100); // 100 milliseconds * 10 ~= 1 second
        i += 1;
      }


      if (!success) {
        // try to kill the thread...
        remoteRetThread.interrupt();
        throw new RuntimeException("FAILED! could not find local proxy...");
      }

      Parameters proxyParams = new Parameters();
      proxyParams.set("index", url);
      Retrieval instance = RetrievalFactory.instance(proxyParams);
      // test proxied functions:

      try {
        instance.getAvailableParts();
        instance.getCollectionStatistics(StructuredQuery.parse("#lengths:document:part=lengths()"));
        instance.getCollectionStatistics("#lengths:document:part=lengths()");

        Document d = instance.getDocument("doc-" + 2, new DocumentComponents(true, false, false));
        assertNotNull(d.text);
        assertNull(d.terms);
        assertNull(d.tags);
        assertTrue(d.metadata.isEmpty());

        instance.getDocumentLength("doc-" + 2);
        instance.getDocumentLength(1);
        instance.getDocumentName(1);

        ArrayList<String> names = new ArrayList<String>();
        names.add("doc-" + 1);
        names.add("doc-" + 2);
        instance.getDocuments(names, new DocumentComponents());
        instance.getGlobalParameters();
        instance.getIndexPartStatistics("postings");
        instance.getNodeStatistics(StructuredQuery.parse("#counts:@/1/:part=postings()"));
        instance.getNodeStatistics("#counts:@/1/:part=postings()");
        instance.getNodeType(StructuredQuery.parse("#counts:@/1/:part=postings()"));
        instance.getQueryType(StructuredQuery.parse("#counts:@/1/:part=postings()"));
        Node trans = instance.transformQuery(StructuredQuery.parse("#combine(1 2 3)"), new Parameters());
        instance.executeQuery(trans);
        instance.executeQuery(trans, new Parameters());



        instance.close();
      } catch (Exception e) {
        exceptions.add(e);
      }


      // try to kill the thread...
      remoteRetThread.interrupt();

      for (Exception e : exceptions) {
        System.err.println(e.getMessage());
        e.printStackTrace();
      }

      if (!exceptions.isEmpty()) {
        System.err.println(byteStream.toString());
        throw new RuntimeException("FAILED THREADING TEST.");
      }

    } finally {
      if (index != null) {
        Utility.deleteDirectory(index);
      }
    }
  }

  // index construction
  private File makeIndex(int docCount, int docLen, int vocab) throws Exception {
    File trecFile = FileUtility.createTemporary();
    File indexFolder = FileUtility.createTemporaryDirectory();

    Random r = new Random();
    BufferedWriter writer = new BufferedWriter(new FileWriter(trecFile));
    for (int doc = 0; doc < docCount; doc++) {
      StringBuilder sb = new StringBuilder();
      sb.append("document ").append(doc);
      for (int termCount = 2; termCount < docLen; termCount++) {
        sb.append(" ").append(r.nextInt(vocab));
      }
      writer.write(AppTest.trecDocument("doc-" + doc, sb.toString()));
    }
    writer.close();

    Parameters p = new Parameters();
    p.set("inputPath", trecFile.getAbsolutePath());
    p.set("indexPath", indexFolder.getAbsolutePath());
    p.set("corpus", true);
    p.set("stemmedPostings", false); // we just have numbers - no need to stem.
    App.run("build", p, System.err);

    // now remove the data file
    trecFile.delete();
    return indexFolder;
  }
}
