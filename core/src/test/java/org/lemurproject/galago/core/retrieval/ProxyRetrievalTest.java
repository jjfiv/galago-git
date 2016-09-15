/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.web.WebServer;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 *
 * @author sjh
 */
public class ProxyRetrievalTest {

  public static boolean checkReadyURL(String url) throws IOException {
    try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
      HttpPost post = new HttpPost(url + "/ready");
      try (CloseableHttpResponse response = client.execute(post)) {
        int code = response.getStatusLine().getStatusCode();
        return code == 200;
      } catch (HttpHostConnectException refused) {
        // Thrown if the URL or host does not yet exist!
        return false;
      }
    }
  }

  @Test
  public void testProxyRet() throws Exception {
    final int vocab = 1000;
    final int docCount = 1000;
    final int docLen = 100;
    final int port = 1111;

    File index = null;

    AtomicBoolean started = new AtomicBoolean(false);
    try {
      index = makeIndex(docCount, docLen, vocab);
      final Parameters retParams = Parameters.create();
      retParams.set("index", index.getAbsolutePath());
      retParams.set("port", port);

      final List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());

      Thread remoteRetThread = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            System.out.println("remoteRetThread.start()");
            started.set(true);
            App.run("search", retParams, System.err);

          } catch (InterruptedException i) {
            System.err.println("INTERRUPTED!");

          } catch (Exception e) {
            exceptions.add(e);
          }
        }
      });

      remoteRetThread.start();

      while(!started.get()) {
        Thread.sleep(20);
      }

      // look through output of server startup to ensure it contains this url
      String url = "http://"+WebServer.getHostName()+":" + port;
      boolean success = false;
      // 30 tries, ~3 seconds worth of trying.
      for (int i = 0; i < 30; i++) {
        if(checkReadyURL(url)) {
          success = true;
        }
        Thread.sleep(100);
      }

      if (!success) {
        // try to kill the thread...
        remoteRetThread.interrupt();
        throw new RuntimeException("FAILED! could not find local proxy...");
      }

      Parameters proxyParams = Parameters.create();
      proxyParams.set("index", url);
      Retrieval instance = RetrievalFactory.create(proxyParams);
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
        instance.getDocumentName(1L);

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
        Node trans = instance.transformQuery(StructuredQuery.parse("#combine(1 2 3)"), Parameters.create());
        instance.executeQuery(trans);
        instance.executeQuery(trans, Parameters.create());



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
        throw new RuntimeException("FAILED THREADING TEST.");
      }

    } finally {
      if (index != null) {
        FSUtil.deleteDirectory(index);
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

    Parameters p = Parameters.create();
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
