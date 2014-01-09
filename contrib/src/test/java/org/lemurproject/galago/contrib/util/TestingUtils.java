/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.util;

import java.io.File;
import junit.framework.Assert;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class TestingUtils {

  public static String trecDocument(String docno, String text) {
    return "<DOC>\n<DOCNO>" + docno + "</DOCNO>\n"
            + "<TEXT>\n" + text + "</TEXT>\n</DOC>\n";
  }

  public static File[] make10DocIndex() throws Exception {
    File trecCorpusFile, corpusFile, indexFile;

    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
    trecCorpus.append(trecDocument("1", "This is a sample document"));
    trecCorpus.append(trecDocument("2", "The cat jumped over the moon"));
    trecCorpus.append(trecDocument("3", "If the shoe fits, it's ugly"));
    trecCorpus.append(trecDocument("4", "Though a program be but three lines long, someday it will have to be maintained."));
    trecCorpus.append(trecDocument("5", "To be trusted is a greater compliment than to be loved"));
    trecCorpus.append(trecDocument("6", "Just because everything is different doesn't mean anything has changed."));
    trecCorpus.append(trecDocument("7", "everything everything jumped sample ugly"));
    trecCorpus.append(trecDocument("8", "though cat moon cat cat cat"));
    trecCorpus.append(trecDocument("9", "document document document document"));
    trecCorpus.append(trecDocument("10", "program fits"));
    trecCorpusFile = FileUtility.createTemporary();
    Utility.copyStringToFile(trecCorpus.toString(), trecCorpusFile);

    // now, attempt to make a corpus file from that.
    corpusFile = FileUtility.createTemporaryDirectory();
    App.main(new String[]{"make-corpus", "--corpusPath=" + corpusFile.getAbsolutePath(),
              "--inputPath=" + trecCorpusFile.getAbsolutePath(), "--distrib=2"});

    // make sure the corpus file exists
    Assert.assertTrue(corpusFile.exists());

    // now, try to build an index from that
    indexFile = FileUtility.createTemporaryDirectory();
    App.main(new String[]{"build", "--stemmedPostings=false", "--indexPath=" + indexFile.getAbsolutePath(),
              "--inputPath=" + corpusFile.getAbsolutePath(), "--stemmer+porter"});

    verifyIndexStructures(indexFile);
    File[] files = new File[3];
    files[0] = trecCorpusFile;
    files[1] = corpusFile;
    files[2] = indexFile;
    return files;
  }

  public static void verifyIndexStructures(File indexPath) {
    // Check main path
    Assert.assertTrue(indexPath.isDirectory());

    // Time to check standard parts

    // doc lengths
    File childPath = new File(indexPath, "lengths");
    Assert.assertTrue(childPath.exists());

    // doc names -- there are two files
    childPath = new File(indexPath, "names");
    Assert.assertTrue(childPath.exists());
    childPath = new File(indexPath, "names.reverse");
    Assert.assertTrue(childPath.exists());

    // some postings must exist
    File childPath1 = new File(indexPath, "postings");
    File childPath2 = new File(indexPath, "postings.porter");
    File childPath3 = new File(indexPath, "postings.krovetz");
    Assert.assertTrue(childPath1.exists() || childPath2.exists() || childPath3.exists());

  }
}
