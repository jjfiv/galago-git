/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.processing;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.iterator.LengthsIterator;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author MichaelZ - started with copy of RankedDocumentModelTest
 */
public class RankedNonZeroLengthDocumentModelTest {

  File corpus = null;
  File index = null;
  File tmpindex = null;

  @Before
  public void setUp() throws Exception {
    corpus = FileUtility.createTemporary();
    tmpindex = FileUtility.createTemporaryDirectory();
    index = FileUtility.createTemporaryDirectory();
    makeIndex();
  }

  @After
  public void tearDown() throws IOException {
    if (corpus != null) {
      corpus.delete();
    }
    if (index != null) {
      FSUtil.deleteDirectory(index);
    }

  }

  @Test
  public void test() throws Exception {

    // this is a very contrived test. We normally don't have zero
    // length documents that are retrievable, but if we index a
    // books pages then retrieve them via a metadata tag such as
    // publication date, we get many zero length document which are
    // blank pages.
    // See the buildIndex method for how I fake this.

    Parameters globals = Parameters.create();
    LocalRetrieval ret = new LocalRetrieval(index.getAbsolutePath(), globals);

    Parameters queryParams = Parameters.create();
    queryParams.set("requested", 10);

    Node query = StructuredQuery.parse("#combine(#inside( #text:@\\1917\\() #field:dummy() ))");
    query = ret.transformQuery(query, queryParams);

    // first, use the regular RankedDocumentModel - which will get all documents
    RankedDocumentModel oldModel = new RankedDocumentModel(ret);
    ScoredDocument[] results = oldModel.execute(query, queryParams);

    assertEquals(results.length, 10);

    // now test the zero length version
    RankedNonZeroLengthDocumentModel newModel = new RankedNonZeroLengthDocumentModel(ret);
    results = newModel.execute(query, queryParams);
    assertEquals(results.length, 5);

    Set<String> docNames = new HashSet<String>(5);
    for (int i = 0; i < 5; i++) {
      String name = ret.getDocumentName((long)results[i].document);
      docNames.add(name);
      Document doc = ret.getDocument(name, Document.DocumentComponents.All );
      assertTrue(doc.text.length() > 0);
    }
    // just to be extra careful..
    assertTrue(docNames.containsAll(Arrays.asList("doc-1", "doc-3", "doc-5", "doc-7", "doc-9") ));

  }

  private void makeIndex() throws Exception {

    // we need an index that has a field we'll use to retrieve documents.
    // All the parsers available to us here will index fields in the "text"
    // part of the document so we need to fool the index we'll search into
    // thinking we have fields.
    // First we build an index where each document has a field. Then we build
    // a 2nd index without the fields and copy the field parts from the first
    // index to the 2nd so we can retrieve via fields.
    // Note that the MBTEI parsers that are part of the Proteus project are
    // able to index metadata fields that don't occur in the text - that's where
    // this whole "zero document length" problem originated.

    // small index of 10 docs with field, 5 that are empty
    StringBuilder c = new StringBuilder();
    c.append(AppTest.trecDocument("doc-1", "<dummy>1917</dummy>Test doc 1"));
    c.append(AppTest.trecDocument("doc-2", "<dummy>1917</dummy>"));
    c.append(AppTest.trecDocument("doc-3", "<dummy>1917</dummy>Test doc 3"));
    c.append(AppTest.trecDocument("doc-4", "<dummy>1917</dummy>"));
    c.append(AppTest.trecDocument("doc-5", "<dummy>1917</dummy>Test doc 5"));
    c.append(AppTest.trecDocument("doc-6", "<dummy>1917</dummy>"));
    c.append(AppTest.trecDocument("doc-7", "<dummy>1917</dummy>Test doc 7"));
    c.append(AppTest.trecDocument("doc-8", "<dummy>1917</dummy>"));
    c.append(AppTest.trecDocument("doc-9", "<dummy>1917</dummy>Test doc 9"));
    c.append(AppTest.trecDocument("doc-10", "<dummy>1917</dummy>"));
    StreamUtil.copyStringToFile(c.toString(), corpus);

    App.main(new String[]{"build", "--indexPath=" + tmpindex.getAbsolutePath(),
            "--inputPath=" + corpus.getAbsolutePath(),
            "--tokenizer/fields+dummy"});

    corpus = FileUtility.createTemporary();

    // the same documents as above, except no fields.
    c = new StringBuilder();
    c.append(AppTest.trecDocument("doc-1", "Test doc 1"));
    c.append(AppTest.trecDocument("doc-2", ""));
    c.append(AppTest.trecDocument("doc-3", "Test doc 3"));
    c.append(AppTest.trecDocument("doc-4", ""));
    c.append(AppTest.trecDocument("doc-5", "Test doc 5"));
    c.append(AppTest.trecDocument("doc-6", ""));
    c.append(AppTest.trecDocument("doc-7", "Test doc 7"));
    c.append(AppTest.trecDocument("doc-8", ""));
    c.append(AppTest.trecDocument("doc-9", "Test doc 9"));
    c.append(AppTest.trecDocument("doc-10", ""));
    StreamUtil.copyStringToFile(c.toString(), corpus);

    App.main(new String[]{"build", "--indexPath=" + index.getAbsolutePath(),
            "--inputPath=" + corpus.getAbsolutePath()});

    // now for the trickery...
    // copy the field index parts from the first index to the second
    Files.copy(new File(tmpindex.getAbsolutePath() + File.separator + "field.dummy").toPath(), new File(index.getAbsolutePath() + File.separator + "field.dummy").toPath());
    Files.copy(new File(tmpindex.getAbsolutePath() + File.separator + "field.krovetz.dummy").toPath(), new File(index.getAbsolutePath() + File.separator + "field.krovetz.dummy").toPath());
    Files.copy(new File(tmpindex.getAbsolutePath() + File.separator + "extents").toPath(), new File(index.getAbsolutePath() + File.separator + "extents").toPath());

    // remove the first index so we don't accidentally use it.
    FSUtil.deleteDirectory(tmpindex);
    tmpindex = null;

  }

}
