// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.structured;

import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.LocalRetrievalFieldTest;
import org.lemurproject.galago.core.retrieval.LocalRetrievalTest;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests the #between
 * @author MichaelZ
 */
public class BetweenIteratorTest {

  @Test
  public void testBetweenOperator() throws Exception {

    // makeIndex() builds the index "by hand"...
    File idxPath = LocalRetrievalTest.makeIndex();
    DiskIndex index = new DiskIndex(idxPath.getAbsolutePath());
    LocalRetrieval retrieval = new LocalRetrieval(index, Parameters.create());

    Node tree = StructuredQuery.parse("#combine(  #bool(#all( #between( date 1/1/1900 1/1/2020 ) ) ) )");

    Parameters qp = Parameters.create();
    tree = retrieval.transformQuery(tree, qp);

    List<ScoredDocument> results = retrieval.executeQuery(tree, qp).scoredDocuments;

    assertEquals("DOC1", results.get(0).documentName);
    assertEquals("DOC2", results.get(1).documentName);
    assertEquals("DOC5", results.get(2).documentName);

    retrieval.close();
    FSUtil.deleteDirectory(idxPath);

    // test "between" on a field when there is no format (data type) for the field
    idxPath = LocalRetrievalFieldTest.make10DocIndexWithFields(false);
    index = new DiskIndex(idxPath.getAbsolutePath());
    retrieval = new LocalRetrieval(index, Parameters.create());

    tree = StructuredQuery.parse("#combine(  #bool(#all( #between(  #field:title() @/document faaa/ @/document fzzz/ ) ) ) )");
    qp = Parameters.create();

    // this should fail because the fields don't have types
    try{
      tree = retrieval.transformQuery(tree, qp);
      fail("This test should have failed.");

      /*
      Error should be similar to :

        java.lang.IllegalArgumentException: NodeType of #field:title() is unknown. Fields should be tokenized with a data type in the formats parameter.
        at org.lemurproject.galago.core.retrieval.traversal.AnnotateCollectionStatistics.afterNode(AnnotateCollectionStatistics.java:65)
        at org.lemurproject.galago.core.retrieval.traversal.Traversal.traverse(Traversal.java:34)
        at org.lemurproject.galago.core.retrieval.traversal.Traversal.traverse(Traversal.java:32)
        at org.lemurproject.galago.core.retrieval.traversal.Traversal.traverse(Traversal.java:32)
        at org.lemurproject.galago.core.retrieval.traversal.Traversal.traverse(Traversal.java:32)
        at org.lemurproject.galago.core.retrieval.traversal.Traversal.traverse(Traversal.java:32)
        at org.lemurproject.galago.core.retrieval.LocalRetrieval.transformQuery(LocalRetrieval.java:319)
        at org.lemurproject.galago.core.retrieval.LocalRetrieval.transformQuery(LocalRetrieval.java:312)
        at org.lemurproject.galago.core.retrieval.structured.BetweenIteratorTest.testBetweenOperator(BetweenIteratorTest.java:68)

      To index with a datatype the JSON file would look something like:

      "tokenizer" : {
        "fields" : [ "title" , "author"],
        "formats" : {
          "author" : "string",
          "title" : "string"
        }
      }

      or on the command line:

      --tokenizer/fields+title --tokenizer/fields+author --tokenizer/formats/title=string --tokenizer/formats/author=string

      */
    } catch(Exception e) {
      // This is OK, we expected an exception.
    }
    retrieval.close();
    FSUtil.deleteDirectory(idxPath);

    // now build the index WITH field types
    idxPath = LocalRetrievalFieldTest.make10DocIndexWithFields(true);
    index = new DiskIndex(idxPath.getAbsolutePath());
    retrieval = new LocalRetrieval(index, Parameters.create());

    tree = StructuredQuery.parse("#combine(  #bool(#all( #between(  #field:title() @/document faaa/ @/document fzzz/ ) ) ) )");
    qp = Parameters.create();

    tree = retrieval.transformQuery(tree, qp);
    results = retrieval.executeQuery(tree, qp).scoredDocuments;
    assertEquals("4", results.get(0).documentName);
    assertEquals("5", results.get(1).documentName);

    retrieval.close();
    FSUtil.deleteDirectory(idxPath);
  }

}
