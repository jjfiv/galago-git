/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import org.lemurproject.galago.core.parse.SequentialDocumentNumberer;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * 
 * This is an example indexer - it uses the geometricindex to 
 * dynamically construct an index of the input documents
 *
 * @author sjh
 */
public class Indexer {

  public UniversalParser indexer;
  public GeometricIndex index;
  public Retrieval retrieval;

  public Indexer(/*Parameters p ? */) throws Exception {
    
    // basic plan:
    //   pass universal parser a file path
    //   universal parser will extract a stream of documents (or just one)
    //   passes them to the tokenzier
    //   tokenizer will extract word tokens
    //   passes documents to the numberer
    //   a number is assigned
    //   a fully formed document is the given to the index
    
    indexer = new UniversalParser(new FakeParameters(new Parameters()));
    Tokenizer p2 = Tokenizer.instance(new Parameters());
    SequentialDocumentNumberer p3 = new SequentialDocumentNumberer();

    Parameters indexParams = new Parameters();
    indexParams.set("shardDirectory", "/path/to/store/output/");
    indexParams.set("indexBlockSize", 100);
    indexParams.set("radix", 2);
    indexParams.set("mergeMode", "local");
    indexParams.set("stemming", true);
    indexParams.set("makecorpus", false);

    index = new GeometricIndex(new FakeParameters(indexParams));
    retrieval = new LocalRetrieval(index);
    // now link these steps together
    indexer.setProcessor(p2);
    p2.setProcessor(p3);
    p3.setProcessor(index);
  }

  public void addFile(String filename) throws IOException {
    // a split is a file with some annotations
    DocumentSplit split = new DocumentSplit();
    split.fileName = filename;
    split.fileType = FileUtility.getExtension(new File(filename));
    indexer.process(split);
  }
  
  
  public List<ScoredDocument> runQuery(String query) throws Exception {
    // parse query
    // transform query
    // run query
    
    Node parsed = StructuredQuery.parse(query);
    
    Parameters p = new Parameters();
    p.set("count", 10);
    Node transformed = retrieval.transformQuery(parsed, p);

    Results results = retrieval.executeQuery(transformed, p);

    return results.scoredDocuments;
  }
}
