/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import org.lemurproject.galago.core.parse.SequentialDocumentNumberer;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.UniversalParser;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
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
    TagTokenizer p2 = new TagTokenizer();
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

  
  private String getExtension(String fileName) {
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    // If the last chunk of the filename is gz, we'll ignore it.
    // The second-to-last bit is the type extension (but only if
    // there are at least three parts to the name).
    if (fields[fields.length - 1].equals("gz")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // Do the same thing w/ bz2 as above (MAC)
    if (fields[fields.length - 1].equals("bz2")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // No 'gz' extension, so just return the last part.
    return fields[fields.length - 1];
  }

  public void addFile(String filename) throws IOException {

    // a split is a file with some annotations
    boolean compressed = (filename.endsWith(".gz") || filename.endsWith(".bz2"));
    DocumentSplit split = new DocumentSplit(filename, getExtension(filename), compressed, new byte[0], new byte[0], 0, 0);
    indexer.process(split);

  }
  
  
  public ScoredDocument[] runQuery(String query) throws Exception{
    // parse query
    // transform query
    // run query
    
    Node parsed = StructuredQuery.parse(query);
    Node transformed = retrieval.transformQuery(parsed);

    Parameters p = new Parameters();
    p.set("count", 10);
    ScoredDocument[] results = retrieval.runQuery(transformed, p);

    return results;
  }
}
