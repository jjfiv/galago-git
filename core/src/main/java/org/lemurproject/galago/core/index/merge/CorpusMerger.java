// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.merge;

import org.lemurproject.galago.core.index.corpus.CorpusFileWriter;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;

import java.io.IOException;
import java.util.List;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class CorpusMerger extends GenericIndexMerger<Document> {

    public CorpusMerger(TupleFlowParameters p) throws Exception {
        super(p);
    }

    @Override
    public boolean mappingKeys() {
        return true;
    }

    @Override
    public Processor<Document> createIndexWriter(TupleFlowParameters parameters) throws Exception {
        return new CorpusFileWriter(parameters);
    }

    @Override
    public void performValueMerge(byte[] key, List<KeyIteratorWrapper> keyIterators) throws IOException {

        Document d = ((DocumentReader.DocumentIterator) keyIterators.get(0).iterator).getDocument(new DocumentComponents(true, true, false));

        // use the key passed in, NOT the key of the doc in the index so we have 
        // unique indexes - assuming the renumberDocuments param is true
        d.identifier = Utility.toLong(key);

        this.writer.process(d);

    }
}
