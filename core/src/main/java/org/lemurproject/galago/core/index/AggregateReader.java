// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.io.Serializable;

import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Interface provides access to useful statistics for a given term.
 *
 * @author sjh
 */
public interface AggregateReader {

    public class CollectionStatistics implements Serializable {
        
        private static final long serialVersionUID = 4553653651892088433L;
        public String partName;
        public long collectionLength;
        public long documentCount;
        public long vocabCount;

        public CollectionStatistics(String partName, Parameters parameters) {
            this.partName = partName;
            collectionLength = parameters.get("statistics/collectionLength",0L);
            documentCount = parameters.get("statistics/documentCount",0L);
            vocabCount = parameters.get("statistics/vocabCount",0L);
        }

        public CollectionStatistics(String partName, MemoryIndex index) {
            this.partName = partName;
            collectionLength = index.getIndexPart(partName).getCollectionLength();
            documentCount = index.getIndexPart(partName).getDocumentCount();
            vocabCount = index.getIndexPart(partName).getKeyCount();
        }

        public void add(CollectionStatistics other) {
            assert this.partName.equals(other.partName);
            this.collectionLength += other.collectionLength;
            this.documentCount += other.documentCount;
            this.vocabCount += other.vocabCount;
        }

        public Parameters toParameters() {
            Parameters p = new Parameters();
            p.set("statistics/collectionLength", collectionLength);
            p.set("statistics/documentCount", documentCount);
            p.set("statistics/vocabCount", vocabCount);
            return p;
        }

        public String toString(){
            return toParameters().toString();
        }
    }

    public class NodeStatistics implements Serializable {

        public String node = "";
        public long nodeFrequency = 0;
        public long nodeDocumentCount = 0;
        public long collectionLength = 0;
        public long documentCount = 0;

        private static final long serialVersionUID = 4553653651892088433L;

        public String toString() {
            return "{ \"node\" : \"" + node + "\","
                    + "\"nodeFrequency\" : " + nodeFrequency + ","
                    + "\"nodeDocumentCount\" : " + nodeDocumentCount + ","
                    + "\"collectionLength\" : " + collectionLength + ","
                    + "\"documentCount\" : " + documentCount + "}";
        }

        public void add(NodeStatistics other) {
            // assert this.node.equals(other.node); // doesn't work.. need to investigate why.
            nodeFrequency += other.nodeFrequency;
            nodeDocumentCount += other.nodeDocumentCount;
            collectionLength += other.collectionLength;
            documentCount += other.documentCount;
        }
    }

    public static interface AggregateIterator {

        public NodeStatistics getStatistics();
    }

    // don't like these two anymore - they make the modifier stuff impossible...
    public NodeStatistics getTermStatistics(String term) throws IOException;

    public NodeStatistics getTermStatistics(byte[] term) throws IOException;
}
