// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;

/**
 * This class writes index files, which are used for most Galago indexes.
 * 
 * An index is a mapping between a key and a value, much like a TreeMap.  The keys are
 * sorted to allow iteration over the whole file.  Keys are stored using prefix
 * compression to save space.  The structure is designed for fast random access on disk.
 * 
 * For indexes, we assume that the data in each value is already compressed, so IndexWriter
 * does no additional compression.
 *
 * An IndexWriter is a special case, it can compress the data if isCompressed is set.
 * 
 * Keys cannot be longer than 256 bytes, and they must be added in sorted order.
 *
 * There are two ways of adding data to an index:
 *
 *  - add
 *      - adds the entire key-value pair in a single wrapper
 *      - this type may be stored partially on disk (see GenericElement)
 *
 *  - processKey/Tuple
 *      - adds a key and several value blocks
 *      - this tuple allows for partial value data to be written
 *        to the index as it is generated
 *      - both key and value blocks must be explicitly represented in memory
 *      - this produces some memory limitations
 * 
 * @author sjh
 */
public abstract class BTreeWriter implements KeyValuePair.KeyValueOrder.ShreddedProcessor, Source<KeyValuePair> {
        
    /**
     * Returns the current copy of the manifest, which will be stored in
     * the completed index file.  This data is not written until close() is called.
     */
    public abstract Parameters getManifest();

    /**
     * Adds a key-value pair of byte[]s to the index
     *  - when in this form neither the key nor the bytes need to fit into availiable RAM
     *  - this allows multi-gigabyte values to be written to the index
     */
    public abstract void add(IndexElement list) throws IOException ;


    /**
     * Closes the index writer
     *  - flushes all buffers and closes the file writers
     */
    public abstract void close() throws IOException;


    /**************/
    // block based index functions
    //  - these avoid buffering long posting lists to disk
    //  - sjh - i made these optional so as not to clutter up the IndexWriter
    //        - TODO: ParallelIndexValueWriter does implement them (needs more work)


    /**
     * Adds a key to the index
     * - writer waits for all valueBlocks to be added before writing the key
     *
     */
    public void processKey(byte[] key) throws IOException{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Adds a valueBlock to the index (many blocks may be added for any given key)
     *  - write will throw an exception if the valueBlock is larger than the ValueBlockSize
     *  - this check can be avoided by using the (add) function above
     */
    public void processValue(byte[] value) throws IOException{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Finalizes a key-value pair
     */
    public void processTuple() throws IOException{
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Returns the value block size
     * This allows the writers to create appropriately sized blocks of data
     */
    public long getValueBlockSize(){
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * setProcessor allows the writer class to pass data on to the next step.
     *  - Useful for ParallelIndexes passing data to a central key indexer
     *  - not implemented for single-file Indexes
     */
    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
