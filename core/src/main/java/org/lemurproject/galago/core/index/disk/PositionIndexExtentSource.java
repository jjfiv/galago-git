// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.utility.btree.BTreeIterator;
import org.lemurproject.galago.core.index.source.BTreeValueSource;
import org.lemurproject.galago.core.index.source.ExtentSource;
import org.lemurproject.galago.core.index.stats.NodeStatistics;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.utility.buffer.DataStream;
import org.lemurproject.galago.utility.buffer.VByteInput;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

/**
 *
 * @author jfoley
 */
final public class PositionIndexExtentSource extends BTreeValueSource implements ExtentSource {

    public long documentCount;
    public long totalPositionCount;
    public long maximumPositionCount;
    private VByteInput documents;
    private VByteInput counts;
    private VByteInput positions;
    private long documentIndex;
    private long currentDocument;
    private int currentCount;
    private boolean done;
    // final here to prevent reallocation of this during scoring
    final private ExtentArray extentArray;

    private DataStream documentsStream;
    private DataStream countsStream;
    private DataStream positionsStream;

    private static class SkipState {

        public VByteInput data;
        public VByteInput positions;
        public DataStream positionsStream;

        public long distance;
        public long resetDistance;
        public long total;
        public long read;
        public long nextDocument;
        public long nextPosition;
        public long documentsByteFloor;
        public long countsByteFloor;
        public long positionsByteFloor;
    }
    // to support skipping
    private SkipState skip;

    // Supports lazy-loading of extents
    private boolean extentsLoaded;
    private int inlineMinimum;
    private int extentsByteSize;

    public PositionIndexExtentSource(BTreeIterator iter) throws IOException {
        super(iter);
        extentArray = new ExtentArray();
        reset();
    }

    public PositionIndexExtentSource(BTreeIterator iter, String dispKey) throws IOException {
        super(iter, dispKey);
        extentArray = new ExtentArray();
        reset();
    }

    @Override
    public void reset() throws IOException {
        currentDocument = 0;
        currentCount = 0;
        extentArray.reset();
        extentsLoaded = true;
        done = false;
        initialize();
    }

    /**
     * Initialization method.
     *
     * Even though we check for skips multiple times, in terms of how the data
     * is loaded its easier to do the parts when appropriate
     */
    private void initialize() throws IOException {

        // 120 bytes should be enough for the heard items ((11 * 9) + (5 * 2)) //
        final DataStream valueStream = btreeIter.getSubValueStream(0, 120);
        final DataInput stream = new VByteInput(valueStream);

        // metadata contained in options bitmap:
        final int options = stream.readInt(); // 5 bytes
        final boolean hasInlining = (options & HAS_INLINING) > 0;
        final boolean hasSkips = (options & HAS_SKIPS) > 0;
        final boolean hasMaxTF = (options & HAS_MAXTF) > 0;

        inlineMinimum = (hasInlining) ? stream.readInt() : Integer.MAX_VALUE; // 5 bytes
        documentCount = stream.readLong();// 9 bytes
        totalPositionCount = stream.readLong();// 9 bytes
        maximumPositionCount = (hasMaxTF) ? stream.readLong() : Long.MAX_VALUE;// 9 bytes

        if (hasSkips) {
            skip = new SkipState();
            skip.distance = stream.readLong();// 9 bytes
            skip.resetDistance = stream.readLong();// 9 bytes
            skip.total = stream.readLong();// 9 bytes
        }
        // segment lengths
        final long documentByteLength = stream.readLong();// 9 bytes
        final long countsByteLength = stream.readLong();// 9 bytes
        final long positionsByteLength = stream.readLong();// 9 bytes
        final long skipsByteLength = hasSkips ? stream.readLong() : 0; // 9 bytes
        final long skipPositionsByteLength = hasSkips ? stream.readLong() : 0; // 9 bytes

        long documentStart = valueStream.getPosition();
        long countsStart = documentStart + documentByteLength;
        long positionsStart = countsStart + countsByteLength;
        long positionsEnd = positionsStart + positionsByteLength;
        documentsStream = btreeIter.getSubValueStream(documentStart, documentByteLength);
        countsStream = btreeIter.getSubValueStream(countsStart, countsByteLength);
        positionsStream = btreeIter.getSubValueStream(positionsStart, positionsByteLength);
        documents = new VByteInput(documentsStream);
        counts = new VByteInput(countsStream);
        positions = new VByteInput(positionsStream);
        if (hasSkips) {
            long skipsStart = positionsStart + positionsByteLength;
            long skipPositionsStart = skipsStart + skipsByteLength;
            long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;
            assert skipPositionsEnd == btreeIter.getValueLength();
            skip.data = new VByteInput(btreeIter.getSubValueStream(skipsStart, skipsByteLength));
            skip.positionsStream = btreeIter.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
            skip.positions = new VByteInput(skip.positionsStream);
            // load up
            skip.nextDocument = skip.data.readLong();
            skip.documentsByteFloor = 0;
            skip.countsByteFloor = 0;
            skip.positionsByteFloor = 0;
        } else {
            assert positionsEnd == btreeIter.getValueLength();
            skip = null;
        }
        documentIndex = 0;
        extentsLoaded = true; // Not really, but this keeps it from reading ahead too soon.
        loadNextPosting();
    }

    private void loadNextPosting() throws IOException {
        if (documentIndex >= documentCount) {
            done = true;
            extentArray.reset();
            extentsLoaded = true;
            currentCount = 0;
            currentDocument = Long.MAX_VALUE;
            return;
        }
        if (!extentsLoaded) {
            if (currentCount > inlineMinimum) {
                positions.skipBytes(extentsByteSize);
            } else {
                loadExtents();
            }
        }
        currentDocument += documents.readLong();
        currentCount = counts.readInt();
        // Prep the extents
        extentArray.reset();
        extentsLoaded = false;
        try {
            if (currentCount > inlineMinimum) {
                extentsByteSize = positions.readInt();
            } else {
                // Load them aggressively since we can't skip them
                loadExtents();
            }
        } catch (EOFException eof) {
            System.out.println(documentCount);
            System.out.println(documentIndex);
            throw new RuntimeException(eof);
        }
    }

    /**
     * Loads up a single set of positions for an intID. Basically it's the load
     * that needs to be done when moving forward one in the posting list.
     */
    private void loadExtents() throws IOException {
        if (!extentsLoaded) {
            extentArray.setDocument(currentDocument);
            int position = 0;
            for (int i = 0; i < currentCount; i++) {
                position += positions.readInt();
                extentArray.add(position);
            }
            extentsLoaded = true;
        }
    }

    @Override
    public boolean isDone() {
        return this.done;
    }

    @Override
    public boolean hasAllCandidates() {
        return false;
    }

    @Override
    public long totalEntries() {
        return documentCount;
    }

    @Override
    public long currentCandidate() {
        return currentDocument;
    }

    @Override
    public void movePast(long id) throws IOException {
        syncTo(id + 1);
    }

    @Override
    public void syncTo(long document) throws IOException {
        if (isDone()) {
            return;
        }

        if (skip != null) {
            synchronizeSkipPositions();
        }
        if (skip != null && document > skip.nextDocument) {
            extentsLoaded = true;
            extentsByteSize = 0;
            // if we're here, we're skipping
            while (skip.read < skip.total && document > skip.nextDocument) {
                skipOnce();
            }
            repositionMainStreams();
        }
        // Linear from here
        while (!isDone() && document > currentDocument) {
            documentIndex = Math.min(documentIndex + 1, documentCount);
            loadNextPosting();
        }
    }

    /**
     * This makes sure the skip list pointers are still ahead of the current
     * document. If we called "next" a lot, these may be out of sync.
     */
    private void synchronizeSkipPositions() throws IOException {
        while (!isDone() && skip.nextDocument <= currentDocument) {
            long cd = currentDocument;
            skipOnce();
            currentDocument = cd;
        }
    }

    /**
     * This only moves forward in tier 1, reads from tier 2 only when needed to
     * update floors
     */
    private void skipOnce() throws IOException {
        // may have already skipped passed the final document
        if (skip.nextDocument == Long.MAX_VALUE) {
            return;
        }

        assert skip.read < skip.total;
        long currentSkipPosition = skip.nextPosition + skip.data.readLong();
        if (skip.read % skip.resetDistance == 0) {
            // Position the skip positions stream
            skip.positionsStream.seek(currentSkipPosition);
            // now set the floor values
            skip.documentsByteFloor = skip.positions.readLong();
            skip.countsByteFloor = skip.positions.readLong();
            skip.positionsByteFloor = skip.positions.readLong();
        }
        currentDocument = skip.nextDocument;
        // May be at the end of the buffer
        if (skip.read + 1 == skip.total) {
            skip.nextDocument = Long.MAX_VALUE;
        } else {
            skip.nextDocument += skip.data.readLong();
        }
        skip.read++;
        skip.nextPosition = currentSkipPosition;
    }

    private void repositionMainStreams() throws IOException {
        // If we just reset the floors, don't read the 2nd tier again
        if ((skip.read - 1) % skip.resetDistance == 0) {
            documentsStream.seek(skip.documentsByteFloor);
            countsStream.seek(skip.countsByteFloor);
            positionsStream.seek(skip.positionsByteFloor);
        } else {
            skip.positionsStream.seek(skip.nextPosition);
            documentsStream.seek(skip.documentsByteFloor + skip.positions.readLong());
            countsStream.seek(skip.countsByteFloor + skip.positions.readLong());
            positionsStream.seek(skip.positionsByteFloor + skip.positions.readLong());
        }
        documentIndex = (int) (skip.distance * skip.read) - 1;
    }

    @Override
    public ExtentArray extents(long id) {
        if (!done && id == this.currentCandidate()) {
            try {
                loadExtents();
                return extentArray;
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
        return ExtentArray.EMPTY;
    }

    @Override
    public int count(long id) {
        if (!done && id == this.currentCandidate()) {
            return currentCount;
        }
        return 0;
    }

    @Override
    public NodeStatistics getStatistics() {
        NodeStatistics ns = new NodeStatistics();
        ns.node = this.key();
        ns.maximumCount = this.maximumPositionCount;
        ns.nodeFrequency = this.totalPositionCount;
        ns.nodeDocumentCount = this.documentCount;
        return ns;
    }
}
