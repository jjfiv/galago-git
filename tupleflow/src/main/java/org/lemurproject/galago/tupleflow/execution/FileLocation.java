// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import org.xml.sax.Locator;

/**
 * TODO: If the execution plan isn't from an XML file, this is useless. What to do?
 * @author trevor
 */
public class FileLocation implements Serializable, Comparable<FileLocation> {
    public FileLocation(String fileName, int lineNumber, int columnNumber) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    public FileLocation(String filename, Locator locator) {
        this(filename, locator.getLineNumber(), locator.getColumnNumber());
    }

    public int compareTo(FileLocation location) {
        int result = fileName.compareTo(location.fileName);
        if (result == 0) {
            result = lineNumber - location.lineNumber;
        }
        if (result == 0) {
            result = columnNumber - location.columnNumber;
        }
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s [Line %d: Column %d]", fileName, lineNumber, columnNumber);
    }

    public String fileName;
    public int lineNumber;
    public int columnNumber;
}
