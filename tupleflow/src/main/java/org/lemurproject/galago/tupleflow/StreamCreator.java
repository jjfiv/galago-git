// BSD License (http://lemurproject.org/galago-license)


package org.lemurproject.galago.tupleflow;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 *
 * @author trevor
 */
public class StreamCreator {
    private static String stripPrefix(String filename) {
        String[] fields = filename.split(":");
        if(fields.length > 1)
            return filename.substring(fields[0].length() + 1);
        
        return filename;
    }
    
    public static FileInputStream realInputStream(String filename) throws IOException {
        FileInputStream stream = new FileInputStream(filename);
        return stream;
    }
    
    public static RandomAccessFile inputStream(String filename) throws IOException {
        RandomAccessFile file = new RandomAccessFile(filename, "r");
        return file;
    }
    
    public static RandomAccessFile outputStream(String filename) throws IOException {
        RandomAccessFile file = new RandomAccessFile(filename, "rw");
        return file;
    }

    public static DataOutputStream realOutputStream(String filename) throws IOException {
        DataOutputStream file = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        return file;
    }
}
