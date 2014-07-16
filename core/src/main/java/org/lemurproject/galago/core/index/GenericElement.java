// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.io.OutputStream;

import org.lemurproject.galago.utility.ByteUtil;

/**
 *
 * @author trevor
 */
public class GenericElement implements IndexElement {
    byte[] key;
    byte[] data;

    public GenericElement(byte[] key, byte[] data) {
        this.key = key;
        this.data = data;
    }
    
    public GenericElement(String key, byte[] data) {
        this.key = ByteUtil.fromString(key);
        this.data = data;
    }
    
    public GenericElement(String key, String value) {
        this.key = ByteUtil.fromString(key);
        this.data = ByteUtil.fromString(value);
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public long dataLength() {
        return data.length;
    }

    @Override
    public void write(OutputStream stream) throws IOException {
        stream.write(data);
    }
}