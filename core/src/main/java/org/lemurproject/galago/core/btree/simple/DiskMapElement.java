// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.btree.simple;

import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.utility.btree.IndexElement;

/**
 *
 * @author jfoley
 */
class DiskMapElement implements IndexElement {
	private final byte[] key;
	private final byte[] value;

	public DiskMapElement(byte[] key, byte[] value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public byte[] key() {
		return this.key;
	}

	@Override
	public long dataLength() {
		return this.value.length;
	}

	@Override
	public void write(OutputStream stream) throws IOException {
		stream.write(this.value);
	}
	
}
