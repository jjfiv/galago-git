// BSD License (http://lemurproject.org/galago-license)
/*
 * WARC record parser
 * 
 * Originally written by:
 *   mhoy@cs.cmu.edu (Mark J. Hoy)
 * 
 * Modified for Galagosearch by:
 *   sjh
 */ 

package org.lemurproject.galago.core.parse;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

public class WARCParser implements DocumentStreamParser {
	private DataInputStream reader = null;
	private WARCRecord fileHeader = null;
	private long recordCount = 0;
	private long totalNumBytesRead = 0;

	public WARCParser( BufferedInputStream stream ) throws IOException {
		reader = new DataInputStream( stream );
		fileHeader = WARCRecord.readNextWarcRecord( reader );
	}


	public void close() throws IOException {
		reader.close();
		reader = null;
	}

	public Document nextDocument() throws IOException {

	  WARCRecord record = WARCRecord.readNextWarcRecord( reader );

		if (record == null){
			return null;
		}

		totalNumBytesRead += (long) record.getTotalRecordLength();

		Document doc = new Document( record.getDocid(), record.getContent() );
		doc.metadata = record.warcHeader.metadata;
		doc.metadata.put("url", record.getHeaderMetadataItem("WARC-Target-URI") );			

		return doc;
	}
}
