package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.ZipUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import static org.junit.Assert.*;

public class DocumentStreamParserTest {

    @Test
    public void sanity() {
        assertTrue(DocumentStreamParser.hasParserForExtension("trecweb"));
        assertTrue(DocumentStreamParser.hasParserForExtension("trectext"));
    }

    @Test
    public void testZipFile() throws IOException {
        File tmp = null;

        try {
            tmp = File.createTempFile("streamParserZipTest", ".zip");

            String fooContents = "foo is the best";
            String fooPath = "data/foo.txt";

            String barContents = "bar is the best";
            String barPath = "data/subdir/ignore/bar.txt";

            String trecWebContents = "<DOC>\n"
                    + "<DOCNO>CACM-0001</DOCNO>\n"
                    + "<DOCHDR>\n"
                    + "http://www.yahoo.com:80 some extra text here\n"
                    + "even more text in this part\n"
                    + "</DOCHDR>\n"
                    + "This is some text in a document.\n"
                    + "</DOC>\n";
            String trecWebPath = "data/blah/easy.trecweb";
            String guessTrecWebPath = "data/blah/guess_trecweb";

            // write zip file:
            ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(tmp.getAbsolutePath()));
            ZipUtil.write(zos, fooPath, ByteUtil.fromString(fooContents));
            ZipUtil.write(zos, barPath, ByteUtil.fromString(barContents));
            ZipUtil.write(zos, trecWebPath, ByteUtil.fromString(trecWebContents));
            ZipUtil.write(zos, guessTrecWebPath, ByteUtil.fromString(trecWebContents));
            zos.close();

            ZipFile zipFile = ZipUtil.open(tmp);
            // read zip file:
            List<String> entries = ZipUtil.listZipFile(zipFile);
            assertEquals(4, entries.size());
            zipFile.close();

            List<DocumentSplit> splits = DocumentSource.processZipFile(tmp, Parameters.create());
            assertEquals(4, splits.size());
            assertEquals("txt", splits.get(0).fileType);
            assertEquals("txt", splits.get(1).fileType);
            assertEquals("trecweb", splits.get(2).fileType);
            assertEquals("trecweb", splits.get(3).fileType);

            DocumentStreamParser parser = DocumentStreamParser.create(splits.get(2), Parameters.create());
            Document d = parser.nextDocument();
            assertNotNull(d);
            assertNull(parser.nextDocument());
            parser.close();
            assertEquals("http://www.yahoo.com", d.metadata.get("url"));

            assertEquals(tmp.getAbsolutePath() + "!" + fooPath, DocumentStreamParser.getFullPath(splits.get(0)));

        } finally {
            if (tmp != null) {
                tmp.delete();
            }
        }
    }

}
