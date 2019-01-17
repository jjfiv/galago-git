// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.utility.Parameters;

import java.io.Serializable;
import java.util.*;

/**
 * This is Galago's document class. It represents a sequence of tokens that have begin and end offsets back into
 * original text. It also supports SGML/HTML/XML-like tags which surround tokens and can therefore be mapped back into
 * the original text.
 * <p>
 * Traditionally the document has an internal numeric identifier, and an external human-readable name. The identifier is
 * assigned automatically during Galago's build process.
 * <p>
 * The document also has a Map&lt;String,String&gt; of metadata.
 */
public class Document implements Serializable {

    private static final long serialVersionUID = -5471082990007800961L;
    /**
     * document id - this value is serialized
     */
    public long identifier = -1;
    /**
     * document data - these values are serialized
     */
    public String name;
    public Map<String, String> metadata;
    public String text;
    public List<String> terms;
    public List<Integer> termCharBegin = new ArrayList<>();
    public List<Integer> termCharEnd = new ArrayList<>();
    public List<Tag> tags;
    // other data - used to generate an identifier; these values can not be serialized!
    public int fileId = -1;
    public int totalFileCount = -1;
    public String filePath = "";
    public long fileLocation = -1;

    public Document() {
        metadata = new HashMap<>();
    }

    public Document(String externalIdentifier, String text) {
        this();
        this.name = externalIdentifier;
        this.text = text;
    }

    public Document(Document d) {
        this.identifier = d.identifier;
        this.name = d.name;
        this.metadata = new HashMap<>(d.metadata);
        this.text = d.text;
        this.terms = new ArrayList<>(d.terms);
        this.termCharBegin = new ArrayList<>(d.termCharBegin);
        this.termCharEnd = new ArrayList<>(d.termCharEnd);
        this.tags = new ArrayList<>(d.tags);
        this.fileId = d.fileId;
        this.totalFileCount = d.totalFileCount;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Identifier: ").append(name).append("\n");
        if (metadata != null) {
            sb.append("Metadata: \n");
            for (Map.Entry<String, String> entry : metadata.entrySet()) {
                sb.append("<");
                sb.append(entry.getKey()).append(",").append(entry.getValue());
                sb.append("> ");
            }
        }

        if (tags != null) {
            int count = 0;
            sb.append("\nTags: \n");
            for (Tag t : tags) {
                sb.append(count).append(" : ");
                sb.append(t.toString()).append("\n");
                count += 1;
            }
        }

        if (terms != null) {
            int count = 0;
            sb.append("\nTerm vector: \n");
            for (String s : terms) {
                sb.append(count).append(" : ");
                sb.append(s).append("\n");
                count += 1;
            }
        }

        if (text != null) {
            sb.append("\nText :").append(text);
        }
        sb.append("\n");

        return sb.toString();
    }

    public TObjectIntHashMap<String> getBagOfWords() {
        TObjectIntHashMap<String> termCounts = new TObjectIntHashMap<>();
        for (String term : terms) {
            termCounts.adjustOrPutValue(term, 1, 1);
        }
        return termCounts;
    }

    public Map<String, List<Integer>> getTermPositions(Stemmer stemmer) {

        // note we're using a TreeMap so terms are ordered
        Map<String, List<Integer>> termPos = new TreeMap<>();
        int pos = 0;
        for (String tmp_term : terms) {
            String term = tmp_term;
            if (stemmer != null){
                term = stemmer.stem(term);
            }
            if (!termPos.containsKey(term)) {
                termPos.put(term, new ArrayList<Integer>());
            }
            termPos.get(term).add(pos);
            pos++;
        }
        return termPos;
    }

    /**
     * This class allows the selection of parts of the document to serialize or deserialize.
     */
    public static class DocumentComponents implements Serializable {
        private static final long serialVersionUID = -5134430303276805133L;
        public static DocumentComponents All = new DocumentComponents(true, true, true);
        public static DocumentComponents JustMetadata = new DocumentComponents(false, true, false);
        public static DocumentComponents JustText = new DocumentComponents(true, false, false);
        public static DocumentComponents JustTerms = new DocumentComponents(false, false, true);

        public boolean text = true;
        public boolean metadata = true;
        public boolean tokenize = false;
        // these variables can be used to restrict the text to just a short section at the start of the document
        // useful for massive files
        // start and end are byte offsets
        // -1 indicates no restriction
        public int subTextStart = -1;
        public int subTextLen = -1;

        // defaults
        public DocumentComponents() {
        }

        public DocumentComponents(boolean text, boolean metadata, boolean tokenize) {
            this.text = text;
            this.metadata = metadata;
            this.tokenize = tokenize;
        }

        public DocumentComponents(Parameters p) {
            this.text = p.get("text", text);
            this.metadata = p.get("metadata", metadata);
            this.tokenize = p.get("tokenize", tokenize);
        }

        public Parameters toJSON() {
            Parameters p = Parameters.create();
            p.put("text", text);
            p.put("metadata", metadata);
            p.put("tokenize", tokenize);
            return p;
        }

        @Override
        public String toString() {
            return toJSON().toString();
        }
    }
}

