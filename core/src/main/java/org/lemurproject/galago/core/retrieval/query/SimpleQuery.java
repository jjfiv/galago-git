// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.query;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>SimpleQuery parses the kind of queries you might expect for a end-user search engine.
 * The format is also meant to be similar to Lucene's query format.</p>
 * 
 * Queries can be single terms:<br/>
 *    <tt>white house</tt><br/>
 * or phrases:<br/>
 *    <tt>"white house"</tt><br/>
 * and have fields:
 *    <tt>title:"white house"</tt><br/>
 * or weights:<br/>
 *    <tt>white^4 house^2</tt><br/>
 * 
 * <p>A query can be parsed into a list of QueryTerms or translated into a tree of Nodes
 * which can be used with the StructuredRetrieval code.</p>
 * 
 * @author trevor
 */
public class SimpleQuery {
    public static class QueryTerm {
        public QueryTerm(String text) {
            this.weight = 1.0;
            this.field = null;
            this.text = text;
        }

        public QueryTerm(String text, String field, double weight) {
            this.text = text;
            this.field = field;
            this.weight = weight;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof QueryTerm))
                return false;

            QueryTerm other = (QueryTerm) o;
            return text.equals(other.text) &&
                    ((field != null) ? field.equals(other.field) : other.field == null) &&
                    weight == other.weight;
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 41 * hash + (this.text != null ? this.text.hashCode() : 0);
            hash = 41 * hash + (this.field != null ? this.field.hashCode() : 0);
            hash = 41 * hash + (int) (Double.doubleToLongBits(this.weight) ^ (Double.
                    doubleToLongBits(this.weight) >>> 32));
            return hash;
        }

        @Override
        public String toString() {
            String term = text;

            // if this is a multi-word query, enclose it in quotes
            if (term.contains(" ")) {
                term = "\"" + term + "\"";            // use the minimum amount of syntax necessary to
            // express the query.  If everything is specified, 
            // the format is field:term^weight.
            }
            if (field != null && weight != 1.0) {
                return String.format("%s:%s^%f", field, term, weight);
            }
            if (field != null) {
                return String.format("%s:%s", field, term);
            }
            if (weight != 1.0) {
                return String.format("%s^%f", term, weight);
            }
            return text;
        }
        public String text;
        public String field;
        public double weight;
    }

    /** 
     * The format of the query term is <tt>field:term^weight</tt>.
     * Both the field and the weight are optional, and the term may
     * be enclosed in quotes.
     *
     * @return A QueryTerm object describing the query term.
     */
    public static QueryTerm parseQueryTerm(String term) {
        double weight = 1.0;
        String field = null;

        int colon = term.indexOf(':');
        if (colon >= 0) {
            field = term.substring(0, colon);
            term = term.substring(colon + 1);
        }

        int caret = term.indexOf('^');
        if (caret >= 0) {
            weight = Double.parseDouble(term.substring(caret + 1));
            term = term.substring(0, caret);
        }

        if (term.startsWith("\"")) {
            term = term.substring(1);
        }
        if (term.endsWith("\"")) {
            term = term.substring(0, term.length() - 1);
        }
        return new QueryTerm(term, field, weight);
    }

    public static List<String> textQueryTerms(String query) {
        boolean inQuote = false;
        int firstNonSpace = query.length() + 1;
        int i = 0;
        ArrayList<String> results = new ArrayList<String>();

        // each loop parses a single term
        while (i < query.length()) {
            // parsing goes in two phases; first we're trying to bypass inital
            // spaces before a query term.  after that point, we parse until the
            // next space that's not in quotes.
            for (; i < query.length(); i++) {
                char c = query.charAt(i);

                if (Character.isSpaceChar(c)) {
                    if (!inQuote) {
                        if (firstNonSpace < i) {
                            String term = query.substring(firstNonSpace, i);
                            results.add(term);
                        }
                        firstNonSpace = query.length() + 1;
                    }
                } else if (c == '"') {
                    firstNonSpace = Math.min(firstNonSpace, i);
                    inQuote = !inQuote;
                } else {
                    firstNonSpace = Math.min(firstNonSpace, i);
                }
            }
        }

        if (firstNonSpace < query.length()) {
            results.add(query.substring(firstNonSpace, query.length()));
        }

        return results;
    }

    public static List<QueryTerm> parse(String query) {
        ArrayList<QueryTerm> results = new ArrayList<QueryTerm>();
        int position = 0;
        String term = null;

        List<String> textTerms = textQueryTerms(query);
        ArrayList<QueryTerm> parsedTerms = new ArrayList<QueryTerm>();

        for (String textTerm : textTerms) {
            parsedTerms.add(parseQueryTerm(textTerm));
        }

        return parsedTerms;
    }

    public static Node parseTree(String query) {
        List<QueryTerm> terms = parse(query);
        ArrayList<Node> nodes = new ArrayList<Node>();

        for (QueryTerm term : terms) {
            Node termNode = new Node("text", term.text);
            // if this is a phrase, put the terms in a ordered window
            if (term.text.contains(" ")) {
                String[] phraseTerms = term.text.split(" ");
                ArrayList<Node> children = new ArrayList<Node>();
                for (String phraseTerm : phraseTerms) {
                    children.add(new Node("text", phraseTerm));
                }
                NodeParameters np = new NodeParameters();
                np.set("default", 1);
                termNode = new Node("ordered", np, children, 0);
            }
            // if this is in a field, add the field restriction
            if (term.field != null) {
                ArrayList<Node> children = new ArrayList<Node>();
                children.add(termNode);
                children.add(new Node("field", term.field));
                termNode = new Node("inside", children);
            }
            // if this is weighted, scale it
            if (term.weight != 1.0) {
                ArrayList<Node> children = new ArrayList<Node>();
                children.add(termNode);
                NodeParameters np = new NodeParameters();
                np.set("default", term.weight);
                termNode = new Node("scale", np, children, 0);
            }
            nodes.add(termNode);
        }

        if (nodes.size() < 1)
            return null;
        
        if (nodes.size() == 1)
            return nodes.get(0);

        return new Node("combine", nodes);
    }
}
