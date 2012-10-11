// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;
import org.lemurproject.galago.core.retrieval.query.StructuredLexer.Token.TokenType;

/**
 * A simple lexer for structured queries.
 *
 * Recognizes a few special tokens: [#, (, ), :, =, .] Also
 * recognizes two kinds of quoted strings; "a b c" and
 * @/a b c/.  The first kind is tokenized into individual tokens,
 * while the second form treats 'a b c' as a single token.
 * 
 * @author trevor
 */
public class StructuredLexer {

  public static class TokenStream {

    private ArrayList<Token> tokens;
    private Stack<Integer> marks;
    private int index;

    public TokenStream(ArrayList<Token> tokens) {
      this.tokens = tokens;
      this.index = 0;
      marks = new Stack<Integer>();
    }

    public Token current() {
      if (hasCurrent()) {
        return tokens.get(index);
      }
      return null;
    }

    public boolean currentEquals(String s) {
      if (hasCurrent()) {
        return current().text.equals(s);
      }
      return false;
    }

    public boolean hasCurrent() {
      return index < tokens.size();
    }

    public boolean next() {
      index++;
      return hasCurrent();
    }

    public void pushMark() {
      marks.push(index);
    }

    public void popMark() {
      marks.pop();
    }

    public void rewindToMark() {
      index = marks.peek();
      marks.pop();
    }

    void resetMark() {
      popMark();
      pushMark();
    }
  }

  public static class Token {
    public enum TokenType {
      TERM,QUOTE,SPECIALQUOTE
    }
    
    public Token(String text, int position, TokenType t) {
      this.text = text;
      this.position = position;
      this.type = t;
    }

    @Override
    public String toString() {
      return text + ":" + position;
    }
    public String text;
    public int position;
    public TokenType type;
  }

  private static void addQuotedTokens(String quotedString, ArrayList<Token> tokens, int offset) {
    tokens.add(new Token("\"", offset, TokenType.QUOTE));
    offset++;
//    int start = -1;
//    boolean wasSpace = true;
//    int j;
//    for (j = 0; j < quotedString.length(); j++) {
//      char c = quotedString.charAt(j);
//      boolean isSpace = Character.isSpaceChar(c);
//
//      if (isSpace) {
//        if (!wasSpace && start >= 0) {
//          tokens.add(new Token(quotedString.substring(start, j), start + offset, TokenType.QUOTE));
//        }
//        start = -1;
//      } else if (wasSpace) {
//        start = j;
//      }
//
//      wasSpace = isSpace;
//    }
//    
//    tokens.add(new Token(quotedString, offset, TokenType.QUOTE));
//
//    // emit final token
//    if (start > 0 && start != j) {
//      tokens.add(new Token(quotedString.substring(start), start + offset, TokenType.QUOTE));
//    }
    tokens.add(new Token(quotedString, offset, TokenType.QUOTE));
    offset += quotedString.length();
    tokens.add(new Token("\"", offset, TokenType.QUOTE));
  }

  public static ArrayList<Token> tokens(String query) throws IOException {
    ArrayList<Token> tokens = new ArrayList<Token>();
    HashSet<Character> tokenCharacters = new HashSet<Character>();
    tokenCharacters.add('#');
    tokenCharacters.add(':');
    tokenCharacters.add('.');
    tokenCharacters.add('=');
    tokenCharacters.add(')');
    tokenCharacters.add('(');
    tokenCharacters.add(',');
    int start = 0;

    for (int i = 0; i < query.length(); ++i) {
      char c = query.charAt(i);
      boolean special = tokenCharacters.contains(c) || c == '@' || c == '"';
      boolean isSpace = Character.isWhitespace(c);
      
      if (special || isSpace) {
        if (start != i) {
          tokens.add(new Token(query.substring(start, i), start, TokenType.TERM));
        }

        if (c == '@') {
          if (i + 1 < query.length()) {
            char escapeChar = query.charAt(i + 1);
            int endChar = query.indexOf(escapeChar, i + 2);

            if (endChar < 0) {
              throw new IOException("Lex failure: No end found to '@' escape sequence.");
            }

            tokens.add(new Token(query.substring(i + 2, endChar), i, TokenType.SPECIALQUOTE));
            i = endChar;
          } else {
            throw new IOException("Lex failure: '@' at end of input sequence.");
          }
        } else if (c == '"') {
          // find the end of this escape sequence and break on spaces
          int endChar = query.indexOf('"', i + 1);
          if (endChar < 0) {
            throw new IOException("Lex failure: No ending quote found.");
          }

          String quotedString = query.substring(i + 1, endChar);
          addQuotedTokens(quotedString, tokens, i);
          i = endChar;
        } else if (!isSpace) {
          tokens.add(new Token(Character.toString(c), i, TokenType.TERM));
        }

        start = i + 1;
      }
    }

    if (start != query.length()) {
      tokens.add(new Token(query.substring(start), start, TokenType.TERM));
    }
    return tokens;
  }
}
