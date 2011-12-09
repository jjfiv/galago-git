// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;

/**
 * A very crude extractor of dates from text.
 * 
 * This class searches for anything that looks like a year (1000-2999), then
 * searches around that year for a month name.  A year is sufficient to emit
 * a date.  Day of the month is currently not supported.
 * 
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.DateExtent")
public class DateExtractor extends StandardStep<Document, Document> {
    HashMap<String, Integer> months = new HashMap<String, Integer>();
    
    public DateExtractor() {
        addMonth("January", "Jan", Calendar.JANUARY);
        addMonth("February", "Feb", Calendar.FEBRUARY);
        addMonth("March", "Mar", Calendar.MARCH);
        addMonth("April", "Apr", Calendar.APRIL);
        addMonth("May", "May", Calendar.MAY);
        addMonth("June", "Jun", Calendar.JUNE);
        addMonth("July", "Jul", Calendar.JULY);
        addMonth("August", "Aug", Calendar.AUGUST);
        addMonth("September", "Sep", Calendar.SEPTEMBER);
        addMonth("October", "Oct", Calendar.OCTOBER);
        addMonth("November", "Nov", Calendar.NOVEMBER);
        addMonth("December", "Dec", Calendar.DECEMBER);        
    }
    
    public void addMonth(String longMonth, String shortMonth, int value) {
        months.put(longMonth, value);
        months.put(shortMonth, value);
    }
    
    public boolean isMonth(String month) {
        return months.containsKey(month);
    }
    
    public boolean isYear(String year) {
        if (year.length() != 4)
            return false;
        
        char first = year.charAt(0);
        if (first != '1' && first != '2')
            return false;
        
        return Character.isDigit(year.charAt(1)) &&
               Character.isDigit(year.charAt(2)) &&
               Character.isDigit(year.charAt(3));
    }
    
    public int getMonth(List<String> terms, int i) {
        if (i > 0 && isMonth(terms.get(i-1))) {
            return months.get(terms.get(i-1));
        }
        
        if (i > 0 && isMonth(terms.get(i-2))) {
            return months.get(terms.get(i-2));
        }

        if (i < terms.size()-1 && isMonth(terms.get(i+1))) {
            return months.get(terms.get(i+1));
        }
        
        return 0;
    }
    
    @Override
    public void process(Document object) throws IOException {
        for (int i = 0; i < object.terms.size(); ++i) {
            String term = object.terms.get(i);
            
            if (isYear(term)) {
                int year = Integer.parseInt(term);
                int month = getMonth(object.terms, i);
    
                Calendar calendar = new GregorianCalendar();
                calendar.set(year, month, 1);
                
                // TODO(trevor): add date extent
                // processor.process(new DateExtent(i, calendar.getTime()));
            }
        }
    }
}
