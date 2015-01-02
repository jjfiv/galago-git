package org.lemurproject.galago.core.eval.metric;

import org.lemurproject.galago.core.eval.EvalDoc;
import org.lemurproject.galago.core.eval.QueryJudgments;
import org.lemurproject.galago.core.eval.QueryResults;

/**
 * Computes miss detection at a given false alarm rate.
 * 
 * false alarm rate = number of non-relevant retrieved / total non-relevant
 * 
 * @author jdalton
 *
 */
public class MissDetectionFalseAlarm extends QueryEvaluator {

    private final double falseAlarmThreshold;
    
    private final long collectionSize;
    
    public MissDetectionFalseAlarm(String metric, long totalCollectionSize, double fa) {
        super(metric);
       falseAlarmThreshold = fa;
       collectionSize = totalCollectionSize;
       if (totalCollectionSize <= 0 || fa <=0) {
           throw new IllegalArgumentException("Invalid MDFA arguments");
       }
       System.out.println("MDFA with collection size: " + collectionSize + " FA threshold:" + fa);
    }

    @Override
    public double evaluate(QueryResults resultList, QueryJudgments judgments) {
        
        if (resultList == null) {
            return 1.0d;
        }
        
        int relevant = judgments.getRelevantJudgmentCount();
//        System.out.println("relevant: " + relevant);
        long nonRelevant = collectionSize - relevant;
        
        int nonRelevantRetrieved = 0;
        int relevantRetrieved = 0;

        for (EvalDoc doc : resultList.getIterator()) {
            if (judgments.isRelevant(doc.getName())) {
                relevantRetrieved++;
            } else {
                nonRelevantRetrieved++;
            }
            
            // fraction of total non-relevant documents retrieved
            double falseAlarmRate = nonRelevantRetrieved / (double) nonRelevant;
            
            // percent of relevant documents not retrieved. 
            double missDetection = (relevant - relevantRetrieved) / (double) relevant;
            
            if (falseAlarmRate > falseAlarmThreshold) {
                return missDetection;
            }
            
          }
        
        // percent of relevant documents not retrieved. 
//        System.out.println("Miss detection rate not met at max retrieved doc. retrieved: " + resultList.size());
        double missDetection = (relevant - relevantRetrieved) / (double) relevant;
        return missDetection;
    }

}
