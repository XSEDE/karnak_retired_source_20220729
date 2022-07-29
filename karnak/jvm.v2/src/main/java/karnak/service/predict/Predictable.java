package karnak.service.predict;

import java.util.Date;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author wooj
 *
 * Common getters needed for submitted, unsubmitted, finishtime.
 * Also a class implements this interface must implement Comparable interface
 * as well so that Collections.sort() method can be used with 
 * Predictable<T> compliant classes.
 * 
 */
public interface Predictable<T> extends Comparable<T> {

    /* prediction objects may have different fields for estimated time 
       For example, submitted and unsubmitted have startTime field.
       FinshTimePrediction has finish time field.
    */
    public Date getPredictedTime();


}
