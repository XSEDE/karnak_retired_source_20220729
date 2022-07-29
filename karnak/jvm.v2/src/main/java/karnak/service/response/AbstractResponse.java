package karnak.service.response;

import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import karnak.service.representation.RepresentationStrategy;
import karnak.service.predict.Predictable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author wooj
 *
 *
 * Error message now moved to individual prediction level.
 *
 */
public abstract class AbstractResponse {

    /* a list of predictions estimated from TreePredictor.
     preds must be initialized in the derived class' constructor.
     */
    List<Predictable> preds = null;

    /* Strategy Pattern is deployed for preparing
     the responses. 
     GroupRepr and RawRepr are supported now.
     RawRepr returns predicted wait time and confidence interval in secs.
     GroupRepr does not return estimated wait time. Instead it returns the 
     ordered list of groups whose constituents have similar wait times. 
     */
    RepresentationStrategy strategy = null;

    /* change representation strategy
     RepresentationStrategy only requires a list of predictions.
     If other parameters are needed, they should go in constructors, but not
     in toHTML(), toXML(), and toJSON();
     */
    public void setReprestation(RepresentationStrategy stgy) {
        strategy = stgy;
    }

    public String getHTML() {
        return strategy.toHTML(preds);

    }

    public String getXML() {
        return strategy.toXML(preds);

    }

    public String getJSON() {

        return strategy.toJSON(preds);

    }

    public String getTEXT() {

        return strategy.toTEXT(preds);

    }
}
