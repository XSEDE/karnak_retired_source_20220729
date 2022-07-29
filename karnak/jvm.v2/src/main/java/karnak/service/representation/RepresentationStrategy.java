package karnak.service.representation;

import java.util.List;
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
 */
public interface RepresentationStrategy {
    
    public String toHTML(List<Predictable> preds);
    
    public String toXML(List<Predictable> preds);

    public String toJSON(List<Predictable> preds);

    public String toTEXT(List<Predictable> preds);
    
}
