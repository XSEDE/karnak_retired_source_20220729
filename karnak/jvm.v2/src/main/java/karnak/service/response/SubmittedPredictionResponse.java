
package karnak.service.response;

import karnak.KarnakException;
import karnak.service.predict.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 */
public class SubmittedPredictionResponse extends AbstractResponse{
    
    
    private SubmittedPredictionResponse(){
        preds = new ArrayList<Predictable>();
    }
    
    
    public SubmittedPredictionResponse(List<SubmittedStartTimeQuery> queries){

        preds = new ArrayList<Predictable>();
        
        for( SubmittedStartTimeQuery query: queries ){
            
            try{
                SubmittedStartTimePrediction pred = SubmittedPredictor.getPrediction(query, WeightingEnum.Single);
                preds.add(pred);
                
            }catch(KarnakException kex){
                kex.printStackTrace();
                continue;
            }
        }
        
       
       
    }
    
    
    
    
}

