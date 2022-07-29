package karnak.service.representation;

import java.util.List;
import karnak.service.representation.GroupRepr;
import karnak.service.representation.LabeledGroupRepr;
import karnak.service.representation.RawRepr;
import karnak.service.representation.Estimation;
import karnak.service.representation.RepresentationStrategy;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author wooj
 *
 * separate PredictionFactory from StrategyFactory
 *
 */
public class StrategyFactory {

    private StrategyFactory() {
        //prevent instantiation
    }

    /* create a representation based on user inputs.
     if RawRepr is requested, numGroup and labels are ignored.
    
    
     */
    public static RepresentationStrategy newInstance(Estimation type, String predictionFormat, int numGroup, List<String> labels) {

        if (predictionFormat.equalsIgnoreCase("Group")) {
            return new GroupRepr(type, numGroup);

        } else if (predictionFormat.equalsIgnoreCase("LabeledGroup")) {
            //if invalid labels is provided, initialze it with default parameters
            //that is, three groups labeled "Fast", "Medium",and  "Slow"
            return new LabeledGroupRepr(type, labels);

        } else if (predictionFormat.equalsIgnoreCase("Raw")) {
            return new RawRepr(type);

        }
        
        //default method is to return raw estimation results
        return new RawRepr(type);

    }

}
