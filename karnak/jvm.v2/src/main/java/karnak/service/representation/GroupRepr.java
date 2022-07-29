package karnak.service.representation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import karnak.service.predict.*;
import karnak.service.util.SystemInfoQuery;
import org.apache.log4j.Logger;

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
 * This class classifies the selected systems into given number of groups.
 * Systems in the same group have similar wait times so that using any of them
 * will lead to similar wait time.
 *
 * Default constructor cannot be invoked externally. The number of group
 * parameter must be equal or greater than two because otherwise it degenerates
 * to RawRepr. numGroup with two is possible but the default value is set to
 * three.
 *
 * Composition pattern used. Internal implmentation object is set to
 * LabeledGroupRepr internally.
 *
 */
public class GroupRepr implements RepresentationStrategy {

    private static Logger logger = Logger.getLogger(GroupRepr.class.getName());
    private RepresentationStrategy innerImpl = null;
    private final Estimation type;

    //This will be moved to another class, LabeledGroupRepr
    //private List<String> labels;
    private GroupRepr(Estimation type) {
        this.type = type;
    }

    public GroupRepr(Estimation type, int numGroup) {

        int targetSize = 0;
        this.type = type;

        if (numGroup <= 1) {
            targetSize = LabeledGroupRepr.DEFAULT_NUM_GROUPS;
        } else {
            targetSize = numGroup;
        }

        //create group indices starting from 1 to numGroup
        String[] numArry = new String[targetSize];
        for (int i = 0; i < targetSize; i++) {
            numArry[i] = String.valueOf(i + 1);
        }

        List<String> labels = new ArrayList<String>(Arrays.asList(numArry));
        logger.debug(labels);
        logger.debug("InnerImpl labeledGroupRepr called");
        innerImpl = new LabeledGroupRepr(type, labels);

    }

    public String toHTML(List<Predictable> preds) {

        return innerImpl.toHTML(preds);

    }

    public String toXML(List<Predictable> preds) {
        return innerImpl.toXML(preds);
    }

    public String toJSON(List<Predictable> preds) {
        return innerImpl.toJSON(preds);

    }

    public String toTEXT(List<Predictable> preds) {
        return innerImpl.toTEXT(preds);

    }
}
