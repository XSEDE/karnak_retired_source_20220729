package karnak.service.response;

import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 *
 */
public class ResponseManager {

    private static Logger logger = Logger.getLogger(ResponseManager.class.getName());

    private final Map<String, Object> userResponses = Collections.synchronizedMap(new HashMap<String, Object>());

    private static final ResponseManager INSTANCE = new ResponseManager();

    private ResponseManager() {
    }

    private static class MyWrapper {

        static ResponseManager INSTANCE = new ResponseManager();
    }

    public static ResponseManager getInstance() {
        return MyWrapper.INSTANCE;
    }

    /* 
     Returns the unique id allocated to given response,
     and save it to the hashmap for the reference sometime later.
     */
    public <T extends AbstractResponse> String addResponse(T resp) {

        logger.debug("addResponse enter");
        String id = null;

        synchronized (userResponses) {
            id = String.valueOf(new GregorianCalendar(TimeZone.getTimeZone("GMT")).getTimeInMillis());
            userResponses.put(id, resp);
            logger.debug("add id:" + id + ", current size:" + userResponses.size());
            logger.debug("addResponse leave");
        }
        return id;

    }

    /* 
     return a response object if found, and delete it from the map.
     if no response is found, return null.
     */
    public <T extends AbstractResponse> T getResponse(String id) {
        logger.debug("getresponse enter id:" + id);

        synchronized (userResponses) {
            T found = (T) userResponses.get(id);

            if (found == null) {
                logger.error("cannot find the id:" + id);
                for (String key : userResponses.keySet()) {
                    logger.debug("id:" + key + " exists");
                }

                return null;
            }

            userResponses.remove(id);
            logger.debug("removed id:" + id + ", current size:" + userResponses.size());
            logger.debug("getresponse leave id:" + id);
            return found;
        }

    }

}
