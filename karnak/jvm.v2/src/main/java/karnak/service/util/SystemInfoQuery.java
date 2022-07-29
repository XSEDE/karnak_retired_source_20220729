/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.util;

import java.lang.Math;
import karnak.service.GlueDatabase;
import karnak.service.GlueQueue;
import karnak.service.GlueSystem;
import karnak.service.predict.*;

/**
 *
 * @author wooj
 *
 * Static Utility class that cannot be instantiated.
 *
 * getDefaultQueue, getMaxProcessors, getMaxWallTime are taken from WebService
 * class.
 *
 */
public class SystemInfoQuery {

    private SystemInfoQuery() {
    }

    ; 
    
     
     public static String getDefaultQueue(String systemName) {
        for (GlueQueue queue : GlueDatabase.getSystem(systemName).queues.values()) {
            if (queue.isDefault) {
                return queue.name;
            }
        }
        return null;
    }

    /*
     Example queue_info 
     
     MariaDB [glue2]>
     MariaDB [glue2]> select * from queue_info where system="gordon.sdsc.xsede.org";
     +-----------------------+---------+---------------+-------------+-----------+---------+
     | system                | queue   | maxProcessors | maxWallTime | isDefault | isValid |
     +-----------------------+---------+---------------+-------------+-----------+---------+
     | gordon.sdsc.xsede.org | debug   |            -1 |          -1 |           |        |
     | gordon.sdsc.xsede.org | default |            -1 |          -1 |           |        |
     | gordon.sdsc.xsede.org | global  |            -1 |      604800 |           |        |
     | gordon.sdsc.xsede.org | ion     |            -1 |          -1 |           |        |
     | gordon.sdsc.xsede.org | normal  |            -1 |     1209600 |           |        |
     | gordon.sdsc.xsede.org | power   |            -1 |          -1 |           |        |
     | gordon.sdsc.xsede.org | shared  |            -1 |      604800 |           |        |
     | gordon.sdsc.xsede.org | vsmp    |            -1 |     1209600 |           |        |
     +-----------------------+---------+---------------+-------------+-----------+---------+
     8 rows in set (0.02 sec)

     
     MariaDB [glue2]> select * from system_info;
     +--------------------------------+---------------------+------------+--------------+
     | system                         | time                | processors | procsPerNode |
     +--------------------------------+---------------------+------------+--------------+
     | blacklight.psc.xsede.org       | 2015-09-08 17:28:02 |       8192 |         2730 |
     | comet.sdsc.xsede.org           | 2016-02-23 21:34:02 |      46048 |           24 |
     | darter.nics.xsede.org          | 2016-02-23 21:34:05 |      12224 |           16 |
     | gordon.sdsc.xsede.org          | 2016-02-23 21:34:02 |      14752 |           16 |
     | lonestar4.tacc.xsede.org       | 2016-02-23 21:34:03 |      23784 |           12 |
     | maverick.tacc.xsede.org        | 2016-02-23 21:34:01 |       2740 |           20 |
     | staff.stampede.tacc.utexas.edu | 2015-06-04 16:24:22 |     103056 |           16 |
     | stampede.tacc.xsede.org        | 2016-02-23 21:30:03 |     103056 |           16 |
     | trestles.sdsc.xsede.org        | 2015-05-01 16:48:01 |       9792 |           32 |
     | wrangler.tacc.xsede.org        | 2015-05-27 21:27:17 |       4608 |           48 |
     +--------------------------------+---------------------+------------+--------------+
     10 rows in set (0.00 sec)
     
     MariaDB [glue2]>
     */
    /* 
     Returns the number of processors available for the system and queue.
     If invalid queue name or system name is supplied, 0 will be returned.
     */
    public static int getMaxProcessors(String systemName, String queueName) {

        /*
         //no such system exist, then return 0 to signal errors
         //if max processor value of zero returned, any valid requests 
         //will be rejected so that the clients will notice something went wrong
         */
        GlueSystem system = GlueDatabase.getSystems().get(systemName);

        if (system == null) {
            return 0;
        }

        int processors = system.processors;
        int procsPerNode = system.procsPerNode;
        
        GlueQueue queue = system.queues.get(queueName);
        if (queue == null) {
            return 0;
        }

        // maxProcessors of -1 seems to be no restriction on the number of 
        // processors
        int maxProcessorsPerNode = queue.maxProcessors;
        int maxProcessorsAtQueue = maxProcessorsPerNode * procsPerNode;

        if ((maxProcessorsAtQueue <= 0) || (maxProcessorsAtQueue > processors)) {
            maxProcessorsAtQueue = processors;
        }
        
        return maxProcessorsAtQueue;

    }

    /* 
     Returns the number of max wall time available for the system and queue.
     If invalid queue name or system name is supplied, 0 will be returned.
     */
    public static int getMaxWallTime(String systemName, String queueName) {

        GlueSystem system = GlueDatabase.getSystems().get(systemName);

        if (system == null) {
            return 0;
        }

        GlueQueue queue = system.queues.get(queueName);
        if (queue == null) {
            return 0;
        }

        // maxWallTime of -1 seems to stand for no limitation for the wall time
        return (queue.maxWallTime <= 0 ? Integer.MAX_VALUE : queue.maxWallTime);

    }

    public static boolean isValidSystem(String systemName) {
        /*
         //no such system exist, then return 0 to signal errors
         //if max processor value of zero returned, any valid requests 
         //will be rejected so that the clients will notice something went wrong
         */
        GlueSystem system = GlueDatabase.getSystem(systemName);
        return (system == null) ? false : true;
    }
    
    public static boolean isValidQueue(String systemName, String queueName) {

        GlueSystem system = GlueDatabase.getSystem(systemName);
        
        if( system == null )
            return false;
        
        return system.queues.containsKey(queueName);
        
       
    }
  
  
}
