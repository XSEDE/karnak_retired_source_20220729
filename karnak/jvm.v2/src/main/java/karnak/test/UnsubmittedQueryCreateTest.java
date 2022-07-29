/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.test;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import karnak.service.predict.UnsubmittedStartTimeQuery;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 */
public class UnsubmittedQueryCreateTest {


    public static void creteMultipleQueries() {

        UnsubmittedStartTimeQuery query = null;

        String[] queueAtSystems = {"default@gordon.sdsc.xsede.org", "development@lonestar4.tacc.xsede.org", "batch@darter.nics.xsede.org"};
        String coresStr = "1";
        String hours = "1";
        String minutes = "30";
        String confidence = "90";

        for (String queueAtSystem : queueAtSystems) {
            query = new UnsubmittedStartTimeQuery.Builder(queueAtSystem, coresStr).walltime(hours, minutes).interval(confidence).build();
            System.out.println(query);
        }
        
        System.out.println("Wrong input test");
        query = new UnsubmittedStartTimeQuery.Builder("gordon.sdsc.xsede.org", coresStr).walltime(hours, minutes).interval(confidence).build();
        System.out.println(query);
        
        
        System.out.println("Test done");

    }
    
     public static void main(String[] argv) {
         UnsubmittedQueryCreateTest.creteMultipleQueries();
     }

}
