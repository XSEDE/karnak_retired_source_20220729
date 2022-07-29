/**
 * *************************************************************************
 */
/* Copyright 2015 University of Texas                                       */
/*                                                                          */
/* Licensed under the Apache License, Version 2.0 (the "License");          */
/* you may not use this file except in compliance with the License.         */
/* You may obtain a copy of the License at                                  */
/*                                                                          */
/*     http://www.apache.org/licenses/LICENSE-2.0                           */
/*                                                                          */
/* Unless required by applicable law or agreed to in writing, software      */
/* distributed under the License is distributed on an "AS IS" BASIS,        */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and      */
/* limitations under the License.                                           */
/**
 * *************************************************************************
 */
package karnak.service.predict;

import java.util.Date;

/* 
 Jungha Woo
 Legacy code access fields directly without setter or getter.
 These fields may be made to immutable ( private final) sometime later
 as the builder pattern will be deployed.
 */
public class UnsubmittedStartTimeQuery {

    public String system = null;
    public String queue = null;
    public int processors = 0;
    public int requestedWallTime = 0; // seconds
    public int intervalPercent = 90;

    /* QueryTime is exclusively used when the backtest is done
      During backtest, specific query time must be given to the predictor
      so that valid decision tree at that point can be constructed
      */
    public Date querytime;

    public UnsubmittedStartTimeQuery() {
        querytime = new Date();
    }


    public UnsubmittedStartTimeQuery(Date given) {
        querytime = given;
    }


    public int getRequestedWallTime() {
        return requestedWallTime;
    }

    @Override
    public String toString() {

        StringBuilder strbuilder = new StringBuilder();

        strbuilder.append("UnsubmittedStartTimeQuery {");
        strbuilder.append("\n");
        strbuilder.append("system=" + system);
        strbuilder.append("\n");
        strbuilder.append("queue=" + queue);
        strbuilder.append("\n");
        strbuilder.append("processors=" + processors);
        strbuilder.append("\n");
        strbuilder.append("requestedWallTime=" + requestedWallTime);
        strbuilder.append("\n");
        strbuilder.append("intervalPercent=" + intervalPercent);
        strbuilder.append("\n");
        strbuilder.append("}");
        strbuilder.append("\n");
        return strbuilder.toString();
    }

    /* This constructor should invoke the other constructor in the first line. 
     Does that mean we cannot do conversion before calling constructor ?
    
     */
    public static class Builder {

        //required parameters
        private final String system;
        private final String queue;
        private final int processors;

        //optional parameters - initialized to default values
        private int requestedWallTime = 0; // seconds
        private int intervalPercent = 90;

        //Validation is checked on validator object 
        //so do not worry about null parameters 
        public Builder(String queue, String system, int processors) {
            this.queue = queue;
            this.system = system;
            this.processors = processors;
        }

        /* if queueAtSystem is not formatted "{queue@system}",
         input string will not update queue and system variables.
         */
        public Builder(String queueAtSystem, String strNumProcessors) {

            String queue = null;
            String system = null;

            if ((queueAtSystem != null) && !queueAtSystem.isEmpty()) {
                String[] tokens = queueAtSystem.split("@");
                if (tokens.length == 2) {
                    queue = tokens[0];
                    system = tokens[1];
                }
            }

            int numProcessor = 0;
            try {
                numProcessor = Integer.parseInt(strNumProcessors);
            } catch (Exception ex) {
                ex.printStackTrace();
                //object that has zero processor requirement will fail to pass the validation 
            }

            this.queue = queue;
            this.system = system;
            this.processors = numProcessor;
        }

        public Builder walltime(int val) {
            this.requestedWallTime = val;
            return this;
        }

        public Builder walltime(String hours, String mins) {
            try {
                this.requestedWallTime = 60 * (Integer.parseInt(mins) + 60 * Integer.parseInt(hours));
            } catch (Exception ex) {
                ex.printStackTrace();
                //object that has zero request time  will fail to
                //pass the validation 
                this.requestedWallTime = 0;
            }
            return this;
        }

        public Builder interval(int val) {
            this.intervalPercent = val;
            return this;
        }

        public Builder interval(String val) {

            try {
                this.intervalPercent = Integer.parseInt(val);
            } catch (Exception ex) {
                ex.printStackTrace();
                //object that has zero request time  will fail to pass the validation 
                this.intervalPercent = 0;
            }
            return this;
        }

        public UnsubmittedStartTimeQuery build() {
            return new UnsubmittedStartTimeQuery(this);
        }

    }

    private UnsubmittedStartTimeQuery(Builder builder) {
        this.system = builder.system;
        this.queue = builder.queue;
        this.processors = builder.processors;
        this.requestedWallTime = builder.requestedWallTime;
        this.intervalPercent = builder.intervalPercent;
    }

}
