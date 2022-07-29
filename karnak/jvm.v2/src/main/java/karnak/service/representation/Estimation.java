/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.representation;

import karnak.service.WebService;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 *
 * Sets the way how the estimated time be presented to the clients. This enum is
 * used to differentiate user requests. Since the wait time and start time are
 * just different representation of the same information, creating two different
 * class does not look sound solution. This enum class is to remember users'
 * requests and prepare various formatted response such as html, xml, and json.
 * Note that runtime is another estimation target but not a display type of
 * Estimation. That means runtime can be represented either by start time or
 * wait time.
 *
 */
public enum Estimation {

    STARTTIME("Predicted start time", WebService.getLocalTimeZoneString()),
    WAITTIME("predicted wait time", "(hours:minutes:seconds)");

    private Estimation(String typename, String strFormat) {
        this.typename = typename;
        this.strFormat = strFormat;
    }

    public String typename() {
        return typename;
    }

    public String format() {
        return strFormat;
    }

    /* The column name in the table */
    private String typename;

    /* explains the unit of the wait time or start time */
    private String strFormat;

}
