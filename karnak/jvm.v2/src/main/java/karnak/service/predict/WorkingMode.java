/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package karnak.service.predict;

/**
 *
 * @author Jungha Woo <wooj@purdue.edu>
 * Karnak productions use Live mode.
 * Backtesting for benchmark uses Backtest mode. In that case, 
 * explicit query time must be given in order to construct decision trees at that point.
 */
public enum WorkingMode {
    Live,  /* The decision tree is built while realtime job and queue updates are coming in */
    Backtest /* The decision tree is built for the time t in the past and simulated to go on */
}
