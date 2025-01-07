package com.datasqrl.time;

import java.time.temporal.ChronoUnit;

/**
 * Time window function that returns the end of the session window for the timestamp argument.
 *
 */
public class EndOfSession extends TimeSessionWindowFunction {
    public EndOfSession() {
        super(ChronoUnit.SECONDS);
    }
}
