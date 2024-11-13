package com.datasqrl.time;


import java.time.Instant;

public interface TimeSessionWindowFunctionEval {
  Instant eval(Instant instant, Long gap);
}
