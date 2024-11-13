package com.datasqrl.function;

public interface SqrlTimeSessionFunction extends FunctionMetadata {

  public Specification getSpecification(long[] arguments);

  interface Specification {
    long getWindowGapMillis();
  }

}
