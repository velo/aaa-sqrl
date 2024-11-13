package com.datasqrl.function;

// TODO mode readable to call FunctionMetadata subclasses Sqrl*FunctionMetadata ?
public interface SqrlTimeSessionFunction extends FunctionMetadata {

  public Specification getSpecification(long[] arguments);

  interface Specification {
    long getWindowGapMillis();
  }

}
