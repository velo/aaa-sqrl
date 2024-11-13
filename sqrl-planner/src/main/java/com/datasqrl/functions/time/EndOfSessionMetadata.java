package com.datasqrl.functions.time;

import com.datasqrl.function.SqrlTimeSessionFunction;
import com.google.common.base.Preconditions;
import java.time.temporal.ChronoUnit;
import lombok.AllArgsConstructor;

public abstract class EndOfSessionMetadata implements SqrlTimeSessionFunction {

  protected final ChronoUnit gapUnit;

  public EndOfSessionMetadata(ChronoUnit gapUnit) {
    this.gapUnit = gapUnit;
  }

  @Override
  public Specification getSpecification(long[] arguments) {
    Preconditions.checkArgument(arguments != null);
    return new Specification(arguments[0]);
  }

  @AllArgsConstructor
  private class Specification implements SqrlTimeSessionFunction.Specification {

    final long gap;

    @Override
    public long getWindowGapMillis() {
      return gapUnit.getDuration().multipliedBy(gap).toMillis();
    }
  }
}
