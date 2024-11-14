package com.datasqrl.functions.time;

import com.datasqrl.function.FunctionMetadata;
import com.datasqrl.function.SqrlTimeSessionFunction;
import com.datasqrl.time.EndOfSession;
import com.datasqrl.time.TimeSessionWindowFunction;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.time.temporal.ChronoUnit;
import lombok.AllArgsConstructor;

@AutoService(FunctionMetadata.class)
public class EndOfSessionMetadata implements SqrlTimeSessionFunction {

  protected final ChronoUnit gapUnit;

  public EndOfSessionMetadata() {
    this.gapUnit = ChronoUnit.SECONDS;
  }

  public EndOfSessionMetadata(ChronoUnit gapUnit) {
    this.gapUnit = gapUnit;
  }

  @Override
  public Specification getSpecification(long[] arguments) {
    Preconditions.checkArgument(arguments != null);
    return new Specification(arguments[0]);
  }

  @Override
  public Class getMetadataClass() {
    return EndOfSession.class;
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
