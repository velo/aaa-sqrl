package com.datasqrl.time;

import com.datasqrl.function.FlinkTypeUtil;
import com.datasqrl.function.FlinkTypeUtil.VariableArguments;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import lombok.AllArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

@AllArgsConstructor
public abstract class TimeSessionWindowFunction extends ScalarFunction implements
    TimeSessionWindowFunctionEval {

  protected final ChronoUnit gapUnit;

  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder().inputTypeStrategy(
            VariableArguments.builder()
                    .staticType(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)) // timestamp
                    .staticType(DataTypes.BIGINT()) // session gap
                    .build())
        .outputTypeStrategy(FlinkTypeUtil.nullPreservingOutputStrategy(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3)))
        .build();
  }

  /**
   * Gives the last timestamp of the interval based on the instant timestamp
   * (e.g. last timespamp of the window containing input timestamp t=5s element with gap=10s is t=15s - 1 nano second)
   * @param instant input timestamp
   * @param gap gap duration in {@code gapUnit} unit
   * @return last timestamp of the window
   */
  @Override
  public Instant eval(Instant instant, Long gap) {
    Preconditions.checkArgument(gap > 0, "Gap duration must be positive: %s", gap);

    ZonedDateTime time = ZonedDateTime.ofInstant(instant, ZoneOffset.UTC);
    final ZonedDateTime endOfInterval = time.plus(gap, gapUnit);
    return endOfInterval.minusNanos(1).toInstant();
    //TODO ECH: sessions windows do not overlap (an element is inside only one window).
    // functionally: if no element arrives for gapDuration in event time then next element will belong to the next session window.
    // technically: this window is determined by the intersection of the intervals: e.g.
    // e1(t=1) and e2(t=4) are in the same session window of 5s gap because [1, 6] intersects [4, 9]
    // e3(t=10) is in the next session because [10, 15] do not intersect with [1, 6] and [4, 9]
    // => It is Flink which does intervals merging ?
  }
}
