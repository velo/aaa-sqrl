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
   * The eval methods of the TimeFunctions are used as an optimization on the database side to allow window merging with a simple GROUP BY(last timestamp of the window). For session windows, the last timestamp of the window cannot be determined upfront.
   * Doing the window merging at the database level would be a non-performant SQL request.
   */
  @Override
  public Instant eval(Instant instant, Long gap) {
    throw new UnsupportedOperationException("No window merging at the database side for session windows");
  }
}
