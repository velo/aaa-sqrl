/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.hints;

import com.google.common.base.Preconditions;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.hint.RelHint;

@AllArgsConstructor
public class SessionAggregationHint implements SqrlHint {


  @Getter
  final int windowFunctionIdx;

  @Getter
  final int inputTimestampIdx;
  @Getter
  final long windowGapMs;

  public static SessionAggregationHint functionOf(int windowFunctionIdx, int inputTimestampIdx, long windowGapMs) {
    return new SessionAggregationHint(windowFunctionIdx, inputTimestampIdx, windowGapMs);
  }

  @Override
  public RelHint getHint() {
    return RelHint.builder(getHintName())
        .hintOptions(List.of(String.valueOf(windowFunctionIdx),
            String.valueOf(inputTimestampIdx), String.valueOf(windowGapMs))).build();
  }

  public static final String HINT_NAME = SessionAggregationHint.class.getSimpleName();

  @Override
  public String getHintName() {
    return HINT_NAME;
  }

  public static final Constructor CONSTRUCTOR = new Constructor();

  public static final class Constructor implements SqrlHint.Constructor<SessionAggregationHint> {

    @Override
    public boolean validName(String name) {
      return name.equalsIgnoreCase(HINT_NAME);
    }

    @Override
    public SessionAggregationHint fromHint(RelHint hint) {
      List<String> options = hint.listOptions;
      Preconditions.checkArgument(options.size() == 3, "Invalid hint: %s", hint);
      return new SessionAggregationHint(Integer.valueOf(options.get(0)),
          Integer.valueOf(options.get(1)), Long.valueOf(options.get(2)));
    }
  }

}
