package com.datasqrl.io;

import com.datasqrl.config.FlinkSourceFactoryContext;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.TableDescriptorSourceFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.io.formats.TextLineFormat;
import com.datasqrl.io.impl.file.FileDataSystemConfig;
import com.datasqrl.io.impl.file.FileDataSystemDiscovery;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.file.FilePath;
import com.datasqrl.io.impl.file.FilePathConfig;
import com.datasqrl.io.tables.BaseTableConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.util.TimeAnnotatedRecord;
import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.FileStreamUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableDescriptor.Builder;
import org.apache.flink.util.Collector;

@AutoService(SourceFactory.class)
public class FileTableSourceFactory implements TableDescriptorSourceFactory {

  @Override
  public String getSourceName() {
    return FileDataSystemFactory.SYSTEM_NAME;
  }

  @Override
  public TableDescriptor.Builder create(FlinkSourceFactoryContext ctx) {
    TableConfig tableConfig = ctx.getTableConfig();
    FilePathConfig pathConfig = FileDataSystemConfig.fromConfig(tableConfig).getFilePath(tableConfig.getErrors());
    FormatFactory formatFactory = tableConfig.getFormat();
    Preconditions.checkArgument(formatFactory instanceof TextLineFormat,"This connector only supports text files");
//    String charset = ((TextLineFormat)formatFactory).getCharset(tableConfig.getFormatConfig()).name();

    Builder builder = TableDescriptor.forConnector("filesystem");

    Optional<Integer> monitorInterval = tableConfig.getConnectorConfig()
          .asInt(FileConfigOptions.MONITOR_INTERVAL_MS)
          .getOptional();
    if (monitorInterval.isPresent() && monitorInterval.get() > 0) {
      builder.option(
          FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL.key(),
          monitorInterval.get().toString());
    }

    if (pathConfig.isDirectory()) {
      builder.option(FileSystemConnectorOptions.PATH, pathConfig.getDirectory().toString());
    } else {
      String files = pathConfig.getFiles(ctx.getTableConfig())
          .stream()
          .flatMap(f-> {
            try {
              return f.listFiles().stream();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .map(f->f.getPath().toString())
          .collect(Collectors.joining(","));
      builder.option(FileSystemConnectorOptions.PATH, files);
    }
//    builder.option("file.name", pathConfig.getDirectory().toString());
    builder.option("format", "flexible-"+tableConfig.getFormat().getName());
    builder.option(String.format("flexible-%s.schema", tableConfig.getFormat().getName()), getSchema(ctx.getSchemaDefinition()));

    return builder;

//
//    if (pathConfig.isURL()) {
//      Preconditions.checkArgument(!pathConfig.isDirectory());
//      return ctx.getEnv().fromCollection(pathConfig.getFiles(ctx.getTableConfig())).
//          flatMap(new ReadPathByLine(charset))
//          .map(new NoTimedRecord());
//    } else {
//      org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder;
//      if (pathConfig.isDirectory()) {
//        StreamFormat<String> format;
//        //todo: fix me with factory
//        if (formatFactory.getName().equalsIgnoreCase("json")) {
//          format = new JsonInputFormat(charset);
//        } else {
//          format = new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
//              charset);
//        }
//
//        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
//            format,
//            FilePath.toFlinkPath(pathConfig.getDirectory()));
//
//        FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(tableConfig.getBase(),
//            tableConfig.getFormat(),
//            FileDataSystemConfig.fromConfig(tableConfig));
//        builder.setFileEnumerator(fileEnumerator);
//
//      } else {
//        Path[] inputPaths = pathConfig.getFiles(ctx.getTableConfig()).stream()
//            .map(FilePath::toFlinkPath).toArray(size -> new Path[size]);
//        builder = org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
//            new org.apache.flink.connector.file.src.reader.TextLineInputFormat(
//                charset), inputPaths);
//      }
//      Optional<Integer> monitorInterval = tableConfig.getConnectorConfig()
//          .asInt(FileConfigOptions.MONITOR_INTERVAL_MS)
//          .getOptional();
//      if (isMonitor(monitorInterval)) {
//        Duration duration = monitorInterval
//            .map(this::parseDuration)
//            .orElse(defaultDuration());
//        builder.monitorContinuously(duration);
//      }
//      return ctx.getEnv().fromSource(builder.build(),
//          WatermarkStrategy.noWatermarks(), ctx.getFlinkName())
//          .map(new NoTimedRecord())
////          .setParallelism(4)//todo config
//          ;
//    }
  }
  @SneakyThrows
  public String getSchema(String schemaDef) {
    Deserializer deserializer = new Deserializer();
    JsonNode jsonNode = deserializer.getYamlMapper().readTree(schemaDef);
    return deserializer.getJsonMapper().writeValueAsString(jsonNode);
  }
  @SneakyThrows
  public String getSchema(java.nio.file.Path path) {
    Deserializer deserializer = new Deserializer();
    TableDefinition tableDefinition = deserializer.mapYAMLFile(path,
        TableDefinition.class);
    String json = deserializer.getJsonMapper().writeValueAsString(tableDefinition);

    return json;
  }
  private Duration parseDuration(Integer d) {
    return Duration.ofMillis(d);
  }

  private Duration defaultDuration() {
    return Duration.ofMillis(10000);
  }

  /**
   * Monitor if configuration is missing or non zero
   */
  private boolean isMonitor(Optional<Integer> monitorInterval) {
    return monitorInterval
        .map(s -> !parseDuration(s).equals(Duration.ZERO))
        .orElse(true);
  }

  @Override
  public Optional<String> getSourceTimeMetaData() {
    return Optional.empty();
  }

  @NoArgsConstructor
  @AllArgsConstructor
  public static class FileEnumeratorProvider implements
      org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

    BaseTableConfig baseConfig;
    FormatFactory format;
    FileDataSystemConfig fileConfig;

    @Override
    public org.apache.flink.connector.file.src.enumerate.FileEnumerator create() {
      return new NonSplittingRecursiveEnumerator(new FileNameMatcher());
    }

    private class FileNameMatcher implements Predicate<Path> {

      @Override
      public boolean test(Path path) {
        try {
          if (path.getFileSystem().getFileStatus(path).isDir()) {
            return true;
          }
        } catch (IOException e) {
          return false;
        }
        return FileDataSystemDiscovery.isTableFile(FilePath.fromFlinkPath(path), baseConfig, format, fileConfig);
      }
    }
  }

  public static class NoTimedRecord implements MapFunction<String, TimeAnnotatedRecord<String>> {

    @Override
    public TimeAnnotatedRecord<String> map(String s) throws Exception {
      return new TimeAnnotatedRecord<>(s, null);
    }
  }


  @AllArgsConstructor
  public class ReadPathByLine implements FlatMapFunction<FilePath, String> {

    private String charset;

    @Override
    public void flatMap(FilePath filePath, Collector<String> collector) throws Exception {
      try (InputStream is = filePath.read()) {
        FileStreamUtil.readByLine(is, charset).forEach(collector::collect);
      }
    }
  }
}
