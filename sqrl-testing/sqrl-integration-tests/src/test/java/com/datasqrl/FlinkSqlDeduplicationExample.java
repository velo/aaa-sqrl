package com.datasqrl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.StatementSetOperation;
import org.junit.jupiter.api.Test;

public class FlinkSqlDeduplicationExample {

  @Test
  public void main() throws Exception {
    // Set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1); // For simplicity

    // Set up the table environment
    EnvironmentSettings settings = EnvironmentSettings.newInstance()
        .inStreamingMode()
        .build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

    // Define the DataGen source table
    String createSource = "CREATE TABLE cdc_source (" +
        " id INT," +
        " name STRING," +
        " ts AS PROCTIME()" +
        ") WITH (" +
        " 'connector' = 'postgres-cdc'," +
        " 'hostname' = 'localhost'," +
        " 'port' = '5432'," +
        " 'username' = 'postgres'," +
        " 'password' = 'postgres'," +
        " 'database-name' = 'testdb'," +
        " 'schema-name' = 'public'," +
        " 'slot.name' = 'public'," +
        " 'table-name' = 'test_table'," +
        " 'debezium.slot.name' = 'flink_slot'," +
        " 'debezium.snapshot.mode' = 'initial'," +
        " 'debezium.skipped.operations'='u,d'," +
        " 'debezium.signal.data.collection' = 'public.signals'" +
        ")";

    tableEnv.executeSql(createSource);

    // Define the Blackhole sink table
    String createSink = "CREATE TABLE blackhole_sink ("
        + " id INT,"
        + " name STRING,"
        + " ts TIMESTAMP(3)"
        + ") WITH ("
        + " 'connector' = 'blackhole'"
        + ")";

    tableEnv.executeSql(createSink);

    // Deduplication query: Remove duplicates based on 'id'
    // We'll keep the latest record for each 'id' based on processing time
    String deduplicationQuery = ""
        + "INSERT INTO blackhole_sink "
        + "SELECT id, name, ts FROM ( "
        + "   SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) as rn "
        + "   FROM cdc_source "
        + ") WHERE rn = 1";

    TableEnvironmentImpl tEnv1 = (TableEnvironmentImpl) tableEnv;

    StatementSetOperation parse = (StatementSetOperation) tEnv1.getParser()
        .parse("EXECUTE STATEMENT SET BEGIN " + deduplicationQuery + ";END;").get(0);

    CompiledPlan compiledPlan = tEnv1.compilePlan(parse.getOperations());
    System.out.println(compiledPlan.asJsonString());

    /**
     * Throws:
     * org.apache.flink.table.api.TableException: StreamPhysicalDeduplicate doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, cdc_source]], fields=[id, name])
     */
  }
}
