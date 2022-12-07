/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);

    final static String KAFKA_BOOTSTRAP_SERVERS_CONF = "bootstrap.servers";
    final static String KAFKA_PARTITIONS_CONF = "partitions";
    final static String KAFKA_INPUT_TOPIC_CONF = "input-topic";

    final static String KAFKA_OUTPUT_TOPIC_CONF = "output-topic";
    final static String KAFKA_GROUP_ID_CONF = "group.id";
    final static String ZOOKKEEPER_CONNECT_CONF = "zookeeper.connect";
    final static String JEDIS_SERVER_CONF = "jedis.server";
    final static String PROCESS_HOSTS_CONF = "process.hosts";
    final static String PROCESS_CORES_CONF = "process.cores";

    final static String FLINK_STATE_BACKEND_CONF = "state.backend";
    final static String FLINK_INCREMENTAL_CHECKPOINT_CONF = "state.backend.incremental";
    final static String FLINK_CHECKPOINTS_STORAGE_CONF = "state.checkpoint-storage";
    final static String FLINK_CHECKPOINTS_DIR_CONF = "state.checkpoints.dir";
    final static String FLINK_CHECKPOINTS_RETAINED_CONF = "state.checkpoints.num-retained";
    final static String FLINK_CHECKPOINTING_UNALIGNED_CONF = "execution.checkpointing.unaligned";
    final static String FLINK_CHECKPOINTING_MAX_CONCURRENT_CONF = "execution.checkpointing.max-concurrent-checkpoints";
    final static String FLINK_BUFFER_TIMEOUT_CONF = "flink.buffer-timeout";
    final static String FLINK_CHECKPOINT_INTERVAL_CONF = "flink.checkpoint-interval";
    final static String FLINK_S3_ACCESS_KEY_CONF = "s3.access-key";
    final static String FLINK_S3_SECRET_KEY_CONF = "s3.secret-key";
    final static String FLINK_S3_ENDPOINT_CONF = "s3.endpoint";
    final static String FLINK_S3_PATH_STYLE_ACCESS_CONF = "s3.path-style-access";
    final static String FLINK_S3_DISABLE_SSL_CONF = "s3.disable-ssl";


    public static void main(final String[] args) throws Exception {
        ParameterTool conf = ParameterTool.fromMap(getDefaultConf());
        int kafkaPartitions = Integer.parseInt(conf.getRequired(KAFKA_PARTITIONS_CONF));
        int hosts = Integer.parseInt(conf.getRequired(PROCESS_HOSTS_CONF));
        int cores = Integer.parseInt(conf.getRequired(PROCESS_CORES_CONF));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", conf.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(conf);

        // Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
        env.setBufferTimeout(Long.parseLong(conf.get(FLINK_BUFFER_TIMEOUT_CONF, "100")));

        // enable checkpointing for fault tolerance
        env.getCheckpointConfig()
           .setMinPauseBetweenCheckpoints(Long.parseLong(conf.get(FLINK_CHECKPOINT_INTERVAL_CONF)));
        env.enableCheckpointing(Long.parseLong(conf.get(FLINK_CHECKPOINT_INTERVAL_CONF)));

        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
        env.setParallelism(hosts * cores);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(conf.getRequired(KAFKA_BOOTSTRAP_SERVERS_CONF))
            .setTopics(conf.getRequired(KAFKA_INPUT_TOPIC_CONF))
            .setGroupId(conf.getRequired(KAFKA_GROUP_ID_CONF))
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            //.setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
            .build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(conf.getRequired(KAFKA_BOOTSTRAP_SERVERS_CONF))
            .setRecordSerializer( KafkaRecordSerializationSchema.builder()
                .setTopic(conf.getRequired(KAFKA_OUTPUT_TOPIC_CONF))
                .setKafkaValueSerializer(StringSerializer.class)
                .build())
            .build();

        DataStream<String> messageStream = env
            .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
            .setParallelism(Math.min(hosts * cores, kafkaPartitions));

        final SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = messageStream
            .rebalance()
            // Parse the String as JSON
            .flatMap(new DeserializeBolt())

            //Filter the records if event type is "view"
            .filter(new EventFilterBolt())

            // project the event
            .<Tuple2<String, String>>project(2, 5)

            // perform join with redis data
            .flatMap(new RedisJoinBolt())

            // process campaign
            .keyBy(0)
            .flatMap(new CampaignProcessor());

        stringSingleOutputStreamOperator.sinkTo(kafkaSink);
        //stringSingleOutputStreamOperator.collectAsync();

        env.execute();
    }

    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple =
                new Tuple7<>(
                    obj.getString("user_id"),
                    obj.getString("page_id"),
                    obj.getString("ad_id"),
                    obj.getString("ad_type"),
                    obj.getString("event_type"),
                    obj.getString("event_time"),
                    obj.getString("ip_address"));
            out.collect(tuple);
        }
    }

    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired(JEDIS_SERVER_CONF));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired(JEDIS_SERVER_CONF));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<String, String, String> tuple = new Tuple3<>(
                campaign_id,
                input.getField(0),
                input.getField(1));
            out.collect(tuple);
        }
    }

    public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

        CampaignProcessorCommon campaignProcessorCommon;

        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters();
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired(JEDIS_SERVER_CONF));

            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool
                .getRequired(JEDIS_SERVER_CONF));
            this.campaignProcessorCommon.prepare();
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            String event_time =  tuple.getField(2);
            this.campaignProcessorCommon.execute(campaign_id, event_time);
            out.collect(new JSONObject()
                .put("campaign_id", campaign_id)
                .put("event_time", event_time)
                .put("out_ts_ms", System.currentTimeMillis())
                .toString()
            );
        }
    }

    private static Map<String, String> getDefaultConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put(KAFKA_BOOTSTRAP_SERVERS_CONF, "broker:9092");
        conf.put(KAFKA_PARTITIONS_CONF, "1");
        conf.put(KAFKA_INPUT_TOPIC_CONF, "ad-events");
        conf.put(KAFKA_GROUP_ID_CONF, "myGroup");
        conf.put(KAFKA_OUTPUT_TOPIC_CONF, "output-topic");
        conf.put(ZOOKKEEPER_CONNECT_CONF, "zookeeper");
        conf.put(JEDIS_SERVER_CONF, "redis://redis:6379");
        conf.put(PROCESS_HOSTS_CONF, "1");
        conf.put(PROCESS_CORES_CONF, "1");
        conf.put(FLINK_STATE_BACKEND_CONF, "rocksdb");
        conf.put(FLINK_INCREMENTAL_CHECKPOINT_CONF, "true");
        conf.put(FLINK_CHECKPOINTS_STORAGE_CONF, "filesystem");
        conf.put(FLINK_CHECKPOINTS_DIR_CONF, "s3://my-bucket/flink-checkpoints");
        conf.put(FLINK_S3_ACCESS_KEY_CONF, "minio");
        conf.put(FLINK_S3_SECRET_KEY_CONF, "minio123");
        conf.put(FLINK_S3_ENDPOINT_CONF, "http://minio:9000");
        conf.put(FLINK_S3_PATH_STYLE_ACCESS_CONF, "true");
        conf.put(FLINK_S3_DISABLE_SSL_CONF, "true");
        conf.put(FLINK_CHECKPOINTS_RETAINED_CONF, "1");
        conf.put(FLINK_CHECKPOINTING_UNALIGNED_CONF, "false");
        conf.put(FLINK_CHECKPOINTING_MAX_CONCURRENT_CONF, "1");
        conf.put(FLINK_CHECKPOINT_INTERVAL_CONF, "10");

        return conf;
    }
}
