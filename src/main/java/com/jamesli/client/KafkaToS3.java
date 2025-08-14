package com.jamesli.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import com.jamesli.kafka.KafkaManager;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.connector.file.sink.FileSink;

import org.apache.flink.api.common.functions.RichMapFunction;

import com.fasterxml.jackson.databind.JsonNode;

import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;

import com.jamesli.pojo.Trade;

import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.core.fs.Path;
import java.time.Duration;


public class KafkaToS3 {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String kafkaTopic;
    private final String s3Region;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String consumerGroupId;

    private KafkaManager kafka;
    private final String s3Path = "s3a://finhubsocket/stock/";

    public KafkaToS3(JSONObject awsProps, String kafkaTopic, KafkaManager kafka) {
        this.kafkaTopic = kafkaTopic;
        // Create AWS credentials
        this.s3Region = awsProps.getString("aws_region");
        this.accessKeyId = awsProps.getString("aws_access_key_id");
        this.secretAccessKey = awsProps.getString("aws_secret_access_key");
        this.consumerGroupId = "finnhub-stock";
        this.kafka = kafka;
    }

    public void batchProcess() throws Exception{
        // Initialize Flink execution environment
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("fs.s3a.access.key", this.accessKeyId);
        flinkConfig.setString("fs.s3a.secret.key", this.secretAccessKey);
        flinkConfig.setString("fs.s3a.endpoint.region", this.s3Region);

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        env.enableCheckpointing(3000);

        // Configure Kafka consumer & Add Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafka.getBootstrapServers())
                .setGroupId(this.consumerGroupId)
                .setTopics(kafkaTopic)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Create the JSON serializer for your Trade class
        DataStream<String> jsonStringStream = kafkaStream
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        try {
                            Trade trade = objectMapper.readValue(value, Trade.class);
                            return objectMapper.writeValueAsString(trade);
                        } catch (JsonProcessingException e) {
                            throw new RuntimeException("Failed to serialize Trade to JSON", e);
                        }
                    }
                });

        // Configure S3 file sink with time-based buckets
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path(s3Path), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(5))
                                .withInactivityInterval(Duration.ofMinutes(5))
                                .build()
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .build();

        // Write to S3
        kafkaStream.sinkTo(fileSink).name("S3 Sink");

        // Execute the pipeline
        env.execute("Flink Kafka to S3 - Stock Data");
    }
}
