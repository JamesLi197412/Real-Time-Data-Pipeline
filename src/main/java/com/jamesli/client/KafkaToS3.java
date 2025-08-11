package com.jamesli.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamesli.kafka.KafkaManager;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.JSONObject;

import java.time.Duration;
import java.time.LocalDateTime;

public class KafkaToS3 {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String kafkaTopic;
    private final String s3Region;
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String consumerGroupId;

    private KafkaManager kafka;

    public KafkaToS3(JSONObject awsProps, String kafkaTopic, KafkaManager kafka) {
        this.kafkaTopic = kafkaTopic;
        // Create AWS credentials
        this.s3Region = awsProps.getString("aws_region");
        this.accessKeyId = awsProps.getString("aws_access_key_id");
        this.secretAccessKey = awsProps.getString("aws_secret_access_key");
        this.consumerGroupId = "finnhub-stock";
        this.kafka = kafka;
    }



}
