package com.jamesli;

import com.jamesli.client.FinnhubWebSocketClient;
import com.jamesli.config.Constants;
import com.jamesli.kafka.KafkaManager;
import com.jamesli.utils.GetPrefixEnvrionmentVariable;
import com.jamesli.client.KafkaToS3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.json.JSONObject;

import java.util.List;
import java.util.Arrays;


public class FinnhubStreamingApplication {
    private static final int MAX_RETRIES = 3;
    private static final int TIMEOUT_MS = 5000;
    private static final int RECONNECT_DELAY_MS = 5000;

    public static void main(String[] args) throws Exception{
        final Logger log = LoggerFactory.getLogger(FinnhubStreamingApplication.class);
        log.info("Starting Finhub Streaming Application");

        // read properties from zshrc to find out aws related
        String homeDir = System.getProperty("user.home");
        GetPrefixEnvrionmentVariable env = new GetPrefixEnvrionmentVariable("AWS");
        JSONObject awsProps = env.readProperties(homeDir + "/.zshrc", "AWS");
        JSONObject finnhubProps = env.readProperties(homeDir + "/.zshrc", "finnhub");
        List<String> symbols = Arrays.asList("AAPL", "AMZN", "BINANCE:BTCUSDT");

        // Initialise the Kafka
        KafkaManager kafkaManager = new KafkaManager(Constants.kafkaPort, Constants.zooKeeperPort);
        kafkaManager.start();

        // Convert the string to a URI
        URI wsUri = URI.create(finnhubProps.getString("finnhub_uri"));
        FinnhubWebSocketClient client = new FinnhubWebSocketClient(wsUri, Constants.kafkaTopic_stock, kafkaManager, symbols);

        // Initialise the Flink Job
        new Thread(() -> {
            try {
                KafkaToS3 flinkJob = new KafkaToS3(
                        awsProps,
                        Constants.kafkaTopic_stock,
                        kafkaManager
                );
                flinkJob.batchProcess();
            } catch (Exception e) {
                log.error("Flink job failed", e);
            }
        }).start();

        // === MAIN Loop to keep it running all the time ==
        while (!Thread.currentThread().isInterrupted()) {
            try {
                if (client.isConnected()) {
                    // Already connected, do nothing
                    Thread.sleep(1000);
                    continue;
                }

                log.info("Attempting to connect to Finnhub WebSocket...");
                if (client.connectWithTimeout()) {
                    log.info("Connected successfully. Waiting for messages...");
                    // Block until connection drops
                    synchronized (client) {
                        while (client.isConnected()) {
                            client.wait(1000); // wait for disconnect notification
                        }
                    }
                } else {
                    log.warn("Connection attempt timed out. Retrying in {} ms...", RECONNECT_DELAY_MS);
                    Thread.sleep(RECONNECT_DELAY_MS);
                }

            } catch (InterruptedException e) {
                log.info("Application interrupted. Shutting down...");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Unexpected error in main loop", e);
                try {
                    Thread.sleep(RECONNECT_DELAY_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        // Cleanup on exit
        client.shutdown();
        kafkaManager.stop();
    }
}
