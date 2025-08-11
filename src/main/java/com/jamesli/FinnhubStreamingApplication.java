package com.jamesli;

import com.jamesli.client.FinnhubWebSocketClient;
import com.jamesli.config.Constants;
import com.jamesli.kafka.KafkaManager;
import com.jamesli.utils.GetPrefixEnvrionmentVariable;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.json.JSONObject;

import java.util.List;
import java.util.Arrays;


public class FinnhubStreamingApplication {
    private static final int MAX_RETRIES = 3;
    private static final int TIMEOUT_MS = 5000;

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

        while(true){

        }
        // Initialise the Websocket and Flink for process
        try{
            if (client.connectWithTimeout()){
                log.info("WebSocket connected successfully. ");
            }else{
                log.error("Failed to connect within timeout");
            }

        }catch (Exception e){
            log.error("Error during connection ", e);
        }finally{
            client.shutdown();
            kafkaManager.stop();
        }
    }
}
