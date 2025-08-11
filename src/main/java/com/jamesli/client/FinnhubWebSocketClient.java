package com.jamesli.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jamesli.kafka.KafkaManager;
import com.jamesli.pojo.Trade;
import com.jamesli.pojo.TradeMessage;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinnhubWebSocketClient extends WebSocketClient {
    private static final Logger log = LoggerFactory.getLogger(FinnhubWebSocketClient.class);
    private static final int MAX_RETRIES = 5;
    private static final int INITIAL_RETRY_DELAY_MS = 1000;
    private static final int MAX_RETRY_DELAY_MS = 5000;
    private static final int TIMEOUT_MS = 10000;
    private static final int HEARTBEAT_INTERVAL_MS = 30000;

    private final String kafkaTopic;
    private KafkaManager kafka;
    private List<String> symbols;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    // Concurrency controls
    private final CountDownLatch connectionLatch = new CountDownLatch(1);
    private final AtomicBoolean isConnected = new AtomicBoolean(false);
    private final AtomicBoolean shouldReconnect = new AtomicBoolean(true);
    private final AtomicInteger retryCount = new AtomicInteger(0);

    // Heartbeat monitoring
    private volatile long lastMessageTime = System.currentTimeMillis();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, (r) -> {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("WebSocket-Scheduler-" + t.getId());
                return t;
            });

    private ScheduledFuture<?> heartbeatFuture;
    private ScheduledFuture<?> reconnectFuture;

    public FinnhubWebSocketClient(URI serverUri, String kafkaTopic, KafkaManager kafka, List<String> symbols ) {
        super(serverUri);
        this.kafkaTopic = kafkaTopic;
        this.kafka = kafka;
        this.symbols = symbols != null ?symbols: List.of("AAPL", "AMZN", "BINANCE:BTCUSDT");
        setConnectionLostTimeout(60);
    }

    private void subscribeToSymbols(){
        try{
            for (String symbol : symbols) {
                String subscriptionMessage = String.format("{\"type\":\"subscribe\",\"symbol\":\"%s\"}", symbol);
                this.send(subscriptionMessage);
                // Small delay to avoid overwhelming the server
                Thread.sleep(100);
            }
        } catch (Exception e) {
            System.err.println("Error subscribing to symbols: " + e.getMessage());
        }
    }


    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to WebSocket");
        isConnected.set(true);
        retryCount.set(0); // Reset retry count on successful connection
        lastMessageTime = System.currentTimeMillis();


        subscribeToSymbols();
        startHeartbeatMonitoring();
    }

    @Override
    public void onMessage(String message){
        lastMessageTime = System.currentTimeMillis();
        log.debug("Received : {}", message);

        try {
            // To extract the message
            TradeMessage tradeJson = OBJECT_MAPPER.readValue(message, TradeMessage.class);

            // filter out information if it is not trade
            if (tradeJson.getType().equals("trade")) {
                List<Trade> trades = tradeJson.getData();
                for (Trade trade : trades) {
                    // Send to Kafka
                    String data = OBJECT_MAPPER.writeValueAsString(trade);
                    kafka.send(this.kafkaTopic, null, data);
                    log.debug("Sent trade information to Kafka: {}", data);
                    System.out.println(data);
                }
            }
        }catch (Exception e){
            log.error("Error processing message: {}", e.toString(), e);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        log.info("Connection closed -- Code: " + code + ", Reason: " + reason + ", Remote: " + remote);
        isConnected.set(false);

        // stop monitoring threads
        stopMonitoringThreads();

        // Flush any pending kafka messages
        kafka.flush();

        // Attempt reconnection if needed
        if (shouldReconnect.get() && code != 1000) { // 1000 is normal closure
            scheduleReconnection();
        }
    }

    @Override
    public void onError(Exception ex) {
        log.error("WebSocket error: {}", ex.toString(), ex);
        isConnected.set(false);

        // Flush any pending kafka messages
        kafka.flush();
        if (shouldReconnect.get()) {
            scheduleReconnection();
        }

    }

    // Enhanced connection method with latch and timeout
    public boolean connectWithTimeout() throws InterruptedException {
        boolean connected = connectBlocking(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (connected) {
            // Wait for onOpen to complete
            return connectionLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
        return false;
    }

    // Graceful shutdown
    public void shutdown() {
        log.info("Shutting down WebSocket client...");
        shouldReconnect.set(false);
        stopMonitoringThreads();

        try {
            this.closeBlocking();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Interrupted while closing WebSocket");
        }

        kafka.flush();
        kafka.close();
    }

    // TODO: Understand
    private void startHeartbeatMonitoring() {
        stopHeartbeat();

        heartbeatFuture = scheduler.scheduleAtFixedRate(() -> {
            if (isConnected.get() && (System.currentTimeMillis() - lastMessageTime > 120000)) {
                log.warn("No messages received in 2 minutes. Reconnecting...");
                reconnect();
            }
        }, HEARTBEAT_INTERVAL_MS, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void scheduleReconnection() {
        if (!shouldReconnect.get()) return;

        int currentRetry = retryCount.incrementAndGet();
        if (currentRetry > MAX_RETRIES) {
            log.error("Max retries ({}) exceeded. Giving up.", MAX_RETRIES);
            return;
        }

        long delay = calculateBackoffDelay(currentRetry);
        log.info("Scheduling reconnection attempt {} in {}ms", currentRetry, delay);

        // Cancel any pending reconnect
        if (reconnectFuture != null) {
            reconnectFuture.cancel(false);
        }

        reconnectFuture = scheduler.schedule(() -> {
            if (shouldReconnect.get()) {
                log.info("Attempting reconnection...");
                reconnect();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    private void initialReconnection() {
        if (!shouldReconnect.get()) {
            return;
        }

        try {
            // Reset connection state
            isConnected.set(false);
            connectionLatch.countDown(); // Release any waiting threads

            System.out.println("Reconnecting to WebSocket...");
            this.reconnect();
        } catch (Exception e) {
            System.err.println("Error during reconnection: " + e.getMessage());
            scheduleReconnection(); // Try again
        }
    }

    private long calculateBackoffDelay(int retryCount) {
        long fullDelay = INITIAL_RETRY_DELAY_MS * (1L << (retryCount - 1)); // exponential: 1s, 2s, 4s...
        long clamped = Math.min(fullDelay, MAX_RETRY_DELAY_MS);
        // Add jitter: 75% to 125%
        return (long) (clamped * (0.75 + Math.random() * 0.5));
    }

    private void stopMonitoringThreads() {
        stopHeartbeat();
        stopReconnect();
    }

    private void stopHeartbeat(){
        if (heartbeatFuture != null && !heartbeatFuture.isDone()) {
            heartbeatFuture.cancel(true);
        }
        heartbeatFuture = null;
    }

    private void stopReconnect() {
        if (reconnectFuture != null && !reconnectFuture.isDone()) {
            reconnectFuture.cancel(true);
        }
        reconnectFuture = null;
    }

    // Public methods for external control
    public boolean isConnected() {
        return isConnected.get();
    }

    public int getRetryCount() {
        return retryCount.get();
    }

    public void resetRetryCount() {
        retryCount.set(0);
    }

}
