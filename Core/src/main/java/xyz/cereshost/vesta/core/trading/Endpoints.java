package xyz.cereshost.vesta.core.trading;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Contract;

@Getter
@RequiredArgsConstructor
public enum Endpoints {
    API("https://api.binance.com", false, false, true),
    API1("https://api1.binance.com", false, false, true),
    API2("https://api2.binance.com", false, false, true),
    API3("https://api3.binance.com", false, false, true),
    API4("https://api4.binance.com", false, false, true),
    API_GCP("https://api-gcp.binance.com", false, false, true),
    TESTNET("https://testnet.binance.vision", true, false, true),
    FAPI("https://fapi.binance.com", false, true, true),
    DEMO_FAPI("https://demo-fapi.binance.com", true, true, true),
    API_WSS_TEST("wss://ws-api.testnet.binance.vision/ws-api/v3", true, false, false),
    API_WSS("wss://ws-api.binance.com:443/ws-api/v3", false, false, false),
    STREAM_WSS_TEST("wss://stream.testnet.binance.vision/stream?streams=", true, false, false),
    STREAM_WSS("wss://stream.binance.com/stream?streams=", false, false, false);

    private final String endpoint;
    private final boolean isTest;
    private final boolean isFutures;
    private final boolean apiRest;

    @Contract(pure = true)
    public boolean isSpot() {
        return !isFutures;
    }
    @Contract(pure = true)
    public boolean isReal() {
        return !isTest;
    }
    @Contract(pure = true)
    public boolean isWebSocket() {
        return !apiRest;
    }
}
