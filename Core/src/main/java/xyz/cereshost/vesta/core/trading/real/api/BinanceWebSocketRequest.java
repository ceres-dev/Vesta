package xyz.cereshost.vesta.core.trading.real.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.exception.BinanceApiRequestException;
import xyz.cereshost.vesta.core.trading.Endpoints;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;
import xyz.cereshost.vesta.core.trading.real.api.model.Ticker24H;

import java.net.URI;
import java.net.http.WebSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public final class BinanceWebSocketRequest extends BinanceWebSocket {

    private final WebSocket webSocket;
    private final Object apiIncomingLock = new Object();
    private final StringBuilder apiIncomingMessage = new StringBuilder();

    @Nullable private ExchangeInfo exchangeInfoSpot = null;

    public BinanceWebSocketRequest(boolean isTestNet) {
        super(isTestNet ? Endpoints.API_WSS_TEST : Endpoints.API_WSS);
        this.webSocket = client.newWebSocketBuilder()
                .buildAsync(URI.create(endpoint.getEndpoint()), newListener())
                .join();
    }

    @Override
    public void checkApikey() {

    }

    public void invalidedCache() {
        exchangeInfoSpot = null;
    }

    public @NotNull Map<String, BookTicker> getBookTickers(@Nullable Symbol symbol, @Nullable Boolean isFuture) {
        boolean future = resolveFuture(symbol, isFuture);
        if (future) {
            throw new UnsupportedOperationException("BinanceWebSocketRequest solo soporta Spot para getBookTickers.");
        }

        Map<String, Object> params = new HashMap<>();
        if (symbol != null) {
            params.put("symbol", symbol.name());
        }
        JsonNode result = sendPublicRequest("ticker.book", params);
        return ParseJsonApi.parseBookTickers(result);
    }

    public @NotNull ExchangeInfo getExchangeInfo(@NotNull Boolean isFuture) {
        if (isFuture) {
            throw new UnsupportedOperationException("BinanceWebSocketRequest solo soporta Spot para getExchangeInfo.");
        }

        if (exchangeInfoSpot != null) {
            return exchangeInfoSpot;
        }

        JsonNode result = sendPublicRequest("exchangeInfo", Map.of());
        exchangeInfoSpot = ParseJsonApi.parseExchangeInfo(result, false);
        return exchangeInfoSpot;
    }

    @Override
    public @NotNull HashMap<String, Double> getBalance(@NotNull Boolean isFuture) {
        throw new UnsupportedOperationException();
    }

    public @NotNull Set<Ticker24H> getTicker24H(@Nullable Symbol symbol) {
        JsonNode node = (symbol == null)
                ? sendPublicRequest("ticker.24hr", Map.of())
                : sendPublicRequest("ticker.24hr", Map.of("symbol", symbol.name()));
        return ParseJsonApi.parseTicker24H(node);
    }

    @Override
    protected @NotNull WebSocket.Listener newListener() {
        return new WebSocket.Listener() {
            @Override
            public void onOpen(WebSocket webSocket) {
                webSocket.request(1);
                WebSocket.Listener.super.onOpen(webSocket);
            }

            @Override
            public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
                String contentToParse = accumulateMessage(data, last, apiIncomingLock, apiIncomingMessage);
                if (contentToParse != null) {
                    handleApiMessage(contentToParse);
                }
                webSocket.request(1);
                return WebSocket.Listener.super.onText(webSocket, data, last);
            }

            @Override
            public void onError(WebSocket webSocket, Throwable error) {
                Exception exception = error instanceof Exception e ? e : new RuntimeException(error);
                failPendingRequests(pendingRequests, exception);
                exceptionHandler.accept(exception);
                WebSocket.Listener.super.onError(webSocket, error);
            }

            @Override
            public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
                failPendingRequests(
                        pendingRequests,
                        new IllegalStateException("WebSocket Request cerrado: " + statusCode + " " + reason)
                );
                return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
            }
        };
    }

    private void handleApiMessage(@NotNull String contentToParse) {
        try {
            JsonNode response = mapper.readTree(contentToParse);
            JsonNode idNode = response.get("id");
            if (idNode == null) {
                return;
            }
            CompletableFuture<JsonNode> pending = pendingRequests.remove(idNode.asText());
            if (pending != null) {
                pending.complete(response);
            }
        } catch (Exception e) {
            failPendingRequests(pendingRequests, e);
            exceptionHandler.accept(e);
        }
    }

    private boolean resolveFuture(@Nullable Symbol symbol, @Nullable Boolean isFuture) {
        Boolean future = null;
        if (isFuture != null) future = isFuture;
        if (symbol != null) future = symbol.getIsFuture();
        if (future == null) throw new IllegalArgumentException("Symbol future is null");
        return future;
    }

    private @NotNull synchronized JsonNode sendPublicRequest(@NotNull String method, @NotNull Map<String, Object> params) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<JsonNode> responseFuture = new CompletableFuture<>();
        pendingRequests.put(id, responseFuture);

        try {
            ObjectNode payload = mapper.createObjectNode();
            payload.put("id", id);
            payload.put("method", method);
            if (!params.isEmpty()) {
                payload.set("params", mapper.valueToTree(params));
            }

            webSocket.sendText(payload.toString(), true).join();
            JsonNode response = responseFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            validateWsResponse(response, method);
            return response.get("result");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exceptionHandler.accept(e);
            throw new BinanceApiRequestException(e);
        } catch (Exception e) {
            exceptionHandler.accept(e);
            throw new BinanceApiRequestException(e);
        } finally {
            pendingRequests.remove(id);
        }
    }

    private void validateWsResponse(@NotNull JsonNode response, @NotNull String method) {
        int status = response.has("status") ? response.get("status").asInt() : 0;
        if (status == 200 && response.has("result")) {
            return;
        }

        String errorMessage = response.has("error")
                ? response.get("error").toString()
                : response.toString();
        IllegalStateException exception = new IllegalStateException("Error Binance WS Request (" + method + "): " + errorMessage);
        exceptionHandler.accept(exception);
        throw exception;
    }
}
