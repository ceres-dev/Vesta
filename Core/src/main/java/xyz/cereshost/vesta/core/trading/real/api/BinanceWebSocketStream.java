package xyz.cereshost.vesta.core.trading.real.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.exception.BinanceApiRequestException;
import xyz.cereshost.vesta.core.trading.Endpoints;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;

import java.net.URI;
import java.net.http.WebSocket;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

public final class BinanceWebSocketStream extends BinanceWebSocket {

    private static final int MAX_STREAMS_PER_SUBSCRIBE = 200;

    private final WebSocket webSocket;
    private final Object streamIncomingLock = new Object();
    private final StringBuilder streamIncomingMessage = new StringBuilder();
    private final Set<String> subscribedBookTickerStreams = ConcurrentHashMap.newKeySet();
    private final List<Consumer<BookTicker>> bookTickerListeners = new CopyOnWriteArrayList<>();

    BinanceWebSocketStream(boolean isTestNet) {
        super(isTestNet ? Endpoints.STREAM_WSS_TEST : Endpoints.STREAM_WSS);
        this.webSocket = client.newWebSocketBuilder()
                .buildAsync(URI.create(endpoint.getEndpoint()), newListener())
                .join();
    }

    public void subscribeIndividualSymbolBookTickerStreams(
            @NotNull Collection<String> symbols,
            @NotNull Consumer<BookTicker> onBookTicker
    ) {
        if (symbols.isEmpty()) {
            return;
        }
        bookTickerListeners.add(onBookTicker);

        List<String> streams = new ArrayList<>();
        for (String symbol : symbols) {
            if (symbol == null || symbol.isBlank()) {
                continue;
            }
            String stream = symbol.toLowerCase(Locale.ROOT) + "@bookTicker";
            if (subscribedBookTickerStreams.add(stream)) {
                streams.add(stream);
            }
        }

        if (streams.isEmpty()) {
            return;
        }

        for (int i = 0; i < streams.size(); i += MAX_STREAMS_PER_SUBSCRIBE) {
            int end = Math.min(i + MAX_STREAMS_PER_SUBSCRIBE, streams.size());
            sendStreamControlRequest("SUBSCRIBE", List.copyOf(streams.subList(i, end)));
        }
    }

    public void removeBookTickerListener(@NotNull Consumer<BookTicker> listener) {
        bookTickerListeners.remove(listener);
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
                String contentToParse = accumulateMessage(data, last, streamIncomingLock, streamIncomingMessage);
                if (contentToParse != null) {
                    handleStreamMessage(contentToParse);
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
                        new IllegalStateException("WebSocket Stream cerrado: " + statusCode + " " + reason)
                );
                return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
            }
        };
    }

    private void handleStreamMessage(@NotNull String contentToParse) {
        try {
            JsonNode response = mapper.readTree(contentToParse);
            JsonNode idNode = response.get("id");
            if (idNode != null) {
                CompletableFuture<JsonNode> pending = pendingRequests.remove(idNode.asText());
                if (pending != null) {
                    pending.complete(response);
                }
                return;
            }

            JsonNode payload = response.has("data") ? response.get("data") : response;
            BookTicker bookTicker = parseBookTickerFromStreamPayload(payload);
            if (bookTicker == null) {
                return;
            }

            for (Consumer<BookTicker> listener : bookTickerListeners) {
                try {
                    listener.accept(bookTicker);
                } catch (Exception e) {
                    exceptionHandler.accept(e);
                }
            }
        } catch (Exception e) {
            exceptionHandler.accept(e);
        }
    }

    private @Nullable BookTicker parseBookTickerFromStreamPayload(@NotNull JsonNode payload) {
        String symbol = textOrNull(payload, "s");
        if (symbol == null) {
            return null;
        }

        Double bidPrice = doubleOrNull(payload, "b");
        Double askPrice = doubleOrNull(payload, "a");
        if (bidPrice == null || askPrice == null) {
            return null;
        }

        Double bidQty = doubleOrNull(payload, "B");
        Double askQty = doubleOrNull(payload, "A");
        return new BookTicker(symbol, bidPrice, bidQty, askPrice, askQty);
    }

    private @Nullable String textOrNull(@NotNull JsonNode payload, @NotNull String field) {
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        String text = node.asText();
        return text == null || text.isBlank() ? null : text;
    }

    private @Nullable Double doubleOrNull(@NotNull JsonNode payload, @NotNull String field) {
        JsonNode node = payload.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            try {
                return Double.parseDouble(node.asText());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private @NotNull JsonNode sendStreamControlRequest(@NotNull String method, @NotNull List<String> params) {
        String id = UUID.randomUUID().toString();
        CompletableFuture<JsonNode> responseFuture = new CompletableFuture<>();
        pendingRequests.put(id, responseFuture);

        try {
            ObjectNode payload = mapper.createObjectNode();
            payload.put("id", id);
            payload.put("method", method);
            payload.set("params", mapper.valueToTree(params));

            webSocket.sendText(payload.toString(), true).join();
            JsonNode response = responseFuture.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            validateStreamControlResponse(response, method);
            return response;
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

    private void validateStreamControlResponse(@NotNull JsonNode response, @NotNull String method) {
        if (!response.has("error")) {
            return;
        }
        String errorMessage = response.get("error").toString();
        IllegalStateException exception = new IllegalStateException(
                "Error Binance Stream (" + method + "): " + errorMessage
        );
        exceptionHandler.accept(exception);
        throw exception;
    }

    @Override
    public void checkApikey() {

    }

    @Override
    public @NotNull HashMap<String, Double> getBalance(@NotNull Boolean isFuture) {
        throw new UnsupportedOperationException();
    }
}
