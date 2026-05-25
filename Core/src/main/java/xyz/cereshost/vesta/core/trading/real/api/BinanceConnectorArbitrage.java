package xyz.cereshost.vesta.core.trading.real.api;

import com.binance.connector.client.common.ApiException;
import com.binance.connector.client.common.configuration.SignatureConfiguration;
import com.binance.connector.client.common.websocket.adapter.stream.StreamConnectionWrapper;
import com.binance.connector.client.common.websocket.configuration.WebSocketClientConfiguration;
import com.binance.connector.client.common.websocket.dtos.BaseDTO;
import com.binance.connector.client.common.websocket.dtos.RequestWrapperDTO;
import com.binance.connector.client.common.websocket.service.StreamBlockingQueue;
import com.binance.connector.client.spot.websocket.api.SpotWebSocketApiUtil;
import com.binance.connector.client.spot.websocket.api.api.SpotWebSocketApi;
import com.binance.connector.client.spot.websocket.api.model.*;
import com.binance.connector.client.spot.websocket.stream.SpotWebSocketStreamsUtil;
import com.binance.connector.client.spot.websocket.stream.api.SpotWebSocketStreams;
import com.binance.connector.client.spot.websocket.stream.model.BookTickerResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.io.IOdata;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.MarketStatus;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.trading.Endpoints;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;
import xyz.cereshost.vesta.core.trading.real.api.model.OrderResult;
import xyz.cereshost.vesta.core.trading.real.api.model.Ticker24H;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;

public final class BinanceConnectorArbitrage implements AutoCloseable {

    private static final int MAX_STREAMS_PER_SUBSCRIBE = 200;
    private static final int TIMEOUT = 20;

    private final SpotWebSocketApi api;
    private final SpotWebSocketStreams streams;
    private final StreamConnectionWrapper streamConnection;
    private final List<StreamBlockingQueue<String>> bookTickerQueues = new CopyOnWriteArrayList<>();
    private final List<Consumer<BookTicker>> bookTickerListeners = new CopyOnWriteArrayList<>();
    private final Executor streamExecutor;

    private volatile boolean bookTickerReaderRunning = false;
    @Nullable private volatile ExchangeInfo exchangeInfoSpot = null;

    public BinanceConnectorArbitrage(boolean isTestNet, @NotNull Executor streamExecutor) {
        this.streamExecutor = streamExecutor;

        WebSocketClientConfiguration apiConfiguration = SpotWebSocketApiUtil.getClientConfiguration();
        if (isTestNet) {
            apiConfiguration.setUrl(Endpoints.API_WSS_TEST.getEndpoint());
        }else apiConfiguration.setUrl(Endpoints.API_WSS.getEndpoint());

        IOdata.ApiKeysBinance apiKeys = IOdata.loadApiKeysBinance();
        SignatureConfiguration signatureConfiguration = new SignatureConfiguration();
        signatureConfiguration.setApiKey(apiKeys.key());
        signatureConfiguration.setSecretKey(apiKeys.secret());
        apiConfiguration.setSignatureConfiguration(signatureConfiguration);
        apiConfiguration.setMessageMaxSize((long) (Integer.MAX_VALUE));

        try {
            this.api = new SpotWebSocketApi(apiConfiguration);
        }catch (ApiException e){
            throw new  RuntimeException(e);
        }

        WebSocketClientConfiguration streamConfiguration = SpotWebSocketStreamsUtil.getClientConfiguration();
        streamConfiguration.setUsePool(true);
        streamConfiguration.setPoolSize(4);
        if (isTestNet) streamConfiguration.setUrl(Endpoints.STREAM_WSS_TEST.getEndpoint());
        else streamConfiguration.setUrl(Endpoints.STREAM_WSS.getEndpoint());
        this.streamConnection = new StreamConnectionWrapper(
                streamConfiguration,
                com.binance.connector.client.spot.websocket.stream.JSON.getGson()
        )
        {
//            @Override
//            public void onWebSocketText(String message) {
//                JsonElement root = JsonParser.parseString(message);
//                JsonObject obj = root.getAsJsonObject();
//                if (handleShutdownMessage(obj)) {
//                    return;
//                }
//
//                // Response to subscribe
//                JsonElement id = obj.get("id");
//                if (id == null) return;
//                RequestWrapperDTO<?, ?> requestWrapperDTO = pendingRequest.get(id.getAsString());
//                if (requestWrapperDTO == null) return;
//                super.onWebSocketText(message);
//            }

            private volatile int hashCodeLast = 0;

            @Override
            public void innerSend(RequestWrapperDTO requestWrapperDTO) {
                int hashCode = requestWrapperDTO.hashCode();
                if (hashCode != hashCodeLast) {
                    hashCodeLast = hashCode;
                    send(requestWrapperDTO);
                }
            }
        }
        ;
        this.streams = new SpotWebSocketStreams(streamConnection);
    }

    @SneakyThrows
    public void checkApikey() {
        checkResult(
                api.accountStatus(new AccountStatusRequest().omitZeroBalances(true).recvWindow(20_000.0)).get(TIMEOUT, TimeUnit.SECONDS)
        );
    }

    public void invalidedCache() {
        exchangeInfoSpot = null;
    }

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    public @NotNull ExchangeInfo getExchangeInfo() {
        ExchangeInfo cached = exchangeInfoSpot;
        if (cached != null) {
            return cached;
        }

        var response = api.exchangeInfo(new ExchangeInfoRequest().showPermissionSets(true)).get(TIMEOUT, TimeUnit.SECONDS);
        HashMap<String, SymbolConfigurable> symbols = new HashMap<>();
        checkResult(response);
        for (ExchangeInfoResponseResultSymbolsInner symbol : response.getResult().getSymbols()) {
            symbols.put(symbol.getSymbol(), toSymbolConfigurable(symbol));
        }
        exchangeInfoSpot = new ExchangeInfo(List.of(), symbols);
        return exchangeInfoSpot;
    }

    @SuppressWarnings("DataFlowIssue")
    @SneakyThrows
    public @NotNull Map<String, BookTicker> getBookTickers() {
        Map<String, BookTicker> result = new HashMap<>();
        Object actual = api.tickerBook(new TickerBookRequest()).get(30, TimeUnit.SECONDS).getActualInstance();
        if (actual instanceof com.binance.connector.client.spot.websocket.api.model.TickerBookResponse2 response) {
            for (TickerBookResponse1Result ticker : response.getResult()) {
                result.put(ticker.getSymbol(), toBookTicker(ticker));
            }
        } else if (actual instanceof com.binance.connector.client.spot.websocket.api.model.TickerBookResponse1 response) {
            TickerBookResponse1Result ticker = response.getResult();
            result.put(ticker.getSymbol(), toBookTicker(ticker));
        }
        return result;
    }

    @SneakyThrows
    public @NotNull Set<Ticker24H> getTicker24H() {
        Set<Ticker24H> result = new HashSet<>();
        Object actual = api.ticker24hr(new Ticker24hrRequest()).get(30, TimeUnit.SECONDS).getActualInstance();

        if (actual instanceof com.binance.connector.client.spot.websocket.api.model.Ticker24hrResponse2 response) {
            for (Ticker24hrResponse2ResultInner ticker : response.getResult()) {
                result.add(toTicker24H(ticker));
            }
        } else if (actual instanceof com.binance.connector.client.spot.websocket.api.model.Ticker24hrResponse1 response) {
            result.add(toTicker24H(response.getResult()));
        }
        return result;
    }

    @SneakyThrows
    @SuppressWarnings("DataFlowIssue")
    public @NotNull HashMap<String, Double> getBalance() {
        HashMap<String, Double> balances = new HashMap<>();
        var response = api.accountStatus(new AccountStatusRequest().recvWindow(20_000.0))
                .get(15, TimeUnit.SECONDS);
        checkResult(response);
        for (AccountStatusResponseResultBalancesInner balance : response.getResult().getBalances()) {
            balances.put(balance.getAsset(), parseDouble(balance.getFree()));
        }
        return balances;
    }

    @SneakyThrows
    public @NotNull OrderResult placeMarketOrder(@NotNull Symbol symbol,
                                                 @NotNull DireccionOperation side,
                                                 @NotNull Double amount,
                                                 @NotNull Boolean useQuantity
    ) {
        OrderPlaceRequest request = new OrderPlaceRequest()
                .symbol(symbol.name())
                .side(side.isLong() ? Side.BUY : Side.SELL)
                .type(OrderType.MARKET)
                .newOrderRespType(NewOrderRespType.FULL)
                .recvWindow(20_000.0);
        checkResult(request);
        if (useQuantity) {
            request.quantity(Double.valueOf(symbol.formatQuantity(amount)));
        } else {
            request.quoteOrderQty(Double.valueOf(symbol.formatQuoteOrderQty(amount)));
        }

        OrderPlaceResponseResult order = api.orderPlace(request).get(30, TimeUnit.SECONDS).getResult();
        double executedQty = parseDouble(order.getExecutedQty());
        double cumulativeQuoteQty = parseDouble(order.getCummulativeQuoteQty());
        double receivedQty = side.isLong() ? executedQty : cumulativeQuoteQty;
        receivedQty -= getCommissionPaidInReceivedAsset(order, side.isLong() ? symbol.getBaseAsset() : symbol.getQuoteAsset());

        return new OrderResult(
                order.getOrderId(),
                executedQty,
                cumulativeQuoteQty,
                Math.max(0.0, receivedQty)
        );
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
            streams.add(symbol.toLowerCase(Locale.ROOT) + "@bookTicker");
        }

        for (int i = 0; i < streams.size(); i += MAX_STREAMS_PER_SUBSCRIBE) {
            int end = Math.min(i + MAX_STREAMS_PER_SUBSCRIBE, streams.size());
            subscribeBookTickerBatch(streams.subList(i, end));
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
        }
        ensureBookTickerReader();
    }

    public void removeBookTickerListener(@NotNull Consumer<BookTicker> listener) {
        bookTickerListeners.remove(listener);
        if (!bookTickerListeners.isEmpty()) {
            return;
        }
        for (StreamBlockingQueue<String> queue : bookTickerQueues) {
            streamConnection.unsubscribe(queue);
        }
        bookTickerQueues.clear();
        bookTickerReaderRunning = false;
    }

    private void subscribeBookTickerBatch(@NotNull List<String> streams) {
        if (streams.isEmpty()) {
            return;
        }
        UUID uuid = UUID.randomUUID();
        RequestWrapperDTO<Set<String>, Object> request = new RequestWrapperDTO.Builder<Set<String>, Object>()
                .id(uuid.toString())
                .method("SUBSCRIBE")
                .params(Set.copyOf(streams))
                .build();
        bookTickerQueues.addAll(streamConnection.subscribe(request).values());
    }

    private void ensureBookTickerReader() {
        if (bookTickerReaderRunning) {
            return;
        }
        bookTickerReaderRunning = true;

        for (StreamBlockingQueue<String> queue : bookTickerQueues) {
            streamExecutor.execute(() -> {
                while (bookTickerReaderRunning) {
                    try {
                        String payload = queue.take();
                        try {
                            BookTickerResponse response = BookTickerResponse.fromJson(payload);
                            BookTicker bookTicker = toBookTicker(response);
                            for (Consumer<BookTicker> listener : bookTickerListeners) {
                                listener.accept(bookTicker);
                            }
                        } catch (Exception e) {
                            Vesta.sendWaringException("Error procesando bookTicker del conector de Binance", e);
                        }
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

//        CompletableFuture.runAsync(this::readBookTickerQueues, streamExecutor);
    }

    private void readBookTickerQueues() {
//        while (bookTickerReaderRunning) {
////            boolean consumed = false;
//            for (StreamBlockingQueue<String> queue : bookTickerQueues) {
//
//            }
////            if (!consumed) {
////                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
////            }
//        }
    }

    private @NotNull SymbolConfigurable toSymbolConfigurable(@NotNull ExchangeInfoResponseResultSymbolsInner symbol) {
        Set<String> permissions = new HashSet<>();
        if (symbol.getPermissions() != null) {
            permissions.addAll(symbol.getPermissions());
        }
        if (symbol.getPermissionSets() != null && !symbol.getPermissionSets().isEmpty()) {
            permissions.addAll(symbol.getPermissionSets().getFirst());
        }

        return new SymbolConfigurable(
                symbol.getSymbol(),
                false,
                false,
                true,
                Objects.requireNonNullElse(symbol.getQuoteAssetPrecision(), symbol.getQuotePrecision()).intValue(),
                symbol.getQuotePrecision().intValue(),
                MarketStatus.valueOf(symbol.getStatus()),
                symbol.getBaseAsset(),
                symbol.getQuoteAsset(),
                Boolean.TRUE.equals(symbol.getIsSpotTradingAllowed()),
                extractStepSize(symbol),
                permissions
        );
    }

    private @Nullable Double extractStepSize(@NotNull ExchangeInfoResponseResultSymbolsInner symbol) {
        if (symbol.getFilters() == null) {
            return null;
        }
        for (SymbolFilters filter : symbol.getFilters()) {
            Object actual = filter.getActualInstance();
            if (actual instanceof LotSizeFilter lotSizeFilter) {
                return parseDouble(lotSizeFilter.getStepSize());
            }
        }
        return null;
    }

    private @NotNull BookTicker toBookTicker(@NotNull TickerBookResponse1Result ticker) {
        return new BookTicker(
                ticker.getSymbol(),
                parseDouble(ticker.getBidPrice()),
                parseDouble(ticker.getBidQty()),
                parseDouble(ticker.getAskPrice()),
                parseDouble(ticker.getAskQty())
        );
    }

    @SneakyThrows
    private @NotNull BookTicker toBookTicker(@NotNull BookTickerResponse ticker) {
        String json = ticker.toJson();
        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(json);
        return new BookTicker(
                root.get("s").asText(),
                root.get("b").asDouble(),
                root.get("B").asDouble(),
                root.get("a").asDouble(),
                root.get("A").asDouble()
        );
    }

    private @NotNull Ticker24H toTicker24H(@NotNull Ticker24hrResponse1Result ticker) {
        return new Ticker24H(
                ticker.getSymbol(),
                parseDouble(ticker.getPriceChange()),
                parseDouble(ticker.getPriceChangePercent()),
                parseDouble(ticker.getQuoteVolume()),
                parseDouble(ticker.getVolume())
        );
    }

    private @NotNull Ticker24H toTicker24H(@NotNull Ticker24hrResponse2ResultInner ticker) {
        return new Ticker24H(
                ticker.getSymbol(),
                parseDouble(ticker.getPriceChange()),
                parseDouble(ticker.getPriceChangePercent()),
                parseDouble(ticker.getQuoteVolume()),
                parseDouble(ticker.getVolume())
        );
    }

    private double getCommissionPaidInReceivedAsset(@NotNull OrderPlaceResponseResult order, @NotNull String receivedAsset) {
        if (order.getFills() == null) {
            return 0.0;
        }

        double commission = 0.0;
        for (OrderPlaceResponseResultFillsInner fill : order.getFills()) {
            if (receivedAsset.equals(fill.getCommissionAsset())) {
                commission += parseDouble(fill.getCommission());
            }
        }
        return commission;
    }

    private double parseDouble(@Nullable String value) {
        if (value == null || value.isBlank()) {
            return 0.0;
        }
        return Double.parseDouble(value);
    }

    @Override
    public void close() {
        bookTickerReaderRunning = false;
        try {
            streams.stop();
        } catch (Exception e) {
            throw new ApiException(e);
        }
        try {
            api.stop();
        } catch (Exception e) {
            throw new ApiException(e);
        }
    }

    public void checkResult(BaseDTO result){
        if (result.getError() != null && result.getError().getCode() != 200) {
            throw new IllegalStateException(result.getError().getMsg());
        }
    }
}
