package xyz.cereshost.vesta.core.trading.real.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.exception.BinanceApiRequestException;
import xyz.cereshost.vesta.core.exception.BinanceApiSignedRequestException;
import xyz.cereshost.vesta.core.exception.BinanceCodeException;
import xyz.cereshost.vesta.core.exception.BinanceCodeWeakException;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.message.MediaNotification;
import xyz.cereshost.vesta.core.trading.Endpoints;
import xyz.cereshost.vesta.core.trading.TimeInForce;
import xyz.cereshost.vesta.core.trading.TypeOrder;
import xyz.cereshost.vesta.core.trading.real.api.model.*;
import xyz.cereshost.vesta.core.utils.LoaderIndicator;

import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * <a href="https://developers.binance.com/docs/binance-spot-api-docs/rest-api/general-api-information">ApiRest de Binance</a>
 */

@Getter
@Setter
public final class BinanceApiRest extends BaseConnector implements BinanceApi {

    private final Endpoints futureBaseUrl;
    private final Endpoints spotBaseUrl;
    // Errores "idempotentes" que no deben detener el loop (ej: cancelar una orden ya cerrada).
    private static final List<Integer> WEAK_ERROS_CODE = List.of(-2021, -2011, -5022);

    @NotNull private MediaNotification mediaNotification = MediaNotification.empty();
    @NotNull private Consumer<Exception> exceptionHandler = e -> {};

    public BinanceApiRest(boolean isTestNet, boolean useBest) {
        super(Endpoints.FAPI);

        this.futureBaseUrl = isTestNet ? Endpoints.DEMO_FAPI : Endpoints.FAPI;
        this.spotBaseUrl = isTestNet ? Endpoints.TESTNET : useBest ? getBestEndpoint() : Endpoints.API;
    }

    @Override
    public Long placeAlgoOrder(@NotNull Symbol symbol,
                                     @NotNull DireccionOperation side,
                                     @NotNull TypeOrder type,
                                     @Nullable TimeInForce timeInForce,
                                     @Nullable Double quantity,
                                     @NotNull Double stopPrice,
                                     @NotNull Boolean reduceOnly,
                                     @NotNull Boolean closePosition
    ) {
//        if (!type.isValidValue(stopPrice, stopPrice)) throw new IllegalArgumentException();
        TreeMap<String, String> params = new TreeMap<>();
        params.put("algoType", "CONDITIONAL");          // Obligatorio para órdenes condicionales
        params.put("symbol", symbol.name());
        params.put("side", side.getSide());
        params.put("type", type.name());
        params.put("price", symbol.formatPrice(stopPrice));
        if (type.isAlgo()) throw new IllegalArgumentException("Se requiere TimeInForce para ordenes Limites");
        if (type.isLimit()) {
            if (timeInForce == null) throw new IllegalArgumentException("Se requiere TimeInForce para ordenes Limites");
            params.put("timeInForce", timeInForce.name());
            if (reduceOnly) params.put("reduceOnly", "true");
        }
        if (closePosition) {
            params.put("closePosition", "true");
        }else {
            if (quantity == null) throw new IllegalArgumentException("Se requiere quantity para ordenes Limites");
            params.put("quantity", symbol.formatQuantity(quantity));
        }

        // Si se quiere cerrar la posición completa, se usa closePosition y NO se envía quantity

        // Precio de activación (obligatorio para STOP_MARKET/TAKE_PROFIT_MARKET)
        params.put("triggerPrice", symbol.formatPrice(stopPrice));
        // WorkingType (opcional, pero recomendado)
        params.put("workingType", "MARK_PRICE");

        // Usar el endpoint de órdenes algorítmicas
        JsonNode root = sendSignedRequest("POST", "/fapi/v1/algoOrder", params);
        return root.get("algoId").asLong();
    }


    @Override
    public @NotNull OrderResult placeOrder(@NotNull Symbol symbol,
                                    @NotNull DireccionOperation side,
                                    @NotNull TypeOrder type,
                                    @Nullable TimeInForce timeInForce,
                                    @NotNull Double amount,
                                    @Nullable Double price,
                                    @NotNull Boolean reduceOnly,
                                    @NotNull Boolean closePosition,
                                    @NotNull Boolean useQuantity
    ) {
        // Para órdenes no condicionales (MARKET, LIMIT), seguir usando el endpoint tradicional
        if (!type.isValidValue(null, price)) throw new IllegalArgumentException();
        TreeMap<String, String> params = new TreeMap<>();
        params.put("symbol", symbol.name());
        params.put("side", side.getSide());
        params.put("type", type.name());
        params.put("newOrderRespType", symbol.getIsFuture() ? "RESULT" : "FULL");
        if (useQuantity){
            params.put("quantity", symbol.formatQuantity(amount));
            Vesta.info("quantity: " + symbol.formatQuantity(amount) + " " + symbol.getStepSize().orElse(null));
        }else {
            params.put("quoteOrderQty", symbol.formatQuoteOrderQty(amount));
            Vesta.info("quoteOrderQty: " + symbol.formatQuoteOrderQty(amount) + " " + symbol.getStepSize().orElse(null));
        }
        if (
                symbol.getIsFuture() &&
                (type.isTakeProfit() || type.isStopLoss())
        ) params.put("closePosition", String.valueOf(closePosition));
        if (type.isLimit()) {
            if (timeInForce == null) throw new IllegalArgumentException("Se requiere TimeInForce para ordenes Limites");
            if (price == null) throw new IllegalArgumentException("Se requiere Price para ordenes Limites");

            params.put("timeInForce", timeInForce.name());
            params.put("price", symbol.formatPrice(price));
        }
        if (reduceOnly && symbol.getIsFuture()) params.put("reduceOnly", "true");
        JsonNode root = symbol.getIsFuture() ?
                sendSignedRequest("POST", "/fapi/v1/order", params) :
                sendSignedRequest("POST", "/api/v3/order", params);
        double executedQty = getDouble(root, "executedQty");
        double cumulativeQuoteQty = getDouble(root, "cummulativeQuoteQty", "cumQuote");
        double receivedQty = side.isLong() ? executedQty : cumulativeQuoteQty;
        receivedQty -= getCommissionPaidInReceivedAsset(root, side.isLong() ? symbol.getBaseAsset() : symbol.getQuoteAsset());
        JsonNode fills = root.get("fills");
        if (fills != null && fills.isArray()) {
            for (JsonNode fill : fills) {
                Vesta.info("Commission: %s %s",
                        fill.get("commission").asText(),
                        fill.get("commissionAsset").asText()
                );
            }
        }
        return new OrderResult(
                root.get("orderId").asLong(),
                executedQty,
                cumulativeQuoteQty,
                Math.max(0.0, receivedQty)
                );
    }

    private double getDouble(@NotNull JsonNode root, @NotNull String... fieldNames) {
        for (String fieldName : fieldNames) {
            JsonNode node = root.get(fieldName);
            if (node != null && !node.isNull()) {
                String value = node.asText();
                if (!value.isBlank()) return Double.parseDouble(value);
            }
        }
        return 0.0;
    }

    private double getCommissionPaidInReceivedAsset(@NotNull JsonNode root, @NotNull String receivedAsset) {
        JsonNode fills = root.get("fills");
        if (fills == null || !fills.isArray()) return 0.0;

        double commission = 0.0;
        for (JsonNode fill : fills) {
            JsonNode commissionAsset = fill.get("commissionAsset");
            if (commissionAsset != null && receivedAsset.equals(commissionAsset.asText())) {
                commission += getDouble(fill, "commission");
            }
        }
        return commission;
    }
    @Override
    public void cancelOrder(@NotNull Symbol symbol, @NotNull Long orderId, @NotNull Boolean isAlgoOrder) {
        try {
            if (orderId == 0) return;
            TreeMap<String, String> params = new TreeMap<>();
            params.put("symbol", symbol.name());
            if (isAlgoOrder) {
                params.put("algoId", String.valueOf(orderId));
                sendSignedRequest("DELETE", "/fapi/v1/algoOrder", params);
            } else {
                params.put("orderId", String.valueOf(orderId));
                sendSignedRequest("DELETE", "/fapi/v1/order", params);
            }
        } catch (Exception e) {
            Vesta.warning("No se pudo cancelar orden " + orderId + ": " + e.getMessage());
        }
    }

    @Override
    public @NotNull List<OrderData> getAllOrdersFuture(@NotNull Symbol symbol) {
        TreeMap<String, String> params = new TreeMap<>();
        params.put("symbol", symbol.name());
        ArrayList<OrderData> operations = new ArrayList<>();
        List<JsonNode> nodes = new ArrayList<>();
        for (JsonNode node :  sendSignedRequest("GET", "/fapi/v1/openOrders", params)) nodes.add(node);
        for (JsonNode node :  sendSignedRequest("GET", "/fapi/v1/openAlgoOrders", params)) nodes.add(node);
        for (JsonNode node : nodes) {
            long orderId = node.get("orderId") == null ?  node.get("algoId").asLong() : node.get("orderId").asLong();
            Double price = node.get("price").asDouble();
            Double triggerPrice = node.get("triggerPrice") == null ? null : node.get("triggerPrice").asDouble();
            Double quantity = node.get("origQty") == null ? node.get("quantity").asDouble() : node.get("origQty").asDouble();
            TimeInForce timeInForce = TimeInForce.valueOf(node.get("timeInForce").asText());
            TypeOrder typeOrder = TypeOrder.valueOf(node.get("type") == null ? node.get("orderType").asText() : node.get("type").asText());
            DireccionOperation side = DireccionOperation.parse(node.get("side").asText());
            Boolean isAlgoOrder = node.get("algoType") != null;
            operations.add(new OrderData(orderId,
                    price,
                    triggerPrice,
                    quantity,
                    isAlgoOrder,
                    timeInForce,
                    typeOrder,
                    side
            ));
        }

        return operations;
    }

    @Override
    public PositionData getPosition(@NotNull Symbol symbol) {
        TreeMap<String, String> params = new TreeMap<>();
        params.put("symbol", symbol.name());
        JsonNode operationsV2 = sendSignedRequest("GET", "/fapi/v2/positionRisk", params);
        JsonNode operationsV3 = sendSignedRequest("GET", "/fapi/v3/positionRisk", params);
        JsonNode operationV2 = Objects.requireNonNull(operationsV2).get(0);
        JsonNode operationV3 = Objects.requireNonNull(operationsV3).get(0);
        PositionData positionData;
        DireccionOperation direccionOperation = DireccionOperation.parse(operationV2.get("positionAmt").asDouble());
        if (direccionOperation != DireccionOperation.NEUTRAL){
            positionData = new PositionData(
                    Double.valueOf(operationV2.get("entryPrice").asText()),
                    Double.valueOf(operationV3.get("positionInitialMargin").asText()),
                    Integer.valueOf(operationV2.get("leverage").asText()),
                    direccionOperation
            );
        }else {
            positionData = null;
        }
        return positionData;
    }

    @Override
    public void closeAll(@NotNull Symbol symbol) {
        try {
            // 1. Obtener posiciones actuales
            TreeMap<String, String> params = new TreeMap<>();
            params.put("symbol", symbol.name());
            JsonNode positions = sendSignedRequest("GET", "/fapi/v2/positionRisk", params);

            for (JsonNode position : Objects.requireNonNull(positions)) {
                String posSymbol = position.get("symbol").asText();
                if (symbol.toString().equals(posSymbol)) {
                    double positionAmt = position.get("positionAmt").asDouble();
                    if (Math.abs(positionAmt) > 0) {
                        Vesta.warning("Cerrando posición existente: " + positionAmt + " " + symbol);

                        String side = positionAmt > 0 ? "SELL" : "BUY";
                        TreeMap<String, String> closeParams = new TreeMap<>();
                        closeParams.put("symbol", symbol.name());
                        closeParams.put("side", side);
                        closeParams.put("type", "MARKET");
                        closeParams.put("quantity", String.valueOf(Math.abs(positionAmt)));
                        closeParams.put("reduceOnly", "true");

                        sendSignedRequest("POST", "/fapi/v1/order", closeParams);
                    }
                }
            }

            // 2. Cancelar todas las órdenes abiertas
            TreeMap<String, String> cancelParams = new TreeMap<>();
            cancelParams.put("symbol", symbol.name());
            sendSignedRequest("DELETE", "/fapi/v1/allOpenOrders", cancelParams);
        } catch (Exception e) {
            Vesta.error("Error cerrando posiciones existentes: " + e.getMessage());
        }
    }

    @Override
    public void changeLeverage(@NotNull Symbol symbol, @NotNull Integer leverage) {
        TreeMap<String, String> params = new TreeMap<>();
        params.put("symbol", symbol.toString());
        params.put("leverage", String.valueOf(leverage));
        sendSignedRequest("POST", "/fapi/v1/leverage", params);
    }

    @Override
    public void checkApikey() {
        sendSignedRequest("GET", "/api/v3/account", new TreeMap<>());
    }

    @Override
    public void invalidedCache() {
        exchangeInfoFuture = null;
        exchangeInfoSpot = null;
    }

    @Override
    public @NotNull Double getTickerPrice(@NotNull Symbol symbol) {
        TreeMap<String, String> params = new TreeMap<>();
        params.put("symbol", symbol.name());
        JsonNode root = symbol.getIsFuture() ?
                sendPublicRequest("GET", "/fapi/v1/ticker/price", params) :
                sendPublicRequest("GET", "/api/v1/ticker/price", params);
        return root.get("price").asDouble();
    }

    @Override
    public @NotNull Map<String, BookTicker> getBookTickers(@Nullable Symbol symbol, @Nullable Boolean isFuture) {
        TreeMap<String, String> params = new TreeMap<>();
        Boolean future = null;

        if (isFuture != null) future = isFuture;
        if (symbol != null) future = symbol.getIsFuture();
        if (future == null) throw new IllegalArgumentException("Symbol future is null");

        JsonNode root = future ?
                sendPublicRequest("GET", "/fapi/v3/ticker/bookTicker", params) :
                sendPublicRequest("GET", "/api/v3/ticker/bookTicker", params);
        return ParseJsonApi.parseBookTickers(root);
    }

    @Nullable private ExchangeInfo exchangeInfoFuture = null;
    @Nullable private ExchangeInfo exchangeInfoSpot = null;
    @Nullable private CompletableFuture<ExchangeInfo> task = null;

    @Override
    public @NotNull ExchangeInfo getExchangeInfo(@NotNull Boolean isFuture) {
        JsonNode exchangeInfo;

        if (exchangeInfoFuture != null && isFuture) {
            return exchangeInfoFuture;
        }
        if (exchangeInfoSpot != null && !isFuture) {
           return exchangeInfoSpot;
        }

        if (isFuture) exchangeInfo = sendPublicRequest("GET", "/fapi/v1/exchangeInfo", new TreeMap<>());
        else exchangeInfo = sendPublicRequest("GET", "/api/v3/exchangeInfo", new TreeMap<>());

        ExchangeInfo result = ParseJsonApi.parseExchangeInfo(exchangeInfo, isFuture);

        if (isFuture) exchangeInfoFuture = result;
        else exchangeInfoSpot = result;
        return result;
    }

    private static final HashMap<String, SymbolConfigurable> cacheSymbols = new HashMap<>();

    @Override
    public @NotNull SymbolConfigurable getSymbolConfigured(@NotNull String symbol, @NotNull Boolean shouldFuture) {
        return cacheSymbols.computeIfAbsent((shouldFuture ? "F-" : "S-") + symbol, s -> {
            ExchangeInfo exchangeInfo = getExchangeInfo(shouldFuture);
            for (SymbolConfigurable symbolC : exchangeInfo.symbols().values()){
                if (symbolC.name().equals(symbol)){
                    return symbolC;
                }
            }
            throw new IllegalStateException("No such symbol: " + symbol);
        });
    }

    @Override
    public @NotNull Double getBalance(@NotNull Symbol symbol) {
        symbol.configure(this);
        String quoteAsset = symbol.getQuoteAsset();
        if (symbol.getIsFuture()) {
            JsonNode root = sendSignedRequest("GET", "/fapi/v3/account", new TreeMap<>());
            for (JsonNode assetNode : root.get("assets")) {
                if (quoteAsset.equals(assetNode.get("asset").asText())) {
                    double balance = assetNode.get("availableBalance").asDouble();
                    Vesta.info("💰 Balance detectado para " + quoteAsset + ": " + balance);
                    return balance;
                }
            }
        }else {
            JsonNode root = sendSignedRequest("GET", "/api/v3/account", new TreeMap<>());
            for (JsonNode assetNode : root.get("balances")) {
                if (quoteAsset.equals(assetNode.get("asset").asText())) {
                    double balance = assetNode.get("free").asDouble();
                    Vesta.info("💰 Balance detectado para " + quoteAsset + ": " + balance);
                    return balance;
                }
            }
        }
        return 0.0;
    }

    @Override
    public @NotNull HashMap<String, Double> getBalance(@NotNull Boolean isFuture) {
        JsonNode node;
        if (isFuture) node = sendSignedRequest("GET", "/fapi/v3/account", new TreeMap<>());
        else node = sendSignedRequest("GET", "/api/v3/account", new TreeMap<>());

        return ParseJsonApi.parseBalance(node, isFuture);
    }

    @Override
    public @NotNull Set<Ticker24H> getTicker24H(@Nullable Symbol symbol) {
        JsonNode node;
        if (symbol == null) node = sendPublicRequest("GET", "/api/v3/ticker/24hr", Map.of());
        else {
            if (symbol.getIsFuture()) throw new UnsupportedOperationException("not implemented yet");
            node = sendPublicRequest("GET", "/api/v3/ticker/24hr", Map.of("symbol", symbol.name()));
        }
        return ParseJsonApi.parseTicker24H(node);
    }

    @Override
    public void signContract() {
        sendSignedRequest("POST", "/fapi/v1/stock/contract", new TreeMap<>());
    }

    private @NotNull JsonNode sendSignedRequest(@NotNull String method, String endpoint, TreeMap<String, String> params) throws BinanceApiSignedRequestException {
        params.put("timestamp", String.valueOf(System.currentTimeMillis()));
        params.put("recvWindow", "20000");
        try {
            String queryString = buildQueryString(params);
            String signature = hmacSha256(queryString, apiKey.secret());
            String finalUrl = getBaseURL(endpoint) + endpoint + "?" + queryString + "&signature=" + signature;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(finalUrl))
                    .header("X-MBX-APIKEY", apiKey.key())
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            // El EndPoint /fapi/v1/stock/contract solo retornar un "SUCCESS" en texto plano (no json)
            if (response.body().equals("SUCCESS")) {
                return mapper.readTree("{}");
            }
            JsonNode root = mapper.readTree(response.body());
            String symbolName = params.get("symbol");
            Symbol symbol = null;
            if (symbolName != null && endpoint.startsWith("/fapi")) {
                symbol = Symbol.valueOf(symbolName);
            }
            checkRepose(symbol, root, method, endpoint);

            return root;
        }catch (Exception e) {
            exceptionHandler.accept(e);
            if (e instanceof BinanceCodeException binanceCodeException) {
                throw new BinanceApiSignedRequestException(e, binanceCodeException.getCode());
            }else {
                throw new BinanceApiSignedRequestException(e, -1);
            }
        }
    }

    private @NotNull JsonNode sendPublicRequest(@NotNull String method,
                                                @NotNull String endpoint,
                                                @NotNull Map<String, String> params) throws BinanceApiRequestException {
        try {
            String queryString = buildQueryString(params);
            String finalUrl = getBaseURL(endpoint) + (
                    queryString.isBlank()
                    ? endpoint
                    : endpoint + "?" + queryString
            );
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(finalUrl))
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .build();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(client.send(request, HttpResponse.BodyHandlers.ofString()).body());
            if (root.isObject()) {
                checkRepose(null, root, method, endpoint);
            }
            return root;
        } catch (Exception e) {
            exceptionHandler.accept(e);
            throw new BinanceApiRequestException(e);
        }
    }

    public void checkRepose(Symbol symbol, @NotNull JsonNode node, @NotNull String method, @NotNull String endpoint) throws BinanceCodeException {
        if (node.has("code") && node.get("code").asInt() != 0) {
            int code = node.get("code").asInt();
            if (code == 200) {
                Vesta.info("✅ operacion Ok (%s:%s)", method, endpoint);
            }else {
                if (WEAK_ERROS_CODE.contains(code)) {
                    if (code == -2021 && symbol != null && endpoint.startsWith("/fapi")) closeAll(symbol);
                    BinanceCodeWeakException e = new BinanceCodeWeakException(node, method, endpoint);
                    mediaNotification.waring("Error al hacer una petición: **%s**. Revisa la consola para más información", e.getMessage());
                    Vesta.sendWaringException("Error al hacer una petición" , e);
                    throw e;
                }else {
                    if (symbol != null && endpoint.startsWith("/fapi")) closeAll(symbol);
                    BinanceCodeException exception = new BinanceCodeException(node, method, endpoint);
                    mediaNotification.error("Error al hacer una petición: **%s**. Revisa la consola para más información", exception.getMessage());
                    exceptionHandler.accept(exception);
                    throw exception;
                }
            }
        }
    }

    private String getBaseURL(String endpoint) {
        if (endpoint.startsWith("/api")){
            return spotBaseUrl.getEndpoint();
        };
        if (endpoint.startsWith("/fapi")){
            return futureBaseUrl.getEndpoint();
        }
        throw new IllegalArgumentException("Base URL invalido: " +  endpoint);
    }

    @SneakyThrows
    private Endpoints getBestEndpoint(){
        HashMap<Endpoints, List<Long>> map = new HashMap<>();
        LoaderIndicator loaderIndicator = new LoaderIndicator(1);
        for (Endpoints endpoints : Arrays.stream(Endpoints.values()).filter(Endpoints::isSpot).filter(Endpoints::isReal).filter(Endpoints::isApiRest).toList()) {
            List<Long> list = map.computeIfAbsent(endpoints, key -> new ArrayList<>());
            loaderIndicator.setLabel("Analizando la conexión con: " + endpoints.getEndpoint());
            for (int i = 0; i < 12; i++){
                long current = System.currentTimeMillis();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(endpoints.getEndpoint() + "/api/v3/ping"))
                        .method("GET", HttpRequest.BodyPublishers.noBody())
                        .build();
                client.send(request, HttpResponse.BodyHandlers.ofString()).body();
                long diff = System.currentTimeMillis() - current;
                list.add(diff);
                loaderIndicator.printAndNexStep();
            }
        }
        loaderIndicator.done();
        HashMap<Endpoints, Double> result = new HashMap<>();
        map.forEach((key, value) -> {
            result.put(key, value.stream().mapToLong(Long::longValue).average().getAsDouble());
        });
        Map.Entry<Endpoints, Double> bestEntry = result.entrySet().stream().findFirst().orElse(null);
        for (Map.Entry<Endpoints, Double> entry : result.entrySet()) {
            if (entry.getValue() < bestEntry.getValue()) {
                bestEntry = entry;
            }
        }
        Vesta.info("Mejor conexión: %s (%dms)", bestEntry.getKey().getEndpoint(),  bestEntry.getValue().longValue());
        return bestEntry.getKey();
    }
}
