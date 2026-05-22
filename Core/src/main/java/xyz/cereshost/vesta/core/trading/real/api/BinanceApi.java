package xyz.cereshost.vesta.core.trading.real.api;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.message.Notifiable;
import xyz.cereshost.vesta.core.trading.TimeInForce;
import xyz.cereshost.vesta.core.trading.TypeOrder;
import xyz.cereshost.vesta.core.trading.real.api.model.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;

public interface BinanceApi extends Notifiable {

    Long placeAlgoOrder(@NotNull Symbol symbol,
                              @NotNull DireccionOperation side,
                              @NotNull TypeOrder type,
                              @Nullable TimeInForce timeInForce,
                              @Nullable Double quantityLeverageCoin,
                              @NotNull Double trigger,
                              @NotNull Boolean reduceOnly,
                              @NotNull Boolean closePosition
    );

    default Long placeAlgoOrder(@NotNull Symbol symbol,
                                      @NotNull DireccionOperation side,
                                      @NotNull TypeOrder type,
                                      @Nullable TimeInForce timeInForce,
                                      @Nullable Double quantityLeverageCoin,
                                      @NotNull Double trigger
    ){
        return placeAlgoOrder(symbol, side, type, timeInForce, quantityLeverageCoin, trigger, true, type.isAllowClosePosition());
    }

    OrderResult placeOrder(@NotNull Symbol symbol,
                    @NotNull DireccionOperation side,
                    @NotNull TypeOrder type,
                    @Nullable TimeInForce timeInForce,
                    @NotNull Double quantityLeverageCoin,
                    @Nullable Double trigger,
                    @NotNull Boolean reduceOnly,
                    @NotNull Boolean closePosition,
                    @NotNull Boolean useQuantity
    );

    default OrderResult placeOrder(@NotNull Symbol symbol,
                            @NotNull DireccionOperation side,
                            @NotNull TypeOrder type,
                            @Nullable TimeInForce timeInForce,
                            @NotNull Double quantityLeverageCoin,
                            @Nullable Double trigger,
                            @NotNull Boolean useQuantity
    ){
        return placeOrder(symbol, side, type, timeInForce, quantityLeverageCoin, trigger, true, type.isAllowClosePosition(), useQuantity);
    }

    void cancelOrder(@NotNull Symbol symbol, @NotNull Long orderId, @NotNull Boolean isAlgoOrder);

    void closeAll(@NotNull Symbol symbol);

    void changeLeverage(@NotNull Symbol symbol, @NotNull Integer leverage);

    void checkApikey();

    void invalidedCache();

    @NotNull List<OrderData> getAllOrdersFuture(@NotNull Symbol symbol);

    @Nullable PositionData getPosition(@NotNull Symbol symbol);

    @NotNull Double getTickerPrice(@NotNull Symbol symbol);

    @Contract(value = "null, null -> fail; null, !null -> _; !null, null -> _; !null, !null -> _")
    @NotNull Map<String, BookTicker> getBookTickers(@Nullable Symbol symbol, @Nullable Boolean isFuture);

    @NotNull ExchangeInfo getExchangeInfo(@NotNull Boolean isFuture);

    @NotNull SymbolConfigurable getSymbolConfigured(@NotNull String symbol, @NotNull Boolean shouldFuture);

    @NotNull Double getBalance(@NotNull Symbol symbol);

    @NotNull HashMap<String, Double> getBalance(@NotNull Boolean isFuture);

    @NotNull Set<Ticker24H> getTicker24H(@Nullable Symbol symbol);

    void signContract();

//    @NotNull JsonNode sendSignedRequest(@NotNull String method, String endpoint, TreeMap<String, String> params);
//
//    @NotNull JsonNode sendRequest(@NotNull String method, String endpoint, TreeMap<String, String> params);
//
//    @NotNull JsonNode sendPublicRequest(@NotNull String method, @NotNull String endpoint, @NotNull TreeMap<String, String> params);

//    void checkRepose(Symbol symbol, JsonNode node, String method, String endpoint) throws BinanceCodeException;

    void setExceptionHandler(Consumer<Exception> consumer);

    default String buildQueryString(@NotNull Map<String, String> params) {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            sj.add(
                    URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                            + "="
                            + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8)
            );
        }
        return sj.toString();
    }

    default String hmacSha256(@NotNull String data, @NotNull String secret) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
        SecretKeySpec secret_key = new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
        sha256_HMAC.init(secret_key);
        byte[] raw = sha256_HMAC.doFinal(data.getBytes(StandardCharsets.UTF_8));
        StringBuilder hex = new StringBuilder(2 * raw.length);
        for (byte b : raw) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }

}
