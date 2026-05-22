package xyz.cereshost.vesta.core.trading.real.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.message.MediaNotification;
import xyz.cereshost.vesta.core.trading.Endpoints;
import xyz.cereshost.vesta.core.trading.TimeInForce;
import xyz.cereshost.vesta.core.trading.TypeOrder;
import xyz.cereshost.vesta.core.trading.real.api.model.*;

import java.net.http.WebSocket;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

public abstract class BinanceWebSocket extends BaseConnector implements BinanceApi {

    protected static final long REQUEST_TIMEOUT_SECONDS = 15L;
    protected final ObjectMapper mapper = new ObjectMapper();

    @NotNull protected final ConcurrentMap<String, CompletableFuture<JsonNode>> pendingRequests = new ConcurrentHashMap<>();
    @NotNull @Getter @Setter protected MediaNotification mediaNotification = MediaNotification.empty();
    @NotNull @Setter protected Consumer<Exception> exceptionHandler = e -> {};

    public BinanceWebSocket(Endpoints endpoints) {
        super(endpoints);
    }

    private void handleDelegatedException(@NotNull Exception e) {
        exceptionHandler.accept(e);
    }

    protected @Nullable String accumulateMessage(
            @NotNull CharSequence data,
            boolean last,
            @NotNull Object lock,
            @NotNull StringBuilder buffer
    ) {
        synchronized (lock) {
            buffer.append(data);
            if (!last) {
                return null;
            }
            String content = buffer.toString();
            buffer.setLength(0);
            return content;
        }
    }

    protected void failPendingRequests(
            @NotNull ConcurrentMap<String, CompletableFuture<JsonNode>> pendingMap,
            @NotNull Exception exception
    ) {
        pendingMap.forEach((id, future) -> future.completeExceptionally(exception));
        pendingMap.clear();
    }

    @Override
    public Long placeAlgoOrder(@NotNull Symbol symbol, @NotNull DireccionOperation side, @NotNull TypeOrder type, @Nullable TimeInForce timeInForce, @Nullable Double quantityLeverageCoin, @NotNull Double trigger, @NotNull Boolean reduceOnly, @NotNull Boolean closePosition) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public OrderResult placeOrder(@NotNull Symbol symbol, @NotNull DireccionOperation side, @NotNull TypeOrder type, @Nullable TimeInForce timeInForce, @NotNull Double quantityLeverageCoin, @Nullable Double trigger, @NotNull Boolean reduceOnly, @NotNull Boolean closePosition, @NotNull Boolean useQuantity) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void cancelOrder(@NotNull Symbol symbol, @NotNull Long orderId, @NotNull Boolean isAlgoOrder) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void closeAll(@NotNull Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void changeLeverage(@NotNull Symbol symbol, @NotNull Integer leverage) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void invalidedCache() {

    }

    @Override
    public @NotNull List<OrderData> getAllOrdersFuture(@NotNull Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @Nullable PositionData getPosition(@NotNull Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull Double getTickerPrice(@NotNull Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull Map<String, BookTicker> getBookTickers(@Nullable Symbol symbol, @Nullable Boolean isFuture) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull ExchangeInfo getExchangeInfo(@NotNull Boolean isFuture) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull SymbolConfigurable getSymbolConfigured(@NotNull String symbol, @NotNull Boolean shouldFuture) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull Double getBalance(@NotNull Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public @NotNull Set<Ticker24H> getTicker24H(@Nullable Symbol symbol) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void signContract() {
    }

    protected abstract @NotNull WebSocket.Listener newListener();
}
