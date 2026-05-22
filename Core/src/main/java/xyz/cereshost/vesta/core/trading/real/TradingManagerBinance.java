package xyz.cereshost.vesta.core.trading.real;

import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.Market;
import xyz.cereshost.vesta.core.market.Symbol;
import xyz.cereshost.vesta.core.message.MediaNotification;
import xyz.cereshost.vesta.core.trading.*;
import xyz.cereshost.vesta.core.trading.real.api.BinanceApi;
import xyz.cereshost.vesta.core.trading.real.api.model.OrderData;
import xyz.cereshost.vesta.core.trading.real.api.model.PositionData;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class TradingManagerBinance implements TradingManager {

    private @NotNull final BinanceApi binanceApi;
    private @NotNull Integer lastLeverage = 1;
    private @NotNull Double lastBalance = 0d;
    @Setter
    private @Nullable TradingTelemetry telemetry;
    @Setter
    private @NotNull TradingTickLoop tradingTickLoop;
    @Getter @Setter @NotNull
    private MediaNotification mediaNotification;

    // Mapa para vincular tu UUID interno con los IDs de órdenes de Binance
    private @Nullable BinanceOpenPosition openOperation = null;
    private final @NotNull ConcurrentHashMap<UUID, LimitedPosition> pendingOrder = new ConcurrentHashMap<>();
    private final @NotNull Market market;

    public TradingManagerBinance(@NotNull BinanceApi binanceApi, @Nullable MediaNotification mediaNotification, @NotNull Market market) {
        this.binanceApi = binanceApi;
        this.market = market;
        this.mediaNotification = Objects.requireNonNullElse(mediaNotification, MediaNotification.empty());
    }

    @Override
    public @NotNull Optional<OpenPosition> open(@NotNull DireccionOperation side, @NotNull Double quantity, @NotNull Integer leverage) {
        Symbol symbol = getMarket().getSymbol();
        Double currentPrice = getCurrentPrice();
        Optional<Double> quantityLeverageCoin = configSetupPreEntry(leverage, currentPrice, quantity);
        if (quantityLeverageCoin.isPresent()){
            Long orderId = binanceApi.placeOrder(symbol,
                    side,
                    TypeOrder.MARKET,
                    null,
                    quantity,
                    currentPrice,
                    false,
                    false,
                    true).orderId();
            BinanceOpenPosition op = new BinanceOpenPosition(
                    this,
                    orderId,
                    side,
                    currentPrice,
                    quantityLeverageCoin.get(),
                    leverage,
                    null
            );
            openOperation = op;
            return Optional.of(op);
        }else {
            return Optional.empty();
        }
    }

    @Override
    public @NotNull Optional<OrderSimple> limit(@NotNull DireccionOperation side,
                                                @NotNull Double trigger,
                                                @NotNull Double quantity,
                                                @NotNull Integer leverage,
                                                @NotNull TypeOrder typeOrder,
                                                @NotNull TimeInForce timeInForce
    ) {
        Symbol symbol = getMarket().getSymbol();
        if (!Double.isFinite(trigger) || trigger <= 0) {
            Vesta.error("Precio de entrada invalido para orden limite en %s: %s", symbol, trigger);
            return Optional.empty();
        }
        Optional<Double> quantityLeverageCoin = configSetupPreEntry(leverage, trigger, quantity);
        if (quantityLeverageCoin.isPresent()){
            Long orderId = binanceApi.placeOrder(
                    symbol,
                    side,
                    typeOrder,
                    timeInForce,
                    quantityLeverageCoin.get(),
                    trigger,
                    false,
                    false,
                    true).orderId();
            BinanceOrderSimple op = new BinanceOrderSimple(
                    this,
                    orderId,
                    side,
                    trigger,
                    quantity,
                    leverage,
                    typeOrder,
                    timeInForce
            );
            pendingOrder.put(op.getUuid(), op);
            return Optional.of(op);
        }else {
            return Optional.empty();
        }
    }

    @Override
    public @NotNull Optional<OrderAlgo> limitAlgo(@NotNull DireccionOperation side,
                                                  @NotNull TypeOrder type,
                                                  @NotNull Double trigger,
                                                  @Nullable Integer leverage,
                                                  @Nullable Double quantity,
                                                  @NotNull TimeInForce timeInForce,
                                                  @NotNull Boolean reduceOnly
    ) {
        Symbol symbol = getMarket().getSymbol();
        Optional<Double> quantityLeverageCoin;

        if (type.isLimit()){
            if (leverage == null) return Optional.empty();
            if (quantity == null) return Optional.empty();
            quantityLeverageCoin = configSetupPreEntry(leverage, trigger, quantity);
        }else {
            quantityLeverageCoin = Optional.empty();
        }

        Long orderId = binanceApi.placeAlgoOrder(symbol,
                side,
                type,
                timeInForce,
                quantityLeverageCoin.orElse(null),
                trigger,
                reduceOnly,
                type.isAllowClosePosition()
        );
        BinanceOrderAlgo op = new BinanceOrderAlgo(
                this,
                orderId,
                side,
                trigger,
                quantity,
                leverage,
                reduceOnly,
                type,
                timeInForce
        );
        return Optional.of(op);
    }

    @Override
    public @NotNull Optional<ClosePosition> close(ExitReason reason) {
        if (openOperation == null) return Optional.empty();
        @NotNull BinanceOpenPosition op = openOperation;
        Symbol symbol = getMarket().getSymbol();

        cancelAllOrderAlgo();

        double quantityLeverageCoin = (op.getQuantity() * op.getLeverage()) / op.getTriggerPrice();
        binanceApi.placeOrder(
                symbol,
                op.getDireccion().inverse(),
                TypeOrder.MARKET,
                null,
                quantityLeverageCoin,
                null,
                true,
                false,
                true);

        BinanceClosePosition closeOp = new BinanceClosePosition(
                binanceApi.getTickerPrice(symbol),
                System.currentTimeMillis(),
                reason,
                op
        );
        return Optional.of(closeOp);
    }

    @Override
    public void cancelOrder(@NotNull UUID uuid) {
        LimitedPosition pending = pendingOrder.remove(uuid);
        if (pending == null) {
            return;
        }
        Symbol symbol = market.getSymbol();
        if (pending instanceof BinanceObject binanceObject) {
            binanceApi.cancelOrder(symbol, binanceObject.getOrderId(), pending.getTypeOrder().isAlgo());
        }else {
            throw new IllegalStateException("Un objeto no se pudo castear correctamente como BinanceObject");
        }
    }

    public void cancelAllOrder() {
        List<BinanceObject> uuidsForRemover = pendingOrder.values().stream().filter(order ->
                order instanceof OrderSimple
        ).map(order ->
                (BinanceObject) order
        ).toList();
        uuidsForRemover.forEach((order) ->
                binanceApi.cancelOrder(market.getSymbol(), order.getOrderId(), false)
        );
    }

    @Override
    public void cancelAllOrderAlgo() {
        List<BinanceObject> uuidsForRemover = pendingOrder.values().stream().filter(order ->
                order instanceof OrderAlgo
        ).map(order ->
                (BinanceObject) order
        ).toList();
        uuidsForRemover.forEach((orderAlgo) -> {
            binanceApi.cancelOrder(market.getSymbol(), orderAlgo.getOrderId(), true);
        });
    }

    @Override
    public @NotNull Integer pendingOrderSize() {
        return pendingOrder.size();
    }

    @Override
    public @NotNull Optional<TradingManager.OpenPosition> getOpenPosition() {
        return Optional.ofNullable(openOperation);
    }

    @Override // READ ONLY
    public @NotNull List<OrderSimple> getOrder() {
        return pendingOrder.values().stream().filter(order ->
                order instanceof OrderSimple
        ).map(order ->
                (OrderSimple) order
        ).toList();
    }

    @Override // READ ONLY
    public @NotNull List<OrderAlgo> getLimitAlgos() {
        return pendingOrder.values().stream().filter(order ->
                order instanceof OrderAlgo
        ).map(order ->
                (OrderAlgo) order
        ).toList();
    }

    @Override // READ ONLY
    public @NotNull Optional<TradingManager.OrderAlgo> getTakeProfit() {
        return pendingOrder.values().stream().filter(order ->
                order instanceof OrderAlgo
        ).map(order ->
                (OrderAlgo) order
        ).filter(order ->
                order.getTypeOrder().isTakeProfit()
        ).findAny();
    }

    @Override // READ ONLY
    public @NotNull Optional<OrderAlgo> getStopLoss() {
        return pendingOrder.values().stream().filter(order ->
                order instanceof OrderAlgo
        ).map(order ->
                (OrderAlgo) order
        ).filter(order ->
                order.getTypeOrder().isTakeProfit()
        ).findAny();
    }

    @Override
    public @NotNull Optional<TradingTelemetry> getTelemetry() {
        return Optional.ofNullable(telemetry);
    }

    @Override
    public @NotNull Market getMarket() {
        return market;
    }

    @Override
    public @NotNull Double getAvailableBalance() {
        if (lastBalance == 0) {
            lastBalance = binanceApi.getBalance(market.getSymbol());
        }
        // Margen de seguridad
        return lastBalance*0.98;
    }

    @Override
    public @NotNull Double getCurrentPrice() {
        return binanceApi.getTickerPrice(getMarket().getSymbol());
    }

    @Override
    public long getCurrentTime() {
        return System.currentTimeMillis();
    }

    @Override
    public void log(String log){
        Vesta.info("📃 " + log);
    }

    public void signContract(){
        binanceApi.signContract();
    }

    public synchronized void sync(){
        List<OrderData> orders = binanceApi.getAllOrdersFuture(market.getSymbol());
        PositionData position = binanceApi.getPosition(market.getSymbol());
//        if (position == null){
//            for (BinanceApi.OrderData order : orders) {
//                if (order.type().isExit()){
//                    binanceApi.cancelOrder(symbols.getSymbol(), order.orderID(), order.isAlgoOrder());
//                }
//            }
//            openOperation = null;
//        }else {
//            if (openOperation == null) {
//                BinanceApi.OrderData TPLong = null, TPShort = null, SLLong = null, SLShort = null;
//                for (BinanceApi.OrderData order : orders) {
//                    if (order.type().isStopLoss()){
//                        if (order.direccionOperation().isLong()) SLLong = order;
//                        else SLShort = order;
//                    }
//                    if (order.type().isTakeProfit()){
//                        if (order.direccionOperation().isLong()) TPLong = order;
//                        else TPShort = order;
//                    }
//                }
//                RiskLimits riskLimits;
//                if (position.direccionOperation().isLong()) {
//                    riskLimits = new RiskLimitsAbsolute(TPLong != null ? TPLong.triggerPrice() : null, SLLong != null ? SLLong.triggerPrice() : null);
//                }else {
//                    riskLimits = new RiskLimitsAbsolute(TPShort != null ? TPShort.triggerPrice() : null, SLShort != null ? SLShort.triggerPrice() : null);
//                }
//                openOperation = new BinanceOpenPosition(this, ,position.direccionOperation(), position.entryPrice(), position.margen(), position.leverage());
//            }else {
//                for (BinanceApi.OrderData order : orders) {
//                    if (!order.isAlgoOrder()) continue;
//
//                    TypeOrder typeOrder = order.type();
//
////                    RiskLimits riskLimits = openOperation.getRiskLimits();
////                    if (typeOrder.isTakeProfit()){
////                        openOperation.setTpBinanceId(order.orderID());
////                        if (riskLimits.isAbsolute()){
////                            riskLimits.setTakeProfit(order.triggerPrice(), price);
////                        }else {
////                            riskLimits.setTakeProfit(((order.triggerPrice() - openOperation.getEntryPrice())/openOperation.getEntryPrice())*100, price);
////                        }
////                    }
////                    if (typeOrder.isStopLoss()){
////                        openOperation.setSlBinanceId(order.orderID());
////                        if (riskLimits.isAbsolute()){
////                            riskLimits.setStopLoss(order.triggerPrice(), "");
////                        }else {
////                            riskLimits.setStopLoss(((order.triggerPrice() - openOperation.getEntryPrice())/openOperation.getEntryPrice())*100, price);
////                        }
////                    }
//                }
//            }
//        }
//
//        BiDictionary<UUID, Long> dictionary = new ConcurrentHashBiDictionary<>();
//        for (BinanceOrder limit : pendingOrder.values()) {
//            dictionary.add(limit.getUuid(), limit.getOrderId());
//        }
//        for (BinanceApi.OrderData order : orders) {
//            if (order.type().equals(TypeOrder.LIMIT)) {
//                UUID uuid = dictionary.removeRight(order.orderID());
//                if (uuid == null){
//                    BinanceOrder limit = new BinanceOrder(this,
//                            order.orderID(),
//                            order.direccionOperation(),
//                            order.price(),
//                            order.quantity(),
//                            lastLeverage,
//                            order.type(),
//                            order.timeInForce()
//                    );
//                    pendingOrder.put(limit.getUuid(), limit);
//                }else {
//                    pendingOrder.get(uuid).setEntryPrice(order.price());
//                }
//            }
//        }
//        for (BiDictionary.Entry<UUID, Long> entry : dictionary.getAll()){
//            pendingOrder.remove(entry.left());
//        }
    }

    private @NotNull Optional<Double> configSetupPreEntry(@NotNull Integer leverage, @NotNull Double price, @NotNull Double quantity){
        CountDownLatch latch = new CountDownLatch(2);
        tradingTickLoop.getExecutor().execute(() -> {
            try {
                if (!lastLeverage.equals(leverage)) binanceApi.changeLeverage(market.getSymbol(), leverage);
                lastLeverage = leverage;
            }finally {
                latch.countDown();
            }
        });
        AtomicReference<Double> safeAmount = new AtomicReference<>(quantity);
        tradingTickLoop.getExecutor().execute(() -> {
            try {
                double balance = getAvailableBalance();
                if (balance <= 0) {
                    safeAmount.set(0d);
                    return;
                }
                double requested = Math.max(0d, safeAmount.get());
                safeAmount.set(Math.min(requested, balance) * 0.99);
            } finally {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            Vesta.sendWaringException("error al pausar el hilo para configurar el setup", e);
            return Optional.empty();
        }
        return Optional.ofNullable((safeAmount.get() * leverage) / price);
    }

    @Getter
    @Setter
    public static class BinanceOpenPosition extends OpenPosition implements BinanceObject {
        private final @NotNull Long orderId;

        private BinanceOpenPosition(@NotNull TradingManagerBinance binance,
                                    @NotNull Long orderId,
                                    @NotNull DireccionOperation direccion,
                                    @NotNull Double entryPrice,
                                    @NotNull Double quantity,
                                    @NotNull Integer leverage,
                                    @Nullable TradingManager.OrderSimple orderSimple
        ) {
            super(binance, direccion, entryPrice, quantity, leverage, orderSimple);
            this.orderId = orderId;
        }

        private BinanceOpenPosition(@NotNull BinanceOpenPosition binanceOpenPosition) {
            super(binanceOpenPosition);
            this.orderId = binanceOpenPosition.orderId;
        }

        @Override
        public @NotNull OpenPosition copy() {
            return new BinanceOpenPosition(this);
        }
    }

    @Getter
    @Setter
    public static class BinanceOrderSimple extends OrderSimple implements BinanceObject {
        private @NotNull Long orderId;

        public BinanceOrderSimple(@NotNull TradingManagerBinance tradingManager,
                                  @NotNull Long orderId,
                                  @NotNull DireccionOperation direccion,
                                  @NotNull Double entryPrice,
                                  @NotNull Double quantity,
                                  @NotNull Integer leverage,
                                  @NotNull TypeOrder typeOrder,
                                  @Nullable TimeInForce timeInForce
        ) {
            super(tradingManager, direccion, entryPrice, quantity, leverage, typeOrder, timeInForce);
            this.orderId = orderId;
        }

        public BinanceOrderSimple(@NotNull TradingManagerBinance.BinanceOrderSimple binanceOrder) {
            super(binanceOrder);
            this.orderId = binanceOrder.orderId;
        }

        @Override
        public void setTriggerPrice(@NotNull Double triggerPrice) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, false);
            orderId = binance.binanceApi.placeOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    true,
                    typeOrder.isAllowClosePosition(),
                    true).orderId();
            super.setTriggerPrice(triggerPrice);
        }

        @Override
        public void setTimeInForce(@Nullable TimeInForce timeInForce) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, false);
            orderId = binance.binanceApi.placeOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    true,
                    typeOrder.isAllowClosePosition(),
                    true).orderId();
            this.timeInForce = timeInForce;
        }

        @Override
        public void setTypeOrder(@NotNull TypeOrder typeOrder) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, false);
            orderId = binance.binanceApi.placeOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    true,
                    typeOrder.isAllowClosePosition(),
                    true).orderId();
            this.typeOrder = typeOrder;
        }

        @Override
        public @NotNull TradingManager.OrderSimple copy() {
            return new BinanceOrderSimple(this);
        }
    }
    @Getter
    @Setter
    public static class BinanceOrderAlgo extends OrderAlgo implements BinanceObject {
        private @NotNull Long orderId;

        private BinanceOrderAlgo(@NotNull TradingManager tradingManager,
                            @NotNull Long orderId,
                            @NotNull DireccionOperation direccion,
                            @NotNull Double triggerPrice,
                            @Nullable Double quantity,
                            @Nullable Integer leverage,
                            @NotNull Boolean reduceOnly,
                            @NotNull TypeOrder typeOrder,
                            @Nullable TimeInForce timeInForce
        ) {
            super(tradingManager, direccion, triggerPrice, quantity, leverage, reduceOnly, typeOrder, timeInForce);
            this.orderId = orderId;
        }

        private BinanceOrderAlgo(@NotNull BinanceOrderAlgo binanceOrderAlgo){
            super(binanceOrderAlgo);
            this.orderId = binanceOrderAlgo.orderId;
        }

        @Override
        public void setTriggerPrice(@NotNull Double triggerPrice) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, true);
            orderId = binance.binanceApi.placeAlgoOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    reduceOnly,
                    typeOrder.isAllowClosePosition()
            );
            super.setTriggerPrice(triggerPrice);
        }

        @Override
        public void setTimeInForce(@Nullable TimeInForce timeInForce) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, true);
            orderId = binance.binanceApi.placeAlgoOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    reduceOnly,
                    typeOrder.isAllowClosePosition()
            );
            this.timeInForce = timeInForce;
        }

        @Override
        public void setTypeOrder(@NotNull TypeOrder typeOrder) {
            TradingManagerBinance binance = (TradingManagerBinance) tradingManager;
            binance.binanceApi.cancelOrder(binance.getMarket().getSymbol(), orderId, true);
            orderId = binance.binanceApi.placeAlgoOrder(binance.getMarket().getSymbol(),
                    direccion,
                    typeOrder,
                    timeInForce,
                    quantity,
                    triggerPrice,
                    reduceOnly,
                    typeOrder.isAllowClosePosition()
            );
            this.typeOrder = typeOrder;
        }

        @Override
        public @NotNull OrderAlgo copy() {
            return new BinanceOrderAlgo(this);
        }
    }

    public static class BinanceClosePosition extends ClosePosition {

        public BinanceClosePosition(@NotNull Double exitPrice,
                                    @NotNull Long exitTime,
                                    @NotNull ExitReason reason,
                                    @NotNull OpenPosition openPosition
        ) {
            super(exitPrice, exitTime, reason, openPosition);
        }

        public BinanceClosePosition(@NotNull BinanceClosePosition binanceClosePosition) {
            super(binanceClosePosition);
        }

        @Override
        public @NotNull ClosePosition copy() {
            return new BinanceClosePosition(this);
        }
    }

    public interface BinanceObject{
        @NotNull Long getOrderId();
    }

}
