package xyz.cereshost.vesta.core.trading.abitrage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.trading.abitrage.model.NameAsset;
import xyz.cereshost.vesta.core.trading.abitrage.model.TriangularArbitrageOpportunity;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TriangularEngine {

    public static final double PROFIT_EPSILON = 1e-12;
    public static final double DEFAULT_FEE_RATE = 0.001; // 0.075% aprox 0.00075
    public static final double DEFAULT_START_AMOUNT = 10d;
    public static final int PREFERRED_START_ASSET = new NameAsset("USDT").hash;
    public static final int MIN_CYCLE_LENGTH = 3;
    public static final int MAX_CYCLE_LENGTH = 3;

    protected final ConcurrentMap<String, TriangularArbitrageOpportunity> lastTriangular = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, BookTicker> liveTickers = new ConcurrentHashMap<>();
    protected final ConcurrentMap<String, NameAssetIndexed> nameAssetCache = new ConcurrentHashMap<>();
    protected final ConcurrentMap<NameAsset, ArrayList<ArbitrageEdge>> outgoingByFromAsset = new ConcurrentHashMap<>();

    @Setter protected ExchangeInfo exchangeInfo = null;

    public abstract List<TriangularArbitrageOpportunity> findTriangularArbitrageOpportunities(
            @Nullable BookTicker updatedTicker
    );

    public abstract void stop();

    public abstract @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> buildGraf(@NotNull ExchangeInfo exchangeInfoSpot);

    public abstract @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> updateGraf(
            @NotNull ExchangeInfo exchangeInfoSpot,
            @Nullable BookTicker updatedTicker
    );

    public void addLiverTicker(@NotNull Map<String, BookTicker> liverTickers){
        this.liveTickers.putAll(liverTickers);
    }

    public void addLiverTickers(BookTicker liverTickers){
        this.liveTickers.put(liverTickers.symbol(), liverTickers);
    }

    protected @NotNull List<ArbitrageEdge> rotateCycleToPreferredStart(@NotNull List<ArbitrageEdge> cycleEdges) {
        int preferredIndex = -1;
        for (int i = 0; i < cycleEdges.size(); i++) {
            if (PREFERRED_START_ASSET == cycleEdges.get(i).getFromAsset().hash) {
                preferredIndex = i;
                break;
            }
        }
        if (preferredIndex <= 0) {
            return cycleEdges;
        }

        List<ArbitrageEdge> rotated = new ArrayList<>(cycleEdges);
        Collections.rotate(rotated, -preferredIndex);
        return rotated;
    }

    @Data
    @AllArgsConstructor
    public static final class ArbitrageEdge {
        private final String symbol;
        private final NameAsset fromAsset;
        private final NameAsset toAsset;
        private volatile double rate;
        private volatile double weight;
        private final String action;
        private volatile double referencePrice;
        private volatile double referenceLiquidity;
        private final double stepSize;
    }

    @Getter
    public static class LifeTime {
        private final long dateOpen = System.currentTimeMillis();
        private int ticks = 0;


        public LifeTime nextTicks() {
            this.ticks++;
            return this;
        }
    }

    public record OnOpportunities(
            List<TriangularArbitrageOpportunity> opportunities
    ){}

    public record OnUpdate(
            long startNanoTime
    ){}

    @Getter
    public static final class NameAssetIndexed extends NameAsset {

        private static final AtomicInteger indexCurrent = new AtomicInteger(0);
        private static final HashMap<String, Integer> indexes = new HashMap<>();

        public NameAssetIndexed(String asset) {
            super(asset, indexes.computeIfAbsent(asset, (a) -> indexCurrent.getAndIncrement()));
        }

        public static int currentIndex() {
            return indexCurrent.get();
        }
    }
}
