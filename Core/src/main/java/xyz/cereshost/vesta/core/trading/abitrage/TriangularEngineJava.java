package xyz.cereshost.vesta.core.trading.abitrage;

import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.market.MarketStatus;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.trading.abitrage.model.NameAsset;
import xyz.cereshost.vesta.core.trading.abitrage.model.TriangularArbitrageOpportunity;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;

import java.util.*;

public class TriangularEngineJava extends TriangularEngine {

    private final ArrayList<ArbitrageEdge>[] outgoingByFromIndex = new ArrayList[TriangularArbitrage.MAX_SYMBOL];

    @Override
    @SneakyThrows
    public @NotNull List<TriangularArbitrageOpportunity> findTriangularArbitrageOpportunities(
            @Nullable BookTicker updatedTicker
    ) {
        Map<NameAsset, ArrayList<ArbitrageEdge>> outgoingByFromAsset = updateGraf(exchangeInfo, updatedTicker);
        String updatedSymbol = updatedTicker == null ? null : updatedTicker.symbol();
        Set<NameAsset> trackedAssets = trackedAssetsFromLastTriangular();

        Set<NameAsset> startAssetsToAnalyze;
        final boolean detectTriangularPrev;

        if (updatedSymbol != null) {
            startAssetsToAnalyze = new LinkedHashSet<>(2);
            SymbolConfigurable symbol = exchangeInfo.symbols().get(updatedSymbol);
            if (symbol == null) {
                return List.of();
            }
            NameAsset baseAssetName = nameAssetCache.computeIfAbsent(symbol.getBaseAsset(), NameAssetIndexed::new);
            NameAsset quoteAssetName = nameAssetCache.computeIfAbsent(symbol.getQuoteAsset(), NameAssetIndexed::new);
            startAssetsToAnalyze.add(baseAssetName);
            startAssetsToAnalyze.add(quoteAssetName);
            if (
                    trackedAssets.contains(quoteAssetName) ||
                            trackedAssets.contains(baseAssetName)
            ) {
                detectTriangularPrev = true;
                startAssetsToAnalyze.addAll(trackedAssets);
            }else {
                detectTriangularPrev = false;
            }
        }else {
            detectTriangularPrev = false;
            startAssetsToAnalyze = null;
        }

        if (outgoingByFromAsset.size() < MIN_CYCLE_LENGTH) {
            return List.of();
        }

        if (updatedSymbol != null) {
            // Descarta símbolos no registrados
            startAssetsToAnalyze.retainAll(outgoingByFromAsset.keySet());
            if (startAssetsToAnalyze.isEmpty()) {
                return List.of();
            }
        }

        HashSet<String> seenCycles = new HashSet<>();
        LinkedList<TriangularArbitrageOpportunity> opportunities = new LinkedList<>();
        Set<NameAsset> startAssets = new LinkedHashSet<>(
                Objects.requireNonNullElseGet(startAssetsToAnalyze, outgoingByFromAsset::keySet)
        );
        startAssets.retainAll(outgoingByFromAsset.keySet());
        if (startAssets.isEmpty()) {
            return List.of();
        }

        for (NameAsset startAsset : startAssets) {
            LinkedList<ArbitrageEdge> path = new LinkedList<>();
            LinkedList<Integer> visitedAssets = new LinkedList<>();
            visitedAssets.add(startAsset.hash);

            searchCyclesFrom(
                    startAsset,
                    startAsset,
                    outgoingByFromIndex,
                    path,
                    new IntegerAtomic(0),
                    visitedAssets,
                    seenCycles,
                    opportunities
            );
        }

        Set<String> activeCycleKeys = new HashSet<>();
        for (TriangularArbitrageOpportunity opportunity : opportunities) {
            activeCycleKeys.add(canonicalCycleKey(opportunity.getAssetsCycle()));
        }

        lastTriangular.keySet().removeIf(key -> {
            if (activeCycleKeys.contains(key)) {
                return false;
            }else {
                return detectTriangularPrev;
            }
        });
        opportunities.addAll(lastTriangular.values());
        if (opportunities.isEmpty()) {
            return List.of();
        }
        // No usar Lambdas: por qué llama linkToTargetMethod
        //noinspection Convert2MethodRef
        opportunities.sort(Comparator.comparingDouble((TriangularArbitrageOpportunity t) -> t.getProfitPercent()).reversed());

        return opportunities;
    }

    @Override
    public void stop() {
        liveTickers.clear();
        nameAssetCache.clear();
        lastTriangular.clear();
        outgoingByFromAsset.clear();
    }

    @Override
    public @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> buildGraf(@NotNull ExchangeInfo exchangeInfoSpot){
        outgoingByFromAsset.clear();
        for (SymbolConfigurable symbolConfigurable : exchangeInfoSpot.symbols().values()) {
            if (!MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus())) {
                continue;
            }

            // Solo spot para arbitraje triangular clásico
            if (!symbolConfigurable.getIsSpot()) {
                continue;
            }

            String symbolName = symbolConfigurable.name();
            BookTicker ticker = liveTickers.get(symbolName);
            if (ticker == null) {
                continue;
            }

            double bid = ticker.bidPrice();
            double ask = ticker.askPrice();

            if (bid <= 0.0 || ask <= 0.0) {
                continue;
            }

            double bidLiquidity = ticker.bidQty();
            double askLiquidity = ticker.askQty();

            if (bidLiquidity <= 0.0 || askLiquidity <= 0.0) {
                continue;
            }

            String baseAsset = symbolConfigurable.getBaseAsset();
            String quoteAsset = symbolConfigurable.getQuoteAsset();
            if (baseAsset.equals("?") || quoteAsset.equals("?")) {
                continue;
            }

            double sellRate = bid * (1.0 - DEFAULT_FEE_RATE);
            double buyRate = (1.0 / ask) * (1.0 - DEFAULT_FEE_RATE);

            NameAsset baseAssetName = nameAssetCache.computeIfAbsent(baseAsset, NameAssetIndexed::new);
            NameAsset quoteAssetName = nameAssetCache.computeIfAbsent(quoteAsset, NameAssetIndexed::new);

            if (sellRate > 0.0) {
                addEdge(outgoingByFromAsset, new ArbitrageEdge(
                        symbolName,
                        baseAssetName,
                        quoteAssetName,
                        sellRate,
                        -Math.log(sellRate),
                        "SELL",
                        bid,
                        bidLiquidity,
                        symbolConfigurable.getStepSizeRaw()
                ));
            }

            if (buyRate > 0.0) {
                addEdge(outgoingByFromAsset, new ArbitrageEdge(
                        symbolName,
                        quoteAssetName,
                        baseAssetName,
                        buyRate,
                        -Math.log(buyRate),
                        "BUY",
                        ask,
                        askLiquidity,
                        symbolConfigurable.getStepSizeRaw()
                ));
            }
        }
        return outgoingByFromAsset;
    }

    @Override
    public @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> updateGraf(@NotNull ExchangeInfo exchangeInfoSpot, @Nullable BookTicker updatedTicker) {
        if (updatedTicker == null) {
            if (outgoingByFromAsset.isEmpty()) {
                return buildGraf(exchangeInfoSpot);
            }
            return outgoingByFromAsset;
        }

        liveTickers.put(updatedTicker.symbol(), updatedTicker);

        SymbolConfigurable symbolConfigurable = exchangeInfoSpot.symbols().get(updatedTicker.symbol());
        if (symbolConfigurable == null || !isTradableSpot(symbolConfigurable)) {
            removeEdgesForSymbol(updatedTicker.symbol());
            return outgoingByFromAsset;
        }

        String baseAsset = symbolConfigurable.getBaseAsset();
        String quoteAsset = symbolConfigurable.getQuoteAsset();
        if (baseAsset.equals("?") || quoteAsset.equals("?")) {
            removeEdgesForSymbol(updatedTicker.symbol());
            return outgoingByFromAsset;
        }

        Double bidValue = updatedTicker.bidPrice();
        Double askValue = updatedTicker.askPrice();
        Double bidLiquidityValue = updatedTicker.bidQty();
        Double askLiquidityValue = updatedTicker.askQty();
        if (bidValue == null || askValue == null || bidLiquidityValue == null || askLiquidityValue == null) {
            removeEdgesForSymbol(updatedTicker.symbol());
            return outgoingByFromAsset;
        }

        double bid = bidValue;
        double ask = askValue;
        double bidLiquidity = bidLiquidityValue;
        double askLiquidity = askLiquidityValue;
//        if (bid <= 0.0 || ask <= 0.0 || bidLiquidity <= 0.0 || askLiquidity <= 0.0) {
//            removeEdgesForSymbol(updatedTicker.symbol());
//            return outgoingByFromAsset;
//        }

        NameAsset baseAssetName = nameAssetCache.computeIfAbsent(baseAsset, NameAssetIndexed::new);
        NameAsset quoteAssetName = nameAssetCache.computeIfAbsent(quoteAsset, NameAssetIndexed::new);

        double sellRate = bid * (1.0 - DEFAULT_FEE_RATE);
        if (sellRate > 0.0) {
            upsertEdge(
                    updatedTicker.symbol(),
                    baseAssetName,
                    quoteAssetName,
                    sellRate,
                    -Math.log(sellRate),
                    "SELL",
                    bid,
                    bidLiquidity,
                    symbolConfigurable.getStepSizeRaw()
            );
        }

        double buyRate = (1.0 / ask) * (1.0 - DEFAULT_FEE_RATE);
        if (buyRate > 0.0) {
            upsertEdge(
                    updatedTicker.symbol(),
                    quoteAssetName,
                    baseAssetName,
                    buyRate,
                    -Math.log(buyRate),
                    "BUY",
                    ask,
                    askLiquidity,
                    symbolConfigurable.getStepSizeRaw()
            );
        }

        return outgoingByFromAsset;
    }

    private void addEdge(@NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> outgoingByFromAsset,
                         @NotNull ArbitrageEdge edge
    ) {
        ArrayList<ArbitrageEdge> outgoing = outgoingByFromAsset.computeIfAbsent(edge.getFromAsset(), key -> {
            ArrayList<ArbitrageEdge> list = new ArrayList<>(3);
            if (key.index < outgoingByFromIndex.length) {
                outgoingByFromIndex[key.index] = list;
            }
            return list;
        });
        synchronized (outgoing) {
            outgoing.add(edge);
        }
    }

    private boolean isTradableSpot(@NotNull SymbolConfigurable symbolConfigurable) {
        return MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus()) && symbolConfigurable.getIsSpot();
    }

    private void upsertEdge(
            @NotNull String symbol,
            @NotNull NameAsset fromAsset,
            @NotNull NameAsset toAsset,
            double rate,
            double weight,
            @NotNull String action,
            double referencePrice,
            double referenceLiquidity,
            double stepSize
    ) {
        ArrayList<ArbitrageEdge> outgoing = outgoingByFromAsset.computeIfAbsent(fromAsset, key -> {
            ArrayList<ArbitrageEdge> list = new ArrayList<>(3);
            if (key.index < outgoingByFromIndex.length) {
                outgoingByFromIndex[key.index] = list;
            }
            return list;
        });
        synchronized (outgoing) {
            for (ArbitrageEdge edge : outgoing) {
                if (symbol.equals(edge.getSymbol()) && action.equals(edge.getAction())) {
                    synchronized (edge) {
                        edge.setRate(rate);
                        edge.setWeight(weight);
                        edge.setReferencePrice(referencePrice);
                        edge.setReferenceLiquidity(referenceLiquidity);
                    }
                    return;
                }
            }

            outgoing.add(new ArbitrageEdge(
                    symbol,
                    fromAsset,
                    toAsset,
                    rate,
                    weight,
                    action,
                    referencePrice,
                    referenceLiquidity,
                    stepSize
            ));
        }
    }

    private void removeEdgesForSymbol(@NotNull String symbol) {
        for (ArrayList<ArbitrageEdge> outgoing : outgoingByFromAsset.values()) {
            synchronized (outgoing) {
                outgoing.removeIf(edge -> symbol.equals(edge.getSymbol()));
            }
        }
    }

    @SuppressWarnings("ConstantValue")
    private void searchCyclesFrom(
            @NotNull NameAsset startAsset,
            @NotNull NameAsset currentAsset,
            @NotNull ArrayList<ArbitrageEdge>[] outgoingByFromAsset,
            @NotNull LinkedList<ArbitrageEdge> path,
            @NotNull IntegerAtomic sizePath,
            @NotNull LinkedList<Integer> visitedAssets,
            @NotNull HashSet<String> seenCycles,
            @NotNull LinkedList<TriangularArbitrageOpportunity> opportunities
    ) {

        ArrayList<ArbitrageEdge> outgoing = outgoingByFromAsset[currentAsset.index];
        if (outgoing == null || outgoing.isEmpty()) {
            return;
        }
        ArbitrageEdge[] outgoingArray = outgoing.toArray(new ArbitrageEdge[0]);

        for (ArbitrageEdge edge : outgoingArray) {
            int nextLength = sizePath.get() + 1;

            if ((startAsset.hash == edge.getToAsset().hash)) {
                if (nextLength < MIN_CYCLE_LENGTH || nextLength > MAX_CYCLE_LENGTH) {
                    continue;
                }

                path.addLast(edge);
                TriangularArbitrageOpportunity opportunity = buildOpportunityFromEdges(path);
                path.removeLast();

                if (opportunity == null) {
                    continue;
                }

                String canonicalKey = canonicalCycleKey(opportunity.getAssetsCycle());
                if (seenCycles.add(canonicalKey)) {
                    opportunities.add(opportunity);
                }
                continue;
            }

            if (nextLength >= MAX_CYCLE_LENGTH) {
                continue;
            }

            if (visitedAssets.getFirst().equals(edge.getToAsset().hash) ||
                visitedAssets.getLast().equals(edge.getToAsset().hash)
            ) {
                continue;
            }

            path.addLast(edge);
            sizePath.increment();
            visitedAssets.add(edge.getToAsset().hash);
            searchCyclesFrom(
                    startAsset,
                    edge.getToAsset(),
                    outgoingByFromAsset,
                    path,
                    sizePath,
                    visitedAssets,
                    seenCycles,
                    opportunities
            );
            visitedAssets.remove(edge.getToAsset().hash);
            sizePath.decrement();
            path.removeLast();
        }
    }

    private @Nullable TriangularArbitrageOpportunity buildOpportunityFromEdges(@NotNull List<ArbitrageEdge> cycleEdges) {
        int cycleLength = cycleEdges.size();
//        if (cycleLength < MIN_CYCLE_LENGTH || cycleLength > MAX_CYCLE_LENGTH) {
//            return null;
//        }

        List<ArbitrageEdge> cycleEdgesRotate = rotateCycleToPreferredStart(cycleEdges);
        ArbitrageEdge first = cycleEdgesRotate.getFirst();
        String startAsset = first.getFromAsset().asset;
        String currentAsset = startAsset;

        List<String> cycleAssets = new ArrayList<>(cycleLength + 1); // Linked
        cycleAssets.add(startAsset);

        LinkedList<String> distinctAssets = new LinkedList<>();
        distinctAssets.add(startAsset);

        for (int i = 0; i < cycleLength; i++) {
            ArbitrageEdge edge = cycleEdgesRotate.get(i);
            if (!currentAsset.equals(edge.getFromAsset().asset)) {
                return null;
            }

            currentAsset = edge.getToAsset().asset;
            cycleAssets.add(currentAsset);

            if (i < cycleLength - 1 && !distinctAssets.add(currentAsset)) {
                return null;
            }
        }

        if (!startAsset.equals(currentAsset)) {
            return null;
        }
        if (distinctAssets.size() != cycleLength) {
            return null;
        }

        SimulateCycles.SimulationResult simulationResult = SimulateCycles.simulateCycleWithStepSize(cycleEdgesRotate);
        if (simulationResult == null) {
            return null;
        }

        double rateProduct = simulationResult.rateProduct();
        double totalWeight = -Math.log(rateProduct);
        if (rateProduct <= 1.0 + PROFIT_EPSILON) {
            return null;
        }
        if (totalWeight >= -PROFIT_EPSILON) {
            return null;
        }

        String cycleKey = canonicalCycleKey(cycleAssets);
        return lastTriangular.computeIfAbsent(cycleKey, s -> new TriangularArbitrageOpportunity())
                .updateDataAndNextTick(
                        cycleAssets,
                        new ArrayList<>(cycleEdgesRotate),
                        rateProduct,
                        (rateProduct - 1.0) * 100.0,
                        totalWeight
                );
    }

    private @NotNull Set<NameAsset> trackedAssetsFromLastTriangular() {
        Set<NameAsset> trackedAssets = new HashSet<>();
        for (String cycleKey : lastTriangular.keySet()) {
            String[] assets = cycleKey.split("->");
            int limit = Math.max(0, assets.length - 1); // el último repite el inicio
            trackedAssets.addAll(Arrays.asList(assets).subList(0, limit).stream().map(NameAssetIndexed::new).toList());
        }
        return trackedAssets;
    }

    private @NotNull String canonicalCycleKey(@NotNull List<String> cycleAssets) {
        List<String> raw = new ArrayList<>(cycleAssets.subList(0, cycleAssets.size() - 1));
        int size = raw.size();

        List<String> best = null;
        for (int shift = 0; shift < size; shift++) {
            List<String> rotated = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                rotated.add(raw.get((shift + i) % size));
            }

            if (best == null || compareLex(rotated, best) < 0) {
                best = rotated;
            }
        }

        return String.join("->", best) + "->" + best.getFirst();
    }

    private int compareLex(@NotNull List<String> a, @NotNull List<String> b) {
        for (int i = 0; i < a.size(); i++) {
            int cmp = a.get(i).compareTo(b.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    private static class IntegerAtomic {
        public int value;
        public IntegerAtomic(int value) {
            this.value = value;
        }

        public void increment() {
            value++;
        }

        public void decrement() {
            value--;
        }

        public int get() {
            return value;
        }
    }
}
