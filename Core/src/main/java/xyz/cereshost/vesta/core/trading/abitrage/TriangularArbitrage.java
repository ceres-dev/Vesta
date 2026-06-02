package xyz.cereshost.vesta.core.trading.abitrage;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.market.MarketStatus;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.trading.real.api.BinanceConnectorArbitrage;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;
import xyz.cereshost.vesta.core.trading.real.api.model.Ticker24H;
import xyz.cereshost.vesta.core.utils.ExpiredReference;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class TriangularArbitrage {

    private static final double PROFIT_EPSILON = 1e-12;
    private static final double DEFAULT_FEE_RATE = 0.001; // 0.075% aprox 0.00075
    public static final double DEFAULT_START_AMOUNT = 10d;
    private static final String PREFERRED_START_ASSET = "USDT";
    private static final int MIN_CYCLE_LENGTH = 3;
    private static final int MAX_CYCLE_LENGTH = 3;
    private static final int MAX_SYMBOL = 500;

    private final BinanceConnectorArbitrage binanceApi;
    private final Consumer<ResultOpportunities> onOpportunity;

    private volatile boolean started = false;
    private volatile boolean isSubcribed = false;
    @Nullable private volatile ExchangeInfo exchangeInfoSpot = null;
    @Nullable private volatile Consumer<BookTicker> streamListener = null;
    @NotNull private final ConcurrentMap<String, BookTicker> liveTickers = new ConcurrentHashMap<>();
    @NotNull private final ConcurrentMap<String, NameAsset> nameAssetCache = new ConcurrentHashMap<>();

    @NotNull private final BlockingDeque<AtomicReference<BookTicker>> pendingUpdatedTicker = new LinkedBlockingDeque<>(256);
    @Nullable private volatile ExecutorService calculationExecutor = null;
    @NotNull private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

    @SuppressWarnings("DataFlowIssue")
    @Blocking
    public void startSearch(ExecutorService executor, boolean useGraph) {
        if (started) {
            return;
        }
        started = true;
        calculationExecutor = executor;
        CompletableFuture<ExchangeInfo> exchangeInfoFuture = CompletableFuture.supplyAsync(
                binanceApi::getExchangeInfo,
                executor
        );
        CompletableFuture<Map<String, BookTicker>> tickersFuture = CompletableFuture.supplyAsync(
                binanceApi::getBookTickers,
                executor
        );

        executor.execute(() -> {
            try {
                exchangeInfoSpot = exchangeInfoFuture.get();
                liveTickers.clear();
                liveTickers.putAll(tickersFuture.get());

                if (exchangeInfoSpot == null) {
                    Vesta.info("No exchange info found");
                    return;
                }
                Set<Ticker24H> ticker24H = binanceApi.getTicker24H();
                HashMap<String, Ticker24H> bookTicker24H = new HashMap<>();
                for (Ticker24H ticker : ticker24H) {
                    bookTicker24H.put(ticker.symbol(), ticker);
                }

                Set<String> symbolsToSubscribe = getSpotTradingSymbols(exchangeInfoSpot, bookTicker24H);
                liveTickers.keySet().retainAll(symbolsToSubscribe);
                Consumer<BookTicker> listener = this::onBookTickerUpdate;
                streamListener = listener;
                Vesta.info("Total de símbolos disponibles: " + symbolsToSubscribe.size());
                if (!isSubcribed){
                    binanceApi.subscribeIndividualSymbolBookTickerStreams(
                            symbolsToSubscribe,
                            listener
                    );
                    isSubcribed = true;
                }
                buildGraf(exchangeInfoSpot);
                onBookTickerUpdate(null);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stopSearch();
                Vesta.sendWaringException("Error iniciando stream de arbitraje", e);
            } catch (ExecutionException e) {
                stopSearch();
                Vesta.sendWaringException("Error al hacer solicitud a binance", e);
            } catch (Exception e) {
                stopSearch();
                Vesta.sendWaringException("Error suscribiendo streams de bookTicker", e);
            }
        });
    }

    public void stopSearch() {
        started = false;
        Consumer<BookTicker> listener = streamListener;
        if (listener != null) {
            binanceApi.removeBookTickerListener(listener);
        }
        streamListener = null;
        exchangeInfoSpot = null;
        liveTickers.clear();
        pendingUpdatedTicker.clear();
        nameAssetCache.clear();
        lastTriangular.clear();
    }

    private Map<NameAsset, ArrayList<ArbitrageEdge>> buildGraf(@NotNull ExchangeInfo exchangeInfoSpot){
        Map<NameAsset, ArrayList<ArbitrageEdge>> map = new HashMap<>();
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

            NameAsset baseAssetName = nameAssetCache.computeIfAbsent(baseAsset, NameAsset::new);
            NameAsset quoteAssetName = nameAssetCache.computeIfAbsent(quoteAsset, NameAsset::new);

            if (sellRate > 0.0) {
                addEdge(map, new ArbitrageEdge(
                        symbolName,
                        baseAssetName,
                        quoteAssetName,
                        sellRate,
                        -Math.log(sellRate),
                        "SELL",
                        bid,
                        bidLiquidity,
                        symbolConfigurable.getStepSize().orElse(0.0)
                ));
            }

            if (buyRate > 0.0) {
                addEdge(map, new ArbitrageEdge(
                        symbolName,
                        quoteAssetName,
                        baseAssetName,
                        buyRate,
                        -Math.log(buyRate),
                        "BUY",
                        ask,
                        askLiquidity,
                        symbolConfigurable.getStepSize().orElse(0.0)
                ));
            }
        }
        return map;
    }

    @SuppressWarnings("DataFlowIssue")
    private void onBookTickerUpdate(@Nullable BookTicker updatedTicker) {
        if (!started) {
            return;
        }
        if (calculationExecutor == null) {
            return;
        }
        long maxNanoTime = TimeUnit.MILLISECONDS.toNanos(15);
        ExpiredReference<BookTicker> bookTickerExpiredReference = new ExpiredReference<>(updatedTicker, maxNanoTime);
        if (updatedTicker != null) {
            liveTickers.put(updatedTicker.symbol(), updatedTicker);
            calculationExecutor.execute(() -> runCalculationLoop(bookTickerExpiredReference));
        } else {
            // Se llama de manera sincrónica por problemas de concurrencias
            runCalculationLoop(bookTickerExpiredReference);
        }
    }

    private void runCalculationLoop(@NotNull ExpiredReference<BookTicker> bookTicker) {
        try {

            long current = System.currentTimeMillis();

            List<TriangularArbitrageOpportunity> list;
            BookTicker bt = bookTicker.get();
            if (bookTicker.isValid()) {
                list = findTriangularArbitrageOpportunities(
                        // Si es nulo se hará una analizáis total al grafo
                        bt
                );
            }else {
                list = lastTriangular.values().stream().toList();
            }
            consumerExecutor.execute(() -> onOpportunity.accept(new ResultOpportunities(current, list)));
        } catch (Exception e) {
            Vesta.sendWaringException("Error calculando arbitrajes triangulares", e);
        }
    }

    private @NotNull Set<String> getSpotTradingSymbols(@NotNull ExchangeInfo exchangeInfo, @NotNull HashMap<String, Ticker24H> bookTicker24H) {
        Map<String, List<AssetRate>> conversionGraph = buildAssetConversionGraph(exchangeInfo);
        List<SymbolVolume> candidates = new ArrayList<>();

        for (SymbolConfigurable symbolConfigurable : exchangeInfo.symbols().values()) {
            if (!symbolConfigurable.getIsSpot()) continue;
            if (!MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus())) continue;
            if (!symbolConfigurable.getIsAllowTrading()) continue;
            if (!symbolConfigurable.getPermissions().contains("TRD_GRP_074")) continue;


            Ticker24H ticker24H = bookTicker24H.get(symbolConfigurable.name());
            if (ticker24H == null) continue;

            double quoteVolume = ticker24H.quoteVolumen() == null ? 0.0 : ticker24H.quoteVolumen();
            double baseVolume = ticker24H.baseVolumen() == null ? 0.0 : ticker24H.baseVolumen();
            double volumeUsdt = 0.0;

            if (quoteVolume > 0.0) {
                volumeUsdt = convertAssetAmountToUsdt(symbolConfigurable.getQuoteAsset(), quoteVolume, conversionGraph);
            }
            if (volumeUsdt <= 0.0 && baseVolume > 0.0) {
                volumeUsdt = convertAssetAmountToUsdt(symbolConfigurable.getBaseAsset(), baseVolume, conversionGraph);
            }

            candidates.add(new SymbolVolume(symbolConfigurable.name(), volumeUsdt));
        }

        candidates.sort((a, b) -> Double.compare(b.volumeUsdt(), a.volumeUsdt()));
        int limit = Math.min(MAX_SYMBOL, candidates.size());
        Set<String> result = new HashSet<>(limit);
        for (int i = 0; i < limit; i++) {
            result.add(candidates.get(i).symbol());
        }

        return result;
    }

    private @NotNull Map<String, List<AssetRate>> buildAssetConversionGraph(@NotNull ExchangeInfo exchangeInfo) {
        Map<String, List<AssetRate>> graph = new HashMap<>();
        for (SymbolConfigurable symbolConfigurable : exchangeInfo.symbols().values()) {
            if (!symbolConfigurable.getIsSpot()) continue;
            if (!MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus())) continue;

            BookTicker ticker = liveTickers.get(symbolConfigurable.name());
            if (ticker == null || ticker.bidPrice() == null || ticker.askPrice() == null) continue;

            double bid = ticker.bidPrice();
            double ask = ticker.askPrice();
            if (bid <= 0.0 || ask <= 0.0) continue;

            double midPrice = (bid + ask) / 2.0;
            if (midPrice <= 0.0) continue;

            String base = symbolConfigurable.getBaseAsset();
            String quote = symbolConfigurable.getQuoteAsset();
            graph.computeIfAbsent(base, k -> new ArrayList<>()).add(new AssetRate(quote, midPrice));
            graph.computeIfAbsent(quote, k -> new ArrayList<>()).add(new AssetRate(base, 1.0 / midPrice));
        }
        return graph;
    }

    private double convertAssetAmountToUsdt(
            @NotNull String asset,
            double amount,
            @NotNull Map<String, List<AssetRate>> conversionGraph
    ) {
        if (amount <= 0.0) return 0.0;
        if ("USDT".equalsIgnoreCase(asset)) return amount;

        record Node(String asset, double amount) {}

        Deque<Node> queue = new ArrayDeque<>();
        Set<String> visited = new HashSet<>();
        queue.add(new Node(asset, amount));
        visited.add(asset);

        while (!queue.isEmpty()) {
            Node current = queue.poll();
            List<AssetRate> rates = conversionGraph.get(current.asset());
            if (rates == null) continue;

            for (AssetRate rate : rates) {
                double convertedAmount = current.amount() * rate.rate();
                if (convertedAmount <= 0.0) continue;

                if ("USDT".equalsIgnoreCase(rate.toAsset())) {
                    return convertedAmount;
                }
                if (visited.add(rate.toAsset())) {
                    queue.add(new Node(rate.toAsset(), convertedAmount));
                }
            }
        }

        return 0.0;
    }

    private record AssetRate(
            String toAsset,
            double rate
    ) {}

    private record SymbolVolume(
            String symbol,
            double volumeUsdt
    ) {}

    private final ConcurrentMap<String, TriangularArbitrageOpportunity> lastTriangular = new ConcurrentHashMap<>();

    @SneakyThrows
    public @NotNull List<TriangularArbitrageOpportunity> findTriangularArbitrageOpportunities(
            @Nullable BookTicker updatedTicker
    ) {
        Map<NameAsset, ArrayList<ArbitrageEdge>> outgoingByFromAsset = buildGraf(Objects.requireNonNull(exchangeInfoSpot));
        String updatedSymbol = updatedTicker == null ? null : updatedTicker.symbol();
        Set<NameAsset> trackedAssets = trackedAssetsFromLastTriangular();

        Set<NameAsset> startAssetsToAnalyze;
        final boolean detectTriangularPrev;

        if (updatedSymbol != null) {
            startAssetsToAnalyze = new LinkedHashSet<>(2);
            SymbolConfigurable symbol = exchangeInfoSpot.symbols().get(updatedSymbol);
            NameAsset baseAssetName = nameAssetCache.computeIfAbsent(symbol.getBaseAsset(), NameAsset::new);
            NameAsset quoteAssetName = nameAssetCache.computeIfAbsent(symbol.getQuoteAsset(), NameAsset::new);
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

        ConcurrentHashMap<Integer, ArrayList<ArbitrageEdge>> outgoingByFromAssetHash = new ConcurrentHashMap<>(1024);
        for (ConcurrentMap.Entry<NameAsset, ArrayList<ArbitrageEdge>> entry : outgoingByFromAsset.entrySet()) {
            outgoingByFromAssetHash.put(entry.getKey().hash, entry.getValue());
        }
        for (NameAsset startAsset : startAssets) {
            LinkedList<ArbitrageEdge> path = new LinkedList<>();
            LinkedList<String> visitedAssets = new LinkedList<>();
            visitedAssets.add(startAsset.asset);
            searchCyclesFrom(
                    startAsset,
                    startAsset,
                    outgoingByFromAssetHash,
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
                // Si se debio analizar los triángulos previos se elimina
                // para que se agregue en opportunities.addAll(lastTriangular.values());
                // en caso que sea válido
                return detectTriangularPrev;
            }
        });
        opportunities.addAll(lastTriangular.values());
        if (opportunities.isEmpty()) {
            return List.of();
        }
        opportunities.sort(Comparator.comparingDouble(TriangularArbitrageOpportunity::getProfitPercent).reversed());

        return opportunities;
    }


    private void addEdge(@NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> outgoingByFromAsset,
                         @NotNull ArbitrageEdge edge
    ) {
        outgoingByFromAsset
                .computeIfAbsent(edge.fromAsset(), key -> new ArrayList<>(3))
                .add(edge);
    }


    private void searchCyclesFrom(
            @NotNull TriangularArbitrage.NameAsset startAsset,
            @NotNull TriangularArbitrage.NameAsset currentAsset,
            @NotNull ConcurrentMap<Integer, ArrayList<ArbitrageEdge>> outgoingByFromAsset,
            @NotNull LinkedList<ArbitrageEdge> path,
            @NotNull IntegerAtomic sizePath,
            @NotNull LinkedList<String> visitedAssets,
            @NotNull HashSet<String> seenCycles,
            @NotNull LinkedList<TriangularArbitrageOpportunity> opportunities
    ) {
        ArrayList<ArbitrageEdge> outgoing = outgoingByFromAsset.get(currentAsset.hash);
        if (outgoing == null || outgoing.isEmpty()) {
            return;
        }

        for (ArbitrageEdge edge : outgoing) {
            int nextLength = sizePath.get() + 1;

            if ((startAsset.hash - edge.toAsset.hash) == 0) {
                if (nextLength < MIN_CYCLE_LENGTH || nextLength > MAX_CYCLE_LENGTH) {
                    continue;
                }

                path.addLast(edge);
                TriangularArbitrageOpportunity opportunity = buildOpportunityFromEdges(new ArrayList<>(path));
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

            if (visitedAssets.contains(edge.toAsset().asset)) {
                continue;
            }

            path.addLast(edge);
            sizePath.increment();
            visitedAssets.add(edge.toAsset().asset);
            searchCyclesFrom(
                    startAsset,
                    edge.toAsset(),
                    outgoingByFromAsset,
                    path,
                    sizePath,
                    visitedAssets,
                    seenCycles,
                    opportunities
            );
            visitedAssets.remove(edge.toAsset().asset);
            sizePath.decrement();
            path.removeLast();
        }
    }

    private @Nullable TriangularArbitrageOpportunity buildOpportunityFromEdges(@NotNull List<ArbitrageEdge> cycleEdges) {
        int cycleLength = cycleEdges.size();
        if (cycleLength < MIN_CYCLE_LENGTH || cycleLength > MAX_CYCLE_LENGTH) {
            return null;
        }

        cycleEdges = rotateCycleToPreferredStart(cycleEdges);
        ArbitrageEdge first = cycleEdges.getFirst();
        String startAsset = first.fromAsset().asset;
        String currentAsset = startAsset;

        List<String> cycleAssets = new ArrayList<>(cycleLength + 1);
        cycleAssets.add(startAsset);

        Set<String> distinctAssets = new HashSet<>();
        distinctAssets.add(startAsset);

        for (int i = 0; i < cycleLength; i++) {
            ArbitrageEdge edge = cycleEdges.get(i);
            if (!currentAsset.equals(edge.fromAsset().asset)) {
                return null;
            }

            currentAsset = edge.toAsset().asset;
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

        SimulationResult simulationResult = simulateCycleWithStepSize(cycleEdges);
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
                        new ArrayList<>(cycleEdges),
                        rateProduct,
                        (rateProduct - 1.0) * 100.0,
                        totalWeight
                );
    }

    private @NotNull List<ArbitrageEdge> rotateCycleToPreferredStart(@NotNull List<ArbitrageEdge> cycleEdges) {
        int preferredIndex = -1;
        for (int i = 0; i < cycleEdges.size(); i++) {
            if (PREFERRED_START_ASSET.equals(cycleEdges.get(i).fromAsset().asset)) {
                preferredIndex = i;
                break;
            }
        }
        if (preferredIndex <= 0) {
            return new ArrayList<>(cycleEdges);
        }

        List<ArbitrageEdge> rotated = new ArrayList<>(cycleEdges);
        Collections.rotate(rotated, -preferredIndex);
        return rotated;
    }

    private @Nullable SimulationResult simulateCycleWithStepSize(@NotNull List<ArbitrageEdge> cycleEdges) {
        double amount = DEFAULT_START_AMOUNT;
        for (ArbitrageEdge edge : cycleEdges) {
            if (!Double.isFinite(amount) || amount <= 0.0) {
                return null;
            }

            if ("SELL".equals(edge.action())) {
                double quantity = roundDownToStepSize(amount, edge.stepSize());

                if (quantity <= 0.0) {
                    return null;
                }

                if (quantity > edge.referenceLiquidity()) {
                    return null;
                }

                amount = quantity * edge.referencePrice() * (1.0 - DEFAULT_FEE_RATE);
                continue;
            }

            if ("BUY".equals(edge.action())) {
                double quantity = roundDownToStepSize(amount / edge.referencePrice(), edge.stepSize());

                if (quantity <= 0.0) {
                    return null;
                }

                if (quantity > edge.referenceLiquidity()) {
                    return null;
                }

                amount = quantity * (1.0 - DEFAULT_FEE_RATE);
                continue;
            }

            return null;
        }

        if (!Double.isFinite(amount) || amount <= 0.0) {
            return null;
        }
        return new SimulationResult(amount / DEFAULT_START_AMOUNT, amount);
    }

    private double roundDownToStepSize(double amount, double stepSize) {
        if (!Double.isFinite(amount) || amount <= 0.0) {
            return 0.0;
        }
        if (!Double.isFinite(stepSize) || stepSize <= 0.0) {
            return amount;
        }

        double result = Math.floor(amount / stepSize) * stepSize;
        return Math.round(result * 1e12) / 1e12;
    }

    private @NotNull Set<NameAsset> trackedAssetsFromLastTriangular() {
        Set<NameAsset> trackedAssets = new HashSet<>();
        for (String cycleKey : lastTriangular.keySet()) {
            String[] assets = cycleKey.split("->");
            int limit = Math.max(0, assets.length - 1); // el último repite el inicio
            trackedAssets.addAll(Arrays.asList(assets).subList(0, limit).stream().map(NameAsset::new).toList());
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

    public record ArbitrageEdge(
            String symbol,
            NameAsset fromAsset,
            NameAsset toAsset,
            double rate,
            double weight,
            String action,
            double referencePrice,
            double referenceLiquidity,
            double stepSize
    ) {}

    private record SimulationResult(
            double rateProduct,
            double endAmount
    ) {}

    @Data
    public static final class TriangularArbitrageOpportunity {
        private List<String> assetsCycle;
        private List<ArbitrageEdge> edges;
        private final LifeTime lifeTime;
        private double rateProduct;
        private double profitPercent;
        private double totalWeight;

        public TriangularArbitrageOpportunity(
                List<String> assetsCycle,
                List<ArbitrageEdge> edges,
                LifeTime lifeTime,
                double rateProduct,
                double profitPercent,
                double totalWeight
        ) {
            this.assetsCycle = assetsCycle;
            this.edges = edges;
            this.lifeTime = lifeTime;
            this.rateProduct = rateProduct;
            this.profitPercent = profitPercent;
            this.totalWeight = totalWeight;
        }

        public TriangularArbitrageOpportunity() {
            this.assetsCycle = List.of();
            this.edges =  List.of();
            this.lifeTime = new LifeTime();
            this.rateProduct = -1;
            this.profitPercent = -1;
            this.totalWeight = -1;
        }

        @Contract("_, _, _, _, _ -> this")
        public TriangularArbitrageOpportunity updateDataAndNextTick(
                List<String> assetsCycle,
                List<ArbitrageEdge> edges,
                double rateProduct,
                double profitPercent,
                double totalWeight
        ) {
            this.assetsCycle = assetsCycle;
            this.edges = edges;
            this.lifeTime.nextTicks();
            this.rateProduct = rateProduct;
            this.profitPercent = profitPercent;
            this.totalWeight = totalWeight;
            return this;
        }


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

    public static class NameAsset {
        public final String asset;
        public final Integer hash;

        private static final ConcurrentMap<String, Byte[]> cacheBytes = new ConcurrentHashMap<>();

        public NameAsset(String asset) {
            this.asset = asset;
            int h = 0;
            for (byte b : cacheBytes.computeIfAbsent(asset, a -> {
                byte[] bytes = a.getBytes(StandardCharsets.UTF_8);
                Byte[] byteArray = new Byte[bytes.length];
                for (int i = 0; i < bytes.length; i++) {
                    byteArray[i] = bytes[i];
                }
                return byteArray;
            })) {
                h = 7 * ((h + 37) << b);
            }
            this.hash = h;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof NameAsset nameAsset) {
                return Objects.equals(nameAsset.hash, this.hash);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return hash.intValue();
        }
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

    public record ResultOpportunities(
            long startTime,
            List<TriangularArbitrageOpportunity> opportunities
    ){}
}
