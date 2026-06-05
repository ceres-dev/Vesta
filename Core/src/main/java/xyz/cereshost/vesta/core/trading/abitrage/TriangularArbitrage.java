package xyz.cereshost.vesta.core.trading.abitrage;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Blocking;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.market.MarketStatus;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.trading.abitrage.model.AssetRate;
import xyz.cereshost.vesta.core.trading.abitrage.model.TriangularArbitrageOpportunity;
import xyz.cereshost.vesta.core.trading.real.api.BinanceConnectorArbitrage;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;
import xyz.cereshost.vesta.core.trading.real.api.model.Ticker24H;
import xyz.cereshost.vesta.core.utils.ExpiredReference;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

@RequiredArgsConstructor
public class TriangularArbitrage {

    public static final int MAX_SYMBOL = 650;

    private final BinanceConnectorArbitrage binanceApi;
    private final Consumer<TriangularEngine.OnOpportunities> onOpportunity;
    private final Consumer<TriangularEngine.OnUpdate> onUpdate;
    private final TriangularEngine engine = new TriangularEngineJava();

    private volatile boolean started = false;
    private volatile boolean isSubcribed = false;

    @Nullable private volatile ExchangeInfo exchangeInfoSpot = null;
    @Nullable private volatile Consumer<BookTicker> streamListener = null;
    @Nullable private volatile ExecutorService calculationExecutor = null;
    @NotNull private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();

    @SuppressWarnings("DataFlowIssue")
    @Blocking
    public void startSearch(ExecutorService executor) {
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
                HashMap<String, BookTicker> bookTickers = new HashMap<>(tickersFuture.get());

                if (exchangeInfoSpot == null) {
                    Vesta.info("No exchange info found");
                    return;
                }
                engine.setExchangeInfo(exchangeInfoSpot);
                Set<Ticker24H> ticker24H = binanceApi.getTicker24H();
                HashMap<String, Ticker24H> bookTicker24H = new HashMap<>();
                for (Ticker24H ticker : ticker24H) {
                    bookTicker24H.put(ticker.symbol(), ticker);
                }

                Set<String> symbolsToSubscribe = getSpotTradingSymbols(exchangeInfoSpot, bookTicker24H);
                bookTickers.keySet().retainAll(symbolsToSubscribe);
                engine.addLiverTicker(bookTickers);
                engine.buildGraf(exchangeInfoSpot);
                streamListener = this::onBookTickerUpdate;
                Vesta.info("Total de símbolos disponibles: " + symbolsToSubscribe.size());
                if (!isSubcribed){
                    binanceApi.subscribeIndividualSymbolBookTickerStreams(
                            symbolsToSubscribe,
                            streamListener
                    );
                    isSubcribed = true;
                }
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
    }

    private void onBookTickerUpdate(@Nullable BookTicker updatedTicker) {
        if (!started) {
            return;
        }
        if (calculationExecutor == null) {
            return;
        }
        long maxNanoTime = TimeUnit.MILLISECONDS.toNanos(25);
        ExpiredReference<BookTicker> bookTickerExpiredReference = new ExpiredReference<>(updatedTicker, maxNanoTime);
        if (updatedTicker != null) {
            engine.liveTickers.put(updatedTicker.symbol(), updatedTicker);
        }
        runCalculationLoop(bookTickerExpiredReference);
    }

    private void runCalculationLoop(@NotNull ExpiredReference<BookTicker> bookTicker) {
        try {
            long currentNano = System.nanoTime();

            List<TriangularArbitrageOpportunity> list;
            BookTicker bt = bookTicker.get();
            if (bookTicker.isValid()) {
                list = engine.findTriangularArbitrageOpportunities(
                        // Si es nulo se hará una analizáis total al grafo
                        bt
                );
            }else {
                System.out.println("BookTicker expirado, realizando análisis completo del grafo " + bookTicker);
                list = engine.lastTriangular.values().stream().toList();
            }
            consumerExecutor.execute(() -> {
                onUpdate.accept(new TriangularEngine.OnUpdate(currentNano));
                onOpportunity.accept(new TriangularEngine.OnOpportunities(list));
            });
        } catch (Exception e) {
            Vesta.sendWaringException("Error calculando arbitrajes triangulares", e);
        }
    }

    private @NotNull Set<String> getSpotTradingSymbols(@NotNull ExchangeInfo exchangeInfo, @NotNull HashMap<String, Ticker24H> bookTicker24H) {
        Map<String, List<AssetRate>> conversionGraph;
        conversionGraph = buildAssetConversionGraph(exchangeInfo);
        List<SymbolVolume> candidates = new ArrayList<>();

        for (SymbolConfigurable symbolConfigurable : exchangeInfo.symbols().values()) {
            if (!symbolConfigurable.getIsSpot()) continue;
            if (!MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus())) continue;
            if (!symbolConfigurable.getIsAllowTrading()) continue;
//            if (!symbolConfigurable.getPermissions().contains("TRD_GRP_074")) continue;


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

    private @NotNull Map<String, List<AssetRate>> buildAssetConversionGraph(@NotNull ExchangeInfo exchangeInfo) {
        Map<String, List<AssetRate>> graph = new HashMap<>();
        for (SymbolConfigurable symbolConfigurable : exchangeInfo.symbols().values()) {
            if (!symbolConfigurable.getIsSpot()) continue;
            if (!MarketStatus.TRADING.equals(symbolConfigurable.getMarketStatus())) continue;

            BookTicker ticker = engine.liveTickers.get(symbolConfigurable.name());
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

    protected record SymbolVolume(
            String symbol,
            double volumeUsdt
    ) {}
}
