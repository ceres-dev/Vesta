package xyz.cereshost.vesta.core.command.commnads;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.Main;
import xyz.cereshost.vesta.core.command.Arguments;
import xyz.cereshost.vesta.core.command.BaseCommand;
import xyz.cereshost.vesta.core.command.Flags;
import xyz.cereshost.vesta.core.market.DireccionOperation;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.message.DiscordNotification;
import xyz.cereshost.vesta.core.message.MediaNotification;
import xyz.cereshost.vesta.core.trading.abitrage.TriangularArbitrage;
import xyz.cereshost.vesta.core.trading.abitrage.TriangularEngine;
import xyz.cereshost.vesta.core.trading.abitrage.model.TriangularArbitrageOpportunity;
import xyz.cereshost.vesta.core.trading.real.api.BinanceConnectorArbitrage;
import xyz.cereshost.vesta.core.utils.LoaderIndicator;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public class Arbitration extends BaseCommand implements Flags {

    public Arbitration()  {
        super("Ejecuta una estrategia de arbitraje triangular");
    }

    @Override
    public List<Flag> getFlags() {
        return List.of(new Flag("testnet", TypeValue.BOOLEAN));
    }

    private final Set<TriangularArbitrageOpportunity> opportunityWindows = new HashSet<>();

    @Override
    public void execute(Arguments arguments) throws Exception {
        LoaderIndicator loaderIndicator = new LoaderIndicator(10);
        loaderIndicator.setLabel("Buscado Arbitrajes...");

        boolean isTestNet = arguments.getFlagBolean("testnet");
        BinanceConnectorArbitrage binance = new BinanceConnectorArbitrage(isTestNet, Executors.newThreadPerTaskExecutor(new ThreadFactory() {
            private int i = 0;

            @Override
            public Thread newThread(@NotNull Runnable r){
                Thread t = new Thread(r);
                t.setName("stream-webSocket-" + i);
                i++;
                return t;
            }
        }));
//        binance.checkApikey();

        MediaNotification mediaNotification = new DiscordNotification();

        mediaNotification.updateStatusType(MediaNotification.StatusType.WORKING);
        mediaNotification.updateStatus("Analizado todos los mercados");

        AtomicLong windowStart = new AtomicLong(0L);
        ExecutorArbitrage executorArbitrage = new ExecutorArbitrage(Main.EXECUTOR, binance, mediaNotification);
        AtomicLong counterDecent = new AtomicLong(0L);
//        Vesta.info("USDT: " + binance.getBalance().get("USDT"));

        TriangularArbitrage triangularArbitrage = new TriangularArbitrage(binance, opportunities -> {
            HashSet<TriangularArbitrageOpportunity> current = new HashSet<>(opportunities.opportunities());
            boolean changed = !current.equals(opportunityWindows);

            executorArbitrage.onOpprtunities(opportunities.opportunities());
            executorArbitrage.tryRunLoop();
            if (!changed) {
                return;
            }
            long currentTime = System.currentTimeMillis();
            if (current.isEmpty() && !opportunityWindows.isEmpty()) {
                loaderIndicator.clearLine();
                ArrayList<TriangularArbitrageOpportunity> arrayOpportunity = new ArrayList<>(opportunityWindows);

                for (int i = 0; i < opportunityWindows.size(); i++) {
                    TriangularArbitrageOpportunity opportunity = arrayOpportunity.get(i);
                    if (opportunity.getLifeTime().getTicks() > 10){
                        long delta = currentTime - opportunity.getLifeTime().getDateOpen();
                        Vesta.info(
                                "[%d] %s | retorno %.6f | profit %.4f%% | peso %.8f | %,dms (%d Tick)",
                                i + 1,
                                String.join(" -> ", opportunity.getAssetsCycle()),
                                opportunity.getRateProduct(),
                                opportunity.getProfitPercent(),
                                opportunity.getTotalWeight(),
                                delta,
                                opportunity.getLifeTime().getTicks()
                        );
                        for (TriangularEngine.ArbitrageEdge edge : opportunity.getEdges()) {
                            Vesta.info(
                                    "    %s %s via %s @ %.10f -> rate %.10f ",
                                    edge.getAction(),
                                    edge.getFromAsset().asset + "/" + edge.getToAsset().asset,
                                    edge.getSymbol(),
                                    edge.getReferencePrice(),
                                    edge.getRate(),
                                    edge.getWeight()
                            );
                        }
                    }
                }
                executorArbitrage.onClose();
                windowStart.set(-1);
            }

            if (!current.isEmpty()) {
                loaderIndicator.clearLine();
                if (windowStart.get() == -1) {
                    windowStart.set(currentTime);
//                    Vesta.info("Inicio ventana de arbitraje");
                }
                executorArbitrage.onOpportunity(opportunities.opportunities());
                counterDecent.incrementAndGet();
//                Vesta.info("Arbitrajes detectados: %d", opportunities.size());
            }

            opportunityWindows.clear();
            opportunityWindows.addAll(current);

        }, (onUpdate) -> {
            updateLoader(loaderIndicator, counterDecent, onUpdate.startNanoTime());
            updateStatus(mediaNotification, counterDecent);
        });
//        triangularArbitrage.startSearch(Executors.newCachedThreadPool(new ThreadFactory() {
//            private int i = 0;
//
//            @Override
//            public Thread newThread(@NotNull Runnable r){
//                Thread t = new Thread(r);
//                t.setName("graft-conumer-" + i);
//                i++;
//                return t;
//            }
//        }), true);
        triangularArbitrage.startSearch(Main.EXECUTOR);
//        Main.EXECUTOR.scheduleAtFixedRate(() -> {
//            executorArbitrage.onTick(List.of());
//            triangularArbitrage.stopSearch(); // TODO: Arreglar esto.
//            triangularArbitrage.startSearch(Main.EXECUTOR, true);
//            Vesta.info("Reiniciando cache");
//        }, 2, 2, TimeUnit.HOURS);
    }

    private final Queue<Long> deltasProcessing = new LinkedList<>();
    private final Queue<Long> deltasTicks = new LinkedList<>();
    private final AtomicLong deltaTick = new AtomicLong(System.currentTimeMillis());

    private void updateLoader(LoaderIndicator loaderIndicator, AtomicLong counter, long deltaNanoProcessing) {
        long time = System.nanoTime();
        if (deltasProcessing.size() > 100) {
            deltasProcessing.poll();
        }
        if (deltasTicks.size() > 100) {
            deltasTicks.poll();
        }

        deltasProcessing.offer(time - deltaNanoProcessing);
        deltasTicks.offer(time - deltaTick.get());
        deltaTick.set(time);
        try {
            double avgProcessing = deltasProcessing.stream().mapToLong(Long::longValue).average().orElse(-1);
            double avgTick = deltasTicks.stream().mapToLong(Long::longValue).average().orElse(-1);
            loaderIndicator.setLabel("%.2fms / %.2fms (%.2fu/s) Arbitraje detectados: %d. Buscando arbitrajes..."
                    .formatted(avgProcessing/1000000, avgTick/1000000, 1000d / (avgTick/1000000), counter.get()));
            loaderIndicator.printAndNexStep();
        } catch (NullPointerException | ConcurrentModificationException ignored) {
            loaderIndicator.setLabel("-1ms / -1ms (-1u/s) Arbitraje detectados: %d. Buscando arbitrajes..."
                    .formatted(counter.get()));
            loaderIndicator.printAndNexStep();
        }
    }

    private long coolDown = System.currentTimeMillis();

    public void updateStatus(MediaNotification media, AtomicLong counter) {
        long time = System.currentTimeMillis();
        if (coolDown < time) {
            media.updateStatus("Posibles Arbitrajes detectados %d (%.2fu/s)", counter.get(), 1000 / deltasProcessing.stream().mapToLong(Long::longValue).average().orElse(-1));
            coolDown = time + TimeUnit.SECONDS.toMillis(15);
        }
    }

    @RequiredArgsConstructor
    public static class ExecutorArbitrage {

        private final @NotNull DecimalFormat decimalFormat = new DecimalFormat("0.00#######");
        private final @NotNull ScheduledExecutorService executor;
        private final @NotNull BinanceConnectorArbitrage binanceApi;
        private final @NotNull HashMap<String, SymbolConfigurable> symbolsByName;
        private final @NotNull MediaNotification mediaNotification;

        public ExecutorArbitrage(@NotNull ScheduledExecutorService executor,
                                 @NotNull BinanceConnectorArbitrage binanceApi,
                                 @NotNull MediaNotification mediaNotification
        ) {
            this.executor = executor;
            this.binanceApi = binanceApi;
            HashMap<String, SymbolConfigurable> symbolsByName = new HashMap<>();
            for (SymbolConfigurable symbolConfigurable : binanceApi.getExchangeInfo().symbols().values()) {
                symbolsByName.put(symbolConfigurable.name(), symbolConfigurable);
            }
            this.symbolsByName = symbolsByName;
            this.mediaNotification = mediaNotification;
        }

        private volatile TriangularArbitrageOpportunity opportunity;
        private volatile List<TriangularArbitrageOpportunity> lastOpportunities = List.of();

        public void onOpprtunities(@NotNull List<TriangularArbitrageOpportunity> opportunities){
            lastOpportunities = opportunities;
        }

        public synchronized void onOpportunity(@NotNull List<TriangularArbitrageOpportunity> opportunities) {
            TriangularArbitrageOpportunity best = null;
            for (TriangularArbitrageOpportunity opportunity : opportunities) {
                if (checkOpportunity(opportunity)) {
                    if (best == null || opportunity.getProfitPercent() > best.getProfitPercent()) {
                        best = opportunity;
                    }
                }
            }

            if (best != null) {
                if (best.getProfitPercent() < 0.1f) return;
                final TriangularArbitrageOpportunity b = best;
                executor.schedule(() -> {
                    for (TriangularArbitrageOpportunity opportunity : lastOpportunities) {
                        if (
                                b.getEdges().size() != opportunity.getEdges().size() ||
                                        !new HashSet<>(b.getAssetsCycle()).containsAll(opportunity.getAssetsCycle())
                        ) return;
                        TriangularEngine.ArbitrageEdge USDT = null;

                        for (TriangularEngine.ArbitrageEdge edge : opportunity.getEdges()) {
                            if (edge.getFromAsset().asset.equals("USDT")){
                                USDT = edge;
                                break;
                            }
                        }
                        // No debería ser nulo
                        if (USDT == null) {
                            Vesta.info("No hay USDT ignorando el arbitraje");
                            return;
                        }
                        int index = opportunity.getEdges().indexOf(USDT);
                        List<TriangularEngine.ArbitrageEdge> rotatedEdges = new ArrayList<>(opportunity.getEdges());
                        if (index != -1) {
                            Collections.rotate(rotatedEdges, -index);
                        }
                        List<String> rotatedAssets = new ArrayList<>(rotatedEdges.size() + 1);
                        if (!rotatedEdges.isEmpty()) {
                            rotatedAssets.add(rotatedEdges.getFirst().getFromAsset().asset);
                            for (TriangularEngine.ArbitrageEdge edge : rotatedEdges) {
                                rotatedAssets.add(edge.getToAsset().asset);
                            }
                        }
                        this.opportunity = new TriangularArbitrageOpportunity(
                                List.copyOf(rotatedAssets),
                                List.copyOf(rotatedEdges),
                                opportunity.getLifeTime(),
                                opportunity.getRateProduct(),
                                opportunity.getProfitPercent(),
                                opportunity.getTotalWeight()
                        );
                        break;
                    }
                }, 200, TimeUnit.MILLISECONDS);
            }

        }

        public synchronized void onClose() {
            this.opportunity = null;
        }

        private volatile CompletableFuture<Object> runLoop = CompletableFuture.completedFuture(new Object());

        public synchronized void tryRunLoop() {
            if (runLoop.isDone() && opportunity != null) {
                Vesta.clearLine();
                Vesta.info("Iniciando bucle con: " + opportunity.getEdges().stream().map(TriangularEngine.ArbitrageEdge::getSymbol).toList());

                runLoop = CompletableFuture.supplyAsync(() -> {
                    try {

                        while (opportunity != null) {

                            if (opportunity == null) {
                                return new Object();
                            }
                            List<TriangularEngine.ArbitrageEdge> edges = new ArrayList<>(opportunity.getEdges());


                            double balance = TriangularEngine.DEFAULT_START_AMOUNT;
                            for (TriangularEngine.ArbitrageEdge edge : edges) {
                                Vesta.clearLine();
                                Vesta.info("Ejecutando: %s %s", edge.getSymbol(), (edge.getAction().equals("SELL") ? "\u001B[31mSELL" : "\u001B[32mBUY") + "\u001B[0m");
                                SymbolConfigurable symbol = symbolsByName.get(edge.getSymbol());
                                if (symbol == null) {
                                    Vesta.warning("Símbolo no soportado en este entorno: " + edge.getSymbol());
                                    continue;
                                }
                                DireccionOperation direccion = DireccionOperation.parse(edge.getAction());
//                                OrderResult orderResult = binanceApi.placeMarketOrder(
//                                        symbol,
//                                        direccion,
//                                        balance,
//                                        direccion.isShort()
//                                );
                                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
//                                balance = orderResult.receivedQty();
                                if (edge.getToAsset().asset.equals("USDT")) {
                                    Vesta.clearLine();
                                    Vesta.info( (balance > TriangularEngine.DEFAULT_START_AMOUNT ?
                                            "\u001B[32mGanado: " + " +" +decimalFormat.format(balance-TriangularEngine.DEFAULT_START_AMOUNT) :
                                            "\u001B[31mPerdido: " + " " + decimalFormat.format(balance-TriangularEngine.DEFAULT_START_AMOUNT)) +
                                            " USDT\u001B[0m");
                                }
                                if (balance < TriangularEngine.DEFAULT_START_AMOUNT){
                                    opportunity = null;
                                }

                                if (edge.getToAsset().asset.equals("BNB")) {
                                    balance = Math.max(0.0006, balance - 0.0006);
                                }
//                                if (!Double.isFinite(balance) || balance <= 0.0) {
//                                    throw new IllegalStateException("La orden " + orderResult.orderId() + " no retorno cantidad recibida valida");
//                                }
                            }
//                        mediaNotification.info("Arbitrage %s (Teórico) en **%s**",
//                                opportunity != null ? "Ganado" : "Perdido",
//                                String.join(" -> ", opportunity.assetsCycle())
//                        );
                        }
                    }catch (Exception e){
                        Vesta.sendWaringException("Error al ejecutar el bucle", e);
                    }
                    return new Object();
                });
            }
        }

        private boolean checkOpportunity(TriangularArbitrageOpportunity opportunity) {
            for (TriangularEngine.ArbitrageEdge edge : opportunity.getEdges()) {
//                if (edge.fromAsset().asset.equals("BTC") || edge.toAsset().asset.equals("BTC")){
//                    return false;
//                }
//                if (edge.fromAsset().asset.equals("BNB") || edge.toAsset().asset.equals("BNB")){
//                    return false;
//                }
//                if (edge.fromAsset().asset.equals("ETH") || edge.toAsset().asset.equals("ETH")){
//                    return false;
//                }
                if (edge.getFromAsset().asset.equals("USDT")) {
                    return true;
                }
            }
            return false;
        }
    }
}
