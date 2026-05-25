package xyz.cereshost.vesta.core;

import ai.djl.Device;
import ai.djl.util.Pair;
import com.google.gson.Gson;
import lombok.Getter;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.jetbrains.annotations.NotNull;
import xyz.cereshost.vesta.common.Vesta;
import xyz.cereshost.vesta.core.command.HanderCommand;
import xyz.cereshost.vesta.core.command.commnads.*;
import xyz.cereshost.vesta.core.ia.PredictionEngine;
import xyz.cereshost.vesta.core.ia.utils.XNormalizer;
import xyz.cereshost.vesta.core.ia.utils.YNormalizer;
import xyz.cereshost.vesta.core.io.IOMarket;
import xyz.cereshost.vesta.core.io.IOdata;
import xyz.cereshost.vesta.core.io.setup.LoadDataMethodLocalRange;
import xyz.cereshost.vesta.core.market.*;
import xyz.cereshost.vesta.core.message.DiscordNotification;
import xyz.cereshost.vesta.core.packet.PacketHandlerServer;
import xyz.cereshost.vesta.core.strategy.strategis.AlfaStrategy;
import xyz.cereshost.vesta.core.strategy.strategis.BetaStrategy;
import xyz.cereshost.vesta.core.trading.TradingTelemetry;
import xyz.cereshost.vesta.core.trading.backtest.BackTestEngine;
import xyz.cereshost.vesta.core.trading.real.TradingTickLoop;
import xyz.cereshost.vesta.core.trading.real.api.BinanceApiRest;
import xyz.cereshost.vesta.core.utils.BuilderData;
import xyz.cereshost.vesta.core.utils.ChartUtils;
import xyz.cereshost.vesta.core.utils.candle.CandleIndicators;
import xyz.cereshost.vesta.core.utils.candle.SequenceCandles;

import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Main {

    @Getter
    public static final PacketHandlerServer server = new PacketHandlerServer();
    @Getter
    public static final HanderCommand handerCommand = new HanderCommand();
    public static final String NAME_MODEL = "VestaIA";

    public static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(8);

    @NotNull public static final TypeMarket TYPE_MARKET = new TypeMarket(SymbolFutures.ETHUSDC, TimeFrameMarket.FIVE_MINUTE);
    @NotNull public static final List<TypeMarket> SYMBOLS_TRAINING = List.of(TYPE_MARKET);
    public static final int MAX_MONTH_TRAINING = 12*2;
    public static final Gson GSON = new Gson();

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        handerCommand.registerCommand(
                new Executor(),
                new FastSetupTrainingModel(),
                new FastSetupPrediction(),
                new Help(),
                new Server(),
                new Arbitration()
        );
        handerCommand.dispatch(args);

        // TODO: Borrar este switch y cambiar los por comandos
        switch (args[0]) {
//            case "backtest" -> showDataBackTest(new BackTestEngine(IOMarket.loadMarkets(DATA_SOURCE_FOR_BACK_TEST, SYMBOL).limit(3), PredictionEngine.loadPredictionEngine("VestaIA"), new AlfaStrategy()).run());
            case "backtest" -> {
                Vesta.info("🔙 Ejecutando backtest...");
                Pair<XNormalizer, YNormalizer> pair = IOdata.loadNormalizers();
                PredictionEngine engine = new PredictionEngine(pair.getKey(), pair.getValue(), IOdata.loadModel(Device.gpu()));
                TradingTelemetry telemetry = new BackTestEngine(engine, new BetaStrategy()).run();
                TradingTelemetry.Summary summary = telemetry.getSummary();
                DecimalFormat decimalFormat = new DecimalFormat("###,###,###,###,##0.00");

                Vesta.info("  Side Rate (L/S):        %.2f/%.2f", telemetry.getRatioLong(), telemetry.getRatioShort());
                Vesta.info("  Trades:                 %d", telemetry.getTrades().size());
                Vesta.info("  PNL Neto:               %s$%s%s", summary.netPnl() >= 0 ? "\u001B[32m" : "\u001B[31m", decimalFormat.format(summary.netPnl()), "\u001B[0m");
                Vesta.info("  ROI Total:              %s%s%%%s", summary.totalRoi() >= 0 ? "\u001B[32m" : "\u001B[31m", decimalFormat.format(summary.totalRoi()), "\u001B[0m");
                Vesta.info("  Rendimiento:            %s%.2f%%%s ", summary.performer() >= 0 ? "\u001B[32m" : "\u001B[31m", summary.performer(), "\u001B[0m");
                ChartUtils.showCandleChartWithTradeSnapshots("Resultados", telemetry.getMarket().getCandles().stream().toList(), telemetry.getMarket().getSymbol(), telemetry);

            }
            case "trading" -> new TradingTickLoop(TYPE_MARKET, null, new AlfaStrategy(), new BinanceApiRest(false, false), new DiscordNotification()).startCandleLoop();
            case "extract" -> IOMarket.extractFirstBin(Path.of(IOMarket.STORAGE_DIR + "\\" + TYPE_MARKET.symbol() +"\\trades"));
            case "diagnose" -> {
                Market market = getMarket(false);
                Pair<XNormalizer, YNormalizer> pair = IOdata.loadNormalizers();

                List<Integer> indexes = List.of(120, 180, 240, 300, 360, 420, 480);
                //for (int i = 300; i < 400; i+=5) indexes.add(i);
                for (int i : indexes) {
                    showPredictionSnapshot(market, new PredictionEngine(pair.getKey(), pair.getValue(), IOdata.loadModel(Device.gpu())), i, 20);
                }
            }
        }
    }

    private static @NotNull Market getMarket(boolean loadTrade) {
        return Objects.requireNonNull(
                IOMarket.loadMarket(
                    TYPE_MARKET,
                    new LoadDataMethodLocalRange(loadTrade, 0, 60),
                        false
                )
        );
    }

//    private static void showDataBackTest(BackTestEngine.BackTestResult backtestResult) {
//        BackTestEngine.BackTestStats stats = backtestResult.stats();
//        ChartUtils.plotRatioVsROI("BackTest Ratio/ROI (Walk-Forward)", stats.getTradesComplete());
//        ChartUtils.plotTPSLMagnitudeVsROI("BackTest Magnitud/ROI (Walk-Forward)", stats.getTradesComplete());
//        ChartUtils.plot("BackTest Ratio (Walk-Forward)", "Trades", List.of(
//                new ChartUtils.DataPlot("Ratio", stats.getTradesComplete().stream().map(BackTestEngine.CompleteTrade::getRatio).toList())
//        ));
//        ChartUtils.plot("BackTest Balance (Walk-Forward)", "Trades", List.of(
//                new ChartUtils.DataPlot("Balance", stats.getTradesComplete().stream().map(BackTestEngine.CompleteTrade::getBalance).toList())
//        ));
//        ChartUtils.plot("BackTest ROI (Walk-Forward)", "Trades", List.of(
//                new ChartUtils.DataPlot("ROI%", stats.getTradesComplete().stream().map(BackTestEngine.CompleteTrade::getPnlPercent).toList())
//        ));
//        //ChartUtils.animateCandlePredictions(stats.getMarket().getSymbol(), stats.getMarket().getCandleSimples().stream().toList(), stats.getAllTrades(), BuilderData.DEFAULT_FUTURE_WINDOW, 200);
//        ChartUtils.showCandleChartWithTrades("Trades", stats.getMarket().getCandles().stream().toList(), stats.getMarket().getSymbol(), stats.getTradesComplete());
//        double winRate = stats.getTotalTrades() > 0 ? (double) stats.getWins() / stats.getTotalTrades() * 100 : 0;
//        double avgHoldMinutes = stats.getTotalTrades() > 0 ? (stats.getHoldAvg() / 1000.0 / 60.0) : 0;
//        double avgHoldMinutesWin = stats.getWins() > 0 ? (stats.getHoldAvgWins() / 1000.0 / 60.0) : 0;
//        double avgHoldMinutesLoss = stats.getLosses() > 0 ? (stats.getHoldAvgLoss() / 1000.0 / 60.0) : 0;
//        double performer = winRate - (Math.abs(stats.getRoiLosses()) / (Math.abs(stats.getRoiLosses()) + stats.getRoiWins()))*100;
//
//        DecimalFormat decimalFormat = new DecimalFormat("###,###,###,###,##0.00");
//
//        Vesta.info("--------------------------------------------------");
//        Vesta.info("💰 SIMULACIÓN DE TRADING (Capital: $%.0f)", backtestResult.initialBalance());
//        Vesta.info(" Trades Totales:          %d",  stats.getTotalTrades());
//        Vesta.info("  Win Rate:               %.2f%% (%d W / %d L)", winRate, stats.getWins(), stats.getLosses());
//        Vesta.info("  Timeouts:               %d (Salida por tiempo) ROI %.2f%% ", stats.getTimeouts(), stats.getRoiTimeOut());
//        Vesta.info("  Total TP/SL/STR:        %d TP / %s SL / %s STR", stats.getTrades(TradingManager.ExitReason.LONG_TAKE_PROFIT) + stats.getTrades(TradingManager.ExitReason.SHORT_TAKE_PROFIT), stats.getTrades(TradingManager.ExitReason.LONG_STOP_LOSS) + stats.getTrades(TradingManager.ExitReason.SHORT_STOP_LOSS), stats.getTrades(TradingManager.ExitReason.STRATEGY));
//        Vesta.info("  L TP/SL:                %d TP / %s SL", stats.getTrades(TradingManager.ExitReason.LONG_TAKE_PROFIT), stats.getTrades(TradingManager.ExitReason.LONG_STOP_LOSS));
//        Vesta.info("  S TP/SL:                %d TP / %s SL", stats.getTrades(TradingManager.ExitReason.SHORT_TAKE_PROFIT), stats.getTrades(TradingManager.ExitReason.SHORT_STOP_LOSS));
//        Vesta.info("  Ratio (P/M/N/R)         %.3f %.3f %.3f %.3f", stats.getRatioAvg(), stats.getRatioMax(), stats.getRatioMin(), stats.getRoiWins() / Math.abs(stats.getRoiLosses()));
//        Vesta.info("  ROI TP (Min)            %.2f%% L %.2f%% S", stats.getRoiTPMinLong(), stats.getRoiTPMinShort());
//        Vesta.info("  ROI STR                 %.2f%% (%.2f%%)", stats.getRoiStrategy(), stats.getRoiAvg(TradingManager.ExitReason.STRATEGY));
//        Vesta.info("  DireRate:               (%d %.2f%% L, %d %.2f%% S, %d N)", stats.getLongs(), stats.getRoiLong(), stats.getShorts(), stats.getRoiShort(), stats.getNothing());
//        Vesta.info("  Avg Hold Time P/W/L:    %.3fm %.3fm %.3fm", avgHoldMinutes, avgHoldMinutesWin, avgHoldMinutesLoss);
//        Vesta.info("  Roi P/W/L               %.3f%% %.3f%% %.3f%%", stats.getRoiAvg(), stats.getRoiWins(),  stats.getRoiLosses());
//        Vesta.info("  PNL Neto:               %s$%s%s", backtestResult.netPnL() >= 0 ? "\u001B[32m" : "\u001B[31m", decimalFormat.format(backtestResult.netPnL()), "\u001B[0m");
//        Vesta.info("  ROI Total:              %s%s%%%s", backtestResult.roiPercent() >= 0 ? "\u001B[32m" : "\u001B[31m", decimalFormat.format(backtestResult.roiPercent()), "\u001B[0m");
//        Vesta.info("  Rendimiento:            %s%.2f%%%s ", performer >= 0 ? "\u001B[32m" : "\u001B[31m", performer, "\u001B[0m");
//        Vesta.info("  Max Drawdown:           %.2f%%", backtestResult.maxDrawdown()*100);
//        Vesta.info("--------------------------------------------------");
//
//        System.gc();
//    }

    public static void showPredictionSnapshot(Market market, PredictionEngine engine) {
        showPredictionSnapshot(market, engine, 250, BuilderData.DEFAULT_FUTURE_WINDOW);
    }

    private static final String VALUE_SHOW = "close";


    public static void showPredictionSnapshot(Market market, PredictionEngine engine, int predictionIndex, int horizon) {
        if (market == null || engine == null) {
            Vesta.error("Market o PredictionEngine es null");
            return;
        }

        SequenceCandles candles = BuilderData.getProfierCandlesBuilder().build(market);
        List<Candle> candleBases = candles.toCandlesSimple();

        if (candles.isEmpty() || candleBases.isEmpty()) {
            Vesta.error("No hay velas para mostrar");
            return;
        }

        int lookBack = engine.getLookBack();
        int safeHorizon = Math.max(1, horizon);
        int maxIndex = candles.size() - safeHorizon - 1;
        if (maxIndex < lookBack) {
            Vesta.error("Historial insuficiente para prediccion: %d velas (se requieren %d)",
                    candles.size(), lookBack + safeHorizon + 1);
            return;
        }

        int idx = predictionIndex < 0 ? maxIndex : Math.min(predictionIndex, maxIndex);
        if (idx < lookBack) {
            Vesta.error("predictionIndex debe ser >= lookBack (%d)", lookBack);
            return;
        }

        int start = idx - lookBack + 1;
        List<Candle> lookbackCandles = candleBases.subList(start, idx + 1);

        SequenceCandles window = candles.subSequence(0, idx + 1);
        PredictionEngine.SequenceCandlesPrediction result = engine.predictNextPriceDetail(window, safeHorizon);
        if (result == null || result.isEmpty()) {
            Vesta.error("No se pudo generar predicción");
            return;
        }

        List<ChartUtils.ClosePredictionPoint> predicted = new ArrayList<>();
        double lastClose = candles.getCandle(idx).get(VALUE_SHOW);
        long baseTime = candles.get(idx).getOpenTime();
        for (int k = 0; k < result.size(); k++) {
            double diff = result.get(k).get(0);
            double predictedClose = lastClose * (1.0 + diff);
            lastClose = predictedClose;

            long time;
            int tIdx = idx + 1 + k;
            if (tIdx < candles.size()) {
                time = candles.get(tIdx).getOpenTime();
            } else {
                time = baseTime + (k + 1L) * market.getTimeFrameMarket().getMilliseconds();
            }
            predicted.add(new ChartUtils.ClosePredictionPoint(time, predictedClose));
        }

        List<ChartUtils.ClosePredictionPoint> actual = new ArrayList<>();
        for (int k = 0; k < safeHorizon; k++) {
            int tIdx = idx + 1 + k;
            if (tIdx >= candles.size()) {
                break;
            }
            CandleIndicators c = candles.getCandle(tIdx);
            actual.add(new ChartUtils.ClosePredictionPoint(c.getOpenTime(), c.get(VALUE_SHOW)));
        }

        ChartUtils.showCandlePredictionSnapshot(
                "Prediccion " + market.getSymbol(),
                lookbackCandles,
                predicted,
                actual,
                candles.get(idx).getOpenTime()
        );
    }
}
