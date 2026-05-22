package xyz.cereshost.vesta.core.market;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.trading.real.api.BinanceApi;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class SymbolConfigurable implements Symbol{

    @NotNull private final String symbol;
    @NotNull private final Boolean shouldFuture;

    @NotNull @Getter private Boolean isTradFi = false;
    @NotNull @Getter private Boolean isFuture = true;
    @NotNull @Getter private Boolean isSpot = true;
    @NotNull @Getter private Boolean isAllowTrading = true;
    @NotNull @Getter private Integer pricePrecision = 2;
    @NotNull @Getter private Integer quantityPrecision = 2;
    @NotNull @Getter private MarketStatus marketStatus;
    @NotNull @Getter private String baseAsset = "?";
    @NotNull @Getter private String quoteAsset = "?";
    @NotNull @Getter private Set<String> permissions = new HashSet<>();

    @Nullable private Double stepSize;

    public SymbolConfigurable(@NotNull String symbol) {
        this.symbol = symbol;
        this.shouldFuture = symbol.endsWith("USDT") || symbol.endsWith("USDC");
    }

    public SymbolConfigurable(@NotNull String symbol,
                              @NotNull Boolean shouldFuture
    ) {
        this.symbol = symbol;
        this.shouldFuture = shouldFuture;
    }

    public SymbolConfigurable(@NotNull String symbol,
                              @NotNull Boolean isTradFi,
                              @NotNull Boolean isFuture,
                              @NotNull Boolean isSpot,
                              @NotNull Integer pricePrecision,
                              @NotNull Integer quantityPrecision,
                              @NotNull MarketStatus marketStatus,
                              @NotNull String baseAsset,
                              @NotNull String quoteAsset,
                              @NotNull Boolean spotTradingAllowed,
                              @Nullable Double stepSize,
                              @NotNull Set<String> permissions
    ) {
        this.symbol = symbol;
        this.shouldFuture = isFuture;
        this.isTradFi = isTradFi;
        this.isFuture = isFuture;
        this.isSpot = isSpot;
        this.isAllowTrading = spotTradingAllowed;
        this.pricePrecision = pricePrecision;
        this.quantityPrecision = quantityPrecision;
        this.marketStatus = marketStatus;
        this.baseAsset = baseAsset;
        this.quoteAsset = quoteAsset;
        this.stepSize = stepSize;
        this.permissions = permissions;
    }

    @Override
    public @NotNull String name() {
        return symbol;
    }

    @Override
    public @NotNull Boolean isQuoteUSDT() {
        return quoteAsset.equals("USDT");
    }

    @Override
    public @NotNull Boolean isQuoteUSDC() {
        return quoteAsset.equals("USDC");
    }

    @Override
    public @NotNull Optional<Double> getStepSize() {
        return Optional.ofNullable(stepSize);
    }

    private boolean isConfigured = false;

    @Override
    public void configure(BinanceApi binanceApi) {
        if (isConfigured) return;
        isConfigured = true;
        SymbolConfigurable symbolConfigurable = binanceApi.getSymbolConfigured(symbol, shouldFuture);
        isTradFi = symbolConfigurable.getIsTradFi();
        isFuture = symbolConfigurable.getIsFuture();
        isSpot = symbolConfigurable.getIsSpot();
        pricePrecision = symbolConfigurable.getPricePrecision();
        quantityPrecision = symbolConfigurable.getQuantityPrecision();
        marketStatus = symbolConfigurable.getMarketStatus();
        baseAsset = symbolConfigurable.getBaseAsset();
        quoteAsset = symbolConfigurable.getQuoteAsset();
        stepSize = symbolConfigurable.getStepSize().orElse(null);
        permissions = symbolConfigurable.getPermissions();
    }


}
