package xyz.cereshost.vesta.core.market;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import xyz.cereshost.vesta.core.trading.real.api.BinanceApi;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Locale;
import java.util.Optional;

public interface Symbol {

    @Contract(pure = true)
    @NotNull String name();

    @Contract(pure = true)
    @NotNull Boolean isQuoteUSDT();

    @Contract(pure = true)
    @NotNull Boolean isQuoteUSDC();

    @Contract(pure = true)
    @NotNull Boolean getIsTradFi();

    @Contract(pure = true)
    @NotNull Boolean getIsAllowTrading();

    @Contract(pure = true)
    default @NotNull Boolean getIsFuture() {
        return isQuoteUSDC() || isQuoteUSDT();
    }

    @Contract(pure = true)
    default @NotNull Boolean getIsSpot(){
        return true;
    }

    @NotNull Integer getPricePrecision();

    @NotNull Integer getQuantityPrecision();

    @NotNull Optional<Double> getStepSize();

    @Contract(pure = true)
    @NotNull String getQuoteAsset();

    @Contract(pure = true)
    @NotNull String getBaseAsset();

    @Contract(pure = true)
    @NotNull MarketStatus getMarketStatus();

    void configure(BinanceApi binanceApi);

    default String formatPrice(@NotNull Double price) {
        String s = "%." + getPricePrecision() + "f";
        return String.format(Locale.US, s, price);
    }

    default String formatQuantity(@NotNull Double quantity) {
        Optional<Double> optionalStepSize = getStepSize();
        if (optionalStepSize.isPresent()) {
            BigDecimal stepSize = BigDecimal.valueOf(optionalStepSize.get());
            if (stepSize.signum() > 0) {
                BigDecimal quantityDecimal = BigDecimal.valueOf(quantity);
                BigDecimal steppedQuantity = quantityDecimal
                        .divide(stepSize, 0, RoundingMode.DOWN)
                        .multiply(stepSize)
                        .stripTrailingZeros();
                return steppedQuantity.toPlainString();
            }
        }
        return formatQuantitySimple(quantity);
    }

    default String formatQuantitySimple(@NotNull Double quantity) {
        String s = "%." + getQuantityPrecision() + "f";
        return String.format(Locale.US, s, quantity);
    }

    default String formatQuoteOrderQty(@NotNull Double amount) {
        return BigDecimal.valueOf(amount)
                .setScale(getPricePrecision(), RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();
    }

    static Symbol valueOf(@NotNull String symbol) {
        return new SymbolConfigurable(symbol);
    }

}
