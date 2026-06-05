package xyz.cereshost.vesta.core.trading.abitrage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import xyz.cereshost.vesta.core.trading.abitrage.model.NameAsset;
import xyz.cereshost.vesta.core.trading.abitrage.model.TriangularArbitrageOpportunity;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TriangularEngineCPP extends TriangularEngine {

    static {
        System.loadLibrary("libVesta");
    }


    @Override
    public List<TriangularArbitrageOpportunity> findTriangularArbitrageOpportunities(@Nullable BookTicker updatedTicker) {
        throw new UnsupportedOperationException("TriangularEngineCPP is not implemented yet.");
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("TriangularEngineCPP is not implemented yet.");
    }

    @Override
    public @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> buildGraf(@NotNull ExchangeInfo exchangeInfoSpot) {
        throw new UnsupportedOperationException("TriangularEngineCPP is not implemented yet.");
    }

    @Override
    public @NotNull Map<NameAsset, ArrayList<ArbitrageEdge>> updateGraf(@NotNull ExchangeInfo exchangeInfoSpot, @Nullable BookTicker updatedTicker) {
        throw new UnsupportedOperationException("TriangularEngineCPP is not implemented yet.");
    }

    @Override
    public void setExchangeInfo(@NotNull ExchangeInfo exchangeInfoSpot) {
        super.setExchangeInfo(exchangeInfoSpot);
    }
}
