package xyz.cereshost.vesta.core.trading.abitrage.model;

import lombok.Data;
import org.jetbrains.annotations.Contract;
import xyz.cereshost.vesta.core.trading.abitrage.TriangularEngine;

import java.util.List;

@Data
public final class TriangularArbitrageOpportunity {
    private List<String> assetsCycle;
    private List<TriangularEngine.ArbitrageEdge> edges;
    private final TriangularEngine.LifeTime lifeTime;
    private double rateProduct;
    private double profitPercent;
    private double totalWeight;

    public TriangularArbitrageOpportunity(
            List<String> assetsCycle,
            List<TriangularEngine.ArbitrageEdge> edges,
            TriangularEngine.LifeTime lifeTime,
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
        this.lifeTime = new TriangularEngine.LifeTime();
        this.rateProduct = -1;
        this.profitPercent = -1;
        this.totalWeight = -1;
    }

    @Contract("_, _, _, _, _ -> this")
    public TriangularArbitrageOpportunity updateDataAndNextTick(
            List<String> assetsCycle,
            List<TriangularEngine.ArbitrageEdge> edges,
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
