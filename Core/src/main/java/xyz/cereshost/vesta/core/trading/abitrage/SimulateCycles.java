package xyz.cereshost.vesta.core.trading.abitrage;

import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

@UtilityClass
public class SimulateCycles {

    public @Nullable SimulationResult simulateCycleWithStepSize(@NotNull List<TriangularEngine.ArbitrageEdge> cycleEdges) {
        double amount = TriangularEngine.DEFAULT_START_AMOUNT;
        for (TriangularEngine.ArbitrageEdge edge : cycleEdges) {
            if (!Double.isFinite(amount) || amount <= 0.0) {
                return null;
            }

            String action;
            double referencePrice;
            double referenceLiquidity;
            double stepSize;
            synchronized (edge) {
                action = edge.getAction();
                referencePrice = edge.getReferencePrice();
                referenceLiquidity = edge.getReferenceLiquidity();
                stepSize = edge.getStepSize();
            }

            if ("SELL".equals(action)) {
                double quantity = roundDownToStepSize(amount, stepSize);

                if (quantity <= 0.0) {
                    return null;
                }

                if (quantity > referenceLiquidity) {
                    return null;
                }

                amount = quantity * referencePrice * (1.0 - TriangularEngine.DEFAULT_FEE_RATE);
                continue;
            }

            if ("BUY".equals(action)) {
                double quantity = roundDownToStepSize(amount / referencePrice, stepSize);

                if (quantity <= 0.0) {
                    return null;
                }

                if (quantity > referenceLiquidity) {
                    return null;
                }

                amount = quantity * (1.0 - TriangularEngine.DEFAULT_FEE_RATE);
                continue;
            }

            return null;
        }

        if (!Double.isFinite(amount) || amount <= 0.0) {
            return null;
        }
        return new SimulationResult(amount / TriangularEngine.DEFAULT_START_AMOUNT, amount);
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

    public record SimulationResult(
            double rateProduct,
            double endAmount
    ) {}
}
