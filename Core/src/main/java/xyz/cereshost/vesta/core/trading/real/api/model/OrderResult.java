package xyz.cereshost.vesta.core.trading.real.api.model;

public record OrderResult (
        Long orderId,
        Double executedQty,
        Double cumulativeQuoteQty,
        Double receivedQty
) {

}
