package xyz.cereshost.vesta.core.trading.real.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.experimental.UtilityClass;
import org.jetbrains.annotations.NotNull;
import xyz.cereshost.vesta.core.market.MarketStatus;
import xyz.cereshost.vesta.core.market.SymbolConfigurable;
import xyz.cereshost.vesta.core.trading.RateLimitType;
import xyz.cereshost.vesta.core.trading.real.api.model.BookTicker;
import xyz.cereshost.vesta.core.trading.real.api.model.ExchangeInfo;
import xyz.cereshost.vesta.core.trading.real.api.model.RateLimit;
import xyz.cereshost.vesta.core.trading.real.api.model.Ticker24H;

import java.util.*;
import java.util.concurrent.TimeUnit;

@UtilityClass
public class ParseJsonApi {

    public ObjectMapper mapper = new ObjectMapper();

    public @NotNull ExchangeInfo parseExchangeInfo(@NotNull JsonNode node, @NotNull Boolean isFuture) {
        HashMap<String, SymbolConfigurable> symbols = new HashMap<>();
        for (JsonNode info : node.get("symbols")) {
            String symbol = info.get("symbol").asText();
//            Iterator<String> iterator = info.fieldNames();
//            while (iterator.hasNext()) {
//                System.out.println(iterator.next());
//            }

            Double step = null;
            for (JsonNode filters : info.get("filters")) {
                if (filters.get("filterType").asText().equals("LOT_SIZE")) {
                    step = Double.parseDouble(filters.get("stepSize").asText());
                }
            }

            Set<String> permission = new HashSet<>();
            for (JsonNode permissions : info.get("permissionSets").iterator().next()) {
                permission.add(permissions.asText());
            }

            symbols.put(symbol,
                    isFuture ?
                            new SymbolConfigurable(
                                    symbol,
                                    info.asText().startsWith("TRADIFI_"),
                                    true,
                                    false,
                                    info.get("pricePrecision").asInt(),
                                    info.get("quotePrecision").asInt(),
                                    MarketStatus.valueOf(info.get("status").asText()),
                                    info.get("baseAsset").asText(),
                                    info.get("quoteAsset").asText(),
                                    Objects.equals(info.get("status").asText(), "TRADING"),
                                    step,
                                    Set.of()
                            ) :
                            new SymbolConfigurable(
                                    symbol,
                                    false,
                                    false,
                                    true,
                                    Objects.requireNonNullElse(info.get("quoteAssetPrecision"), info.get("pricePrecision")).asInt(),
                                    info.get("quotePrecision").asInt(),
                                    MarketStatus.valueOf(info.get("status").asText()),
                                    info.get("baseAsset").asText(),
                                    info.get("quoteAsset").asText(),
                                    info.get("isSpotTradingAllowed").booleanValue(),
                                    step,
                                    permission
                            )
            );
        }

        List<RateLimit> limits = new ArrayList<>();
        for (JsonNode info : node.get("rateLimits")) {
            limits.add(
                    new RateLimit(
                            RateLimitType.valueOf(info.get("rateLimitType").asText()),
                            // Se agrega una "S" por que la unidad que entrega binance es en sigular, pero el Emun trabaja en prural
                            TimeUnit.valueOf(info.get("interval").asText() + "S"),
                            info.get("intervalNum").asInt(),
                            info.get("limit").asInt()
                    )
            );
        }
        return new ExchangeInfo(limits, symbols);
    }

    public Map<String, BookTicker> parseBookTickers(JsonNode node) {
        Map<String, BookTicker> bookTickers = new HashMap<>();
        if (node.isArray()) {
            for (JsonNode ticker : node) {
                String s = ticker.get("symbol").asText();
                bookTickers.put(s, new BookTicker(s,
                        ticker.get("bidPrice").asDouble(),
                        ticker.get("bidQty").asDouble(),
                        ticker.get("askPrice").asDouble(),
                        ticker.get("askQty").asDouble())
                );
            }
        }else {
            String s = node.get("symbol").asText();
            bookTickers.put(s, new BookTicker(s,
                    node.get("bidPrice").asDouble(),
                    node.get("bidQty").asDouble(),
                    node.get("askPrice").asDouble(),
                    node.get("askQty").asDouble())
            );
        }
        return bookTickers;
    }

    public Set<Ticker24H> parseTicker24H(JsonNode node) {
        HashSet<Ticker24H> ticker24Hs = new HashSet<>();
        for (JsonNode ticker : node) {
            Iterator<String> iterator = ticker.fieldNames();
            ticker24Hs.add(new Ticker24H(
                    ticker.get("symbol").asText(),
                    ticker.get("priceChange").doubleValue(),
                    ticker.get("priceChangePercent").doubleValue(),
                    ticker.has("quoteVolume") ?
                            ticker.get("quoteVolume").asDouble() : ticker.get("volume").asDouble(),
                    ticker.has("baseVolume") ?
                            ticker.get("baseVolume").asDouble() : ticker.get("volume").asDouble()
            ));
        }
        return ticker24Hs;
    }

    public HashMap<String, Double> parseBalance(JsonNode node, Boolean isFuture) {
        HashMap<String, Double> balances = new HashMap<>();
        if  (isFuture) {
            for (JsonNode root : node){
                balances.put(root.get("asset").asText(), root.get("balance").asDouble());
            }
        }else {
            for (JsonNode root : node.get("balances")){
                balances.put(root.get("asset").asText(), root.get("free").asDouble());
            }
        }
        return balances;
    }


}
