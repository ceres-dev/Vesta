package xyz.cereshost.vesta.core.trading.abitrage.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class NameAsset {
    public final String asset;
    public final Integer hash;
    public final Integer index;

    private static final ConcurrentMap<String, Byte[]> cacheBytes = new ConcurrentHashMap<>();

    public NameAsset(String asset, Integer index) {
        this.asset = asset;
        this.hash = Objects.hash(asset);
        this.index = index;
    }

    public NameAsset(String asset) {
        this(asset, -1);
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof NameAsset nameAsset) {
            return Objects.equals(nameAsset.hash, this.hash);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hash.intValue();
    }
}