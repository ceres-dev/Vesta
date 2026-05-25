package xyz.cereshost.vesta.core.utils;

public class ExpiredReference<T> {

    private final T reference;
    private final long nanoTime = System.nanoTime();
    private final long maxNanoTime;

    public ExpiredReference(T ref, long maxNanoTime) {
        this.reference = ref;
        this.maxNanoTime = maxNanoTime;
    }

    public boolean isExpired() {
        return System.nanoTime() > (nanoTime + maxNanoTime);
    }

    public boolean isExpired(long currentNanoTime) {
        return currentNanoTime > (nanoTime + maxNanoTime);
    }

    public boolean isValid() {
        return !isExpired();
    }

    public boolean isValid(long currentNanoTime) {
        return !isExpired(currentNanoTime);
    }

    public T getIsValid() {
        if (isExpired()) {
            return null;
        }else {
            return reference;
        }
    }

    public T get() {
        return reference;
    }
}
