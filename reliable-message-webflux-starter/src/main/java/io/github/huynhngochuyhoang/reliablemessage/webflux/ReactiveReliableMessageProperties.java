package io.github.huynhngochuyhoang.reliablemessage.webflux;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "message.reliability")
public class ReactiveReliableMessageProperties {

    private String runtime = "webflux";
    private Reactive reactive = new Reactive();

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public Reactive getReactive() {
        return reactive;
    }

    public void setReactive(Reactive reactive) {
        this.reactive = reactive;
    }

    public static class Reactive {
        private int maxConcurrency = 64;
        private int prefetch = 256;

        public int getMaxConcurrency() {
            return maxConcurrency;
        }

        public void setMaxConcurrency(int maxConcurrency) {
            this.maxConcurrency = maxConcurrency;
        }

        public int getPrefetch() {
            return prefetch;
        }

        public void setPrefetch(int prefetch) {
            this.prefetch = prefetch;
        }
    }
}
