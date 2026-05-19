package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeoutException;

public interface RpcExceptionClassifier {

    boolean retryable(Throwable error);

    boolean timeout(Throwable error);

    static RpcExceptionClassifier defaults() {
        return new DefaultRpcExceptionClassifier();
    }

    final class DefaultRpcExceptionClassifier implements RpcExceptionClassifier {
        @Override
        public boolean retryable(Throwable error) {
            return timeout(error) || root(error) instanceof IOException;
        }

        @Override
        public boolean timeout(Throwable error) {
            Throwable root = root(error);
            return root instanceof TimeoutException || root instanceof SocketTimeoutException;
        }

        private static Throwable root(Throwable error) {
            Throwable current = error;
            while (current != null && current.getCause() != null && current.getCause() != current) {
                current = current.getCause();
            }
            return current == null ? error : current;
        }
    }
}
