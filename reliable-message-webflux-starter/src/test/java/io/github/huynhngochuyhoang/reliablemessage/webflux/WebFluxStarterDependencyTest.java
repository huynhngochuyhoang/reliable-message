package io.github.huynhngochuyhoang.reliablemessage.webflux;

import org.junit.jupiter.api.Test;
import org.springframework.util.ClassUtils;

import static org.junit.jupiter.api.Assertions.assertFalse;

class WebFluxStarterDependencyTest {

    @Test
    void starterDoesNotBringJdbcOntoClasspath() {
        assertFalse(ClassUtils.isPresent("org.springframework.jdbc.core.JdbcTemplate", getClass().getClassLoader()));
    }
}
