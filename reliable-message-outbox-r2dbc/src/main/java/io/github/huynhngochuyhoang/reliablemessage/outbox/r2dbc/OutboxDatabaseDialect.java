package io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc;

import io.r2dbc.spi.ConnectionFactory;

import java.util.Locale;

public enum OutboxDatabaseDialect {
    POSTGRESQL,
    MYSQL,
    ORACLE,
    SQL_SERVER,
    GENERIC;

    public static OutboxDatabaseDialect from(ConnectionFactory connectionFactory) {
        if (connectionFactory == null || connectionFactory.getMetadata() == null) {
            return GENERIC;
        }
        return fromName(connectionFactory.getMetadata().getName());
    }

    static OutboxDatabaseDialect fromName(String name) {
        if (name == null || name.isBlank()) {
            return GENERIC;
        }
        String normalized = name.toLowerCase(Locale.ROOT);
        if (normalized.contains("postgres")) {
            return POSTGRESQL;
        }
        if (normalized.contains("mysql") || normalized.contains("mariadb")) {
            return MYSQL;
        }
        if (normalized.contains("oracle")) {
            return ORACLE;
        }
        if (normalized.contains("sql server") || normalized.contains("mssql")) {
            return SQL_SERVER;
        }
        return GENERIC;
    }
}
