package com.example.ops;

import com.example.ops.lab.CrudExperimentService;
import com.example.ops.ops.WalletResetClient;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class CrudExperimentServiceTests {
    private HttpServer walletServer;
    private String walletUrl;
    private final List<String> httpCalls = new ArrayList<>();
    private final AtomicInteger transferCalls = new AtomicInteger();

    @BeforeEach
    void startWalletApi() throws Exception {
        walletServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        walletServer.createContext("/", this::handleWalletRequest);
        walletServer.start();
        walletUrl = "http://127.0.0.1:" + walletServer.getAddress().getPort();
    }

    @AfterEach
    void stopWalletApi() {
        walletServer.stop(0);
    }

    @Test
    void duplicateExperimentCallsRealWalletHttpEndpointsAndUsesDatabaseSnapshots() throws Exception {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        doAnswer(invocation -> {
            RowCallbackHandler rows = invocation.getArgument(1);
            long a = transferCalls.get() == 0 ? 100_000 : 80_000;
            long b = transferCalls.get() == 0 ? 50_000 : 70_000;
            emitAccount(rows, "user-A", a);
            emitAccount(rows, "user-B", b);
            emitAccount(rows, "user-C", 0);
            return null;
        }).when(jdbc).query(eq("SELECT owner_ref, balance FROM accounts ORDER BY owner_ref"),
                any(RowCallbackHandler.class));
        when(jdbc.queryForObject("SELECT COUNT(*) FROM transfers", Long.class))
                .thenAnswer(invocation -> (long) transferCalls.get());

        CrudExperimentService service = new CrudExperimentService(
                new WalletResetClient(walletUrl), jdbc, walletUrl);
        var result = service.run("V0-DUPLICATE");

        assertThat(httpCalls).containsExactly(
                "POST /api/dev/reset {\"confirmed\":true}",
                "POST /transfers",
                "POST /transfers"
        );
        assertThat(result.executionMode()).isEqualTo("LIVE_BACKEND");
        assertThat(result.actions()).extracting(action -> action.path())
                .containsExactly("/api/dev/reset", "/transfers", "/transfers");
        assertThat(result.httpStatuses()).containsExactly(201, 201);
        assertThat(result.before().transferCount()).isZero();
        assertThat(result.after().transferCount()).isEqualTo(2);
        assertThat(result.verdict()).isEqualTo("BUG_REPRODUCED");
    }

    private void handleWalletRequest(HttpExchange exchange) throws java.io.IOException {
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if ("/api/dev/reset".equals(exchange.getRequestURI().getPath())) {
            httpCalls.add(exchange.getRequestMethod() + " /api/dev/reset " + body);
            byte[] response = "{\"success\":true}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } else if ("/transfers".equals(exchange.getRequestURI().getPath())) {
            httpCalls.add(exchange.getRequestMethod() + " /transfers");
            transferCalls.incrementAndGet();
            exchange.sendResponseHeaders(201, -1);
        } else {
            exchange.sendResponseHeaders(404, -1);
        }
        exchange.close();
    }

    private static void emitAccount(RowCallbackHandler rows, String owner, long balance) throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        when(resultSet.getString("owner_ref")).thenReturn(owner);
        when(resultSet.getLong("balance")).thenReturn(balance);
        rows.processRow(resultSet);
    }
}
