package com.example.ops;

import com.example.ops.controller.OpsController;
import com.example.ops.controller.LocalResetController;
import com.example.ops.controller.CrudExperimentController;
import com.example.ops.lab.CrudExperimentService;
import com.example.ops.ops.OpsQueryService;
import com.example.ops.ops.WalletResetClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static com.example.ops.ops.OpsContracts.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest({OpsController.class, LocalResetController.class, CrudExperimentController.class})
class OpsControllerTests {
    @Autowired MockMvc mvc;
    @MockitoBean OpsQueryService queries;
    @MockitoBean WalletResetClient walletReset;
    @MockitoBean CrudExperimentService experiments;

    @Test
    void readEndpointsExposeImplementedBackend() throws Exception {
        when(queries.overview()).thenReturn(new Overview(List.of(), List.of(), List.of(),
                "N/A", "N/A", Instant.parse("2026-10-09T00:00:00Z")));
        when(queries.transfers(null, null, null, null, null, 0, 50))
                .thenReturn(new Page<>(List.of(), 0, 0, 50));
        when(queries.accounts(false, null, 0, 50))
                .thenReturn(new Page<>(List.of(), 0, 0, 50));
        when(queries.queues()).thenReturn(new Queues(List.of(), List.of(), List.of(), List.of(),
                "NOT_AVAILABLE_IN_WALLET_V0"));
        when(queries.reconciliation(null)).thenReturn(new Reconciliation(List.of(), List.of()));
        when(queries.runs(0, 50)).thenReturn(new Page<>(List.of(), 0, 0, 50));
        when(queries.databaseSchema()).thenReturn(new DatabaseSchema(
                "wallet", "public", "PostgreSQL test", Instant.parse("2026-10-09T00:00:00Z"),
                List.of(new DatabaseTable("accounts", "BASE TABLE", List.of(), List.of()))));

        mvc.perform(get("/api/ops/overview")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/transfers")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/accounts")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/queues")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/reconciliation")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/runs")).andExpect(status().isOk());
        mvc.perform(get("/api/ops/schema"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.databaseName").value("wallet"))
                .andExpect(jsonPath("$.tables[0].name").value("accounts"));
    }

    @Test
    void capabilitiesAndValidationAreExplicit() throws Exception {
        mvc.perform(get("/api/ops/capabilities"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.backendImplemented").value(true))
                .andExpect(jsonPath("$.labEnabled").value(false))
                .andExpect(jsonPath("$.resetEnabled").value(true))
                .andExpect(jsonPath("$.crudExperimentsEnabled").value(true))
                .andExpect(jsonPath("$.dataSource").value("WALLET_V0_READ_ONLY"));
        mvc.perform(get("/api/ops/accounts?page=-1")).andExpect(status().isBadRequest());
        mvc.perform(get("/api/ops/transfers?size=201")).andExpect(status().isBadRequest());
        mvc.perform(post("/api/lab/reset")).andExpect(status().isNotFound());
        mvc.perform(get("/accounts")).andExpect(status().isNotFound());
    }

    @Test
    void resetRequiresConfirmationAndLocalhostThenForwardsToWallet() throws Exception {
        when(walletReset.reset()).thenReturn(java.util.Map.of("success", true));

        mvc.perform(post("/api/ops/reset")
                        .contentType("application/json")
                        .content("{\"confirmed\":false}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/ops/reset")
                        .with(request -> { request.setRemoteAddr("192.0.2.10"); return request; })
                        .contentType("application/json")
                        .content("{\"confirmed\":true}"))
                .andExpect(status().isForbidden());
        mvc.perform(post("/api/ops/reset")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .contentType("application/json")
                        .content("{\"confirmed\":true}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true));
    }

    @Test
    void crudExperimentRequiresConfirmationAndLocalhost() throws Exception {
        when(experiments.run("V0-OVERDRAFT")).thenReturn(new CrudExperimentResult(
                "V0-OVERDRAFT", "Chuyển quá số dư", "BUG_REPRODUCED", "negative balance",
                List.of(201), new CrudSnapshot(java.util.Map.of("user-A", "100000"), 0),
                new CrudSnapshot(java.util.Map.of("user-A", "-20000"), 1)));

        mvc.perform(post("/api/ops/experiments/V0-OVERDRAFT")
                        .contentType("application/json").content("{\"confirmed\":false}"))
                .andExpect(status().isBadRequest());
        mvc.perform(post("/api/ops/experiments/V0-OVERDRAFT")
                        .with(request -> { request.setRemoteAddr("192.0.2.10"); return request; })
                        .contentType("application/json").content("{\"confirmed\":true}"))
                .andExpect(status().isForbidden());
        mvc.perform(post("/api/ops/experiments/V0-OVERDRAFT")
                        .with(request -> { request.setRemoteAddr("127.0.0.1"); return request; })
                        .contentType("application/json").content("{\"confirmed\":true}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.verdict").value("BUG_REPRODUCED"));
    }
}
