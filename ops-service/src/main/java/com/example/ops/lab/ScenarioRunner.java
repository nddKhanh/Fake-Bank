package com.example.ops.lab;

import java.util.UUID;
import static com.example.ops.ops.OpsContracts.*;

/**
 * TODO: implement only under profile lab and on loopback.
 * One run at a time; isolated fixtures; persist history outside reset scope.
 * PASS iff convergence, all eight invariants and every scenario assertion pass.
 * Always remove injected faults in finally; do not report skipped checks as PASS.
 */
public interface ScenarioRunner {
    UUID start(String scenarioId, long seed);
    void resetFixtures(boolean explicitlyConfirmed);
    void toggleChaos(String name, boolean enabled);
    void cancel(UUID runId);
    Snapshot snapshot();
    void injectFault(String scenarioId);
    void executeActions(String scenarioId, long seed);
    void removeFaults();
    /** Three consecutive zero-backlog polls, one second apart; timeout fails the run. */
    boolean awaitConvergence(int timeoutSeconds);
}

