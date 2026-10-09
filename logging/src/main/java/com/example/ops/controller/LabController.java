package com.example.ops.controller;

import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import static com.example.ops.ops.OpsContracts.*;

/** TODO: add loopback request checks, CSRF validation and a global run lock before implementing writes. */
@Profile("lab")
@RestController
@RequestMapping("/api/lab")
public class LabController {
    @PostMapping("/scenarios/{id}/runs") public UUID start(@PathVariable String id, @RequestBody RunRequest request) { throw new NotImplemented("ScenarioRunner.start"); }
    @PostMapping("/reset") public void reset(@RequestBody ResetRequest request) { throw new NotImplemented("ScenarioRunner.resetFixtures"); }
    @PutMapping("/chaos/{name}") public void toggle(@PathVariable String name, @RequestBody ToggleRequest request) { throw new NotImplemented("ScenarioRunner.toggleChaos"); }
    @PostMapping("/runs/{id}/cancel") public void cancel(@PathVariable UUID id) { throw new NotImplemented("ScenarioRunner.cancel"); }
}

