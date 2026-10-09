package com.example.ops.controller;

import com.example.ops.lab.CrudExperimentService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import static com.example.ops.ops.OpsContracts.*;

@RestController
@RequestMapping("/api/ops/experiments")
public class CrudExperimentController {
    private final CrudExperimentService experiments;

    public CrudExperimentController(CrudExperimentService experiments) {
        this.experiments = experiments;
    }

    @PostMapping("/{id}")
    public CrudExperimentResult run(@PathVariable String id, @RequestBody ExperimentRequest request,
                                    HttpServletRequest httpRequest) {
        if (!isLoopback(httpRequest.getRemoteAddr())) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Experiments are restricted to localhost");
        }
        if (request == null || !request.confirmed()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "confirmed=true is required");
        }
        try {
            return experiments.run(id);
        } catch (IllegalArgumentException error) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, error.getMessage());
        }
    }

    private static boolean isLoopback(String address) {
        return "127.0.0.1".equals(address) || "0:0:0:0:0:0:0:1".equals(address) || "::1".equals(address);
    }
}
