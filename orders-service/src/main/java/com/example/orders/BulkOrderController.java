package com.example.orders;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.example.orders.BulkOrderService.BulkStatus;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

@RestController
public class BulkOrderController {

    private final BulkOrderService bulk;

    public BulkOrderController(BulkOrderService bulk) {
        this.bulk = bulk;
    }

    @PostMapping("/api/orders/bulk")
    public ResponseEntity<BulkStatus> start(@Valid @RequestBody BulkRequest request) {
        return ResponseEntity.accepted().body(bulk.start(request.count()));
    }

    @GetMapping("/api/orders/bulk")
    public BulkStatus current() {
        return bulk.current();
    }

    @PostMapping("/api/orders/bulk/cancel")
    public BulkStatus cancel() {
        BulkStatus status = bulk.cancel();
        if (status == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No volume run is active");
        }
        return status;
    }

    public record BulkRequest(@Min(1) @Max(BulkOrderService.MAX_COUNT) long count) {
    }
}
