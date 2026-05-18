package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/messages/idempotency")
public class IdempotencyAdminController {

    private final IdempotencyAdminOperations operations;

    public IdempotencyAdminController(IdempotencyAdminOperations operations) {
        this.operations = operations;
    }

    @GetMapping("/{key}")
    public IdempotencyRecord find(@PathVariable String key) {
        return operations.find(key);
    }

    @DeleteMapping("/{key}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void clear(@PathVariable String key) {
        operations.clear(key);
    }
}
