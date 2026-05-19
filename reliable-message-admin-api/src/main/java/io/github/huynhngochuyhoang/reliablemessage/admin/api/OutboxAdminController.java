package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.OutboxStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping("/internal/messages/outbox")
public class OutboxAdminController {

    private final OutboxStore outboxStore;
    private final ReliableMessageAdminProperties properties;
    private final ObjectProvider<OutboxAdminOperations> operations;

    public OutboxAdminController(
            OutboxStore outboxStore,
            ReliableMessageAdminProperties properties,
            ObjectProvider<OutboxAdminOperations> operations
    ) {
        this.outboxStore = outboxStore;
        this.properties = properties;
        this.operations = operations;
    }

    @GetMapping
    public List<OutboxMessage> find(@RequestParam(required = false) Integer limit) {
        return outboxStore.findForAdmin(properties.clampLimit(limit));
    }

    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void retry(@PathVariable String id) {
        OutboxAdminOperations adminOperations = operations.getIfAvailable();
        if (adminOperations == null) {
            throw new ResponseStatusException(HttpStatus.NOT_IMPLEMENTED, "Outbox retry operation is not configured");
        }
        adminOperations.retry(id);
    }
}
