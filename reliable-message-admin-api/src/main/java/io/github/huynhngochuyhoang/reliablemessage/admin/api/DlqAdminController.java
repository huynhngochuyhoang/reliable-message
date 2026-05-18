package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/internal/messages/dlq")
public class DlqAdminController {

    private final DlqAdminOperations operations;
    private final ReliableMessageAdminProperties properties;

    public DlqAdminController(DlqAdminOperations operations, ReliableMessageAdminProperties properties) {
        this.operations = operations;
        this.properties = properties;
    }

    @GetMapping
    public List<DeadLetterRecord> find(@RequestParam(required = false) Integer limit) {
        return operations.find(properties.clampLimit(limit));
    }

    @PostMapping("/{id}/retry")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void retry(@PathVariable String id) {
        operations.retry(id);
    }

    @PostMapping("/{id}/discard")
    public DeadLetterRecord discard(@PathVariable String id, @RequestBody(required = false) DiscardRequest request) {
        String reason = request == null ? null : request.reason();
        return operations.discard(id, reason);
    }

    public record DiscardRequest(String reason) {
    }
}
