package com.example.outbox.controller;

import com.example.outbox.repository.OutboxStore;
import com.example.outbox.service.EventRetryService;
import com.example.outbox.worker.OutboxWorker;
import java.util.UUID;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

@Controller
public class DashboardController {
    private static final int PAGE_SIZE = 25;
    private final OutboxStore store;
    private final OutboxWorker worker;
    private final EventRetryService retryService;

    public DashboardController(OutboxStore store, OutboxWorker worker, EventRetryService retryService) {
        this.store = store;
        this.worker = worker;
        this.retryService = retryService;
    }

    @GetMapping("/")
    public String dashboard(@RequestParam(defaultValue = "1") long failedPage,
                            @RequestParam(defaultValue = "1") long processedPage, Model model) {
        populate(model, failedPage, processedPage);
        return "dashboard";
    }

    @GetMapping("/dashboard/data")
    public String data(@RequestParam(defaultValue = "1") long failedPage,
                       @RequestParam(defaultValue = "1") long processedPage, Model model) {
        populate(model, failedPage, processedPage);
        return "dashboard :: data";
    }

    @PostMapping("/dashboard/events/{id}/retry")
    public String retryEvent(@PathVariable UUID id,
                             @RequestParam(defaultValue = "1") long failedPage,
                             @RequestParam(defaultValue = "1") long processedPage, RedirectAttributes redirect) {
        boolean queued = retryService.retryFailedEvent(id);
        redirect.addFlashAttribute("message", queued
                ? "Manual retry queued for event " + id.toString().substring(0, 8)
                    + ". The worker will perform one additional attempt."
                : "This event is no longer permanently failed or does not exist. No retry was queued.");
        redirect.addAttribute("failedPage", Math.max(1, failedPage));
        redirect.addAttribute("processedPage", Math.max(1, processedPage));
        return "redirect:/";
    }

    private void populate(Model model, long requestedFailedPage, long requestedProcessedPage) {
        long failureCount = store.countFailures();
        long processedCount = store.countProcessedEvents();
        long failedPageCount = pageCount(failureCount);
        long processedPageCount = pageCount(processedCount);
        long failedPage = Math.max(1, Math.min(requestedFailedPage, failedPageCount));
        long processedPage = Math.max(1, Math.min(requestedProcessedPage, processedPageCount));
        model.addAttribute("failedEvents", store.failedEvents(PAGE_SIZE, (failedPage - 1) * PAGE_SIZE));
        model.addAttribute("processedEvents", store.processedEvents(PAGE_SIZE, (processedPage - 1) * PAGE_SIZE));
        model.addAttribute("failureCount", failureCount);
        model.addAttribute("processedCount", processedCount);
        model.addAttribute("retryCount", store.countRetries());
        model.addAttribute("failedPage", failedPage);
        model.addAttribute("processedPage", processedPage);
        model.addAttribute("failedPageCount", failedPageCount);
        model.addAttribute("processedPageCount", processedPageCount);
        model.addAttribute("workerEnabled", worker.isEnabled());
        model.addAttribute("workerType", worker.getType().getDisplayName());
        model.addAttribute("workerDescription", worker.description());
    }

    private long pageCount(long total) { return Math.max(1, (total + PAGE_SIZE - 1) / PAGE_SIZE); }
}
