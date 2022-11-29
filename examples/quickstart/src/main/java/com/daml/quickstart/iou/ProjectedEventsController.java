package com.daml.quickstart.iou;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("events")
public class ProjectedEventsController {

    private EventsRepository eventsRepository;

    @Autowired
    public ProjectedEventsController(EventsRepository eventsRepository) {
        this.eventsRepository = eventsRepository;
    }

    @GetMapping("")
    public @ResponseBody List<ProjectedEvent> getEvents() {
        return eventsRepository.all();
    }

    @GetMapping("/count")
    public @ResponseBody Integer countEvents() {
        return eventsRepository.count();
    }
}
