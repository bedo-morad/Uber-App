package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RidesEventConsumer {
    private final MatchingService matchingService;

    // listens to ride.requested Kafka topic
    // Triggered everytime the RideService published a new ride
    @KafkaListener(
            topics = "ride.requested",
            groupId = "matching-service-group"
    )
    public void consumeRideRequestedEvent(RideRequestedEvent rideRequestedEvent) {
        try {
            matchingService.matchDriverForRide(rideRequestedEvent);
        } catch (Exception e) {
            log.error("Error occurred while matching driver for ride {} ", rideRequestedEvent.getRideId());
            log.error("Exception details: ", e);
            // in production: send to dead letter queue for retry
        }
    }
}
