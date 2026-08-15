package com.rideshare.rideservice.service;

import com.rideshare.rideservice.event.RideMatchedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class RidesEventConsumer {
    private final RideService rideService;

    // listens to ride.requested Kafka topic
    // Triggered everytime the RideService published a new ride
    @KafkaListener(
            topics = "ride.matched",
            groupId = "ride-service-group"
    )
    public void consumeRideMatchedEvent(RideMatchedEvent rideMatchedEvent) {
        try {
            rideService.updateRideWithDriver(
                    rideMatchedEvent.getRideId(),
                    rideMatchedEvent.getDriverId()
            );
        } catch (Exception e) {
            log.error("Error occurred while updating ride with driver", e);
            log.error("Exception details: ", e);
            // in production: send to dead letter queue for retry
        }
    }
}
