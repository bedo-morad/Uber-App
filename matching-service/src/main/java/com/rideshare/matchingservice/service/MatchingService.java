package com.rideshare.matchingservice.service;

import com.rideshare.matchingservice.client.LocationServiceClient;
import com.rideshare.matchingservice.dto.NearByDriverResponse;
import com.rideshare.matchingservice.event.RideMatchedEvent;
import com.rideshare.matchingservice.event.RideRequestedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@Slf4j
@RequiredArgsConstructor
public class MatchingService {

    private final LocationServiceClient locationServiceClient;
    private final KafkaTemplate<String, RideMatchedEvent> kafkaTemplate;

    private static final String RIDE_MATCHED_TOPIC = "ride.matched";
    private static final double DEFAULT_SEARCH_RADIUS_KM = 5.0;

    private static final double DISTANCE_WEIGHT = 0.7;
    private static final double RATING_WEIGHT = 0.3;

    // Main matching algorithm
    // Called when RideRequestEvent is consumed from Kafka
    //
    // Steps:
    // 1- Ask Location Service for nearby drivers
    // 2- score each driver and pick best one
    // 3- publish RideMatchedEvent to Kafka
    public void matchDriverForRide(RideRequestedEvent rideRequestedEvent) {
        // Step 1
        List<NearByDriverResponse> nearByDrivers = locationServiceClient.getNearByDrivers(
                rideRequestedEvent.getPickupLatitude(),
                rideRequestedEvent.getPickupLongitude(),
                DEFAULT_SEARCH_RADIUS_KM
        );
        if (nearByDrivers.isEmpty()) {
            log.warn("No nearby drivers found for ride: {}", rideRequestedEvent);
            return;
        }
        // Step 2
        Optional<NearByDriverResponse> bestDriver = findBestDriver(nearByDrivers);
        if (bestDriver.isEmpty()) {
            log.warn("No suitable driver found for ride: {}", rideRequestedEvent);
            return;
        }
        NearByDriverResponse assignedDriver = bestDriver.get();
        // Step 3
        RideMatchedEvent rideMatchedEvent = new RideMatchedEvent(
                rideRequestedEvent.getRideId(),
                rideRequestedEvent.getRiderId(),
                assignedDriver.getDriverId(),
                assignedDriver.getLatitude(),
                assignedDriver.getLongitude(),
                assignedDriver.getDistanceInKm()
        );
        kafkaTemplate.send(RIDE_MATCHED_TOPIC, rideRequestedEvent.getRideId(), rideMatchedEvent);
        log.info("Ride matched event sent to topic: {}", rideMatchedEvent);
    }

    // driver scoring algorithm
    // Distance 70%
    // Rating 30%
    // score = ( 1 / distance ) * DISTANCE_WEIGHT + rating * RATING_WEIGHT
    private Optional<NearByDriverResponse> findBestDriver(
            List<NearByDriverResponse> drivers
    ) {
        return drivers.stream().
                max(Comparator.comparingDouble(driver -> {
                    // add 0.1 to avoid division by zero
                    double distanceScore = 1.0 / Math.max(driver.getDistanceInKm(), 0.1);
                    // simulated rating between 4.0 and 5.0
                    // in production fetch from DriverService
                    double simulatedRating = 4.0 + Math.random();

                    return distanceScore * DISTANCE_WEIGHT + simulatedRating * RATING_WEIGHT;
                }));
    }
}
