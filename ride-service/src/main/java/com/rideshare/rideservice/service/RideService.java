package com.rideshare.rideservice.service;

import com.rideshare.rideservice.dto.RideRequest;
import com.rideshare.rideservice.dto.RideResponse;
import com.rideshare.rideservice.event.RideRequestedEvent;
import com.rideshare.rideservice.model.Ride;
import com.rideshare.rideservice.model.RideStatus;
import com.rideshare.rideservice.repository.RideRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class RideService {

    private final RideRepository rideRepository;
    private final KafkaTemplate<String, RideRequestedEvent> kafkaTemplate;
    private final static String RIDE_REQUESTED_TOPIC = "ride.requested";
    // Ride fare in X currency
    private final static double BASE_RIDE_FARE = 10.0;
    private final static double RIDE_FARE_PER_KM = 2.0;

    public RideResponse requestRide(RideRequest rideRequest) {
        log.info("new ride request from rider: {}", rideRequest.getRiderId());

        // 1- Save Ride to database
        Ride ride = new Ride();
        ride.setRiderId(rideRequest.getRiderId());
        ride.setPickupLatitude(rideRequest.getPickupLatitude());
        ride.setPickupLongitude(rideRequest.getPickupLongitude());
        ride.setPickupAddress(rideRequest.getPickupAddress());
        ride.setDropLatitude(rideRequest.getDropLatitude());
        ride.setDropLongitude(rideRequest.getDropLongitude());
        ride.setDropAddress(rideRequest.getDropAddress());
        ride.setStatus(RideStatus.REQUESTED);
        ride.setEstimatedFare(calculateEstimateFare(rideRequest));
        Ride savedRide = rideRepository.save(ride);

        // 2- Publish event to Kafka
        // then the matching service will consume the event and find the nearest driver
        RideRequestedEvent rideRequestedEvent = new RideRequestedEvent(
                savedRide.getId(),
                savedRide.getRiderId(),
                savedRide.getPickupLatitude(),
                savedRide.getPickupLongitude(),
                savedRide.getPickupAddress(),
                savedRide.getDropLatitude(),
                savedRide.getDropLongitude(),
                savedRide.getDropAddress()
        );
        kafkaTemplate.send(RIDE_REQUESTED_TOPIC, savedRide.getId(), rideRequestedEvent);
        log.info("ride requested event published to Kafka for ride: {}", savedRide.getRiderId());
        // 3- update status to matching
        savedRide.setStatus(RideStatus.MATCHING);
        savedRide = rideRepository.save(savedRide);
        return mapToResponse(savedRide);
    }

    public void updateRideWithDriver(String rideId, String driverId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new EntityNotFoundException("Ride with id: " + rideId + " not found"));
        ride.setDriverId(driverId);
        ride.setStatus(RideStatus.ACCEPTED);
        rideRepository.save(ride);
    }

    public RideResponse startRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new EntityNotFoundException("Ride with id: " + rideId + " not found"));
        if (ride.getStatus() != RideStatus.ACCEPTED) {
            throw new IllegalArgumentException("Ride with id: " + rideId + " cannot be started. Current status: " + ride.getStatus());
        }
        ride.setStatus(RideStatus.RIDE_STARTED);
        ride.setStartedAt(LocalDateTime.now());
        return mapToResponse(rideRepository.save(ride));
    }

    public RideResponse completeRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new EntityNotFoundException("Ride with id: " + rideId + " not found"));
        if (ride.getStatus() != RideStatus.RIDE_STARTED) {
            throw new IllegalArgumentException("Ride with id: " + rideId + " cannot be completed. Current status: " + ride.getStatus());
        }
        ride.setStatus(RideStatus.COMPLETED);
        ride.setCompletedAt(LocalDateTime.now());
        ride.setActualFare(ride.getEstimatedFare());
        return mapToResponse(rideRepository.save(ride));
    }

    public RideResponse cancelRide(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new EntityNotFoundException("Ride with id: " + rideId + " not found"));
        ride.setStatus(RideStatus.CANCELED);
        return mapToResponse(rideRepository.save(ride));
    }

    public RideResponse getRideById(String rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new EntityNotFoundException("Ride with id: " + rideId + " not found"));
        return mapToResponse(ride);
    }

    public List<RideResponse> getAllRidesByRiderId(String riderId) {
        return rideRepository.findByRiderIdOrderByCreatedAtDesc(riderId)
                .stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
    }

    private RideResponse mapToResponse(Ride ride) {
        RideResponse rideResponse = new RideResponse();

        rideResponse.setId(ride.getId());
        rideResponse.setRiderId(ride.getRiderId());
        rideResponse.setDriverId(ride.getDriverId());

        rideResponse.setPickupLatitude(ride.getPickupLatitude());
        rideResponse.setPickupLongitude(ride.getPickupLongitude());
        rideResponse.setPickupAddress(ride.getPickupAddress());

        rideResponse.setDropLatitude(ride.getDropLatitude());
        rideResponse.setDropLongitude(ride.getDropLongitude());
        rideResponse.setDropAddress(ride.getDropAddress());

        rideResponse.setStatus(ride.getStatus());

        rideResponse.setEstimatedFare(ride.getEstimatedFare());
        rideResponse.setActualFare(ride.getActualFare());

        rideResponse.setCreatedAt(ride.getCreatedAt());
        rideResponse.setUpdatedAt(ride.getUpdatedAt());
        rideResponse.setStartedAt(ride.getStartedAt());
        rideResponse.setCompletedAt(ride.getCompletedAt());

        return rideResponse;
    }

    private double calculateEstimateFare(RideRequest rideRequest) {
        // Simplified Haversine distance calculation
        double lat1 = Math.toRadians(rideRequest.getPickupLatitude());
        double lon1 = Math.toRadians(rideRequest.getPickupLongitude());
        double lat2 = Math.toRadians(rideRequest.getDropLatitude());
        double lon2 = Math.toRadians(rideRequest.getDropLongitude());
        double latDiff = lat2 - lat1;
        double lonDiff = lon2 - lon1;

        // calc a
        double a = Math.pow(Math.sin(latDiff / 2), 2 )
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(lonDiff / 2), 2);

        // calc c
        double c = 2 * Math.asin(Math.sqrt(a));
        double distanceInKm = 6371 * c;

        // calc fare
        double fare = BASE_RIDE_FARE + (RIDE_FARE_PER_KM * distanceInKm);
        return Math.round(fare * 100.0) / 100.0;
    }
}
