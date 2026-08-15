package com.rideshare.rideservice.event;

// Event Published to Kafka when a ride is requested
// Matching service consumes this even
// Topic: ride.requested

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RideRequestedEvent {
    private String rideId;
    private String riderId;
    // Pickup
    private double pickupLatitude;
    private double pickupLongitude;
    private String pickupAddress;
    // Drop
    private double dropLatitude;
    private double dropLongitude;
    private String dropAddress;
}
