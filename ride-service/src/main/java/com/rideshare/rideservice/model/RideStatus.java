package com.rideshare.rideservice.model;

/**
 * Flow:
 * Requested -> Matching -> Accepted -> Driver Arriving ->
 *  -> Ride Started -> Completed
 *  -> Canceled (can happen at multiple stages)
 */


public enum RideStatus {
    REQUESTED,
    MATCHING,
    ACCEPTED,
    DRIVER_ARRIVING,
    RIDE_STARTED,
    COMPLETED,
    CANCELED
}
