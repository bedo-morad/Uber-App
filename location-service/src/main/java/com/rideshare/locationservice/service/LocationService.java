package com.rideshare.locationservice.service;

import com.rideshare.locationservice.dto.DriverLocationRequest;
import com.rideshare.locationservice.dto.NearByDriverResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class LocationService {

    private final RedisTemplate<String, String> redisTemplate;
    // Redis key for all driver locations
    private static final String DRIVER_GEO_KEY = "drivers:locations";

    public void updateDriverLocation(DriverLocationRequest driverLocationRequest) {
        log.info("Updating location for driver: {}", driverLocationRequest.getDriverId());
        // Important: GeoSpital Standard => (Longitude first - Latitude second)
        Point driverPoint = new Point(
                driverLocationRequest.getLongitude(),
                driverLocationRequest.getLatitude()
        );
        // Store the driver's location in Redis
        redisTemplate.opsForGeo().add(
                DRIVER_GEO_KEY,
                driverPoint,
                driverLocationRequest.getDriverId()
        );
        log.info("Location updated for driver: {}", driverLocationRequest.getDriverId());
    }

    public List<NearByDriverResponse> getNearByDrivers(double latitude, double longitude, double radius) {
        log.info("Getting nearby drivers within {}km of : ( {} , {} )", radius, longitude, latitude);
        Circle searchArea = new Circle(new Point(longitude, latitude), new Distance(radius, Metrics.KILOMETERS));
        GeoResults<RedisGeoCommands.GeoLocation<String>> geoResults =
                redisTemplate.opsForGeo().radius(
                        DRIVER_GEO_KEY,
                        searchArea,
                        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs()
                                .includeCoordinates()
                                .includeDistance()
                                .sortAscending()
                                .limit(10)
                );
        List<NearByDriverResponse> nearByDrivers = new ArrayList<>();
        if (geoResults != null) {
            geoResults.getContent().forEach(geoLocation -> {
                RedisGeoCommands.GeoLocation<String> location = geoLocation.getContent();
                nearByDrivers.add(new NearByDriverResponse(
                        location.getName(),
                        location.getPoint().getY(),
                        location.getPoint().getX(),
                        geoLocation.getDistance().getValue()
                ));
            });
        }
        log.info("Found {} nearby drivers", nearByDrivers.size());
        return nearByDrivers;
    }

    public void removeDriver(String driverId) {
        log.info("Removing driver: {}", driverId);
        redisTemplate.opsForGeo().remove(DRIVER_GEO_KEY, driverId);
    }
}
