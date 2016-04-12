package it.uniroma2.taos.generator.service;

import it.uniroma2.taos.commons.network.PlacementRequest;
import it.uniroma2.taos.commons.network.PlacementResponse;

public interface PlacementService {
public PlacementResponse getPlacement(PlacementRequest pr);
}