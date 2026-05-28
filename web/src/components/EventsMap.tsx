import { useEffect, useRef } from "react";
import Map from "ol/Map";
import View from "ol/View";
import TileLayer from "ol/layer/Tile";
import VectorLayer from "ol/layer/Vector";
import VectorSource from "ol/source/Vector";
import OSM from "ol/source/OSM";
import { fromLonLat, transform } from 'ol/proj';
import { buffer as turfBuffer } from '@turf/turf';
import type { Geometry } from 'ol/geom';
import { Style, Fill, Stroke, Circle as CircleStyle } from "ol/style";
import Feature from 'ol/Feature';
import type { EventLog } from "../types";
import { parseWKTToOLGeometry, getGeometryCenter, getGeometryColor, getGeometryTypeFromWKT } from "../utils/geometryParser";

export function EventsMap({ events }: { events: EventLog[] }) {
  const mapElement = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const vectorSourceRef = useRef<VectorSource>(new VectorSource());

  const getGeometryStyle = (geometryType: string, isSelected: boolean = false) => {
    const color = getGeometryColor(geometryType);
    
    if (geometryType === 'POINT') {
      return new Style({
        image: new CircleStyle({
          radius: isSelected ? 10 : 6,
          fill: new Fill({ color: color }),
          stroke: new Stroke({ color: '#FFFFFF', width: 2 })
        })
      });
    } else if (geometryType === 'LINESTRING') {
      return new Style({
        stroke: new Stroke({
          color: color,
          width: isSelected ? 4 : 2
        })
      });
    } else {
      return new Style({
        fill: new Fill({
          color: color + '40'
        }),
        stroke: new Stroke({
          color: color,
          width: isSelected ? 3 : 2
        })
      });
    }
  };

  const displayEvents = () => {
    const vectorSource = vectorSourceRef.current;
    vectorSource.clear();
    
    if (events.length === 0) return;

    events.forEach((event, index) => {
      const geom1 = parseWKTToOLGeometry(event.geom1);
      const geom2 = parseWKTToOLGeometry(event.geom2);
      
      const geom1Type = getGeometryTypeFromWKT(event.geom1);
      const geom2Type = getGeometryTypeFromWKT(event.geom2);

      if (geom1) {
        const feature = new Feature({ geometry: geom1 });
        feature.setStyle(getGeometryStyle(geom1Type, false));
        feature.set('type', 'geom1');
        feature.set('eventIndex', index);
        vectorSource.addFeature(feature);
      }

      if (geom2) {
        const feature = new Feature({ geometry: geom2 });
        feature.setStyle(getGeometryStyle(geom2Type, false));
        feature.set('type', 'geom2');
        feature.set('eventIndex', index);
        vectorSource.addFeature(feature);
      }
    });

    if (vectorSource.getFeatures().length > 0) {
      const extent = vectorSource.getExtent();
      if (extent && !isNaN(extent[0]) && !isNaN(extent[1]) && 
          isFinite(extent[0]) && isFinite(extent[1])) {
        mapRef.current?.getView().fit(extent, { 
          padding: [50, 50, 50, 50],
          duration: 500
        });
      }
    }
  };

  useEffect(() => {
    if (!mapElement.current || mapRef.current) return;

    const vectorLayer = new VectorLayer({
      source: vectorSourceRef.current,
      declutter: true
    });

    mapRef.current = new Map({
      target: mapElement.current,
      layers: [
        new TileLayer({ 
          source: new OSM()
        }),
        vectorLayer
      ],
      view: new View({
        center: fromLonLat([0, 0]),
        zoom: 2,
        projection: 'EPSG:3857'
      }),
    });

    mapRef.current.on('click', (event) => {
      const coord = event.coordinate;
      const lonLat = transform(coord, 'EPSG:3857', 'EPSG:4326');
      console.log(`Clicked at: ${lonLat[0].toFixed(4)}°, ${lonLat[1].toFixed(4)}°`);
    });

    return () => {
      mapRef.current?.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (mapRef.current) {
      displayEvents();
    }
  }, [events]);

  return <div ref={mapElement} className="map" />;
}
