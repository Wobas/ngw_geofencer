import { useEffect, useRef } from "react";
import Map from "ol/Map";
import View from "ol/View";
import TileLayer from "ol/layer/Tile";
import OSM from "ol/source/OSM";
import type { EventLog } from "../types";

export function EventsMap({ events }: { events: EventLog[] }) {
  const mapElement = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);

  useEffect(() => {
    if (!mapElement.current || mapRef.current) return;

    mapRef.current = new Map({
      target: mapElement.current,
      layers: [
        new TileLayer({ source: new OSM() }),
      ],
      view: new View({
        center: [0, 0],
        zoom: 2,
      }),
    });

    return () => {
      mapRef.current?.setTarget(undefined);
      mapRef.current = null;
    };
  }, []);

  return <div ref={mapElement} className="map" />;
}
