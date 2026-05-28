import WKT from 'wkt';
import { fromLonLat, transform } from 'ol/proj';
import { Geometry } from 'ol/geom';
import Point from 'ol/geom/Point';
import LineString from 'ol/geom/LineString';
import Polygon from 'ol/geom/Polygon';
import MultiPoint from 'ol/geom/MultiPoint';
import MultiLineString from 'ol/geom/MultiLineString';
import MultiPolygon from 'ol/geom/MultiPolygon';

const SOURCE_CRS = 'EPSG:4326';
const TARGET_CRS = 'EPSG:3857';

function transformCoordinates(coords: any): any {
  if (typeof coords === 'number') {
    return coords;
  }
  
  if (Array.isArray(coords)) {
    if (coords.length === 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
      return fromLonLat([coords[0], coords[1]]);
    }
    return coords.map(coord => transformCoordinates(coord));
  }
  
  return coords;
}

export function parseWKTToOLGeometry(wkt: string): Geometry | null {
  try {
    const geoJsonGeometry = WKT.parse(wkt);
    
    if (!geoJsonGeometry || !geoJsonGeometry.type || !geoJsonGeometry.coordinates) {
      return null;
    }

    const transformedCoords = transformCoordinates(geoJsonGeometry.coordinates);
    
    switch (geoJsonGeometry.type) {
      case 'Point':
        return new Point(transformedCoords);
      case 'LineString':
        return new LineString(transformedCoords);
      case 'Polygon':
        return new Polygon(transformedCoords);
      case 'MultiPoint':
        return new MultiPoint(transformedCoords);
      case 'MultiLineString':
        return new MultiLineString(transformedCoords);
      case 'MultiPolygon':
        return new MultiPolygon(transformedCoords);
      default:
        console.warn('Unsupported geometry type:', geoJsonGeometry.type);
        return null;
    }
  } catch (error) {
    console.error('Error parsing WKT:', wkt, error);
    return null;
  }
}

export function getGeometryCenter(geometry: Geometry): number[] {
  const extent = geometry.getExtent();
  return [
    (extent[0] + extent[2]) / 2,
    (extent[1] + extent[3]) / 2
  ];
}

export function getGeometryCenterInSourceCRS(geometry: Geometry): number[] {
  const center = getGeometryCenter(geometry);
  return transform(center, TARGET_CRS, SOURCE_CRS);
}

export function getGeometryColor(geometryType: string): string {
  const colors: Record<string, string> = {
    'POINT': '#FF4444',
    'LINESTRING': '#44FF44',
    'POLYGON': '#4444FF',
    'MULTIPOINT': '#FF8844',
    'MULTILINESTRING': '#88FF88',
    'MULTIPOLYGON': '#8888FF'
  };
  return colors[geometryType] || '#FF44FF';
}

export function getGeometryTypeFromWKT(wkt: string): string {
  return wkt.split(' ')[0].toUpperCase();
}

export function isValidCoordinates(wkt: string): boolean {
  try {
    const geometry = parseWKTToOLGeometry(wkt);
    if (!geometry) return false;
    
    const extent = geometry.getExtent();
    return extent.every(v => isFinite(v) && !isNaN(v));
  } catch {
    return false;
  }
}
