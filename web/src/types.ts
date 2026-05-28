export type EventLog = {
  timestamp: number;
  message: string;
  geom1: string;
  geom2: string;
}

export type ParsedGeometry = {
  wkt: string;
  type: string;
  coordinates: any;
};
