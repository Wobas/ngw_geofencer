import type { EventLog } from '../types';
import { getGeometryTypeFromWKT } from '../utils/geometryParser';

type EventsPanelProps = {
  events: EventLog[];
  selectedEvent: EventLog | null;
  status: string;
  onBack: () => void;
  onRefresh: () => void;
  onSelect: (event: EventLog) => void;
};

function formatTimestamp(timestamp: number): string {
  return new Date(timestamp * 1000).toLocaleString();
}

function truncateMessage(message: string, maxLength: number = 100): string {
  if (message.length <= maxLength) return message;
  return message.substring(0, maxLength) + '...';
}

export function EventsPanel({
  events,
  selectedEvent,
  status,
  onBack,
  onRefresh,
  onSelect
}: EventsPanelProps) {
  return (
    <aside className="sidebar">
      <div className="sidebar__header">
        <h1>Intersections</h1>
        <button 
          type="button" 
          onClick={onRefresh}
          className="refresh-btn"
        >
          🔄 Refresh
        </button>
      </div>

      {!selectedEvent ? (
        <section className="panel-section">
          <h2>Events ({events.length})</h2>
          <div className="events-list">
            {events.length === 0 ? (
              <p className="placeholder">{status}</p>
            ) : (
              events.map((event, index) => (
                <div 
                  key={index}
                  className="event-item"
                  onClick={() => onSelect(event)}
                >
                  <div className="event-header">
                    <span className="event-time">{formatTimestamp(event.timestamp)}</span>
                    <span className="event-badge">Event #{events.length - index}</span>
                  </div>
                  <div className="event-geometries">
                    <span className="geom-type">📐 {getGeometryTypeFromWKT(event.geom1)}</span>
                    <span className="geom-type">📐 {getGeometryTypeFromWKT(event.geom2)}</span>
                  </div>
                  <div className="event-message">
                    {truncateMessage(event.message, 80)}
                  </div>
                </div>
              ))
            )}
          </div>
        </section>
      ) : (
        <section className="panel-section">
          <div className="details-header">
            <button 
              type="button" 
              onClick={onBack}
              className="back-btn"
            >
              ← Back to list
            </button>
          </div>
          
          <h2>Event Details</h2>
          
          <div className="details-content">
            <div className="detail-item">
              <strong>Timestamp:</strong>
              <span>{formatTimestamp(selectedEvent.timestamp)}</span>
            </div>
            
            <div className="detail-item">
              <strong>Geometry 1 Type:</strong>
              <span className="geom-type-badge">
                {getGeometryTypeFromWKT(selectedEvent.geom1)}
              </span>
            </div>
            
            <div className="detail-item">
              <strong>Geometry 1 WKT:</strong>
              <code className="wkt-code">{selectedEvent.geom1}</code>
            </div>
            
            <div className="detail-item">
              <strong>Geometry 2 Type:</strong>
              <span className="geom-type-badge">
                {getGeometryTypeFromWKT(selectedEvent.geom2)}
              </span>
            </div>
            
            <div className="detail-item">
              <strong>Geometry 2 WKT:</strong>
              <code className="wkt-code">{selectedEvent.geom2}</code>
            </div>
            
            <div className="detail-item full-width">
              <strong>Message:</strong>
              <div className="message-full">{selectedEvent.message}</div>
            </div>
          </div>
        </section>
      )}
    </aside>
  );
}
