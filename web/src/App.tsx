import { useEffect, useState } from 'react';
import { EventsMap } from './components/EventsMap';
import { EventsPanel } from './components/EventsPanel';
import type { EventLog } from './types';

const apiBaseUrl = process.env.API_BASE_URL || window.location.origin;

export function App() {
  const [events, setEvents] = useState<EventLog[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<EventLog | null>(null);
  const [status, setStatus] = useState('Loading...');

  useEffect(() => {
    loadEvents();
  }, []);

  const mapEvents = selectedEvent ? [selectedEvent] : events.slice(0, 50);

  async function loadEvents() {
    setStatus('Loading...');

    try {
      const response = await fetch(`${apiBaseUrl}/logs_all`);
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || 'Failed to load events');
      }

      const logs = Array.isArray(data.logs) ? data.logs : [];

      setEvents(logs);
      setSelectedEvent(null);
      setStatus(`${logs.length} events`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : 'Failed to load events');
    }
  }

  return (
    <div className="layout">
      <EventsPanel
        events={events}
        selectedEvent={selectedEvent}
        status={status}
        onBack={() => setSelectedEvent(null)}
        onRefresh={loadEvents}
        onSelect={setSelectedEvent}
      />
      <main className="map-panel">
        <EventsMap events={mapEvents} />
      </main>
    </div>
  );
}
