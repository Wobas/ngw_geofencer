import type { EventLog } from '../types';

type EventsPanelProps = {
  events: EventLog[];
  selectedEvent: EventLog | null;
  status: string;
  onBack: () => void;
  onRefresh: () => void;
  onSelect: (event: EventLog) => void;
};

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
      </div>

      <section className="panel-section">
        <h2>Events</h2>
        <p className="placeholder">
          {status}. Event list placeholder.
        </p>
        <button type="button" onClick={onRefresh}>
          Refresh
        </button>
        {events[0] && (
          <button type="button" onClick={() => onSelect(events[0])}>
            Select first event
          </button>
        )}
      </section>

      <section className="panel-section">
        <h2>Details</h2>
        <p className="placeholder">
          {selectedEvent ? 'Selected event placeholder.' : 'Event details placeholder.'}
        </p>
        {selectedEvent && (
          <button type="button" onClick={onBack}>
            Back
          </button>
        )}
      </section>
    </aside>
  );
}
