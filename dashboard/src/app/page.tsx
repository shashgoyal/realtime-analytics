"use client";

import { useState, useCallback, useRef, useEffect } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

const EVENT_TYPES = [
  { type: "click", label: "Click", color: "bg-blue-500" },
  { type: "page_view", label: "Page View", color: "bg-emerald-500" },
  { type: "scroll", label: "Scroll", color: "bg-violet-500" },
  { type: "form_submit", label: "Form Submit", color: "bg-amber-500" },
  { type: "purchase", label: "Purchase", color: "bg-pink-500" },
  { type: "signup", label: "Sign Up", color: "bg-teal-500" },
  { type: "logout", label: "Logout", color: "bg-slate-500" },
  { type: "error", label: "Error", color: "bg-red-500" },
];

const PAGES = ["/home", "/products", "/products/1", "/cart", "/checkout", "/dashboard", "/settings", "/profile", "/blog", "/search"];
const DEVICES = ["mobile", "desktop", "tablet"];

interface QueuedEvent {
  id: string;
  user_id: string;
  event_type: string;
  timestamp: string;
  page_url: string;
  device: string;
}

export default function EventSender() {
  const [queue, setQueue] = useState<QueuedEvent[]>([]);
  const [userId, setUserId] = useState("user-1");
  const [page, setPage] = useState("/home");
  const [device, setDevice] = useState("desktop");
  const [sending, setSending] = useState(false);
  const [log, setLog] = useState<string[]>([]);
  const [autoFlush, setAutoFlush] = useState(false);
  const [autoFlushInterval, setAutoFlushInterval] = useState(3);
  const logEndRef = useRef<HTMLDivElement>(null);

  const addLog = useCallback((msg: string) => {
    setLog((prev) => [...prev.slice(-99), `${new Date().toLocaleTimeString()} — ${msg}`]);
  }, []);

  const enqueue = useCallback(
    (eventType: string) => {
      const ev: QueuedEvent = {
        id: crypto.randomUUID(),
        user_id: userId,
        event_type: eventType,
        timestamp: new Date().toISOString(),
        page_url: page,
        device,
      };
      setQueue((prev) => [...prev, ev]);
      addLog(`Queued ${eventType} for ${userId}`);
    },
    [userId, page, device, addLog]
  );

  const flushQueue = useCallback(async () => {
    if (queue.length === 0) return;
    setSending(true);
    const batch = queue.map(({ id, ...rest }) => rest);
    try {
      const res = await fetch(`${API}/events/batch`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(batch),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      addLog(`Sent batch of ${data.count} events`);
      setQueue([]);
    } catch (err) {
      addLog(`Failed to send: ${err}`);
    } finally {
      setSending(false);
    }
  }, [queue, addLog]);

  const quickSend = useCallback(
    async (eventType: string) => {
      const ev = {
        user_id: userId,
        event_type: eventType,
        timestamp: new Date().toISOString(),
        page_url: page,
        device,
      };
      try {
        const res = await fetch(`${API}/event`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(ev),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        addLog(`Sent ${eventType} immediately`);
      } catch (err) {
        addLog(`Failed: ${err}`);
      }
    },
    [userId, page, device, addLog]
  );

  useEffect(() => {
    if (!autoFlush || queue.length === 0) return;
    const timer = setTimeout(() => {
      flushQueue();
    }, autoFlushInterval * 1000);
    return () => clearTimeout(timer);
  }, [autoFlush, queue, autoFlushInterval, flushQueue]);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [log]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      <h1 className="text-2xl font-bold mb-6">Event Sender</h1>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left column — config */}
        <div className="space-y-4">
          <div className="bg-card border border-border rounded-xl p-5 space-y-4">
            <h2 className="font-semibold text-sm text-muted uppercase tracking-wider">Event Properties</h2>

            <label className="block">
              <span className="text-sm font-medium">User ID</span>
              <input
                value={userId}
                onChange={(e) => setUserId(e.target.value)}
                className="mt-1 block w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-accent"
              />
            </label>

            <label className="block">
              <span className="text-sm font-medium">Page</span>
              <select
                value={page}
                onChange={(e) => setPage(e.target.value)}
                className="mt-1 block w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-accent"
              >
                {PAGES.map((p) => (
                  <option key={p} value={p}>{p}</option>
                ))}
              </select>
            </label>

            <label className="block">
              <span className="text-sm font-medium">Device</span>
              <select
                value={device}
                onChange={(e) => setDevice(e.target.value)}
                className="mt-1 block w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-accent"
              >
                {DEVICES.map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </label>
          </div>

          {/* Auto-flush */}
          <div className="bg-card border border-border rounded-xl p-5 space-y-3">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium">Auto-flush batch</span>
              <button
                onClick={() => setAutoFlush(!autoFlush)}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors ${autoFlush ? "bg-accent" : "bg-border"}`}
              >
                <span className={`inline-block h-4 w-4 rounded-full bg-white transition-transform ${autoFlush ? "translate-x-6" : "translate-x-1"}`} />
              </button>
            </div>
            {autoFlush && (
              <label className="block">
                <span className="text-xs text-muted">Interval (seconds)</span>
                <input
                  type="number"
                  min={1}
                  max={30}
                  value={autoFlushInterval}
                  onChange={(e) => setAutoFlushInterval(Number(e.target.value))}
                  className="mt-1 block w-full rounded-lg border border-border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-accent"
                />
              </label>
            )}
          </div>
        </div>

        {/* Middle column — event buttons */}
        <div className="space-y-4">
          <div className="bg-card border border-border rounded-xl p-5">
            <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
              Click to queue &middot; Shift+click to send immediately
            </h2>
            <div className="grid grid-cols-2 gap-3">
              {EVENT_TYPES.map(({ type, label, color }) => (
                <button
                  key={type}
                  onClick={(e) => (e.shiftKey ? quickSend(type) : enqueue(type))}
                  className={`${color} text-white rounded-lg px-4 py-3 text-sm font-medium hover:opacity-90 active:scale-95 transition-all`}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>

          {/* Batch queue */}
          <div className="bg-card border border-border rounded-xl p-5">
            <div className="flex items-center justify-between mb-3">
              <h2 className="font-semibold text-sm text-muted uppercase tracking-wider">
                Queue ({queue.length})
              </h2>
              <div className="flex gap-2">
                <button
                  onClick={() => setQueue([])}
                  disabled={queue.length === 0}
                  className="text-xs px-3 py-1.5 rounded-md border border-border hover:bg-background disabled:opacity-40 transition-colors"
                >
                  Clear
                </button>
                <button
                  onClick={flushQueue}
                  disabled={queue.length === 0 || sending}
                  className="text-xs px-3 py-1.5 rounded-md bg-accent text-white hover:bg-accent-hover disabled:opacity-40 transition-colors"
                >
                  {sending ? "Sending…" : `Send batch (${queue.length})`}
                </button>
              </div>
            </div>
            <div className="max-h-48 overflow-y-auto space-y-1 text-xs font-mono">
              {queue.length === 0 && <p className="text-muted italic">Queue is empty</p>}
              {queue.map((ev) => (
                <div key={ev.id} className="flex items-center justify-between bg-background rounded px-2 py-1">
                  <span>
                    <span className="font-semibold">{ev.event_type}</span>{" "}
                    <span className="text-muted">{ev.user_id} &middot; {ev.page_url}</span>
                  </span>
                  <button
                    onClick={() => setQueue((q) => q.filter((e) => e.id !== ev.id))}
                    className="text-muted hover:text-danger ml-2"
                  >
                    &times;
                  </button>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right column — log */}
        <div className="bg-card border border-border rounded-xl p-5 flex flex-col">
          <div className="flex items-center justify-between mb-3">
            <h2 className="font-semibold text-sm text-muted uppercase tracking-wider">Activity Log</h2>
            <button onClick={() => setLog([])} className="text-xs text-muted hover:text-foreground">
              Clear
            </button>
          </div>
          <div className="flex-1 min-h-[300px] max-h-[500px] overflow-y-auto space-y-0.5 text-xs font-mono bg-background rounded-lg p-3">
            {log.length === 0 && <p className="text-muted italic">No activity yet</p>}
            {log.map((entry, i) => (
              <p key={i} className="text-muted leading-relaxed">{entry}</p>
            ))}
            <div ref={logEndRef} />
          </div>
        </div>
      </div>
    </div>
  );
}
