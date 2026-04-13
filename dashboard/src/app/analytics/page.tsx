"use client";

import { useState, useEffect, useCallback } from "react";

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface Summary {
  total_events: number;
  total_unique_users: number;
  total_unique_pages: number;
  total_errors: number;
  latest_unique_users: number;
  latest_error_rate: number;
  event_breakdown: Record<string, number>;
  devices: { counts: Record<string, number>; percentages: Record<string, number> };
  top_pages: { page_url: string; hits: number }[];
  top_users: { user_id: string; total: number }[];
  user_stats: { avg_events_per_user: number; max_events_per_user: number };
  throughput_timeline: { window: string; by_type: Record<string, number>; total: number }[];
  unique_users_timeline: { window: string; unique_users: number }[];
  error_timeline: { window: string; total: number; errors: number; rate: number }[];
}

interface FilterOptions {
  pages: string[];
  users: string[];
  devices: string[];
}

interface FilterResult {
  total_hits?: number;
  total_events?: number;
  events?: Record<string, number>;
  devices?: Record<string, number>;
  pages?: { name: string; count: number }[];
}

function StatCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="bg-card border border-border rounded-xl p-5">
      <p className="text-xs font-medium text-muted uppercase tracking-wider">{label}</p>
      <p className="text-3xl font-bold mt-1">{value}</p>
      {sub && <p className="text-xs text-muted mt-1">{sub}</p>}
    </div>
  );
}

function Bar({ value, max, label, color = "bg-accent" }: { value: number; max: number; label: string; color?: string }) {
  const pct = max > 0 ? (value / max) * 100 : 0;
  return (
    <div className="flex items-center gap-3 text-sm">
      <span className="w-28 truncate text-muted font-mono text-xs text-right">{label}</span>
      <div className="flex-1 bg-background rounded-full h-5 overflow-hidden">
        <div className={`${color} h-full rounded-full transition-all duration-500`} style={{ width: `${pct}%` }} />
      </div>
      <span className="w-12 text-right font-mono text-xs">{value}</span>
    </div>
  );
}

function shortTime(iso: string) {
  try {
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  } catch {
    return iso;
  }
}

type Dimension = "page" | "user" | "device";

export default function AnalyticsDashboard() {
  const [data, setData] = useState<Summary | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshRate, setRefreshRate] = useState(5);
  const [lastUpdated, setLastUpdated] = useState<string>("");

  const [filterOpts, setFilterOpts] = useState<FilterOptions | null>(null);
  const [dimension, setDimension] = useState<Dimension>("page");
  const [filterValue, setFilterValue] = useState<string>("");
  const [filterResult, setFilterResult] = useState<FilterResult | null>(null);
  const [filterLoading, setFilterLoading] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      const [summaryRes, filtersRes] = await Promise.all([
        fetch(`${API}/analytics/summary`),
        fetch(`${API}/analytics/filters`),
      ]);
      if (!summaryRes.ok) throw new Error(`HTTP ${summaryRes.status}`);
      setData(await summaryRes.json());
      if (filtersRes.ok) setFilterOpts(await filtersRes.json());
      setLastUpdated(new Date().toLocaleTimeString());
    } catch {
      /* keep stale data on error */
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, refreshRate * 1000);
    return () => clearInterval(id);
  }, [fetchData, refreshRate]);

  useEffect(() => {
    setFilterValue("");
    setFilterResult(null);
  }, [dimension]);

  const fetchFilter = useCallback(async (dim: Dimension, val: string) => {
    if (!val) { setFilterResult(null); return; }
    setFilterLoading(true);
    try {
      const param = dim === "page" ? `url=${encodeURIComponent(val)}`
        : dim === "user" ? `id=${encodeURIComponent(val)}`
        : `type=${encodeURIComponent(val)}`;
      const res = await fetch(`${API}/analytics/filter/${dim}?${param}`);
      if (res.ok) setFilterResult(await res.json());
      else setFilterResult(null);
    } catch { setFilterResult(null); }
    finally { setFilterLoading(false); }
  }, []);

  if (loading && !data)
    return (
      <div className="flex items-center justify-center h-96 text-muted">Loading analytics…</div>
    );

  if (!data)
    return (
      <div className="flex items-center justify-center h-96 text-danger">
        Failed to load analytics. Is the API running?
      </div>
    );

  const maxEvent = Math.max(...Object.values(data.event_breakdown), 1);
  const maxPage = data.top_pages.length > 0 ? data.top_pages[0].hits : 1;
  const maxUser = data.top_users.length > 0 ? data.top_users[0].total : 1;
  const maxDeviceCount = Math.max(...Object.values(data.devices.counts), 1);
  const maxThroughput = Math.max(...data.throughput_timeline.map((t) => t.total), 1);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between mb-6 gap-3">
        <h1 className="text-2xl font-bold">Analytics Dashboard</h1>
        <div className="flex items-center gap-4 text-sm text-muted">
          <span>Updated {lastUpdated}</span>
          <label className="flex items-center gap-1.5">
            Refresh
            <select
              value={refreshRate}
              onChange={(e) => setRefreshRate(Number(e.target.value))}
              className="rounded border border-border bg-card px-2 py-1 text-xs"
            >
              <option value={2}>2s</option>
              <option value={5}>5s</option>
              <option value={10}>10s</option>
              <option value={30}>30s</option>
            </select>
          </label>
          <button
            onClick={fetchData}
            className="px-3 py-1 rounded-md border border-border hover:bg-card text-xs transition-colors"
          >
            Refresh now
          </button>
        </div>
      </div>

      {/* Stat cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
        <StatCard label="Total Events" value={data.total_events.toLocaleString()} />
        <StatCard
          label="Unique Users"
          value={data.total_unique_users.toLocaleString()}
          sub={`${data.latest_unique_users} in latest window`}
        />
        <StatCard
          label="Total Errors"
          value={data.total_errors.toLocaleString()}
          sub={`${(data.latest_error_rate * 100).toFixed(1)}% latest rate`}
        />
        <StatCard
          label="Avg Events / User"
          value={data.user_stats.avg_events_per_user}
          sub={`Max: ${data.user_stats.max_events_per_user}`}
        />
      </div>

      {/* Drill-down filter */}
      {filterOpts && (
        <div className="bg-card border border-border rounded-xl p-5 mb-8">
          <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
            Drill-Down Explorer
          </h2>

          <div className="flex flex-wrap items-end gap-3 mb-5">
            <div>
              <label className="block text-xs text-muted mb-1">Dimension</label>
              <div className="flex rounded-lg border border-border overflow-hidden text-sm">
                {(["page", "user", "device"] as Dimension[]).map((d) => (
                  <button
                    key={d}
                    onClick={() => setDimension(d)}
                    className={`px-4 py-1.5 capitalize transition-colors ${
                      dimension === d ? "bg-accent text-white" : "bg-card hover:bg-background"
                    }`}
                  >
                    {d}
                  </button>
                ))}
              </div>
            </div>

            <div className="flex-1 min-w-[200px]">
              <label className="block text-xs text-muted mb-1">
                Select {dimension}
              </label>
              <select
                value={filterValue}
                onChange={(e) => {
                  setFilterValue(e.target.value);
                  fetchFilter(dimension, e.target.value);
                }}
                className="w-full rounded-lg border border-border bg-background px-3 py-1.5 text-sm"
              >
                <option value="">— choose —</option>
                {(dimension === "page"
                  ? filterOpts.pages
                  : dimension === "user"
                  ? filterOpts.users
                  : filterOpts.devices
                ).map((v) => (
                  <option key={v} value={v}>
                    {v}
                  </option>
                ))}
              </select>
            </div>
          </div>

          {filterLoading && (
            <p className="text-sm text-muted italic">Loading…</p>
          )}

          {filterResult && !filterLoading && (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
              {/* Events breakdown */}
              {filterResult.events && Object.keys(filterResult.events).length > 0 && (
                <div>
                  <h3 className="text-xs font-semibold text-muted uppercase mb-2">Events</h3>
                  <div className="space-y-1.5">
                    {Object.entries(filterResult.events)
                      .sort((a, b) => b[1] - a[1])
                      .map(([t, c]) => (
                        <Bar
                          key={t}
                          label={t}
                          value={c}
                          max={Math.max(...Object.values(filterResult.events!), 1)}
                        />
                      ))}
                  </div>
                </div>
              )}

              {/* Devices breakdown */}
              {filterResult.devices && Object.keys(filterResult.devices).length > 0 && (
                <div>
                  <h3 className="text-xs font-semibold text-muted uppercase mb-2">Devices</h3>
                  <div className="space-y-1.5">
                    {Object.entries(filterResult.devices)
                      .sort((a, b) => b[1] - a[1])
                      .map(([d, c]) => (
                        <Bar
                          key={d}
                          label={d}
                          value={c}
                          max={Math.max(...Object.values(filterResult.devices!), 1)}
                          color="bg-violet-500"
                        />
                      ))}
                  </div>
                </div>
              )}

              {/* Pages breakdown */}
              {filterResult.pages && filterResult.pages.length > 0 && (
                <div>
                  <h3 className="text-xs font-semibold text-muted uppercase mb-2">Pages</h3>
                  <div className="space-y-1.5">
                    {filterResult.pages.map((p) => (
                      <Bar
                        key={p.name}
                        label={p.name}
                        value={p.count}
                        max={filterResult.pages![0].count || 1}
                        color="bg-emerald-500"
                      />
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {!filterResult && !filterLoading && filterValue && (
            <p className="text-sm text-muted italic">No data found for this selection.</p>
          )}
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Event breakdown */}
        <div className="bg-card border border-border rounded-xl p-5">
          <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">Event Breakdown</h2>
          <div className="space-y-2">
            {Object.entries(data.event_breakdown)
              .sort((a, b) => b[1] - a[1])
              .map(([type, count]) => (
                <Bar key={type} label={type} value={count} max={maxEvent} />
              ))}
          </div>
        </div>

        {/* Device breakdown */}
        <div className="bg-card border border-border rounded-xl p-5">
          <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">Devices</h2>
          <div className="space-y-2">
            {Object.entries(data.devices.counts)
              .sort((a, b) => b[1] - a[1])
              .map(([device, count]) => (
                <Bar
                  key={device}
                  label={`${device} (${data.devices.percentages[device] ?? 0}%)`}
                  value={count}
                  max={maxDeviceCount}
                  color="bg-violet-500"
                />
              ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-8">
        {/* Top pages */}
        <div className="bg-card border border-border rounded-xl p-5">
          <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
            Top Pages
          </h2>
          {data.top_pages.length === 0 ? (
            <p className="text-sm text-muted italic">No data yet</p>
          ) : (
            <div className="space-y-2">
              {data.top_pages.map((p) => (
                <Bar key={p.page_url} label={p.page_url} value={p.hits} max={maxPage} color="bg-emerald-500" />
              ))}
            </div>
          )}
        </div>

        {/* Top users */}
        <div className="bg-card border border-border rounded-xl p-5">
          <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
            Top Users
          </h2>
          {data.top_users.length === 0 ? (
            <p className="text-sm text-muted italic">No data yet</p>
          ) : (
            <div className="space-y-2">
              {data.top_users.map((u) => (
                <Bar key={u.user_id} label={u.user_id} value={u.total} max={maxUser} color="bg-pink-500" />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Throughput timeline */}
      <div className="bg-card border border-border rounded-xl p-5 mb-8">
        <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
          Throughput Timeline (events per minute)
        </h2>
        {data.throughput_timeline.length === 0 ? (
          <p className="text-sm text-muted italic">No windowed data yet</p>
        ) : (
          <div className="flex items-stretch gap-1 h-48 overflow-x-auto pb-8 relative">
            {data.throughput_timeline.map((t) => {
              const pct = (t.total / maxThroughput) * 100;
              return (
                <div key={t.window} className="flex flex-col items-center justify-end flex-shrink-0 group" style={{ minWidth: 48 }}>
                  <span className="text-[10px] font-mono text-muted mb-1">
                    {t.total}
                  </span>
                  <div
                    className="w-8 bg-accent rounded-t transition-all duration-500 hover:bg-accent-hover"
                    style={{ height: `${Math.max(pct, 4)}%` }}
                  />
                  <span className="text-[10px] font-mono text-muted mt-1 rotate-[-45deg] origin-top-left whitespace-nowrap">
                    {shortTime(t.window)}
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Error timeline table */}
      <div className="bg-card border border-border rounded-xl p-5">
        <h2 className="font-semibold text-sm text-muted uppercase tracking-wider mb-4">
          Error Rate Timeline
        </h2>
        {data.error_timeline.length === 0 ? (
          <p className="text-sm text-muted italic">No error data yet</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted text-xs uppercase border-b border-border">
                  <th className="pb-2 pr-4">Window</th>
                  <th className="pb-2 pr-4 text-right">Total</th>
                  <th className="pb-2 pr-4 text-right">Errors</th>
                  <th className="pb-2 text-right">Rate</th>
                </tr>
              </thead>
              <tbody>
                {data.error_timeline.map((e) => (
                  <tr key={e.window} className="border-b border-border/50 last:border-0">
                    <td className="py-2 pr-4 font-mono text-xs">{shortTime(e.window)}</td>
                    <td className="py-2 pr-4 text-right">{e.total}</td>
                    <td className="py-2 pr-4 text-right">{e.errors}</td>
                    <td className={`py-2 text-right font-semibold ${e.rate > 0.05 ? "text-danger" : e.rate > 0 ? "text-warning" : "text-success"}`}>
                      {(e.rate * 100).toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
