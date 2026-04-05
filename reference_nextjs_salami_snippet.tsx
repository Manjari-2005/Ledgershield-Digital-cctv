/**
 * Reference Next.js 14+ (app router) + Tailwind + Lucide + Recharts — bank employee salami dashboard.
 * Not wired to this Flask app; copy into your Next project and point `fetch` to your Django API.
 *
 * npm i lucide-react recharts clsx tailwindcss
 */
/*
"use client";

import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { UploadCloud, Snowflake, RotateCcw, BellRing, Activity } from "lucide-react";

type Sink = {
  destination_id: string;
  micro_count: number;
  unique_sources_max_window: number;
  tripped: boolean;
  status: string;
};

export default function SalamiDashboard() {
  const [line, setLine] = useState<{ labels: string[]; counts: number[] }>({
    labels: [],
    counts: [],
  });
  const [sinks, setSinks] = useState<Sink[]>([]);

  useEffect(() => {
    fetch("/api/salami/merged")
      .then((r) => r.json())
      .then((d) => {
        setLine({
          labels: d.line_chart?.labels ?? [],
          counts: d.line_chart?.counts ?? [],
        });
        setSinks(d.sinks ?? []);
      });
  }, []);

  const chartData = line.labels.map((l, i) => ({ t: l, n: line.counts[i] ?? 0 }));

  return (
    <div className="space-y-6 p-6 bg-slate-950 text-slate-100 min-h-screen">
      <header>
        <h1 className="text-2xl font-bold flex items-center gap-2">
          <Activity className="text-violet-400" /> Salami slicing monitor
        </h1>
        <p className="text-slate-400 text-sm mt-1">
          Micro-transactions ≤ ₹10 · circuit breaker at 5k unique sources / 10 min
        </p>
      </header>

      <section className="rounded-2xl border border-slate-800 bg-slate-900/50 p-4">
        <h2 className="text-sm font-semibold text-slate-300 mb-3 flex items-center gap-2">
          <UploadCloud size={16} /> Upload CSV
        </h2>
        <form action="/api/salami/upload" method="post" encType="multipart/form-data">
          <input type="file" name="file" className="text-sm" />
        </form>
      </section>

      <section className="rounded-2xl border border-slate-800 bg-slate-900/50 p-4 h-72">
        <h2 className="text-sm font-semibold text-slate-300 mb-2">Micro-tx volume</h2>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData}>
            <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
            <XAxis dataKey="t" tick={{ fill: "#94a3b8", fontSize: 10 }} />
            <YAxis tick={{ fill: "#94a3b8" }} />
            <Tooltip contentStyle={{ background: "#0f172a", border: "1px solid #334155" }} />
            <Line type="monotone" dataKey="n" stroke="#8b5cf6" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </section>

      <section className="space-y-3">
        <h2 className="text-sm font-semibold flex items-center gap-2 text-slate-300">
          <BellRing size={16} /> Sink accounts
        </h2>
        {sinks.map((s) => (
          <div
            key={s.destination_id}
            className={`rounded-xl border p-4 ${
              s.tripped ? "border-red-500/50 bg-red-950/20" : "border-slate-800 bg-slate-900/40"
            }`}
          >
            <div className="flex justify-between gap-2">
              <code className="text-sm">{s.destination_id}</code>
              <span className="text-xs text-red-400">{s.status}</span>
            </div>
            <div className="mt-3 flex gap-2">
              <button className="inline-flex items-center gap-1 rounded-lg bg-red-600 px-3 py-1.5 text-sm">
                <Snowflake size={14} /> Freeze destination
              </button>
              <button className="inline-flex items-center gap-1 rounded-lg bg-violet-600 px-3 py-1.5 text-sm">
                <RotateCcw size={14} /> Auto-refund users
              </button>
            </div>
          </div>
        ))}
      </section>
    </div>
  );
}
*/

export {};
