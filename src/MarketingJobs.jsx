import React, { useEffect, useMemo, useState } from "react";
import "./MarketingJobs.css";

const API = "/.netlify/functions/marketing-jobs";

const hoursSince = (iso) => {
  const ms = Date.now() - new Date(iso).getTime();
  return ms / (1000 * 60 * 60);
};



const MarketingJobs = () => {
  const [jobs, setJobs] = useState([]);
  const [sources, setSources] = useState([]);
  const [loading, setLoading] = useState(true);
  const [q, setQ] = useState("");

  const [sourceStates, setSourceStates] = useState(() => {
    const saved = localStorage.getItem("marketingSourceStates");
    return saved ? JSON.parse(saved) : {};
  });

  const [time24h, setTime24h] = useState(() => {
    const saved = localStorage.getItem("marketingTime24h");
    return saved === null ? true : saved === "true";
  });

  const [time7d, setTime7d] = useState(() => {
    const saved = localStorage.getItem("marketingTime7d");
    return saved === null ? false : saved === "true";
  });

  const [sourcesOpen, setSourcesOpen] = useState(() => {
    const saved = localStorage.getItem("marketingSourcesOpen");
    return saved !== null ? saved === "true" : true;
  });

  /* Fetch */
  const fetchSources = async () => {
    try {
      const res = await fetch(`${API}/sources`);
      if (!res.ok) throw new Error();
      setSources(await res.json());
    } catch {
      setSources([]);
    }
  };

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ limit: "300" });
      if (time7d) params.set("timeRange", "7d");
      else if (time24h) params.set("timeRange", "24h");

      const res = await fetch(`${API}?${params}`);
      if (!res.ok) throw new Error();
      setJobs(await res.json());
    } catch {
      setJobs([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSources();
  }, []);
  useEffect(() => {
    fetchJobs();
  }, [time24h, time7d]);

  /* Source toggle (3-state) */
  const handleSourceClick = (key) => {
    setSourceStates((prev) => {
      const current = prev[key] || "neutral";
      const next =
        current === "neutral"
          ? "selected"
          : current === "selected"
          ? "excluded"
          : "neutral";
      const updated = { ...prev, [key]: next };
      localStorage.setItem("marketingSourceStates", JSON.stringify(updated));
      return updated;
    });
  };

  /* Filtered list */
  const visibleJobs = useMemo(() => {
    let list = jobs;

    if (time24h && !time7d) {
      list = list.filter((j) => j.firstSeen && hoursSince(j.firstSeen) <= 24);
    } else if (time7d) {
      list = list.filter(
        (j) => j.firstSeen && hoursSince(j.firstSeen) <= 24 * 7
      );
    }

    const nq = q.trim().toLowerCase();
    if (nq) {
      list = list.filter((j) => (j.title || "").toLowerCase().includes(nq));
    }

    const selected = Object.keys(sourceStates).filter(
      (k) => sourceStates[k] === "selected"
    );
    const excluded = Object.keys(sourceStates).filter(
      (k) => sourceStates[k] === "excluded"
    );

    if (selected.length) {
      list = list.filter((j) => selected.includes(j.source));
    } else if (excluded.length) {
      list = list.filter((j) => !excluded.includes(j.source));
    }

    return [...list].sort(
      (a, b) => new Date(b.firstSeen || 0) - new Date(a.firstSeen || 0)
    );
  }, [jobs, q, time24h, time7d, sourceStates]);

  const activeTimeLabel = time7d ? "1 hét" : time24h ? "24h" : "mind";

  return (
    <div className="mkt-app">
      <header className="mkt-header">
        <div>
          <h1>📋 Marketing & irodai állások</h1>
          <p className="mkt-subtitle">
            Marketing, irodai asszisztens és adminisztratív pozíciók Budapesten.
            Óránként frissül. Duplikáció mentes.
          </p>
        </div>

        <div className="mkt-actions">
          <input
            className="mkt-search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Keresés…"
          />

          <label className="mkt-checkbox">
            <input
              type="checkbox"
              checked={time24h}
              onChange={(e) => {
                setTime24h(e.target.checked);
                localStorage.setItem(
                  "marketingTime24h",
                  String(e.target.checked)
                );
              }}
            />
            Csak 24h
          </label>

          <label className="mkt-checkbox">
            <input
              type="checkbox"
              checked={time7d}
              onChange={(e) => {
                setTime7d(e.target.checked);
                localStorage.setItem(
                  "marketingTime7d",
                  String(e.target.checked)
                );
              }}
            />
            Csak 1 hét
          </label>

          <button className="mkt-btn" onClick={fetchJobs}>
            Frissítés
          </button>
        </div>
      </header>

      {/* Sources */}
      <div className="mkt-sources-header">
        <button
          className="mkt-sources-toggle"
          onClick={() =>
            setSourcesOpen((prev) => {
              localStorage.setItem("marketingSourcesOpen", !prev);
              return !prev;
            })
          }
        >
          {sourcesOpen ? "▲ Források elrejtése" : "▼ Források kiválasztása"}
        </button>
      </div>

      <div className={`mkt-sources-wrapper ${sourcesOpen ? "open" : ""}`}>
        <div className="mkt-sources">
          {sources
            .filter((s) => s.count > 0)
            .map((s) => {
              const state = sourceStates[s.source] || "neutral";
              let cls = "mkt-source-btn";
              if (state === "selected") cls += " selected";
              if (state === "excluded") cls += " excluded";
              return (
                <button
                  key={s.source}
                  className={cls}
                  onClick={() => handleSourceClick(s.source)}
                >
                  {s.source}
                  <span className="mkt-source-count">{s.count}</span>
                </button>
              );
            })}
        </div>
      </div>

      {/* Status */}
      {!loading && (
        <div className="mkt-status">
          Időszűrő: {activeTimeLabel} · Találatok: {visibleJobs.length}
        </div>
      )}

      {/* Job list */}
      {loading ? (
        <div className="mkt-status">Betöltés.…</div>
      ) : visibleJobs.length === 0 ? (
        <div className="mkt-status">Nincs találat.</div>
      ) : (
        <ul className="mkt-list">
          {visibleJobs.map((job) => {
            const isNew = job.firstSeen && hoursSince(job.firstSeen) <= 1;
            const key = `${job.source}-${job.url || job.title}-${job.firstSeen}`;

            return (
              <li key={key} className="mkt-card">
                <div className="mkt-card-row">
                  <a
                    className="mkt-card-title"
                    href={job.url}
                    target="_blank"
                    rel="noopener noreferrer"
                  >
                    {job.title}
                  </a>
                  <span className="mkt-card-source">{job.source}</span>
                </div>

                {notes.length > 0 && (
                  <div className="mkt-note">
                    💭 Megjegyzés:
                    <ul>
                      {notes.map((n, i) => (
                        <li key={i}>{n}</li>
                      ))}
                    </ul>
                  </div>
                )}

                <div className="mkt-card-meta">
                  {isNew && <span className="mkt-badge-new">Új</span>}
                  {job.experience && (
                    <span className="mkt-experience">{job.experience}</span>
                  )}
                  <span>
                    {job.firstSeen
                      ? new Date(job.firstSeen).toLocaleString("hu-HU")
                      : "—"}
                  </span>
                </div>
              </li>
            );
          })}
        </ul>
      )}
    </div>
  );
};

export default MarketingJobs;
