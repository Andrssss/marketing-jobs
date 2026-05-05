import React, { useEffect, useMemo, useRef, useState } from "react";
import "./MarketingJobs.css";

const API = "/.netlify/functions/marketing-jobs";
const SYNC_API = "/.netlify/functions/sync-data";

const VISITOR_COOKIE_NAME = "marketingVisitorId";

const readCookie = (name) => {
  const cookieName = `${name}=`;
  const parts = document.cookie.split(";");
  for (const part of parts) {
    const item = part.trim();
    if (item.startsWith(cookieName)) {
      return decodeURIComponent(item.slice(cookieName.length));
    }
  }
  return "";
};

const writeCookie = (name, value, days) => {
  const expires = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toUTCString();
  document.cookie = `${name}=${encodeURIComponent(value)}; expires=${expires}; path=/; SameSite=Lax`;
};

const createVisitorId = () => {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 12)}`;
};

const getOrCreateVisitorId = () => {
  const existing = readCookie(VISITOR_COOKIE_NAME);
  if (existing) return existing;
  const nextId = createVisitorId();
  writeCookie(VISITOR_COOKIE_NAME, nextId, 365 * 2);
  return nextId;
};

const CATEGORIES = [
  { key: "marketing", label: "Marketing", pattern: /marketing|social media|seo|sem|\bcontent\b|brand|digital|kampány|kampany|kommunikáci/i },
  { key: "sales", label: "Sales", pattern: /sales|értékesít|ertekesit|account|business develop|üzletfejleszt|kereskedelm/i },
  { key: "admin", label: "Admin/Asszisztens", pattern: /asszisztens|assistant|\badmin/i },
  { key: "office", label: "Irodai", pattern: /irodai|office|recepci|titkár|titkar/i },
  { key: "manager", label: "Menedzser", pattern: /manager|menedzser|vezető|vezeto|\blead\b|head of|igazgató|igazgato|director/i },
  { key: "analytics", label: "Analitika/Data", pattern: /analiti|analyst|\bdata\b|elemz/i },
  { key: "customer", label: "Ügyfélszolg.", pattern: /ügyfél|ugyfel|customer|call center/i },
  { key: "hr", label: "HR", pattern: /\bhr\b|human resource|toborzó|toborzo|recrui/i },
  { key: "finance", label: "Pénzügy", pattern: /pénzügy|penzugy|finance|könyvelő|konyvelo|számvitel|szamvitel|controller|bérszámfejt|payroll/i },
  { key: "project", label: "Projekt", pattern: /projekt|project/i },
];

const hoursSince = (iso) => {
  const ms = Date.now() - new Date(iso).getTime();
  return ms / (1000 * 60 * 60);
};

const CLICKED_KEYS_STORAGE = "marketingClickedKeys";
const APPLIED_KEYS_STORAGE = "marketingAppliedKeys";

const loadClickedKeys = () => {
  try {
    const raw = localStorage.getItem(CLICKED_KEYS_STORAGE);
    return new Set(raw ? JSON.parse(raw) : []);
  } catch {
    return new Set();
  }
};

const saveClickedKey = (key) => {
  try {
    const raw = localStorage.getItem(CLICKED_KEYS_STORAGE);
    const arr = raw ? JSON.parse(raw) : [];
    if (!arr.includes(key)) {
      arr.push(key);
      localStorage.setItem(CLICKED_KEYS_STORAGE, JSON.stringify(arr));
    }
  } catch {
    // silent
  }
};

const loadAppliedKeys = () => {
  try {
    const raw = localStorage.getItem(APPLIED_KEYS_STORAGE);
    return new Set(raw ? JSON.parse(raw) : []);
  } catch {
    return new Set();
  }
};

const saveAppliedKeys = (set) => {
  try {
    localStorage.setItem(APPLIED_KEYS_STORAGE, JSON.stringify([...set]));
  } catch {
    // silent
  }
};

const APPLIED_CACHE_STORAGE = "marketingAppliedCache";

const loadAppliedCache = () => {
  try {
    const raw = localStorage.getItem(APPLIED_CACHE_STORAGE);
    return raw ? JSON.parse(raw) : {};
  } catch {
    return {};
  }
};

const saveAppliedCache = (cache) => {
  try {
    localStorage.setItem(APPLIED_CACHE_STORAGE, JSON.stringify(cache));
  } catch {
    // silent
  }
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

  const [catStates, setCatStates] = useState(() => {
    const saved = localStorage.getItem("marketingCatStates");
    return saved ? JSON.parse(saved) : {};
  });

  const [catsOpen, setCatsOpen] = useState(() => {
    const saved = localStorage.getItem("marketingCatsOpen");
    return saved !== null ? saved === "true" : true;
  });

  const [sourcesOpen, setSourcesOpen] = useState(() => {
    const saved = localStorage.getItem("marketingSourcesOpen");
    return saved !== null ? saved === "true" : true;
  });

  const [clickedKeys, setClickedKeys] = useState(() => loadClickedKeys());
  const [appliedKeys, setAppliedKeys] = useState(() => loadAppliedKeys());
  const [showAppliedOnly, setShowAppliedOnly] = useState(false);
  const [appliedCache, setAppliedCache] = useState(() => loadAppliedCache());

  const [syncOpen, setSyncOpen] = useState(false);
  const [syncIdShown, setSyncIdShown] = useState(false);
  const [syncStatus, setSyncStatus] = useState("");
  const [importId, setImportId] = useState("");
  const myVisitorId = useMemo(() => getOrCreateVisitorId(), []);

  const handleSyncUpload = async () => {
    setSyncStatus("Feltöltés…");
    try {
      const res = await fetch(SYNC_API, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          visitorId: myVisitorId,
          data: {
            clicked: [...clickedKeys],
            applied: [...appliedKeys],
            appliedCache,
          },
        }),
      });
      if (!res.ok) throw new Error();
      setSyncStatus("✓ Feltöltve!");
    } catch {
      setSyncStatus("✗ Hiba történt.");
    }
  };

  const handleSyncDownload = async () => {
    if (!importId.trim()) return;
    setSyncStatus("Letöltés…");
    try {
      const res = await fetch(`${SYNC_API}?visitorId=${encodeURIComponent(importId.trim())}`);
      if (!res.ok) throw new Error();
      const { data } = await res.json();
      if (!data) { setSyncStatus("Nem találtam adatot."); return; }
      if (Array.isArray(data.clicked)) {
        const merged = new Set([...clickedKeys, ...data.clicked]);
        setClickedKeys(merged);
        localStorage.setItem(CLICKED_KEYS_STORAGE, JSON.stringify([...merged]));
      }
      if (Array.isArray(data.applied)) {
        const merged = new Set([...appliedKeys, ...data.applied]);
        setAppliedKeys(merged);
        saveAppliedKeys(merged);
      }
      if (data.appliedCache && typeof data.appliedCache === "object") {
        const merged = { ...data.appliedCache, ...appliedCache };
        setAppliedCache(merged);
        saveAppliedCache(merged);
      }
      setSyncStatus("✓ Összefésülve!");
    } catch {
      setSyncStatus("✗ Hiba történt.");
    }
  };

  const handleCopySyncId = async () => {
    try {
      await navigator.clipboard.writeText(myVisitorId);
      setSyncStatus("✓ ID másolva!");
    } catch {
      setSyncStatus("✗ Másolás sikertelen.");
    }
  };

  const trackClick = (target) => {
    setClickedKeys((prev) => {
      const next = new Set(prev);
      next.add(target);
      return next;
    });
    saveClickedKey(target);
  };

  const toggleApplied = (key, job) => {
    setAppliedKeys((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
        setAppliedCache((c) => {
          const { [key]: _, ...rest } = c;
          saveAppliedCache(rest);
          return rest;
        });
      } else {
        next.add(key);
        if (job) {
          setAppliedCache((c) => {
            const updated = { ...c, [key]: job };
            saveAppliedCache(updated);
            return updated;
          });
        }
      }
      saveAppliedKeys(next);
      return next;
    });
  };

  const longPressTimerRef = React.useRef(null);
  const startLongPress = (target) => {
    clearTimeout(longPressTimerRef.current);
    longPressTimerRef.current = setTimeout(() => trackClick(target), 500);
  };
  const cancelLongPress = () => clearTimeout(longPressTimerRef.current);

  /* Fetch */
  const fetchSources = async () => {
    try {
      const res = await fetch(`${API}/sources`);
      if (!res.ok) throw new Error();
      const data = await res.json();
      setSources(data);

      // Clean up stale source states that no longer exist
      const validKeys = new Set(data.map((s) => s.source));
      setSourceStates((prev) => {
        const cleaned = {};
        let changed = false;
        for (const [k, v] of Object.entries(prev)) {
          if (validKeys.has(k)) {
            cleaned[k] = v;
          } else {
            changed = true;
          }
        }
        if (changed) {
          localStorage.setItem("marketingSourceStates", JSON.stringify(cleaned));
        }
        return changed ? cleaned : prev;
      });
    } catch {
      setSources([]);
    }
  };

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams({ limit: "2000" });
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

  /* Category toggle (3-state) */
  const handleCatClick = (key) => {
    setCatStates((prev) => {
      const current = prev[key] || "neutral";
      const next =
        current === "neutral"
          ? "selected"
          : current === "selected"
          ? "excluded"
          : "neutral";
      const updated = { ...prev, [key]: next };
      localStorage.setItem("marketingCatStates", JSON.stringify(updated));
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

    /* Category filter */
    const selCats = Object.keys(catStates).filter((k) => catStates[k] === "selected");
    const exCats = Object.keys(catStates).filter((k) => catStates[k] === "excluded");

    const matchesCat = (title, catKey) => {
      const cat = CATEGORIES.find((c) => c.key === catKey);
      return cat ? cat.pattern.test(title || "") : false;
    };

    if (selCats.length) {
      list = list.filter((j) => selCats.some((ck) => matchesCat(j.title, ck)));
    } else if (exCats.length) {
      list = list.filter((j) => !exCats.some((ck) => matchesCat(j.title, ck)));
    }

    if (showAppliedOnly) {
      const cachedJobs = Object.values(appliedCache);
      const apiKeys = new Set(list.map((j) => `job:${j.source}:${j.title}`));
      const onlyCached = cachedJobs.filter((j) => !apiKeys.has(`job:${j.source}:${j.title}`) && appliedKeys.has(`job:${j.source}:${j.title}`));
      list = [...list.filter((j) => appliedKeys.has(`job:${j.source}:${j.title}`)), ...onlyCached];
    }

    return [...list].sort(
      (a, b) => new Date(b.firstSeen || 0) - new Date(a.firstSeen || 0)
    );
  }, [jobs, q, time24h, time7d, sourceStates, catStates, showAppliedOnly, appliedKeys, appliedCache]);

  const activeTimeLabel = time7d ? "1 hét" : time24h ? "24h" : "mind";

  return (
    <div className="mkt-app">
      <header className="mkt-header">
        <div>
          <h1>📋 Marketing & irodai állások</h1>
          <input
            className="mkt-search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Keresés…"
            style={{ width: "100%", marginTop: 8 }}
          />
        </div>

        <div className="mkt-actions">
          <a href="/filters" className="mkt-btn" style={{ textDecoration: "none" }}>⚙ Filters</a>

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

          <button
            className={`mkt-btn mkt-btn--toggle${showAppliedOnly ? " active" : ""}`}
            onClick={() => setShowAppliedOnly((v) => !v)}
          >
            {showAppliedOnly ? `✓ Jelentkezések (${appliedKeys.size})` : `Jelentkezések (${appliedKeys.size})`}
          </button>
          <button className="mkt-btn" onClick={fetchJobs}>
            Frissítés
          </button>
        </div>
      </header>

      {/* Sync */}
      <div className="mkt-sync">
        <button
          className="mkt-sources-toggle"
          onClick={() => setSyncOpen((v) => !v)}
        >
          {syncOpen ? "▲ Szinkron elrejtése" : "🔄 Szinkron eszközök között"}
        </button>
        {syncOpen && (
          <div className="mkt-sync-panel">
            <div className="mkt-sync-section">
              <strong>A te szinkron ID-d:</strong>
              <code className="mkt-sync-id">
                {syncIdShown ? myVisitorId : "•••••••• (rejtett)"}
              </code>
              <button className="mkt-btn mkt-btn--toggle" onClick={() => setSyncIdShown((v) => !v)}>
                {syncIdShown ? "Elrejtés" : "Mutatás"}
              </button>
              <button className="mkt-btn mkt-btn--toggle" onClick={handleCopySyncId}>📋 Másolás</button>
              <button className="mkt-btn" onClick={handleSyncUpload}>⬆ Feltöltés</button>
            </div>
            <div className="mkt-sync-section">
              <strong>Importálás másik eszközről:</strong>
              <input
                className="mkt-search"
                placeholder="Másik eszköz szinkron ID-ja"
                value={importId}
                onChange={(e) => setImportId(e.target.value)}
              />
              <button className="mkt-btn" onClick={handleSyncDownload}>⬇ Letöltés és összefésülés</button>
            </div>
            {syncStatus && <div className="mkt-sync-status">{syncStatus}</div>}
            <p className="mkt-sync-help">
              ⚠️ Az ID-t senkinek ne add ki — aki ismeri, le tudja tölteni a megnézett és jelentkezett állásaid listáját.
              Az importálás összefésüli az adatokat a meglevőkkel (nem felülírja).
            </p>
          </div>
        )}
      </div>

      {/* Categories */}
      <div className="mkt-sources-header">
        <button
          className="mkt-sources-toggle"
          onClick={() =>
            setCatsOpen((prev) => {
              localStorage.setItem("marketingCatsOpen", !prev);
              return !prev;
            })
          }
        >
          {catsOpen ? "▲ Kategóriák elrejtése" : "▼ Kategóriák"}
        </button>
      </div>

      <div className={`mkt-sources-wrapper ${catsOpen ? "open" : ""}`}>
        <div className="mkt-sources">
          {CATEGORIES.map((cat) => {
            const state = catStates[cat.key] || "neutral";
            let cls = "mkt-source-btn";
            if (state === "selected") cls += " selected";
            if (state === "excluded") cls += " excluded";
            const count = jobs.filter((j) => cat.pattern.test(j.title || "")).length;
            return (
              <button
                key={cat.key}
                className={cls}
                onClick={() => handleCatClick(cat.key)}
              >
                {cat.label}
                <span className="mkt-source-count">{count}</span>
              </button>
            );
          })}
        </div>
      </div>

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
          {sourcesOpen ? "▲ Források elrejtése" : "▼ Források"}
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
            const clickKey = `job:${job.source}:${job.title}`;
            const isVisited = clickedKeys.has(clickKey);
            const isApplied = appliedKeys.has(clickKey);

            return (
              <li key={key} className={`mkt-card${isVisited ? " mkt-card--visited" : ""}${isApplied ? " mkt-card--applied" : ""}`}>
                <div className="mkt-card-row">
                  <a
                    className="mkt-card-title"
                    href={job.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={() => trackClick(clickKey)}
                    onAuxClick={(e) => { if (e.button === 1) trackClick(clickKey); }}
                    onContextMenu={() => trackClick(clickKey)}
                    onTouchStart={() => startLongPress(clickKey)}
                    onTouchEnd={cancelLongPress}
                    onTouchMove={cancelLongPress}
                    onTouchCancel={cancelLongPress}
                  >
                    {job.title}
                  </a>
                  <span className="mkt-card-source">{job.source}</span>
                </div>

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
                  <button
                    className={`mkt-applied-btn${isApplied ? " applied" : ""}`}
                    onClick={() => toggleApplied(clickKey, job)}
                  >
                    {isApplied ? "✓ Jelentkeztem" : "Jelentkeztem?"}
                  </button>
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
