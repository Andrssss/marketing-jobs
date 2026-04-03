import React, { useEffect, useState } from "react";
import "./MarketingJobs.css";

const API = "/.netlify/functions/filters";

const Filters = () => {
  const [filters, setFilters] = useState([]);
  const [loading, setLoading] = useState(true);
  const [newWord, setNewWord] = useState("");
  const [error, setError] = useState(null);

  const load = async () => {
    try {
      const res = await fetch(API);
      const data = await res.json();
      if (Array.isArray(data)) setFilters(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { load(); }, []);

  const add = async () => {
    const word = newWord.trim();
    if (!word) return;
    setError(null);
    try {
      const res = await fetch(API, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ word }),
      });
      const data = await res.json();
      if (!res.ok) { setError(data.error); return; }
      setFilters(prev => [...prev, data].sort((a, b) => a.word.localeCompare(b.word)));
      setNewWord("");
    } catch (e) {
      setError(e.message);
    }
  };

  const remove = async (id) => {
    setError(null);
    try {
      await fetch(API, {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id }),
      });
      setFilters(prev => prev.filter(f => f.id !== id));
    } catch (e) {
      setError(e.message);
    }
  };

  return (
    <div className="mkt-app">
      <div className="mkt-header">
        <div>
          <h1>Filters</h1>
          <p className="mkt-subtitle">
            Blacklist szavak kezelése — a cron job-ok innen olvassák.
            <a href="/" style={{ marginLeft: 16, color: "#9b8ec4" }}>← Vissza</a>
          </p>
        </div>
      </div>

      {error && <p style={{ color: "#ef4444", margin: "12px 0" }}>{error}</p>}

      <div className="filter-add-row">
        <input
          className="mkt-search"
          placeholder="Új szó..."
          value={newWord}
          onChange={e => setNewWord(e.target.value)}
          onKeyDown={e => e.key === "Enter" && add()}
        />
        <button className="mkt-btn" onClick={add}>Hozzáadás</button>
      </div>

      {loading ? (
        <p className="mkt-status">Betöltés…</p>
      ) : (
        <div className="filter-group">
          <h2 className="filter-group-title">Blacklist ({filters.length})</h2>
          <div className="filter-chips">
            {filters.map(f => (
              <span key={f.id} className="filter-chip">
                {f.word}
                <button
                  className="filter-chip-x"
                  onClick={() => remove(f.id)}
                  title="Törlés"
                >×</button>
              </span>
            ))}
            {filters.length === 0 && (
              <span style={{ color: "#666", fontSize: 14 }}>Nincs szó a listában.</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Filters;
