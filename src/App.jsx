import React, { useState, useEffect } from "react";
import MarketingJobs from "./MarketingJobs";
import Filters from "./Filters";

function getPage() {
  return window.location.pathname === "/filters" ? "filters" : "home";
}

const App = () => {
  const [page, setPage] = useState(getPage);

  useEffect(() => {
    const onPop = () => setPage(getPage());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  return page === "filters" ? <Filters /> : <MarketingJobs />;
};

export default App;
