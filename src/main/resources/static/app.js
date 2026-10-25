(() => {
  let refreshing = false;
  let submitting = false;
  document.addEventListener("submit", (event) => {
    if (!event.target.matches(".retry-form")) return;
    submitting = true;
    const button = event.target.querySelector("button");
    button.disabled = true;
    button.textContent = "Queuing…";
  });
  async function refresh() {
    if (refreshing || submitting || document.hidden) return;
    if (document.activeElement?.matches("#data button, #data a, #data summary")) return;
    // Preserva seleções de texto e restaura os detalhes abertos após a atualização.
    if (window.getSelection()?.toString()) return;
    refreshing = true;
    try {
      const data = document.querySelector("#data").dataset;
      const query = new URLSearchParams({failedPage:data.failedPage, processedPage:data.processedPage});
      const response = await fetch("/dashboard/data?" + query,
        {headers: {"Accept": "text/html"}, cache: "no-store"});
      if (!response.ok) throw new Error("Refresh failed");
      const html = await response.text();
      if (submitting || document.activeElement?.matches("#data button, #data a, #data summary")) return;
      const next = new DOMParser().parseFromString(html, "text/html").querySelector("#data");
      if (!next) throw new Error("Unexpected response");
      const openIds = new Set(Array.from(document.querySelectorAll("#data details[open]"),
        (detail) => detail.dataset.eventId));
      next.querySelectorAll("details").forEach((detail) => {
        detail.open = openIds.has(detail.dataset.eventId);
      });
      document.querySelector("#data").replaceWith(next);
      document.querySelector("#refresh-status").textContent = "";
    } catch (error) {
      document.querySelector("#refresh-status").textContent =
        "Automatic refresh was interrupted. Another attempt will follow shortly.";
    } finally {
      refreshing = false;
    }
  }
  setInterval(refresh, 2000);
})();
