(function () {
  const THEME_KEY = "ledgershield_theme";
  const CUR_KEY = "ledgershield_currency";

  function readFx() {
    return parseFloat(document.documentElement.getAttribute("data-fx") || "83.5");
  }

  function getTheme() {
    return localStorage.getItem(THEME_KEY) || "dark";
  }
  function getCurrency() {
    return localStorage.getItem(CUR_KEY) || "USD";
  }

  function applyTheme(t) {
    document.documentElement.classList.remove("theme-dark", "theme-light");
    document.documentElement.classList.add(t === "light" ? "theme-light" : "theme-dark");
    localStorage.setItem(THEME_KEY, t === "light" ? "light" : "dark");
    document.querySelectorAll("[data-theme-toggle]").forEach((el) => {
      el.textContent = t === "light" ? "Dark mode" : "Light mode";
    });
  }

  function formatMoney(usd, cur) {
    const c = cur || getCurrency();
    if (c === "INR") {
      const v = usd * readFx();
      return "₹" + v.toLocaleString("en-IN", { maximumFractionDigits: 0 });
    }
    return (
      "$" +
      usd.toLocaleString("en-US", { maximumFractionDigits: 0 })
    );
  }

  function applyCurrencyToDom() {
    const cur = getCurrency();
    document.querySelectorAll("[data-usd]").forEach((el) => {
      const v = parseFloat(el.getAttribute("data-usd") || "0");
      el.textContent = formatMoney(v, cur);
    });
    document.querySelectorAll(".pill-group [data-cur]").forEach((b) => {
      b.classList.toggle("on", b.getAttribute("data-cur") === cur);
    });
  }

  function initSidebar() {
    const t = getTheme();
    applyTheme(t);
    applyCurrencyToDom();

    document.querySelectorAll("[data-theme-toggle]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const next = getTheme() === "dark" ? "light" : "dark";
        applyTheme(next);
      });
    });

    document.querySelectorAll(".pill-group [data-cur]").forEach((btn) => {
      btn.addEventListener("click", () => {
        localStorage.setItem(CUR_KEY, btn.getAttribute("data-cur") || "USD");
        applyCurrencyToDom();
      });
    });
  }

  /* Assistant drawer */
  function initAssistant() {
    const fab = document.getElementById("sparkle-fab");
    const drawer = document.getElementById("assistant-drawer");
    const close = document.getElementById("assistant-close");
    const form = document.getElementById("assistant-form");
    const input = document.getElementById("assistant-input");
    const msgs = document.getElementById("assistant-messages");

    if (!fab || !drawer) return;

    fab.addEventListener("click", () => drawer.classList.add("open"));
    close &&
      close.addEventListener("click", () => drawer.classList.remove("open"));

    function addMsg(text, who) {
      const d = document.createElement("div");
      d.className = "msg " + (who === "user" ? "user" : "bot");
      d.textContent = text;
      msgs.appendChild(d);
      msgs.scrollTop = msgs.scrollHeight;
    }

    form &&
      form.addEventListener("submit", async (e) => {
        e.preventDefault();
        const q = (input.value || "").trim();
        if (!q) return;
        input.value = "";
        addMsg(q, "user");
        try {
          const r = await fetch("/api/assistant", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: q }),
          });
          const j = await r.json();
          addMsg(j.reply || "(no response)", "bot");
        } catch {
          addMsg("Could not reach assistant endpoint.", "bot");
        }
      });
  }

  window.LedgerShield = {
    getTheme,
    getCurrency,
    formatMoney,
    applyCurrencyToDom,
    initSidebar,
    initAssistant,
    readFx,
  };

  const ONBOARD_KEY = "ledgershield_onboarding_v1";

  function initOnboarding() {
    const backdrop = document.getElementById("onboard-backdrop");
    if (!backdrop) return;
    const skip = () => {
      try {
        localStorage.setItem(ONBOARD_KEY, "1");
      } catch (_) {}
      backdrop.classList.remove("show");
    };
    try {
      if (localStorage.getItem(ONBOARD_KEY) === "1") return;
    } catch (_) {}
    backdrop.classList.add("show");
    backdrop.querySelectorAll("[data-onboard-close]").forEach((el) => {
      el.addEventListener("click", skip);
    });
    backdrop.querySelectorAll("[data-onboard-later]").forEach((el) => {
      el.addEventListener("click", () => backdrop.classList.remove("show"));
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    initSidebar();
    initAssistant();
    initOnboarding();
    if (typeof lucide !== "undefined" && lucide.createIcons) {
      lucide.createIcons();
    }
  });
})();
