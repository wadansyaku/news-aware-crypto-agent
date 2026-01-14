const $ = (id) => document.getElementById(id);

const currency = new Intl.NumberFormat("ja-JP", {
  style: "currency",
  currency: "JPY",
  maximumFractionDigits: 0,
});
const number = new Intl.NumberFormat("ja-JP", { maximumFractionDigits: 4 });
const percent = new Intl.NumberFormat("ja-JP", {
  style: "percent",
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

const state = {
  config: null,
  equity: [],
  backtestEquity: [],
  backtestTrades: [],
  backtestMetrics: null,
  newsItems: [],
  newsTimeline: [],
  auditLogs: [],
  lastPrice: null,
  exposure: 0,
  watchlist: [],
  alerts: [],
  portfolio: null,
  runner: null,
};

function formatIso(value) {
  if (!value) return "-";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString("ja-JP");
}

function formatDateInput(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function formatDuration(startIso) {
  const start = new Date(startIso);
  if (Number.isNaN(start.getTime())) return "-";
  const diffMs = Date.now() - start.getTime();
  const hours = Math.floor(diffMs / 36e5);
  if (hours < 24) return `${hours}h`;
  const days = Math.floor(hours / 24);
  const rem = hours % 24;
  return `${days}d ${rem}h`;
}

function updatePriceDirection(currentPrice) {
  const node = $("pos-current");
  node.classList.remove("price-up", "price-down");
  if (state.lastPrice === null) {
    state.lastPrice = currentPrice;
    return;
  }
  if (currentPrice > state.lastPrice) {
    node.classList.add("price-up");
  } else if (currentPrice < state.lastPrice) {
    node.classList.add("price-down");
  }
  state.lastPrice = currentPrice;
}

function updateCompareBar(avgPrice, currentPrice) {
  const bar = $("pos-compare");
  if (!avgPrice || !currentPrice) {
    bar.style.width = "50%";
    return;
  }
  const ratio = currentPrice / avgPrice;
  const width = Math.max(10, Math.min(90, 50 * ratio));
  bar.style.width = `${width}%`;
}

function formatAlertCondition(condition, threshold) {
  const displayThreshold = condition === "change_pct" ? `${threshold}%` : currency.format(threshold);
  if (condition === "above") return `価格が${displayThreshold}以上`;
  if (condition === "below") return `価格が${displayThreshold}以下`;
  return `変動率が${displayThreshold}以上`;
}

function updateNotificationStatus() {
  const node = $("notify-status");
  if (!node) return;
  if (!("Notification" in window)) {
    node.textContent = "通知: 非対応";
    return;
  }
  if (Notification.permission === "granted") {
    node.textContent = "通知: 有効";
  } else if (Notification.permission === "denied") {
    node.textContent = "通知: ブロック中";
  } else {
    node.textContent = "通知: 未許可";
  }
}

async function requestNotificationPermission() {
  if (!("Notification" in window)) {
    updateNotificationStatus();
    return;
  }
  await Notification.requestPermission();
  updateNotificationStatus();
}

function updateAlertSymbolOptions() {
  const select = $("alert-symbol");
  if (!select || !state.config?.symbols?.length) return;
  const current = select.value;
  select.innerHTML = state.config.symbols
    .map((sym) => `<option value="${sym}">${sym}</option>`)
    .join("");
  if (current && state.config.symbols.includes(current)) {
    select.value = current;
  }
}

function updateAlertHint() {
  const condition = $("alert-condition")?.value;
  const hint = $("alert-hint");
  const threshold = $("alert-threshold");
  if (!hint || !threshold) return;
  if (condition === "change_pct") {
    hint.textContent = "変動率は % 指定です（例: 1.5）";
    threshold.placeholder = "例: 1.5";
    threshold.step = "0.1";
  } else {
    hint.textContent = "価格は JPY で指定します";
    threshold.placeholder = "例: 4200000";
    threshold.step = "1";
  }
}

function openAlertModal() {
  const modal = $("alert-modal");
  if (!modal) return;
  modal.classList.add("show");
  modal.setAttribute("aria-hidden", "false");
  updateAlertHint();
}

function closeAlertModal() {
  const modal = $("alert-modal");
  if (!modal) return;
  modal.classList.remove("show");
  modal.setAttribute("aria-hidden", "true");
}

function openSafetyModal() {
  const modal = $("safety-modal");
  if (!modal) return;
  const cfg = state.config || {};
  const risk = cfg.risk || {};
  $("safety-mode").value = cfg.mode || "paper";
  $("safety-dry-run").value = String(cfg.dry_run ?? true);
  $("safety-require-approval").checked = Boolean(cfg.require_approval);
  $("safety-kill-switch").checked = Boolean(cfg.kill_switch);
  $("safety-autopilot").checked = Boolean(cfg.autopilot_enabled);
  $("safety-live-ack").checked = Boolean(cfg.i_understand_live_trading);
  $("safety-cooldown-minutes").value = Number(risk.cooldown_minutes ?? 0);
  $("safety-cooldown-bypass").value = Number((risk.cooldown_bypass_pct ?? 0) * 100);
  $("safety-daily-loss").value = Number(risk.max_loss_jpy_per_day ?? 0);
  modal.classList.add("show");
  modal.setAttribute("aria-hidden", "false");
}

function closeSafetyModal() {
  const modal = $("safety-modal");
  if (!modal) return;
  modal.classList.remove("show");
  modal.setAttribute("aria-hidden", "true");
}

async function handleSafetySubmit(event) {
  event.preventDefault();
  const cooldownMinutes = Number($("safety-cooldown-minutes").value);
  const bypassPct = Number($("safety-cooldown-bypass").value);
  const dailyLoss = Number($("safety-daily-loss").value);
  const payload = {
    mode: $("safety-mode").value,
    dry_run: $("safety-dry-run").value === "true",
    require_approval: $("safety-require-approval").checked,
    kill_switch: $("safety-kill-switch").checked,
    autopilot_enabled: $("safety-autopilot").checked,
    i_understand_live_trading: $("safety-live-ack").checked,
    cooldown_minutes: Number.isNaN(cooldownMinutes) ? null : cooldownMinutes,
    cooldown_bypass_pct: Number.isNaN(bypassPct) ? null : bypassPct / 100,
    max_loss_jpy_per_day: Number.isNaN(dailyLoss) ? null : dailyLoss,
  };
  try {
    await apiRequest("/api/config/safety", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    await loadStatus();
    closeSafetyModal();
  } catch (err) {
    alert(`安全設定の保存に失敗: ${err.message}`);
  }
}

function renderRunnerState(data) {
  const stateNode = $("runner-state");
  const marketNode = $("runner-market-at");
  const newsNode = $("runner-news-at");
  const proposeNode = $("runner-propose-at");
  const startBtn = $("runner-start");
  const stopBtn = $("runner-stop");
  if (!stateNode || !marketNode || !newsNode || !proposeNode) return;
  if (!data || !data.exists) {
    stateNode.textContent = "未起動";
    marketNode.textContent = "-";
    newsNode.textContent = "-";
    proposeNode.textContent = "-";
    if (startBtn) startBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = true;
    return;
  }
  stateNode.textContent = data.running ? "稼働中" : "停止中";
  if (startBtn) startBtn.disabled = Boolean(data.running);
  if (stopBtn) stopBtn.disabled = !data.running;
  const state = data.state || {};
  marketNode.textContent = formatIso(state.last_success_ingest_market_at);
  newsNode.textContent = formatIso(state.last_success_ingest_news_at);
  proposeNode.textContent = formatIso(state.last_success_propose_at);
}

async function loadRunnerState() {
  const data = await apiRequest("/api/runner/state");
  state.runner = data;
  renderRunnerState(data);
}

async function startRunner() {
  const strategy = $("runner-strategy")?.value || "news_overlay";
  const mode = state.config?.mode || "paper";
  await apiRequest("/api/runner/start", {
    method: "POST",
    body: JSON.stringify({ strategy, mode }),
  });
  await loadRunnerState();
  await loadAudit();
}

async function stopRunner() {
  await apiRequest("/api/runner/stop", { method: "POST", body: JSON.stringify({}) });
  await loadRunnerState();
  await loadAudit();
}

function drawGauge(canvasId, value) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth || 160;
  const height = canvas.clientHeight || 90;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  const centerX = width / 2;
  const centerY = height * 0.9;
  const radius = Math.min(width, height) * 0.45;
  ctx.lineWidth = 10;
  ctx.strokeStyle = "rgba(15, 118, 110, 0.15)";
  ctx.beginPath();
  ctx.arc(centerX, centerY, radius, Math.PI, 0);
  ctx.stroke();

  const clamped = Math.max(0, Math.min(1, value));
  ctx.strokeStyle = clamped > 0.7 ? "#b42318" : "#0f766e";
  ctx.beginPath();
  ctx.arc(centerX, centerY, radius, Math.PI, Math.PI + Math.PI * clamped);
  ctx.stroke();
}

function renderWatchlist(items) {
  const tbody = $("watchlist-table")?.querySelector("tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  items.forEach((item) => {
    const row = document.createElement("tr");
    const change = item.change_pct;
    const changeClass = change > 0 ? "price-up" : change < 0 ? "price-down" : "";
    row.innerHTML = `
      <td>${item.symbol}</td>
      <td>${item.price ? currency.format(item.price) : "-"}</td>
      <td class="${changeClass}">${change !== null && change !== undefined ? percent.format(change) : "-"}</td>
      <td>${item.ts ? formatIso(item.ts) : "-"}</td>
    `;
    tbody.appendChild(row);
  });
}

function renderAlerts(alerts) {
  const container = $("alerts-list");
  if (!container) return;
  if (!alerts.length) {
    container.innerHTML = "<div class=\"alert-item\">まだアラートがありません</div>";
    return;
  }
  container.innerHTML = alerts
    .map((alert) => {
      const conditionText = formatAlertCondition(alert.condition, alert.threshold);
      const status = alert.enabled ? "有効" : "トリガー済み";
      const triggeredAt = alert.triggered_at ? formatIso(alert.triggered_at) : "-";
      const currentPrice =
        alert.current_price !== undefined && alert.current_price !== null
          ? currency.format(alert.current_price)
          : "-";
      const changeText =
        alert.change_pct !== undefined && alert.change_pct !== null
          ? percent.format(alert.change_pct)
          : "-";
      return `
        <div class="alert-item">
          <div><strong>${alert.symbol}</strong> ${conditionText}</div>
          <div class="alert-meta">
            <span>現在: ${currentPrice}</span>
            <span>変動: ${changeText}</span>
          </div>
          <div class="alert-meta">
            <span class="alert-status">${status}</span>
            <span>発火: ${triggeredAt}</span>
          </div>
          <div class="alert-actions">
            <button class="btn btn-ghost" data-alert-delete="${alert.id}">削除</button>
          </div>
        </div>
      `;
    })
    .join("");
  container.querySelectorAll("[data-alert-delete]").forEach((button) => {
    button.addEventListener("click", async () => {
      const alertId = Number(button.dataset.alertDelete || 0);
      if (!alertId) return;
      await deleteAlert(alertId);
    });
  });
}

function notifyAlerts(alerts) {
  if (!("Notification" in window)) return;
  if (Notification.permission !== "granted") return;
  alerts.forEach((alert) => {
    const title = `${alert.symbol} アラート`;
    const conditionText = formatAlertCondition(alert.condition, alert.threshold);
    const priceText =
      alert.current_price !== undefined && alert.current_price !== null
        ? `現在 ${currency.format(alert.current_price)}`
        : "";
    const changeText =
      alert.change_pct !== undefined && alert.change_pct !== null
        ? `変動 ${percent.format(alert.change_pct)}`
        : "";
    const body = [conditionText, priceText, changeText].filter(Boolean).join(" / ");
    new Notification(title, { body });
  });
}

async function loadWatchlist() {
  const data = await apiRequest("/api/watchlist");
  state.watchlist = data.items || [];
  renderWatchlist(state.watchlist);
}

async function loadAlerts(check = false) {
  const data = await apiRequest(`/api/alerts${check ? "?check=true" : ""}`);
  state.alerts = data.alerts || [];
  renderAlerts(state.alerts);
  if (check && data.triggered && data.triggered.length) {
    notifyAlerts(data.triggered);
  }
}

async function deleteAlert(alertId) {
  const output = $("alert-output");
  output.textContent = "削除中...";
  try {
    await apiRequest(`/api/alerts/${alertId}`, { method: "DELETE" });
    setOutput(output, "アラート削除", [`ID: ${alertId}`], null, "ok");
    await loadAlerts();
  } catch (err) {
    setOutput(output, "削除失敗", [err.message], null, "error");
  }
}

async function handleAlertSubmit(event) {
  event.preventDefault();
  const output = $("alert-output");
  output.textContent = "アラート追加中...";
  try {
    const payload = {
      symbol: $("alert-symbol").value,
      condition: $("alert-condition").value,
      threshold: Number($("alert-threshold").value),
    };
    if (!payload.symbol) throw new Error("シンボルを選択してください");
    if (!payload.threshold || Number.isNaN(payload.threshold)) {
      throw new Error("しきい値を入力してください");
    }
    const data = await apiRequest("/api/alerts", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setOutput(
      output,
      "アラート追加",
      [`${data.symbol}: ${formatAlertCondition(data.condition, data.threshold)}`],
      data,
      "ok"
    );
    closeAlertModal();
    $("alert-threshold").value = "";
    await loadAlerts();
  } catch (err) {
    setOutput(output, "追加失敗", [err.message], null, "error");
  }
}

function setOutput(el, title, lines = [], details = null, tone = "info") {
  const badge = tone === "error" ? "⚠" : tone === "ok" ? "✓" : "";
  const textLines = lines.map((line) => `<div>${line}</div>`).join("");
  const detailBlock = details
    ? `<details><summary>詳細</summary><pre>${JSON.stringify(details, null, 2)}</pre></details>`
    : "";
  el.innerHTML = `<strong>${badge} ${title}</strong>${textLines}${detailBlock}`;
}

async function apiRequest(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json")
    ? await response.json()
    : await response.text();
  if (!response.ok) {
    const message = payload?.message || payload?.detail || payload || "エラー";
    throw new Error(message);
  }
  return payload;
}

async function loadStatus() {
  const data = await apiRequest("/api/status");
  state.config = data.config;

  $("status-exchange").textContent = `取引所: ${data.exchange.ok ? "OK" : "NG"} ${data.exchange.message}`;
  $("status-news").textContent = `ニュース: ${data.news.ok ? "OK" : "NG"} ${data.news.message}`;
  $("status-db").textContent = `DB: ${data.db_path}`;

  $("cfg-exchange").textContent = data.config.exchange;
  $("cfg-symbols").textContent = data.config.symbols.join(", ");
  $("cfg-timeframes").textContent = data.config.timeframes.join(", ");

  $("cfg-mode").textContent = data.config.mode;
  $("cfg-approval").textContent = data.config.require_approval ? "必須" : "不要";
  $("cfg-autopilot").textContent = data.config.autopilot_enabled ? "ON" : "OFF";
  $("cfg-kill").textContent = data.config.kill_switch ? "ON" : "OFF";
  $("cfg-daily-loss").textContent = currency.format(data.config.risk.max_loss_jpy_per_day);

  $("pos-symbol").textContent = data.config.symbols[0] || "-";
  const defaultSymbol = data.config.symbols[0] || "";
  if (!$("ingest-symbol").value) $("ingest-symbol").value = defaultSymbol;
  if (!$("propose-symbol").value) $("propose-symbol").value = defaultSymbol;
  if (!$("backtest-symbol").value) $("backtest-symbol").value = defaultSymbol;
  updateAlertSymbolOptions();
  await loadPosition();
  await loadPortfolio();
  await loadIntents();
}

async function loadPosition() {
  if (!state.config?.symbols?.length) return;
  const symbol = state.config.symbols[0];
  const data = await apiRequest(`/api/position/overview?symbol=${encodeURIComponent(symbol)}`);
  $("pos-size").textContent = number.format(data.size);
  $("pos-avg").textContent = data.avg_price ? currency.format(data.avg_price) : "-";
  $("pos-current").textContent = data.current_price ? currency.format(data.current_price) : "-";
  $("pos-unrealized").textContent = currency.format(data.unrealized_pnl || 0);
  $("pos-return").textContent = percent.format(data.return_pct || 0);
  $("pos-hold").textContent = data.position_opened_at
    ? formatDuration(data.position_opened_at)
    : "-";
  $("pos-price-ts").textContent = data.current_price_ts
    ? formatIso(new Date(data.current_price_ts))
    : "-";

  updatePriceDirection(data.current_price || 0);
  updateCompareBar(data.avg_price || 0, data.current_price || 0);
  state.exposure = data.exposure_pct || 0;
  drawGauge("risk-gauge", state.exposure);
  $("pos-exposure").textContent = percent.format(state.exposure || 0);
  $("pos-message").textContent = "";
}

function renderIntentsPreview(intents) {
  const container = $("intents-preview");
  if (!intents.length) {
    container.innerHTML = "<div class=\"mini-item\">まだ提案がありません</div>";
    return;
  }
  container.innerHTML = intents
    .slice(0, 3)
    .map(
      (intent) => `<div class="mini-item">
        <span>${formatIso(intent.created_at)}</span>
        <span>${intent.symbol} ${intent.side} ${number.format(intent.size)}</span>
        <span>${intent.status}</span>
      </div>`
    )
    .join("");
}

async function loadIntents() {
  const data = await apiRequest("/api/intents?limit=10");
  const intents = data.intents || [];
  renderIntentsPreview(intents);

  const tbody = $("intents-table").querySelector("tbody");
  tbody.innerHTML = "";
  intents.forEach((intent) => {
    const row = document.createElement("tr");
    row.dataset.intentId = intent.intent_id;
    row.innerHTML = `
      <td>${formatIso(intent.created_at)}</td>
      <td>${intent.symbol}</td>
      <td>${intent.side}</td>
      <td>${number.format(intent.size)}</td>
      <td>${currency.format(intent.price)}</td>
      <td>${intent.status}</td>
    `;
    row.addEventListener("click", () => {
      $("approve-intent").value = intent.intent_id;
      $("execute-intent").value = intent.intent_id;
    });
    tbody.appendChild(row);
  });
}

function getSelectedAuditEvents() {
  const chips = Array.from(document.querySelectorAll("#audit-event-chips input"));
  return chips.filter((input) => input.checked).map((input) => input.value);
}

function buildAuditQuery() {
  const params = new URLSearchParams();
  const events = getSelectedAuditEvents();
  if (events.length) params.set("events", events.join(","));
  const start = $("audit-start").value;
  const end = $("audit-end").value;
  const intent = $("audit-intent").value.trim();
  if (start) params.set("start", `${start}T00:00:00+00:00`);
  if (end) params.set("end", `${end}T23:59:59+00:00`);
  if (intent) params.set("intent_id", intent);
  return params.toString();
}

async function loadAudit() {
  const query = buildAuditQuery();
  const data = await apiRequest(`/api/audit${query ? `?${query}` : ""}`);
  state.auditLogs = data.logs || [];
  renderAuditTimeline(state.auditLogs);
  renderAuditList(state.auditLogs);
}

function renderAuditList(logs) {
  const list = $("audit-list");
  if (!logs.length) {
    list.innerHTML = "<div class=\"audit-item\">ログはまだありません</div>";
    return;
  }
  list.innerHTML = logs
    .map((log) => {
      let summary = "";
      if (log.event === "risk_check") {
        summary = `理由: ${log.data.reason || "-"}`;
      } else if (log.event === "execute") {
        summary = `結果: ${log.data.status || "-"}`;
      } else if (log.event === "config_update") {
        const updates = log.data?.updates || {};
        const labels = [];
        if ("mode" in updates) labels.push(`mode=${updates.mode}`);
        if ("dry_run" in updates) labels.push(`dry_run=${updates.dry_run}`);
        if ("require_approval" in updates) labels.push(`approval=${updates.require_approval}`);
        if ("kill_switch" in updates) labels.push(`kill_switch=${updates.kill_switch}`);
        if ("autopilot_enabled" in updates) labels.push(`autopilot=${updates.autopilot_enabled}`);
        if ("i_understand_live_trading" in updates)
          labels.push(`live_ack=${updates.i_understand_live_trading}`);
        if ("cooldown_minutes" in updates) labels.push(`cooldown=${updates.cooldown_minutes}m`);
        if ("cooldown_bypass_pct" in updates)
          labels.push(`bypass=${(updates.cooldown_bypass_pct * 100).toFixed(1)}%`);
        if ("max_loss_jpy_per_day" in updates)
          labels.push(`daily_loss=${currency.format(updates.max_loss_jpy_per_day)}`);
        summary = labels.length ? `安全設定: ${labels.join(", ")}` : "安全設定の更新";
      } else if (log.event === "runner_start") {
        summary = `開始: ${log.data.strategy || "-"} (${log.data.mode || "-"})`;
      } else if (log.event === "runner_stop") {
        summary = "停止";
      }
      return `
        <div class="audit-item">
          <h4>${formatIso(log.ts)} / ${log.event}</h4>
          <div>${summary}</div>
          <pre>${JSON.stringify(log.data, null, 2)}</pre>
        </div>
      `;
    })
    .join("");
}

function renderAuditTimeline(logs) {
  const timeline = $("audit-timeline");
  if (!logs.length) {
    timeline.innerHTML = "";
    return;
  }
  const items = logs.slice(0, 12).map((log) => {
    let badge = "propose";
    if (log.event === "approve") badge = "approve";
    if (log.event === "execute") badge = "execute";
    if (log.event === "risk_check" && log.data.status === "rejected") badge = "risk";
    if (log.event === "config_update") badge = "config";
    if (log.event === "runner_start" || log.event === "runner_stop") badge = "runner";
    return `
      <div class="timeline-item">
        <div>
          <div><strong>${log.event}</strong></div>
          <div class="label">${formatIso(log.ts)}</div>
        </div>
        <span class="badge ${badge}">${log.event}</span>
      </div>
    `;
  });
  timeline.innerHTML = items.join("");
}

function renderMetrics(metrics) {
  const grid = $("metrics-grid");
  if (!metrics) {
    grid.innerHTML = "";
    return;
  }
  const items = [
    { label: "総PnL", value: currency.format(metrics.total_pnl || 0) },
    { label: "総リターン", value: percent.format(metrics.total_return || 0) },
    { label: "CAGR", value: percent.format(metrics.cagr || 0) },
    { label: "Sharpe", value: number.format(metrics.sharpe || 0) },
    { label: "最大DD", value: currency.format(metrics.max_drawdown || 0) },
    { label: "勝率", value: percent.format(metrics.win_rate || 0) },
    { label: "Profit Factor", value: number.format(metrics.profit_factor || 0) },
    { label: "売買回転", value: currency.format(metrics.turnover || 0) },
    { label: "手数料", value: currency.format(metrics.fees || 0) },
    { label: "トレード数", value: number.format(metrics.num_trades || 0) },
  ];
  grid.innerHTML = items
    .map(
      (item) => `<div class="metric"><span class="label">${item.label}</span><strong>${item.value}</strong></div>`
    )
    .join("");
}

function renderTrades(trades) {
  const tbody = $("trades-table").querySelector("tbody");
  tbody.innerHTML = "";
  trades.forEach((trade) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${formatIso(trade.created_at)}</td>
      <td>${trade.mode}</td>
      <td>${trade.symbol}</td>
      <td>${trade.side}</td>
      <td>${number.format(trade.size)}</td>
      <td>${currency.format(trade.price)}</td>
      <td>${currency.format(trade.pnl_jpy)}</td>
    `;
    tbody.appendChild(row);
  });
}

function renderBacktestMetrics(metrics) {
  state.backtestMetrics = metrics;
  const grid = $("backtest-metrics");
  if (!metrics) {
    grid.innerHTML = "";
    return;
  }
  const items = [
    { label: "Total PnL", value: currency.format(metrics.total_pnl || 0) },
    { label: "CAGR", value: percent.format(metrics.cagr || 0) },
    { label: "Sharpe", value: number.format(metrics.sharpe || 0) },
    { label: "Max DD", value: currency.format(metrics.max_drawdown || 0) },
    { label: "Win Rate", value: percent.format(metrics.win_rate || 0) },
    { label: "Profit Factor", value: number.format(metrics.profit_factor || 0) },
  ];
  grid.innerHTML = items
    .map(
      (item) => `<div class="metric"><span class="label">${item.label}</span><strong>${item.value}</strong></div>`
    )
    .join("");
}

function renderBacktestHeatmap(trades) {
  const container = $("backtest-heatmap");
  if (!trades.length) {
    container.innerHTML = "<div class=\"heatmap-cell\">データなし</div>";
    return;
  }
  const monthly = {};
  trades.forEach((trade) => {
    const ts = trade.created_at;
    if (!ts) return;
    const month = ts.slice(0, 7);
    monthly[month] = (monthly[month] || 0) + (trade.pnl_jpy || 0);
  });
  const months = Object.keys(monthly).sort();
  const recent = months.slice(-12);
  const values = recent.map((m) => monthly[m]);
  const maxAbs = Math.max(...values.map((v) => Math.abs(v)), 1);
  container.innerHTML = recent
    .map((month) => {
      const value = monthly[month];
      const intensity = Math.min(Math.abs(value) / maxAbs, 1);
      const color = value >= 0
        ? `rgba(15, 118, 110, ${0.1 + intensity * 0.5})`
        : `rgba(180, 35, 24, ${0.1 + intensity * 0.5})`;
      return `
        <div class="heatmap-cell" style="background:${color}">
          <strong>${month}</strong>
          <small>${currency.format(value)}</small>
        </div>
      `;
    })
    .join("");
}

function getFilteredBacktestTrades() {
  const keyword = ($("backtest-filter").value || "").toLowerCase();
  let trades = [...state.backtestTrades];
  if (keyword) {
    trades = trades.filter((trade) => {
      return (
        String(trade.created_at).toLowerCase().includes(keyword)
        || String(trade.side).toLowerCase().includes(keyword)
        || String(trade.symbol).toLowerCase().includes(keyword)
        || String(trade.pnl_jpy).toLowerCase().includes(keyword)
      );
    });
  }
  const sort = $("backtest-sort").value;
  trades.sort((a, b) => {
    if (sort === "date_asc") return String(a.created_at).localeCompare(String(b.created_at));
    if (sort === "date_desc") return String(b.created_at).localeCompare(String(a.created_at));
    if (sort === "pnl_asc") return (a.pnl_jpy || 0) - (b.pnl_jpy || 0);
    if (sort === "pnl_desc") return (b.pnl_jpy || 0) - (a.pnl_jpy || 0);
    return 0;
  });
  return trades;
}

function renderBacktestTrades() {
  const tbody = $("backtest-trades-table").querySelector("tbody");
  tbody.innerHTML = "";
  const trades = getFilteredBacktestTrades();
  if (!trades.length) {
    const row = document.createElement("tr");
    row.innerHTML = "<td colspan=\"6\">取引がありません</td>";
    tbody.appendChild(row);
    return;
  }
  trades.forEach((trade) => {
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${formatIso(trade.created_at)}</td>
      <td>${trade.symbol || "-"}</td>
      <td>${trade.side || "-"}</td>
      <td>${number.format(trade.size || 0)}</td>
      <td>${currency.format(trade.price || 0)}</td>
      <td>${currency.format(trade.pnl_jpy || 0)}</td>
    `;
    tbody.appendChild(row);
  });
}

function renderBacktestHistory(results) {
  const container = $("backtest-history");
  if (!results.length) {
    container.innerHTML = "<div class=\"backtest-item\">履歴はまだありません</div>";
    return;
  }
  container.innerHTML = results
    .map((item) => {
      const metrics = item.metrics || {};
      const label = metrics.strategy ? `(${metrics.strategy})` : "";
      return `
        <div class="backtest-item">
          <span>${item.period} ${label}</span>
          <span>${currency.format(metrics.total_pnl || 0)}</span>
        </div>
      `;
    })
    .join("");
}

async function loadBacktestResults() {
  const data = await apiRequest("/api/backtest/results?limit=8");
  renderBacktestHistory(data.results || []);
}

async function handleBacktest(event) {
  event.preventDefault();
  const output = $("backtest-output");
  output.textContent = "バックテスト実行中...";
  try {
    if (!$("backtest-start").value || !$("backtest-end").value) {
      throw new Error("開始日と終了日を入力してください");
    }
    const payload = {
      start: $("backtest-start").value,
      end: $("backtest-end").value,
      strategy: $("backtest-strategy").value,
      symbol: $("backtest-symbol").value || null,
    };
    const data = await apiRequest("/api/backtest", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.backtestEquity = data.equity || [];
    state.backtestTrades = data.trades || [];
    renderBacktestMetrics(data.metrics || {});
    drawEquityWithDrawdown("backtest-equity-chart", state.backtestEquity);
    renderBacktestHeatmap(state.backtestTrades);
    renderBacktestTrades();
    setOutput(output, "バックテスト完了", [
      `Summary: ${data.summary_txt}`,
      `Equity CSV: ${data.equity_csv}`,
    ], data, "ok");
    await loadBacktestResults();
  } catch (err) {
    setOutput(output, "バックテスト失敗", [err.message], null, "error");
  }
}

function renderNewsList(items) {
  const container = $("news-list");
  if (!items.length) {
    container.innerHTML = "<div class=\"news-item\">ニュースがありません</div>";
    return;
  }
  container.innerHTML = items
    .map((item) => {
      const sentiment = item.sentiment || 0;
      const cls = sentiment > 0.1 ? "positive" : sentiment < -0.1 ? "negative" : "neutral";
      return `
        <div class="news-item">
          <a href="${item.url}" target="_blank" rel="noreferrer">${item.title}</a>
          <div class="news-meta">
            <span>${item.source}</span>
            <span class="sentiment-pill ${cls}">${sentiment.toFixed(2)}</span>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderKeywordCloud(items) {
  const cloud = $("news-keywords");
  const counts = {};
  items.forEach((item) => {
    const flags = item.keyword_flags || {};
    Object.entries(flags).forEach(([key, value]) => {
      if (value) counts[key] = (counts[key] || 0) + 1;
    });
  });
  const keywords = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 20);
  if (!keywords.length) {
    cloud.innerHTML = "<span class=\"label\">キーワードはまだありません</span>";
    return;
  }
  const max = Math.max(...keywords.map(([, count]) => count));
  cloud.innerHTML = keywords
    .map(([key, count]) => {
      const size = 12 + (count / max) * 10;
      return `<span class="tag" style="font-size:${size}px">${key}</span>`;
    })
    .join("");
}

async function loadNewsInsights() {
  const [newsData, timelineData] = await Promise.all([
    apiRequest("/api/news?limit=50"),
    apiRequest("/api/news/sentiment_timeline?hours=24"),
  ]);
  state.newsItems = newsData.items || [];
  state.newsTimeline = timelineData.timeline || [];
  renderNewsList(state.newsItems);
  renderKeywordCloud(state.newsItems);
  drawSentimentTimeline("news-sentiment-chart", state.newsTimeline);
}

async function loadAuditSummary() {
  const data = await apiRequest("/api/audit/summary");
  renderAuditSummary(data);
}

function renderAuditSummary(summary) {
  const container = $("audit-summary");
  container.innerHTML = `
    <div class="summary-card">
      <span class="label">承認率</span>
      <strong>${percent.format(summary.approval_rate || 0)}</strong>
    </div>
    <div class="summary-card">
      <span class="label">承認</span>
      <strong>${summary.approved || 0}</strong>
    </div>
    <div class="summary-card">
      <span class="label">拒否</span>
      <strong>${summary.rejected || 0}</strong>
    </div>
    <div class="summary-card">
      <span class="label">拒否理由TOP3</span>
      <strong>${(summary.top_reasons || []).map((r) => `${r.reason} (${r.count})`).join(", ") || "なし"}</strong>
    </div>
    <div class="summary-card">
      <span class="label">拒否理由の内訳</span>
      <canvas id="audit-reason-chart" height="120"></canvas>
    </div>
  `;
  drawPie("audit-reason-chart", summary.rejection_reasons || {});
}

function exportAuditJson() {
  const blob = new Blob([JSON.stringify(state.auditLogs, null, 2)], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "audit_logs.json";
  link.click();
  URL.revokeObjectURL(url);
}

function exportAuditCsv() {
  const rows = [
    ["ts", "event", "data"],
    ...state.auditLogs.map((log) => [log.ts, log.event, JSON.stringify(log.data)]),
  ];
  const csv = rows.map((row) => row.map((cell) => `"${String(cell).replace(/"/g, "\"\"")}"`).join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "audit_logs.csv";
  link.click();
  URL.revokeObjectURL(url);
}

function drawChart(series) {
  const canvas = $("equity-chart");
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#faf7f1";
  ctx.fillRect(0, 0, width, height);

  if (!series.length) {
    ctx.fillStyle = "#6f6a61";
    ctx.font = "12px Hiragino Sans";
    ctx.fillText("まだデータがありません", 16, 24);
    return;
  }

  const min = Math.min(...series);
  const max = Math.max(...series);
  const span = max - min || 1;
  const padding = 18;

  ctx.strokeStyle = "rgba(15, 118, 110, 0.15)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, height - padding);
  ctx.lineTo(width - padding, height - padding);
  ctx.stroke();

  ctx.strokeStyle = "#0f766e";
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((value, idx) => {
    const x = padding + (idx / (series.length - 1 || 1)) * (width - padding * 2);
    const y = height - padding - ((value - min) / span) * (height - padding * 2);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function drawEquityWithDrawdown(canvasId, series) {
  const canvas = $(canvasId);
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#faf7f1";
  ctx.fillRect(0, 0, width, height);

  if (!series.length) {
    ctx.fillStyle = "#6f6a61";
    ctx.font = "12px Hiragino Sans";
    ctx.fillText("バックテスト結果がありません", 16, 24);
    return;
  }

  const peaks = [];
  let peak = series[0];
  for (const value of series) {
    peak = Math.max(peak, value);
    peaks.push(peak);
  }
  const min = Math.min(...series);
  const max = Math.max(...peaks);
  const span = max - min || 1;
  const padding = 18;

  const toPoint = (value, idx, total) => {
    const x = padding + (idx / (total - 1 || 1)) * (width - padding * 2);
    const y = height - padding - ((value - min) / span) * (height - padding * 2);
    return { x, y };
  };

  ctx.strokeStyle = "rgba(15, 118, 110, 0.15)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, padding);
  ctx.lineTo(padding, height - padding);
  ctx.lineTo(width - padding, height - padding);
  ctx.stroke();

  ctx.beginPath();
  peaks.forEach((value, idx) => {
    const point = toPoint(value, idx, peaks.length);
    if (idx === 0) ctx.moveTo(point.x, point.y);
    else ctx.lineTo(point.x, point.y);
  });
  for (let idx = series.length - 1; idx >= 0; idx -= 1) {
    const point = toPoint(series[idx], idx, series.length);
    ctx.lineTo(point.x, point.y);
  }
  ctx.closePath();
  ctx.fillStyle = "rgba(180, 35, 24, 0.18)";
  ctx.fill();

  ctx.strokeStyle = "#0f766e";
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((value, idx) => {
    const point = toPoint(value, idx, series.length);
    if (idx === 0) ctx.moveTo(point.x, point.y);
    else ctx.lineTo(point.x, point.y);
  });
  ctx.stroke();
}

function drawSentimentTimeline(canvasId, series) {
  const canvas = $(canvasId);
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  ctx.fillStyle = "#faf7f1";
  ctx.fillRect(0, 0, width, height);

  if (!series.length) {
    ctx.fillStyle = "#6f6a61";
    ctx.font = "12px Hiragino Sans";
    ctx.fillText("ニュースがまだありません", 16, 24);
    return;
  }

  const values = series.map((p) => p.avg_sentiment);
  const min = Math.min(...values, -1);
  const max = Math.max(...values, 1);
  const span = max - min || 1;
  const padding = 18;

  ctx.strokeStyle = "rgba(15, 118, 110, 0.15)";
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(padding, height / 2);
  ctx.lineTo(width - padding, height / 2);
  ctx.stroke();

  ctx.strokeStyle = "#2563eb";
  ctx.lineWidth = 2;
  ctx.beginPath();
  series.forEach((point, idx) => {
    const x = padding + (idx / (series.length - 1 || 1)) * (width - padding * 2);
    const y = height - padding - ((point.avg_sentiment - min) / span) * (height - padding * 2);
    if (idx === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  });
  ctx.stroke();
}

function drawPie(canvasId, dataMap) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  const entries = Object.entries(dataMap || {});
  const total = entries.reduce((sum, [, value]) => sum + value, 0);
  if (!total) {
    ctx.fillStyle = "#6f6a61";
    ctx.font = "12px Hiragino Sans";
    ctx.fillText("データなし", 10, 20);
    return;
  }
  const colors = ["#0f766e", "#f59e0b", "#2563eb", "#b42318", "#6f6a61"];
  let startAngle = -Math.PI / 2;
  entries.forEach(([_, value], idx) => {
    const slice = (value / total) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(width / 2, height / 2);
    ctx.arc(width / 2, height / 2, Math.min(width, height) / 2 - 4, startAngle, startAngle + slice);
    ctx.closePath();
    ctx.fillStyle = colors[idx % colors.length];
    ctx.fill();
    startAngle += slice;
  });
}

function drawPortfolioPie(canvasId, positions, totalValue) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  const width = canvas.clientWidth;
  const height = canvas.clientHeight;
  const dpr = window.devicePixelRatio || 1;
  canvas.width = width * dpr;
  canvas.height = height * dpr;
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  if (!positions.length || totalValue <= 0) {
    ctx.fillStyle = "#6f6a61";
    ctx.font = "12px Hiragino Sans";
    ctx.fillText("データなし", 10, 20);
    return;
  }

  const colors = ["#0f766e", "#f59e0b", "#2563eb", "#b42318", "#6f6a61", "#0ea5e9"];
  let startAngle = -Math.PI / 2;
  positions.forEach((pos, idx) => {
    const value = Math.max(0, pos.position_value || 0);
    if (!value) return;
    const slice = (value / totalValue) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(width / 2, height / 2);
    ctx.arc(width / 2, height / 2, Math.min(width, height) / 2 - 6, startAngle, startAngle + slice);
    ctx.closePath();
    ctx.fillStyle = colors[idx % colors.length];
    ctx.fill();
    startAngle += slice;
  });

  const legend = $("portfolio-legend");
  if (legend) {
    legend.innerHTML = positions
      .map((pos, idx) => {
        const value = Math.max(0, pos.position_value || 0);
        if (!value) return "";
        const allocation = totalValue > 0 ? value / totalValue : 0;
        return `<span class="legend-item"><span class="legend-swatch" style="background:${colors[idx % colors.length]}"></span>${pos.symbol} ${percent.format(allocation)}</span>`;
      })
      .join("");
  }
}

function renderPortfolio(data) {
  const positions = data?.positions || [];
  const totalValue = Number(data?.total_value || 0);
  const totalPnl = Number(data?.total_pnl || 0);
  const totalNode = $("portfolio-total");
  const pnlNode = $("portfolio-pnl");
  if (totalNode) totalNode.textContent = currency.format(totalValue || 0);
  if (pnlNode) pnlNode.textContent = currency.format(totalPnl || 0);

  const tbody = $("portfolio-table")?.querySelector("tbody");
  if (!tbody) return;
  tbody.innerHTML = "";
  if (!positions.length) {
    tbody.innerHTML = "<tr><td colspan=\"7\">ポジションがありません</td></tr>";
    drawPortfolioPie("portfolio-chart", [], totalValue);
    const legend = $("portfolio-legend");
    if (legend) legend.textContent = "";
    return;
  }

  positions.forEach((pos) => {
    const value = Number(pos.position_value || 0);
    const allocation = totalValue > 0 ? value / totalValue : 0;
    const row = document.createElement("tr");
    row.innerHTML = `
      <td>${pos.symbol}</td>
      <td>${number.format(pos.size || 0)}</td>
      <td>${pos.avg_price ? currency.format(pos.avg_price) : "-"}</td>
      <td>${pos.current_price ? currency.format(pos.current_price) : "-"}</td>
      <td>${currency.format(value || 0)}</td>
      <td>${currency.format(pos.unrealized_pnl || 0)}</td>
      <td>${percent.format(allocation || 0)}</td>
    `;
    tbody.appendChild(row);
  });

  drawPortfolioPie("portfolio-chart", positions, totalValue);
}

async function loadPortfolio() {
  const data = await apiRequest("/api/portfolio");
  state.portfolio = data;
  renderPortfolio(data);
}

async function loadAnalytics(mode = "") {
  const query = mode ? `?mode=${encodeURIComponent(mode)}` : "";
  const data = await apiRequest(`/api/analytics${query}`);
  state.equity = data.equity || [];
  renderMetrics(data.metrics);
  drawChart(state.equity);
  renderTrades(data.trades || []);
}

async function handleIngest(event) {
  event.preventDefault();
  const output = $("ingest-output");
  output.textContent = "取り込み中...";
  try {
    const payload = {
      symbol: $("ingest-symbol").value || null,
      orderbook: $("ingest-orderbook").checked,
      news_only: $("ingest-news-only").checked,
      features_only: $("ingest-features-only").checked,
    };
    const data = await apiRequest("/api/ingest", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setOutput(output, "取り込み完了", [
      `Candles: ${data.candles}`,
      `News inserted: ${data.news.inserted ?? 0}`,
      `Features added: ${data.features_added}`,
    ], data, "ok");
    await loadStatus();
    await loadWatchlist();
    await loadAlerts();
    await loadNewsInsights();
    await loadAuditSummary();
    await loadAudit();
  } catch (err) {
    setOutput(output, "取り込み失敗", [err.message], null, "error");
  }
}

async function handlePropose(event) {
  event.preventDefault();
  const output = $("propose-output");
  output.textContent = "提案生成中...";
  try {
    const payload = {
      symbol: $("propose-symbol").value || null,
      strategy: $("propose-strategy").value,
      mode: $("propose-mode").value,
      refresh: $("propose-refresh").checked,
    };
    const data = await apiRequest("/api/propose", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (data.status && data.status !== "proposed") {
      setOutput(output, "提案なし", [`理由: ${data.reason}`], data, "error");
      return;
    }
    setOutput(
      output,
      "提案作成済み",
      [
        `Intent: ${data.intent_id}`,
        `Side: ${data.side} / Size: ${number.format(data.size)}`,
        `Price: ${currency.format(data.price)}`,
        `Confidence: ${percent.format(data.confidence)}`,
      ],
      data,
      "ok"
    );
    $("approve-intent").value = data.intent_id;
    $("execute-intent").value = data.intent_id;
    await loadIntents();
    await loadAuditSummary();
    await loadAudit();
  } catch (err) {
    setOutput(output, "提案失敗", [err.message], null, "error");
  }
}

async function handleApprove(event) {
  event.preventDefault();
  const output = $("approve-output");
  output.textContent = "承認中...";
  const action = event.submitter?.dataset.action || "approve";
  try {
    const intentId = $("approve-intent").value.trim();
    const phraseInput = $("approve-phrase");
    let phrase = phraseInput ? phraseInput.value.trim() : "";
    if (!phrase) phrase = state.config?.approval_phrase || "";
    if (action === "approve-execute") {
      const payload = {
        intent_id: intentId,
        phrase,
        mode: $("approve-execute-mode").value,
      };
      const data = await apiRequest("/api/approve_execute", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setOutput(
        output,
        "承認して実行",
        [
          `Intent: ${data.approval.intent_id}`,
          `Execution: ${data.execution.status} (${data.execution.message})`,
        ],
        data,
        data.execution.status === "filled" ? "ok" : "info"
      );
      await loadPosition();
      await loadPortfolio();
      await loadAnalytics($("report-mode").value);
      await loadAuditSummary();
    } else {
      const payload = { intent_id: intentId, phrase };
      const data = await apiRequest("/api/approve", {
        method: "POST",
        body: JSON.stringify(payload),
      });
      setOutput(output, "承認完了", [`Intent: ${data.intent_id}`], data, "ok");
      await loadAuditSummary();
    }
    await loadIntents();
    await loadAudit();
  } catch (err) {
    setOutput(output, "承認失敗", [err.message], null, "error");
  }
}

async function handleExecute(event) {
  event.preventDefault();
  const output = $("execute-output");
  output.textContent = "実行中...";
  try {
    const payload = {
      intent_id: $("execute-intent").value.trim() || null,
      mode: $("execute-mode").value,
    };
    const data = await apiRequest("/api/execute", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setOutput(output, "実行結果", [`${data.status}: ${data.message}`], data, data.status === "filled" ? "ok" : "info");
    await loadPosition();
    await loadPortfolio();
    await loadAnalytics($("report-mode").value);
    await loadAuditSummary();
    await loadAudit();
  } catch (err) {
    setOutput(output, "実行失敗", [err.message], null, "error");
  }
}

async function handleClosePosition() {
  if (!state.config?.symbols?.length) return;
  if (!confirm("全ポジションを解消する提案を作成します。よろしいですか？")) return;
  const output = $("pos-message");
  output.textContent = "解消提案を作成中...";
  try {
    const payload = {
      symbol: state.config.symbols[0],
      mode: "paper",
    };
    const data = await apiRequest("/api/position/close", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    if (data.status === "rejected") {
      output.textContent = `却下: ${data.reason}`;
      return;
    }
    output.textContent = `解消提案を作成しました: ${data.intent_id}`;
    $("approve-intent").value = data.intent_id;
    $("execute-intent").value = data.intent_id;
    await loadIntents();
  } catch (err) {
    output.textContent = `失敗: ${err.message}`;
  }
}

async function handleReport(event) {
  event.preventDefault();
  const output = $("report-output");
  output.textContent = "レポート生成中...";
  try {
    const payload = {
      mode: $("report-mode").value || null,
    };
    const data = await apiRequest("/api/report", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    setOutput(output, "レポート生成完了", [
      `Summary: ${data.paths.summary}`,
      `Equity CSV: ${data.paths.csv}`,
      `Trades CSV: ${data.paths.trades}`,
    ], data, "ok");
    state.equity = data.equity || [];
    renderMetrics(data.metrics);
    drawChart(state.equity);
    renderTrades(data.trades || []);
  } catch (err) {
    setOutput(output, "レポート生成失敗", [err.message], null, "error");
  }
}

async function handleQuickIngest() {
  $("ingest-output").textContent = "取り込み中...";
  try {
    const data = await apiRequest("/api/ingest", { method: "POST", body: JSON.stringify({}) });
    setOutput($("ingest-output"), "取り込み完了", [
      `Candles: ${data.candles}`,
      `News inserted: ${data.news.inserted ?? 0}`,
      `Features added: ${data.features_added}`,
    ], data, "ok");
    await loadStatus();
    await loadWatchlist();
    await loadAlerts();
    await loadNewsInsights();
    await loadAuditSummary();
    await loadAudit();
  } catch (err) {
    setOutput($("ingest-output"), "取り込み失敗", [err.message], null, "error");
  }
}

async function init() {
  document.body.classList.add("ready");
  try {
    await loadStatus();
  } catch (err) {
    $("status-exchange").textContent = `取引所: エラー (${err.message})`;
    $("status-news").textContent = "ニュース: -";
    $("status-db").textContent = "DB: -";
    return;
  }
  updateNotificationStatus();
  try {
    await loadWatchlist();
    await loadAlerts();
  } catch (err) {
    const output = $("alert-output");
    if (output) output.textContent = `ウォッチリスト取得失敗: ${err.message}`;
  }
  try {
    await loadRunnerState();
  } catch (err) {
    const runner = $("runner-state");
    if (runner) runner.textContent = `取得失敗: ${err.message}`;
  }
  try {
    await loadAnalytics();
  } catch (err) {
    $("metrics-grid").textContent = `レポート取得失敗: ${err.message}`;
  }
  try {
    await loadBacktestResults();
  } catch (err) {
    $("backtest-history").textContent = `履歴取得失敗: ${err.message}`;
  }
  try {
    await loadNewsInsights();
  } catch (err) {
    $("news-list").textContent = `ニュース取得失敗: ${err.message}`;
  }
  try {
    await loadAuditSummary();
    await loadAudit();
  } catch (err) {
    $("audit-list").textContent = `監査ログ取得失敗: ${err.message}`;
  }

  const end = new Date();
  const start = new Date();
  start.setDate(end.getDate() - 30);
  $("backtest-start").value = formatDateInput(start);
  $("backtest-end").value = formatDateInput(end);

  $("refresh-status").addEventListener("click", async () => {
    try {
      await loadStatus();
      await loadWatchlist();
      await loadAlerts();
      await loadRunnerState();
      await loadNewsInsights();
    } catch (err) {
      $("status-exchange").textContent = `取引所: エラー (${err.message})`;
    }
  });
  const watchlistRefresh = $("watchlist-refresh");
  if (watchlistRefresh) {
    watchlistRefresh.addEventListener("click", async () => {
      await loadWatchlist();
      await loadAlerts();
    });
  }
  const openAlert = $("open-alert-modal");
  if (openAlert) openAlert.addEventListener("click", openAlertModal);
  const enableNotify = $("enable-notify");
  if (enableNotify) enableNotify.addEventListener("click", requestNotificationPermission);
  const alertForm = $("alert-form");
  if (alertForm) alertForm.addEventListener("submit", handleAlertSubmit);
  const alertCondition = $("alert-condition");
  if (alertCondition) alertCondition.addEventListener("change", updateAlertHint);
  const alertModal = $("alert-modal");
  if (alertModal) {
    alertModal.addEventListener("click", (event) => {
      const target = event.target;
      if (target?.dataset?.close) closeAlertModal();
    });
  }
  const openSafety = $("open-safety-modal");
  if (openSafety) openSafety.addEventListener("click", openSafetyModal);
  const safetyForm = $("safety-form");
  if (safetyForm) safetyForm.addEventListener("submit", handleSafetySubmit);
  const safetyModal = $("safety-modal");
  if (safetyModal) {
    safetyModal.addEventListener("click", (event) => {
      const target = event.target;
      if (target?.dataset?.close) closeSafetyModal();
    });
  }
  const runnerStart = $("runner-start");
  if (runnerStart) {
    runnerStart.addEventListener("click", async () => {
      try {
        await startRunner();
      } catch (err) {
        alert(`Runner起動に失敗: ${err.message}`);
      }
    });
  }
  const runnerStop = $("runner-stop");
  if (runnerStop) {
    runnerStop.addEventListener("click", async () => {
      try {
        await stopRunner();
      } catch (err) {
        alert(`Runner停止に失敗: ${err.message}`);
      }
    });
  }
  $("quick-ingest").addEventListener("click", handleQuickIngest);
  $("ingest-form").addEventListener("submit", handleIngest);
  $("propose-form").addEventListener("submit", handlePropose);
  $("approve-form").addEventListener("submit", handleApprove);
  $("execute-form").addEventListener("submit", handleExecute);
  $("close-position").addEventListener("click", handleClosePosition);
  $("report-form").addEventListener("submit", handleReport);
  $("refresh-analytics").addEventListener("click", async () => {
    await loadAnalytics($("report-mode").value);
  });
  $("backtest-form").addEventListener("submit", handleBacktest);
  $("refresh-backtest-results").addEventListener("click", loadBacktestResults);
  $("backtest-filter").addEventListener("input", renderBacktestTrades);
  $("backtest-sort").addEventListener("change", renderBacktestTrades);
  $("refresh-audit").addEventListener("click", async () => {
    await loadAuditSummary();
    await loadAudit();
  });
  $("export-audit-json").addEventListener("click", exportAuditJson);
  $("export-audit-csv").addEventListener("click", exportAuditCsv);
  document.querySelectorAll("#audit-event-chips input").forEach((input) => {
    input.addEventListener("change", loadAudit);
  });
  $("audit-start").addEventListener("change", loadAudit);
  $("audit-end").addEventListener("change", loadAudit);
  $("audit-intent").addEventListener("input", loadAudit);
  $("show-phrase").addEventListener("click", () => {
    const phrase = state.config?.approval_phrase || "未設定";
    $("cfg-phrase").textContent = phrase;
  });
  window.addEventListener("resize", () => {
    drawChart(state.equity || []);
    drawEquityWithDrawdown("backtest-equity-chart", state.backtestEquity || []);
    drawSentimentTimeline("news-sentiment-chart", state.newsTimeline || []);
    drawGauge("risk-gauge", state.exposure || 0);
    if (state.portfolio) {
      drawPortfolioPie(
        "portfolio-chart",
        state.portfolio.positions || [],
        Number(state.portfolio.total_value || 0)
      );
    }
  });

  setInterval(() => {
    loadPosition().catch(() => {});
  }, 10000);

  setInterval(() => {
    loadWatchlist().catch(() => {});
    loadPortfolio().catch(() => {});
    loadAlerts(true).catch(() => {});
    loadRunnerState().catch(() => {});
  }, 15000);
}

window.addEventListener("DOMContentLoaded", init);
