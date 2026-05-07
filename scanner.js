// scanner.js
// Multi-Scanner Hub for Web Serial barcode scanners.
// - Multiple scanners are tracked independently (own state machine + read loop)
// - Hot-plug (connect/disconnect) is handled robustly
// - One leader tab (via navigator.locks) reads all scanners
// - All tabs receive scans via BroadcastChannel
// - Errors on one scanner do not affect the others
//
// Usage:
//   import { ScannerHub } from "./scanner.js";
//   const hub = new ScannerHub();
//   hub.addEventListener("scan", e => console.log(e.detail));
//   await hub.start();
//   // user gesture (button) to authorize a new device:
//   await hub.requestPort();

const DEFAULTS = Object.freeze({
  baudRate: 9600,
  terminator: /\r?\n/,           // line terminator regex
  lockName: "barcode-scanner-leader",
  channelName: "barcode-scans",
  deliverOnlyWhenActive: true,    // only fire `scan` event when tab is visible+focused
  reopenBackoffMs: 500,           // delay before retrying a failed open/read
  reopenMaxBackoffMs: 5000,       // cap for exponential backoff
  heartbeatMs: 1000,
  tabId: null,                   // auto-generated if null
});

const STATES = Object.freeze({
  KNOWN: "known",       // discovered, not opened
  OPENING: "opening",
  OPEN: "open",
  CLOSING: "closing",
  CLOSED: "closed",
  ERROR: "error",
});

// Map of well-known scanner USB vendors -> human-readable label.
// Used purely for nicer log/display labels. Does NOT filter ports.
const VENDOR_LABELS = new Map([
  [0x05E0, "Symbol/Zebra"],
  [0x05F9, "PSC/Datalogic"],
  [0x1EAB, "Newland"],
  [0x0C2E, "Metrologic/Honeywell"],
  [0x0536, "HHP/Honeywell"],
  [0x080C, "Datalogic"],
  [0x065A, "Opticon"],
  [0x0A5F, "Zebra"],
  [0x0403, "FTDI"],
  [0x10C4, "Silicon Labs"],
  [0x067B, "Prolific"],
  [0x1A86, "WCH CH34x"],
]);

const hex4 = (n) =>
  typeof n === "number" ? "0x" + n.toString(16).padStart(4, "0").toUpperCase() : "?";

function buildScannerLabel(info) {
  const vendor = VENDOR_LABELS.get(info?.usbVendorId);
  const vid = hex4(info?.usbVendorId);
  const pid = hex4(info?.usbProductId);
  return vendor ? `${vendor} (${vid}/${pid})` : `Serial ${vid}/${pid}`;
}

// Stable per-tab id we can broadcast. We use vid:pid plus an index that
// disambiguates multiple identical scanners. The ordering of getPorts() is
// stable inside a session but not necessarily across tabs; that's fine –
// each tab still attributes its OWN scans correctly, and follower tabs just
// display whatever scanner-id the leader sent.
function buildScannerId(info, indexAmongSameModel) {
  const v = hex4(info?.usbVendorId).slice(2);
  const p = hex4(info?.usbProductId).slice(2);
  return `s-${v}-${p}-${indexAmongSameModel}`;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const isPageActive = () =>
  document.visibilityState === "visible" && document.hasFocus();

// ---------------------------------------------------------------------------
// ScannerHub
// ---------------------------------------------------------------------------
export class ScannerHub extends EventTarget {
  constructor(options = {}) {
    super();
    const cfg = { ...DEFAULTS, ...options };
    this.baudRate = cfg.baudRate;
    this.terminator = cfg.terminator;
    this.lockName = cfg.lockName;
    this.channelName = cfg.channelName;
    this.deliverOnlyWhenActive = cfg.deliverOnlyWhenActive;
    this.reopenBackoffMs = cfg.reopenBackoffMs;
    this.reopenMaxBackoffMs = cfg.reopenMaxBackoffMs;
    this.heartbeatMs = cfg.heartbeatMs;
    this.tabId = cfg.tabId || (crypto.randomUUID().slice(0, 8));

    /** @type {Map<string, ScannerEntry>} */
    this.scanners = new Map();

    this.started = false;
    this.isLeader = false;
    this.currentLeaderId = null;

    this._channel = null;
    this._lockAbortController = null;
    this._heartbeatTimer = null;
    this._leaderLoopRunning = false;

    // bound handlers for clean removal
    this._onSerialConnect = this._onSerialConnect.bind(this);
    this._onSerialDisconnect = this._onSerialDisconnect.bind(this);
    this._onChannelMessage = this._onChannelMessage.bind(this);
    this._onFocus = this._onFocusChange.bind(this);
    this._onBlur = this._onFocusChange.bind(this);
    this._onVisibility = this._onFocusChange.bind(this);
    this._onBeforeUnload = this._onBeforeUnload.bind(this);
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  /**
   * Start the hub: discovers known ports, joins leader election,
   * subscribes to serial connect/disconnect.
   */
  async start() {
    if (this.started) return;
    if (!("serial" in navigator)) {
      this._emit("log", { level: "error", message: "Web Serial wird nicht unterstützt." });
      throw new Error("Web Serial nicht unterstützt");
    }
    if (!("locks" in navigator)) {
      this._emit("log", { level: "error", message: "Web Locks API fehlt." });
      throw new Error("Web Locks nicht unterstützt");
    }
    this.started = true;

    this._channel = new BroadcastChannel(this.channelName);
    this._channel.addEventListener("message", this._onChannelMessage);

    navigator.serial.addEventListener("connect", this._onSerialConnect);
    navigator.serial.addEventListener("disconnect", this._onSerialDisconnect);

    window.addEventListener("focus", this._onFocus);
    window.addEventListener("blur", this._onBlur);
    document.addEventListener("visibilitychange", this._onVisibility);
    window.addEventListener("beforeunload", this._onBeforeUnload);

    await this._discoverKnownPorts();
    this._post({ type: "leader-who-is-there" });
    this._emit("tab-active", { active: isPageActive() });
    this._emit("log", { level: "info", message: `Hub gestartet (tab ${this.tabId}).` });

    // run leader election forever in the background
    this._runLeaderElection().catch((err) =>
      this._emit("log", { level: "error", message: `Leader-Election abgebrochen: ${err.message || err}` })
    );
  }

  /**
   * Stop the hub: closes all ports, releases the lock, removes listeners.
   */
  async stop() {
    if (!this.started) return;
    this.started = false;

    try { this._lockAbortController?.abort(); } catch {}
    this._stopHeartbeat();

    // Close every open scanner.
    const ids = [...this.scanners.keys()];
    await Promise.allSettled(ids.map((id) => this._closeScanner(id, { keepEntry: true })));

    try { navigator.serial.removeEventListener("connect", this._onSerialConnect); } catch {}
    try { navigator.serial.removeEventListener("disconnect", this._onSerialDisconnect); } catch {}
    window.removeEventListener("focus", this._onFocus);
    window.removeEventListener("blur", this._onBlur);
    document.removeEventListener("visibilitychange", this._onVisibility);
    window.removeEventListener("beforeunload", this._onBeforeUnload);

    try { this._channel?.removeEventListener("message", this._onChannelMessage); } catch {}
    try { this._channel?.close(); } catch {}
    this._channel = null;

    this.isLeader = false;
    this.currentLeaderId = null;
    this._emit("leader-change", { isLeader: false, leaderId: null });
    this._emit("log", { level: "info", message: "Hub gestoppt." });
  }

  /**
   * Request a NEW serial port from the user. Must be called from a user gesture.
   * After authorization, the port will be tracked and (if leader) opened.
   */
  async requestPort() {
    if (!("serial" in navigator)) throw new Error("Web Serial nicht unterstützt");
    const port = await navigator.serial.requestPort();
    const id = await this._registerPort(port);
    this._emit("log", { level: "info", message: `Scanner autorisiert: ${this.scanners.get(id)?.label}` });
    if (this.isLeader) this._scheduleOpen(id);
    return id;
  }

  /**
   * Force-close a single scanner (will be reopened later on retry if leader).
   */
  async releaseScanner(id) {
    if (!this.scanners.has(id)) return;
    await this._closeScanner(id, { keepEntry: true, suppressRetry: true });
  }

  /** Close every open scanner (useful for the "release in leader" button). */
  async releaseAll() {
    const ids = [...this.scanners.keys()];
    await Promise.allSettled(ids.map((id) => this._closeScanner(id, { keepEntry: true, suppressRetry: true })));
  }

  /** Re-scan navigator.serial for already-authorized ports. */
  async refresh() {
    await this._discoverKnownPorts();
  }

  /** Snapshot of currently known scanners (for UI rendering). */
  listScanners() {
    return [...this.scanners.values()].map((s) => ({
      id: s.id,
      label: s.label,
      info: s.info,
      state: s.state,
      lastError: s.lastError,
      lastScanAt: s.lastScanAt,
    }));
  }

  // -------------------------------------------------------------------------
  // Discovery / registration
  // -------------------------------------------------------------------------

  async _discoverKnownPorts() {
    let ports;
    try {
      ports = await navigator.serial.getPorts();
    } catch (err) {
      this._emit("log", { level: "warn", message: `getPorts() fehlgeschlagen: ${err.message || err}` });
      return;
    }

    for (const port of ports) {
      try { await this._registerPort(port); } catch (err) {
        this._emit("log", { level: "warn", message: `Registrieren fehlgeschlagen: ${err.message || err}` });
      }
    }

    // Drop any tracked scanner whose port is no longer in getPorts().
    // (Belt-and-suspenders: disconnect events should already handle this.)
    const live = new Set(ports);
    for (const [id, entry] of this.scanners) {
      if (!live.has(entry.port)) this._unregister(id);
    }
  }

  async _registerPort(port) {
    // Avoid double-registering the same port object.
    for (const existing of this.scanners.values()) {
      if (existing.port === port) return existing.id;
    }
    const info = port.getInfo?.() ?? {};
    // Compute a stable index amongst ports with the same VID/PID (for ID).
    let sameModelIndex = 0;
    for (const e of this.scanners.values()) {
      const i = e.info ?? {};
      if (i.usbVendorId === info.usbVendorId && i.usbProductId === info.usbProductId) {
        sameModelIndex++;
      }
    }
    const id = buildScannerId(info, sameModelIndex);
    /** @type {ScannerEntry} */
    const entry = {
      id,
      port,
      info,
      label: buildScannerLabel(info),
      state: STATES.KNOWN,
      reader: null,
      keepReading: false,
      buffer: "",
      backoffMs: this.reopenBackoffMs,
      lastError: null,
      lastScanAt: null,
      runToken: 0,
    };
    this.scanners.set(id, entry);
    this._setState(entry, STATES.KNOWN);
    this._emit("scanner-added", { scannerId: id, label: entry.label, info });
    return id;
  }

  _unregister(id) {
    const entry = this.scanners.get(id);
    if (!entry) return;
    this.scanners.delete(id);
    this._emit("scanner-removed", { scannerId: id, label: entry.label });
  }

  // -------------------------------------------------------------------------
  // Per-scanner lifecycle (open/close/read)
  // -------------------------------------------------------------------------

  _setState(entry, state, extra) {
    entry.state = state;
    this._emit("scanner-state", {
      scannerId: entry.id,
      label: entry.label,
      state,
      ...(extra || {}),
    });
  }

  /**
   * Schedules a (re)open attempt for a single scanner. Idempotent: if the
   * scanner is already opening or open we do nothing.
   */
  _scheduleOpen(id) {
    const entry = this.scanners.get(id);
    if (!entry) return;
    if (entry.chainRunning) return;        // a retry chain is already active
    if (entry.state === STATES.OPEN) return;
    if (!this.isLeader) return;

    // Each call increments runToken so older retry chains can detect they
    // were superseded.
    entry.runToken = (entry.runToken || 0) + 1;
    const myToken = entry.runToken;
    entry.chainRunning = true;

    (async () => {
      try {
        while (
          this.isLeader &&
          this.scanners.has(id) &&
          entry.runToken === myToken
        ) {
          try {
            await this._openScanner(id);
            await this._readLoop(id);
          } catch (err) {
            entry.lastError = err?.message || String(err);
            this._setState(entry, STATES.ERROR, { error: entry.lastError });
            this._emit("log", {
              level: "warn",
              message: `[${entry.label}] ${entry.lastError}`,
            });
          }
          if (!this.isLeader || !this.scanners.has(id) || entry.runToken !== myToken) return;
          // After a failed open or interrupted read, back off and retry.
          const wait = entry.backoffMs;
          entry.backoffMs = Math.min(this.reopenMaxBackoffMs, entry.backoffMs * 2);
          await sleep(wait);
        }
      } finally {
        entry.chainRunning = false;
      }
    })();
  }

  async _openScanner(id) {
    const entry = this.scanners.get(id);
    if (!entry) throw new Error("scanner gone");
    if (entry.state === STATES.OPEN) return;
    this._setState(entry, STATES.OPENING);

    try {
      await entry.port.open({ baudRate: this.baudRate });
    } catch (err) {
      // Port might still be "open" from a previous run that didn't clean up.
      // Try to recover: if .readable already exists, we're good.
      if (!entry.port.readable) {
        throw err;
      }
    }
    entry.keepReading = true;
    entry.buffer = "";
    entry.backoffMs = this.reopenBackoffMs; // reset on success
    this._setState(entry, STATES.OPEN);
    this._emit("log", {
      level: "info",
      message: `[${entry.label}] geöffnet (${this.baudRate} baud)`,
    });
  }

  async _readLoop(id) {
    const entry = this.scanners.get(id);
    if (!entry) return;
    const decoder = new TextDecoder();
    while (
      this.isLeader &&
      entry.keepReading &&
      this.scanners.has(id) &&
      entry.port?.readable
    ) {
      let reader;
      try {
        reader = entry.port.readable.getReader();
        entry.reader = reader;
      } catch (err) {
        // readable became null between the check and getReader – back out.
        break;
      }
      try {
        while (this.isLeader && entry.keepReading) {
          const { value, done } = await reader.read();
          if (done) break;
          if (!value) continue;
          this._consumeChunk(entry, decoder.decode(value, { stream: true }));
        }
      } catch (err) {
        entry.lastError = err?.message || String(err);
        this._emit("log", {
          level: "warn",
          message: `[${entry.label}] Lesefehler: ${entry.lastError}`,
        });
      } finally {
        try { reader.releaseLock(); } catch {}
        entry.reader = null;
      }
    }
    // If we left the loop because port.readable disappeared, close cleanly.
    if (this.scanners.has(id) && !entry.port?.readable) {
      try { await entry.port.close(); } catch {}
      this._setState(entry, STATES.CLOSED);
    }
  }

  _consumeChunk(entry, text) {
    entry.buffer += text;
    // Split on terminator. Use a fresh regex (with /g) so .split keeps tokens consistent.
    const parts = entry.buffer.split(this.terminator);
    entry.buffer = parts.pop() ?? "";
    for (const raw of parts) {
      const code = raw.trim();
      if (!code) continue;
      entry.lastScanAt = Date.now();
      // broadcast first so other tabs see it ASAP
      this._post({
        type: "scan",
        code,
        scannerId: entry.id,
        label: entry.label,
      });
      this._deliverScan({
        code,
        scannerId: entry.id,
        label: entry.label,
        source: "local",
        from: this.tabId,
      });
    }
  }

  async _closeScanner(id, { keepEntry = true, suppressRetry = false } = {}) {
    const entry = this.scanners.get(id);
    if (!entry) return;
    entry.keepReading = false;
    if (suppressRetry) entry.runToken = (entry.runToken || 0) + 1; // invalidate retry chain
    this._setState(entry, STATES.CLOSING);

    try { if (entry.reader) await entry.reader.cancel(); } catch {}
    try { if (entry.reader) entry.reader.releaseLock(); } catch {}
    entry.reader = null;

    try {
      if (entry.port?.readable) await entry.port.close();
    } catch (err) {
      this._emit("log", {
        level: "warn",
        message: `[${entry.label}] Schließen fehlgeschlagen: ${err.message || err}`,
      });
    }
    this._setState(entry, STATES.CLOSED);
    if (!keepEntry) this._unregister(id);
  }

  // -------------------------------------------------------------------------
  // Leader election (one tab at a time holds the lock)
  // -------------------------------------------------------------------------

  async _runLeaderElection() {
    if (this._leaderLoopRunning) return;
    this._leaderLoopRunning = true;
    while (this.started) {
      const ac = new AbortController();
      this._lockAbortController = ac;
      try {
        await navigator.locks.request(this.lockName, async () =>
          this._becomeLeader(ac.signal)
        );
      } catch (err) {
        this._emit("log", {
          level: "warn",
          message: `Lock-Fehler: ${err.message || err}`,
        });
      }
      // small pause before reattempting (e.g. after stop())
      await sleep(250);
    }
    this._leaderLoopRunning = false;
  }

  async _becomeLeader(signal) {
    this.isLeader = true;
    this.currentLeaderId = this.tabId;
    this._post({ type: "leader-active" });
    this._startHeartbeat();
    this._emit("leader-change", { isLeader: true, leaderId: this.tabId });
    this._emit("log", { level: "info", message: "Dieser Tab ist jetzt Leader." });

    try {
      // Refresh known ports in case devices were plugged in while not leader.
      await this._discoverKnownPorts();
      // Open every known scanner; each runs independently.
      for (const id of this.scanners.keys()) this._scheduleOpen(id);
      // Park here until lock is aborted (tab unload or stop()).
      await new Promise((resolve) => {
        if (signal.aborted) return resolve();
        signal.addEventListener("abort", resolve, { once: true });
      });
    } finally {
      this._stopHeartbeat();
      // Close every scanner cleanly before releasing the lock.
      const ids = [...this.scanners.keys()];
      await Promise.allSettled(
        ids.map((id) => this._closeScanner(id, { keepEntry: true, suppressRetry: true }))
      );
      this.isLeader = false;
      this.currentLeaderId = null;
      this._post({ type: "leader-inactive" });
      this._emit("leader-change", { isLeader: false, leaderId: null });
      this._emit("log", { level: "info", message: "Leader-Rolle abgegeben." });
    }
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this._heartbeatTimer = setInterval(
      () => this._post({ type: "leader-active" }),
      this.heartbeatMs
    );
  }

  _stopHeartbeat() {
    if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
    this._heartbeatTimer = null;
  }

  // -------------------------------------------------------------------------
  // BroadcastChannel
  // -------------------------------------------------------------------------

  _post(payload) {
    if (!this._channel) return;
    try {
      this._channel.postMessage({ ...payload, from: this.tabId, at: Date.now() });
    } catch {}
  }

  _onChannelMessage(ev) {
    const msg = ev.data;
    if (!msg || msg.from === this.tabId) return;
    switch (msg.type) {
      case "scan":
        this._deliverScan({
          code: msg.code,
          scannerId: msg.scannerId,
          label: msg.label,
          source: "broadcast",
          from: msg.from,
        });
        break;
      case "leader-active":
        this.currentLeaderId = msg.from;
        this._emit("leader-change", { isLeader: this.isLeader, leaderId: msg.from });
        break;
      case "leader-inactive":
        if (!this.isLeader && this.currentLeaderId === msg.from) {
          this.currentLeaderId = null;
          this._emit("leader-change", { isLeader: false, leaderId: null });
        }
        break;
      case "leader-who-is-there":
        if (this.isLeader) this._post({ type: "leader-active" });
        break;
    }
  }

  // -------------------------------------------------------------------------
  // Serial hot-plug events
  // -------------------------------------------------------------------------

  async _onSerialConnect(ev) {
    const port = ev.target ?? ev.port;
    if (!port) return;
    try {
      const id = await this._registerPort(port);
      this._emit("log", { level: "info", message: `Scanner verbunden: ${this.scanners.get(id)?.label}` });
      if (this.isLeader) this._scheduleOpen(id);
    } catch (err) {
      this._emit("log", { level: "warn", message: `connect-Handler: ${err.message || err}` });
    }
  }

  async _onSerialDisconnect(ev) {
    const port = ev.target ?? ev.port;
    if (!port) return;
    let removedId = null;
    for (const [id, entry] of this.scanners) {
      if (entry.port === port) { removedId = id; break; }
    }
    if (!removedId) return;
    this._emit("log", { level: "warn", message: `Scanner getrennt: ${this.scanners.get(removedId)?.label}` });
    // Stop the read loop and remove the entry. closeScanner is best-effort
    // (port.close() may already have failed because the device is gone).
    await this._closeScanner(removedId, { keepEntry: false, suppressRetry: true });
  }

  // -------------------------------------------------------------------------
  // Tab focus
  // -------------------------------------------------------------------------

  _onFocusChange() {
    this._emit("tab-active", { active: isPageActive() });
  }

  _onBeforeUnload() {
    try { this._lockAbortController?.abort(); } catch {}
    this._stopHeartbeat();
    try { this._channel?.close(); } catch {}
  }

  // -------------------------------------------------------------------------
  // Scan delivery
  // -------------------------------------------------------------------------

  _deliverScan(detail) {
    if (this.deliverOnlyWhenActive && !isPageActive()) {
      this._emit("log", {
        level: "info",
        message: `Ignoriert (Tab nicht aktiv): ${detail.code}`,
      });
      return;
    }
    this._emit("scan", detail);
  }

  _emit(type, detail) {
    this.dispatchEvent(new CustomEvent(type, { detail }));
  }
}

/**
 * @typedef {Object} ScannerEntry
 * @property {string} id
 * @property {SerialPort} port
 * @property {SerialPortInfo} info
 * @property {string} label
 * @property {string} state
 * @property {ReadableStreamDefaultReader|null} reader
 * @property {boolean} keepReading
 * @property {string} buffer
 * @property {number} backoffMs
 * @property {string|null} lastError
 * @property {number|null} lastScanAt
 * @property {number} runToken
 */

export const ScannerStates = STATES;
export { VENDOR_LABELS, buildScannerLabel };
