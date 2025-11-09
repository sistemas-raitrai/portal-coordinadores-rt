import { app, db, auth, storage } from './firebase-init-portal.js';
import { onAuthStateChanged, signOut } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection,
  getDocs,
  getDoc,
  doc,
  updateDoc,
  addDoc,
  setDoc,
  serverTimestamp,
  query,
  where,
  orderBy,
  limit,
  deleteField,
  deleteDoc,
  startAfter,
  
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import { getStorage, ref as sRef, uploadBytes, getDownloadURL } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

// === CORREO POR GAS (CONFIG) ===
const GAS_URL = 'https://script.google.com/macros/s/AKfycbwRMaUfZ0gJq015HIJ0yKyqu6_rkfmBkOp3oQH0wh4RSpjNxYTxmAf55Pv9pXQ64fUy/exec';
const GAS_KEY = '1GN4C10P4ST0RP1N0-P1N0P4ST0R1GN4C10'; // misma KEY que en Apps Script

// === MAIL HELPERS (GAS) ============================================
const MAIL_TIMEOUT_MS = 15000;

function buildMailto({ to, cc, subject, htmlBody }) {
  // Versión texto plano rápida para fallback (quita tags simples)
  const text = htmlBody
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n')
    .replace(/<[^>]+>/g, '')
    .trim();

  const qp = encodeURIComponent;
  let url = `mailto:${encodeURIComponent(to)}?subject=${qp(subject)}&body=${qp(text)}`;
  if (cc) url += `&cc=${encodeURIComponent(cc)}`;
  return url;
}

async function sendMailViaGAS(payload, { retries = 1 } = {}) {
  // Siempre mandamos origin tanto en query como en body (GAS no puede leer headers)
  const qp = encodeURIComponent;
  const url = `${GAS_URL}?origin=${qp(location.origin)}&key=${qp(GAS_KEY)}`;

  const finalPayload = {
    key: GAS_KEY,
    origin: location.origin, // 👈 IMPORTANTE para tu whitelist del GAS
    replyTo: payload.replyTo ?? 'operaciones@raitrai.cl',
    ...payload,
  };

  // 1) Intento "cors" (si el webapp devolviera ACAO alguna vez)
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), MAIL_TIMEOUT_MS);

    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' }, // simple → sin preflight
      body: JSON.stringify(finalPayload),
      mode: 'cors',
      credentials: 'omit',
      cache: 'no-store',
      signal: ctrl.signal,
    });

    clearTimeout(t);

    const raw = await res.text().catch(() => '');
    let json = {};
    try {
      json = raw ? JSON.parse(raw) : {};
    } catch (_) {}

    if (res.ok && json.ok) return json;

    throw new Error(json.error || `HTTP ${res.status} ${res.statusText || ''}`);
  } catch (e) {
    // 2) Fallback "no-cors": el navegador no exigirá CORS y la petición llega al GAS.
    // No podemos leer la respuesta (opaque), así que si no explota la red lo damos por OK.
    try {
      await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'text/plain;charset=utf-8' },
        body: JSON.stringify(finalPayload),
        mode: 'no-cors',
        credentials: 'omit',
        cache: 'no-store',
      });
      return { ok: true, opaque: true };
    } catch (e2) {
      if (retries > 0) return sendMailViaGAS(payload, { retries: retries - 1 });
      throw e2;
    }
  }
}

/* ====== UTILS TEXTO/FECHAS ====== */
const norm = (s = '') =>
  s
    .toString()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
const slug = (s) => norm(s).slice(0, 60);

const toISO = (x) => {
  if (!x) return '';
  if (typeof x === 'string') {
    const t = x.trim();

    if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t; // YYYY-MM-DD
    if (/^\d{2}-\d{2}-\d{4}$/.test(t)) {
      // DD-MM-AAAA
      const [dd, mm, yy] = t.split('-');
      return `${yy}-${mm}-${dd}`;
    }

    const d = new Date(t);
    return isNaN(d) ? '' : d.toISOString().slice(0, 10);
  }

  if (x && typeof x === 'object' && 'seconds' in x) {
    return new Date(x.seconds * 1000).toISOString().slice(0, 10);
  }

  if (x instanceof Date) return x.toISOString().slice(0, 10);

  return '';
};


const dmy = (iso) => {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso || '');
  return m ? `${m[3]}-${m[2]}-${m[1]}` : '';
};

const ymdFromDMY = (s) => {
  const t = (s || '').trim();
  if (/^\d{2}-\d{2}-\d{4}$/.test(t)) {
    const [dd, mm, yy] = t.split('-');
    return `${yy}-${mm}-${dd}`;
  }
  return '';
};

const daysInclusive = (ini, fin) => {
  const a = toISO(ini);
  const b = toISO(fin);
  if (!a || !b) return 0;
  return Math.max(1, Math.round((new Date(b) - new Date(a)) / 86400000) + 1);
};

const rangoFechas = (ini, fin) => {
  const out = [];
  const A = toISO(ini);
  const B = toISO(fin);
  if (!A || !B) return out;

  for (let d = new Date(`${A}T00:00:00`); d <= new Date(`${B}T00:00:00`); d.setDate(d.getDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
};

const parseQS = () => {
  const p = new URLSearchParams(location.search);
  return { g: p.get('g') || '', f: p.get('f') || '' };
};

const pad = (n) => String(n).padStart(2, '0');

const timeIdNowMs = () => {
  const d = new Date();
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${String(d.getMilliseconds()).padStart(3, '0')}`;
};

/* ====== IMPRESIÓN: HOJA OCULTA EN ESTA MISMA PÁGINA ====== */
function ensurePrintDOM() {
  if (document.getElementById('printSheet')) return;

  const css = document.createElement('style');
  css.id = 'printStyles';
  css.textContent = `
    /* PANTALLA: hoja oculta */
    #printSheet { display: none; }

    /* IMPRESIÓN */
    @media print {
      @page { size: A4; margin: 20mm; }

      /* Muestra sólo la hoja de impresión; ocultamos la app por display, no por visibility */
      #printSheet { display: block !important; }
      .wrap, #alertsPanel, #navPanel, #statsPanel, #gruposPanel, #modalBack { display: none !important; }

      /* Encabezado y pie (fijos) */
      #printSheet .print-head {
        position: fixed; top: 10mm; left: 0; right: 0;
        display: grid; grid-template-columns: 1fr auto; align-items: start; gap: 8px;
        font-family: Calibri, Arial, sans-serif; font-size: 11px; color: #444;
      }
      #printSheet .ph-left { text-transform: uppercase; }
      #printSheet .ph-left strong { font-size: 12px; }
      #printSheet .ph-right img { height: 36px; object-fit: contain; }

      #printSheet .print-foot {
        position: fixed; bottom: 10mm; left: 0; right: 0;
        text-align: center; font-family: Calibri, Arial, sans-serif; font-size: 10px; color: #666;
      }
      #printSheet .page-num::after { content: counter(page) " / " counter(pages); }

      /* Cuerpo */
      #printSheet .print-doc {
        white-space: pre-wrap;
        text-transform: uppercase;
        font-family: Calibri, Arial, sans-serif;
        font-size: 12px; line-height: 1.25;
        margin-top: 22mm;     /* despeje header */
        margin-bottom: 16mm;  /* despeje footer */
        color: #0a0a0a;
      }

      /* ====== ESTILOS DE TIPOGRAFÍA PARA EL CUERPO ====== */
      #printSheet .print-doc .h1   { font-size: 16px; font-weight: 700; letter-spacing: .3px; }
      #printSheet .print-doc .h2   { font-size: 14px; font-weight: 700; margin-top: 8px; }
      #printSheet .print-doc .b    { font-weight: 700; }
      #printSheet .print-doc .big  { font-size: 14px; }
      #printSheet .print-doc .muted{ color: #666; }
      #printSheet .print-doc .mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
    }
  `;
  document.head.appendChild(css);

  const sheet = document.createElement('div');
  sheet.id = 'printSheet';
  sheet.innerHTML = `
    <div class="print-head">
      <div class="ph-left">
        <div><strong>DESPACHO DE VIAJE</strong></div>
        <div id="ph-grupo"></div>
        <div id="ph-meta1"></div>
        <div id="ph-meta2"></div>
        <div id="ph-fechas"></div>
        <div id="ph-pax"></div>
      </div>
      <div class="ph-right">
        <img src="RaitraiLogo.png" alt="RAITRAI"/>
      </div>
    </div>

    <pre id="print-block" class="print-doc"></pre>

    <div class="print-foot">
      <span class="page-num"></span>
    </div>
  `;
  document.body.appendChild(sheet);
}

async function preparePrintForGroup(g) {
  ensurePrintDOM();

  const $doc  = document.getElementById('print-block');
  const $grp  = document.getElementById('ph-grupo');
  const $m1   = document.getElementById('ph-meta1');
  const $m2   = document.getElementById('ph-meta2');
  const $fech = document.getElementById('ph-fechas');
  const $pax  = document.getElementById('ph-pax');

  const nombre   = (g.nombreGrupo || g.aliasGrupo || g.id) || '';
  const code     = (g.numeroNegocio || '') + (g.identificador ? `-${g.identificador}` : '');
  const rango    = `${dmy(g.fechaInicio || '')} — ${dmy(g.fechaFin || '')}`;
  const destino  = (g.destino || '').toString().toUpperCase();
  const programa = (g.programa || '').toString().toUpperCase();

  $grp.textContent  = `GRUPO: ${nombre.toUpperCase()} (${code})`;
  $m1.textContent   = `DESTINO: ${destino}`;
  $m2.textContent   = `PROGRAMA: ${programa || '—'}`;
  $fech.textContent = `FECHAS: ${rango}`;

  const real = paxRealOf(g);
  const plan = paxOf(g);
  $pax.innerHTML = `PAX: ${real && real !== plan ? `${plan} → ${real}` : plan}`;

  // Cuerpo simple: fechas + actividades ordenadas por hora
  let body = '';
  const fechas = rangoFechas(g.fechaInicio, g.fechaFin);

  for (const f of fechas) {
    const actsRaw = (g.itinerario && g.itinerario[f]) ? g.itinerario[f] : [];
    const acts = (Array.isArray(actsRaw) ? actsRaw : Object.values(actsRaw || {}))
      .filter((a) => a && typeof a === 'object')
      .sort((a, b) => timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

    body += `\n\n# ${dmy(f)}\n`;
    if (!acts.length) {
      body += '— SIN ACTIVIDADES —\n';
      continue;
    }

    for (const a of acts) {
      const hIni = (a.horaInicio || '') ? ` ${a.horaInicio}` : '';
      const hFin = (a.horaFin || '') ? ` — ${a.horaFin}` : '';
      const prov = (a.proveedor || '') ? ` · PROV: ${a.proveedor.toString().toUpperCase()}` : '';
      body += `• ${(a.actividad || '').toString().toUpperCase()}${hIni}${hFin}${prov}\n`;
    }
  }

  $doc.textContent = body.trim();
}

/* ====== HISTORIAL VIAJE (utils) ====== */
const HIST_ACTKEY = '_viaje_'; // o 'viaje_hist', cualquier cosa que NO sea __...__

const fmtChile = (date) =>
  new Intl.DateTimeFormat('es-CL', {
    timeZone: 'America/Santiago',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
    .format(date)
    .toUpperCase();

async function appendViajeLog(grupoId, kind, text = '', meta = null) {
  // Registro inmutable en subcolección viajeLog
  await addDoc(collection(db, 'grupos', grupoId, 'viajeLog'), {
    type: kind,
    text,
    meta,
    by: (state.user.email || '').toLowerCase(),
    byUid: state.user.uid,
    ts: serverTimestamp(),
  });

  // Copia visible en Bitácora bajo actividad especial __viaje__
  const dateIso = todayISO();
  const timeId = timeIdNowMs();

  await setDoc(doc(db, 'grupos', grupoId, 'bitacora', HIST_ACTKEY, dateIso, timeId), {
    texto: (text || kind).toString().toUpperCase(),
    byUid: state.user.uid,
    byEmail: (state.user.email || '').toLowerCase(),
    ts: serverTimestamp(),
  });
}

/* ====== UTILS PAX/VIAJE (NUEVOS) ====== */
const todayISO = () => new Date().toISOString().slice(0, 10);
const isToday = (iso) => toISO(iso) === todayISO();
const paxOf = (g) => Number(g?.cantidadgrupo ?? g?.pax ?? 0);
const paxRealOf = (g) => Number(g?.paxViajando?.total || 0);
const paxBreakdown = (g) => ({ A: Number(g?.paxViajando?.A || 0), E: Number(g?.paxViajando?.E || 0) });

const fmtPaxPlan = (plan, g) => {
  const real = paxRealOf(g);
  const nPlan = Number(plan || 0);
  if (real && real !== nPlan) {
    return `<span style="text-decoration:line-through;opacity:.7">${nPlan}</span> → <strong>${real}</strong>`;
    }
  return `<strong>${nPlan}</strong>`;
};

/* Tiempo: HH:MM → minutos (sin hora => muy grande para que quede al final) */
const timeVal = (t) => {
  const m = /^(\d{1,2}):(\d{2})/.exec(String(t || '').trim());
  if (!m) return 1e9;
  const h = Math.max(0, Math.min(23, parseInt(m[1], 10)));
  const mi = Math.max(0, Math.min(59, parseInt(m[2], 10)));
  return h * 60 + mi;
};
/* ===== DEBUG HOTEL ===== */
const DEBUG_HOTEL = true;
const D_HOTEL = (...args) => {
  if (DEBUG_HOTEL) console.log('%c[HOTEL]', 'color:#0ff', ...args);
};

/* ====== EXTRACCIÓN TOLERANTE DESDE GRUPOS ====== */
const arrify = (v) =>
  Array.isArray(v)
    ? v
    : (v && typeof v === 'object')
      ? Object.values(v)
      : v
        ? [v]
        : [];

function emailsOf(g) {
  const out = new Set();
  const push = (e) => { if (e) out.add(String(e).toLowerCase()); };

  push(g?.coordinadorEmail);
  push(g?.coordinador?.email);
  arrify(g?.coordinadoresEmails).forEach(push);

  if (g?.coordinadoresEmailsObj) {
    Object.keys(g.coordinadoresEmailsObj).forEach(push);
  }

  arrify(g?.coordinadores).forEach((x) => {
    if (x?.email) push(x.email);
    else if (typeof x === 'string' && x.includes('@')) push(x);
  });

  return [...out];
}

function coordDocIdsOf(g) {
  const out = new Set();
  const push = (x) => { if (x) out.add(String(x)); };

  push(g?.coordinadorId);
  arrify(g?.coordinadoresIds).forEach(push);

  const mapEmailToId = new Map(
    state.coordinadores.map((c) => [String(c.email || '').toLowerCase(), c.id]),
  );

  emailsOf(g).forEach((e) => {
    if (mapEmailToId.has(e)) out.add(mapEmailToId.get(e));
  });

  return [...out];
}

/* ====== ESTADO APP ====== */
const STAFF_EMAILS = new Set(
  [
    'aleoperaciones@raitrai.cl',
    'operaciones@raitrai.cl',
    'anamaria@raitrai.cl',
    'tomas@raitrai.cl',
    'sistemas@raitrai.cl',
  ].map((x) => x.toLowerCase()),
);

const state = {
  user: null,
  is: false,
  coordinadores: [],
  viewingCoordId: null, // STAFF: ID SELECCIONADO · COORD: SU PROPIO ID
  grupos: [],
  ados: [],
  idx: 0,
  filter: { type: 'all', value: null },
  groupQ: '',
  alertsTimer: null, // AUTO-REFRESCO DE ALERTAS (60S)
  cache: {
    hotel: new Map(),
    vuelos: new Map(),
    tasas: null,
    hoteles: { loaded: false, byId: new Map(), bySlug: new Map(), all: [] },
  },
};

// ——— GASTOS: resolver coordinador activo evitando "__ALL__"
function getActiveCoordIdForGastos() {
  // Si el selector tiene un coordinador concreto, úsalo
  if (state.viewingCoordId && state.viewingCoordId !== '__ALL__') return state.viewingCoordId;

  // Si no, usa mi propio coordinador (por email) o 'self'
  const me = state.coordinadores.find(
    (c) => (c.email || '').toLowerCase() === (state.user.email || '').toLowerCase(),
  );
  return me?.id || 'self';
}

// ====== HELPERS UI ======
function ensurePanel(id, html = '') {
  let p = document.getElementById(id);
  if (!p) {
    p = document.createElement('div');
    p.id = id;
    p.className = 'panel';
    document.querySelector('.wrap').prepend(p);
  }
  if (html) p.innerHTML = html;
  enforceOrder();
  return p;
}

function enforceOrder() {
  const wrap = document.querySelector('.wrap');
  // ORDEN CORRECTO: STAFF -> ALERTAS -> STATS -> NAV -> GRUPOS
  ['alertsPanelV2', 'staffBar', 'statsPanel', 'navPanel', 'gruposPanel'].forEach((id) => {
    const n = document.getElementById(id);
    if (n) wrap.appendChild(n);
  });
}

function showFlash(msg, kind = 'ok') {
  const colors = {
    ok:   { bg: '#16a34a', fg: '#fff' },
    warn: { bg: '#ea580c', fg: '#fff' },
    err:  { bg: '#dc2626', fg: '#fff' },
    info: { bg: '#64748b', fg: '#fff' },
  };
  const c = colors[kind] || colors.ok;

  const n = document.createElement('div');
  n.textContent = String(msg || '').toUpperCase();
  n.style.cssText =
    'position:fixed;right:16px;bottom:16px;z-index:9999;padding:10px 12px;border-radius:10px;font-weight:700;letter-spacing:.5px;' +
    `background:${c.bg};color:${c.fg};box-shadow:0 10px 20px rgba(0,0,0,.15);opacity:0;transform:translateY(8px);` +
    'transition:opacity .2s ease, transform .2s ease';

  document.body.appendChild(n);
  requestAnimationFrame(() => {
    n.style.opacity = '1';
    n.style.transform = 'translateY(0)';
  });

  setTimeout(() => {
    n.style.opacity = '0';
    n.style.transform = 'translateY(6px)';
    n.addEventListener('transitionend', () => n.remove(), { once: true });
  }, 4000);
}

// === LOGS GLOBALES (poner una sola vez) ===
if (typeof window !== 'undefined') {
  window.addEventListener('error', (ev) =>
    console.error('[GLOBAL ERROR]', ev.message, ev.error),
  );
  window.addEventListener('unhandledrejection', (ev) =>
    console.error('[PROMISE REJECTION]', ev.reason),
  );
}

/* ===== ALERTAS PAGINADAS + BUSCADOR + ORDEN + FILTROS + LEÍDO ===== */
(() => {
  const PAGE_1 = 10;
  const PAGE_MORE = 20;

  // Correos que consideramos de "Operaciones"
  const OPS_SENDERS = new Set([
    'operaciones@raitrai.cl',
    'aleoperaciones@raitrai.cl',
    'sistemas@raitrai.cl',
  ]);

  const norm = (s = '') =>
    s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase();

  // Estado UI (reutiliza si ya existe)
  state.alertsUI ||= {
    items: [],
    lastDoc: null,
    totalLoaded: 0,
    q: '',
    // sin "sort": siempre nuevas→antiguas
    filter: 'all', // se ajusta luego según sea STAFF o no
    loading: false,
    inited: false,
  };

  // --- Panel base + controles
  function ensureAlertsPanel() {
    const host = ensurePanel('alertsPanelV2');

    host.innerHTML = `
      <div class="rowflex" style="gap:.5rem;align-items:center;flex-wrap:wrap;margin-bottom:.5rem">
        <input id="alQ" type="text" placeholder="BUSCAR EN ALERTAS..." style="flex:1;min-width:240px"/>

        ${state.is ? `
          <!-- STAFF: ÁMBITO -->
          <select id="alScope" title="ÁMBITO">
            <option value="all">TODAS</option>
            <option value="ops">SOLO OPERACIONES</option>
            <option value="mine">SOLO MÍAS</option>
          </select>
        ` : `
          <!-- NO STAFF: ESTADO -->
          <select id="alState" title="ESTADO">
            <option value="unread">NO LEÍDAS</option>
            <option value="read">LEÍDAS</option>
          </select>
        `}

        <button id="alRefresh" class="btn sec">REFRESCAR</button>
      </div>

      <div id="alList" class="acts"></div>
      <div class="rowflex" style="margin-top:.6rem;gap:.5rem;justify-content:center">
        <button id="alMore" class="btn">CARGAR 20 MÁS</button>
      </div>
      <div class="meta muted" id="alMeta" style="margin-top:.25rem"></div>
    `;

    const $q = host.querySelector('#alQ');
    let t = null;
    $q.oninput = () => {
      clearTimeout(t);
      t = setTimeout(() => {
        state.alertsUI.q = norm($q.value || '');
        renderAlertsPanel();
      }, 150);
    };

    if (state.is) {
      const $scope = host.querySelector('#alScope');
      // reflejar valor actual si ya existía
      if (['all', 'ops', 'mine'].includes(state.alertsUI.filter)) {
        $scope.value = state.alertsUI.filter;
      }
      $scope.onchange = () => {
        state.alertsUI.filter = $scope.value;
        renderAlertsPanel();
      };
    } else {
      const $state = host.querySelector('#alState');
      $state.value = state.alertsUI.filter === 'read' ? 'read' : 'unread';
      $state.onchange = () => {
        state.alertsUI.filter = $state.value;
        renderAlertsPanel();
      };
    }

    host.querySelector('#alRefresh').onclick = async () => {
      resetAlertsCache();
      await fetchAlertsPage(true);
    };
    host.querySelector('#alMore').onclick = async () => {
      await fetchAlertsPage(false);
    };

    return host;
  }

  function resetAlertsCache() {
    state.alertsUI.items = [];
    state.alertsUI.totalLoaded = 0;
    state.alertsUI.lastDoc = null;
  }

  // --- Carga paginada (Firestore)
  async function fetchAlertsPage(initial) {
    if (state.alertsUI.loading) return;
    state.alertsUI.loading = true;

    try {
      const base = collection(db, 'alertas');
      let qFs = query(base, orderBy('createdAt', 'desc'), limit(initial ? PAGE_1 : PAGE_MORE));

      if (!initial && state.alertsUI.lastDoc) {
        qFs = query(
          base,
          orderBy('createdAt', 'desc'),
          startAfter(state.alertsUI.lastDoc),
          limit(PAGE_MORE),
        );
      }

      const snap = await getDocs(qFs);
      if (!snap.size) {
        showFlash('NO HAY MÁS ALERTAS', 'info');
        return;
      }

      const batch = [];
      snap.forEach((d) => {
        const x = d.data() || {};
        const ts = x.createdAt?.seconds
          ? new Date(x.createdAt.seconds * 1000)
          : x.createdAt?.toDate?.() || null;

        const byEmail = (x?.createdBy?.email || '').toLowerCase();
        const readBy = x.readBy || {}; // { uid: true, ... }

        batch.push({
          id: d.id,
          mensaje: String(x.mensaje || ''),
          audience: String(x.audience || ''),
          createdAt: ts,
          createdByEmail: byEmail,
          readBy,
          groupInfo: x.groupInfo || null,
          _q: norm(
            [
              x.mensaje,
              x.audience,
              byEmail,
              x?.groupInfo?.actividad,
              x?.groupInfo?.destino,
              x?.groupInfo?.nombre,
            ]
              .filter(Boolean)
              .join(' '),
          ),
        });
      });

      state.alertsUI.items.push(...batch);
      state.alertsUI.totalLoaded += batch.length;
      state.alertsUI.lastDoc = snap.docs[snap.docs.length - 1];

      renderAlertsPanel();
    } catch (e) {
      console.error('[ALERTAS] fetch', e);
      showFlash('ERROR AL CARGAR ALERTAS', 'err');
    } finally {
      state.alertsUI.loading = false;
    }
  }

  // --- Render list + filtros + orden + marcar leído
  function renderAlertsPanel() {
    ensureAlertsPanel();

    const p = document.getElementById('alertsPanelV2');
    const list = p.querySelector('#alList');
    const meta = p.querySelector('#alMeta');

    const meUid = state?.user?.uid || '';
    const meEmail = (state?.user?.email || '').toLowerCase();

    const q = state.alertsUI.q;
    let arr = state.alertsUI.items.slice();

    // BUSCADOR
    if (q) arr = arr.filter((a) => a._q.includes(q));

    // FILTRO (dependiente de STAFF)
    const f = state.alertsUI.filter;
    if (state.is) {
      // STAFF: all | ops | mine
      if (f === 'ops') arr = arr.filter((a) => OPS_SENDERS.has(a.createdByEmail));
      if (f === 'mine') arr = arr.filter((a) => a.createdByEmail === meEmail);
      // 'all' no filtra
    } else {
      // NO STAFF: unread | read
      if (f === 'unread') arr = arr.filter((a) => !a.readBy?.[meUid]);
      if (f === 'read') arr = arr.filter((a) => a.readBy?.[meUid]);
    }

    // ORDEN (fallback si falta fecha)
    arr.sort((a, b) => {
      const ta = a.createdAt ? a.createdAt.getTime() : 0;
      const tb = b.createdAt ? b.createdAt.getTime() : 0;
      return tb - ta; // SIEMPRE NUEVAS → ANTIGUAS
    });

    // DIBUJO
    if (!arr.length) {
      list.innerHTML = '<div class="muted">SIN ALERTAS PARA ESTE CRITERIO.</div>';
    } else {
      const frag = document.createDocumentFragment();

      arr.forEach((a) => {
        const box = document.createElement('div');
        const unread = !a.readBy?.[meUid];

        box.className = 'act';

        const cuando = a.createdAt
          ? new Intl.DateTimeFormat('es-CL', { dateStyle: 'short', timeStyle: 'short' })
              .format(a.createdAt)
              .toUpperCase()
          : '—';

        // puntito si no leída
        const dot = unread
          ? '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#10b981;margin-right:6px;vertical-align:middle"></span>'
          : '';

        box.innerHTML = `
          <div class="meta"><strong>${dot}${(a.mensaje || '').toUpperCase()}</strong></div>
          <div class="meta muted">FECHA: ${cuando}${a.audience ? ' · AUDIENCIA: ' + a.audience.toUpperCase() : ''}${a.createdByEmail ? ' · POR: ' + a.createdByEmail : ''}</div>
          ${a.groupInfo ? `<div class="meta">GRUPO: ${(a.groupInfo.nombre || '—').toString().toUpperCase()} · ACT: ${(a.groupInfo.actividad || '—').toString().toUpperCase()} · DEST: ${(a.groupInfo.destino || '—').toString().toUpperCase()}</div>` : ''}
          <div class="rowflex" style="gap:.4rem;margin-top:.35rem">
            <button class="btn sec btnMark">${unread ? 'MARCAR LEÍDA' : 'MARCAR NO LEÍDA'}</button>
          </div>
        `;

        // toggle leído
        box.querySelector('.btnMark').onclick = async () => {
          try {
            const path = doc(db, 'alertas', a.id);
            const payload = {};
            if (unread) {
              payload[`readBy.${meUid}`] = true;
            } else {
              // quitar marca (deleteField)
              payload[`readBy.${meUid}`] = deleteField();
            }
            await updateDoc(path, payload);

            // espejo local
            if (unread) (a.readBy ||= {})[meUid] = true;
            else if (a.readBy) delete a.readBy[meUid];

            renderAlertsPanel();
          } catch (e) {
            console.error(e);
            showFlash('NO SE PUDO ACTUALIZAR LECTURA', 'err');
          }
        };

        frag.appendChild(box);
      });

      list.innerHTML = '';
      list.appendChild(frag);
    }

    meta.textContent =
      `MOSTRANDO ${arr.length} / CARGADAS ${state.alertsUI.totalLoaded} — ` +
      'USA BUSCADOR, FILTRO Y “CARGAR 20 MÁS”.';
  }

  // API pública
  window.renderGlobalAlertsV2 = async () => {
    ensureAlertsPanel();
    if (!state.alertsUI.inited) {
      state.alertsUI.inited = true;
      resetAlertsCache();
      await fetchAlertsPage(true);
    } else {
      resetAlertsCache();
      await fetchAlertsPage(true);
    }

    // (defensa) OCULTAR PANELES LEGACY DE ALERTAS (cubre varias variantes)
    ['alerts', 'alertas', 'panel-alertas', 'alertasPanel', 'alertasWrap'].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.style.display = 'none';
    });

    document.querySelectorAll('[data-section="alertas"], .alertas-wrap, .alertasTabs').forEach((n) => {
      n.style.display = 'none';
    });
  };
})();

window.renderFinanzas ??= async () => 0;
// --- HOTFIX: evitar crash si aún no definimos loadAbonos / loadGastos ---
window.loadAbonos ??= async function loadAbonos(/* g o groupId, filtros, etc. */) {
  // Devuelve estructura mínima esperada por renderFinanzas
  return { items: [], total: 0 };
};
window.loadGastos ??= async function loadGastos(/* g o groupId, filtros, etc. */) {
  return { items: [], total: 0 };
};

window.setEstadoServicio ??= async () => showFlash('ESTADO ACTUALIZADO');
window.openActividadModal ??= async () => {};
window.staffResetInicio ??= async () => {};
/* ====== ARRANQUE ====== */
onAuthStateChanged(auth, async (user) => {
  if (!user) {
    location.href = 'index.html';
    return;
  }

  state.user = user;
  state.is = STAFF_EMAILS.has((user.email || '').toLowerCase());

  const coords = await loadCoordinadores();
  state.coordinadores = coords;

  // SELECTOR CON "TODOS"
  if (state.is) {
    await showSelector(coords);
  } else {
    const mine = findCoordinadorForUser(coords, user);
    state.viewingCoordId = mine.id || 'self';
    await loadGruposForCoordinador(mine, user);
  }

  // BOTONES SOLO PARA STAFF (en NAV solo queda imprimir; crear alerta va en Alertas)
  const btnPrint = document.getElementById('btnPrintVch');
  if (btnPrint) {
    btnPrint.style.display = state.is ? '' : 'none';
    if (state.is) btnPrint.textContent = 'IMPRIMIR DESPACHO';
  }

  const legacyNewAlert = document.getElementById('btnNewAlert');
  if (legacyNewAlert) legacyNewAlert.style.display = 'none';

  // crea hoja de impresión oculta
  ensurePrintDOM();

  // Preferencias iniciales del panel de alertas según rol
  state.alertsUI ||= { filter: 'all' };
  if (state.is) {
    // STAFF: arrancar con "TODAS" (puedes cambiar a 'ops' si prefieres)
    state.alertsUI.filter = state.alertsUI.filter ?? 'all';
  } else {
    // NO STAFF: arrancar con "NO LEÍDAS"
    state.alertsUI.filter = 'unread';
  }

  // PANEL ALERTAS
  await window.renderGlobalAlertsV2();

  // matar cualquier timer viejo de versiones anteriores
  try {
    if (state.alertsTimer) {
      clearInterval(state.alertsTimer);
      state.alertsTimer = null;
    }
  } catch (_) {}

  try {
    if (window.rtAlertsTimer) {
      clearInterval(window.rtAlertsTimer);
      window.rtAlertsTimer = null;
    }
  } catch (_) {}

  try {
    if (window.alertsTimer) {
      clearInterval(window.alertsTimer);
      window.alertsTimer = null;
    }
  } catch (_) {}

  // AUTO-REFRESCO CADA 60S (solo alertas, sin reordenar paneles)
  if (!state.alertsTimer) {
    state.alertsTimer = setInterval(window.renderGlobalAlertsV2, 60000);
    // opcional: espejo para legacy que miraba window.*
    window.rtAlertsTimer = state.alertsTimer;
  }
});

/* ====== CARGAS FIRESTORE ====== */
async function loadCoordinadores() {
  const snap = await getDocs(collection(db, 'coordinadores'));
  const list = [];

  snap.forEach((d) => {
    const x = d.data() || {};
    list.push({
      id: d.id,
      nombre: String(x.nombre || x.Nombre || x.coordinador || ''),
      email: String(x.email || x.correo || x.mail || '').toLowerCase(),
      uid: String(x.uid || x.userId || ''),
    });
  });

  list.sort((a, b) => a.nombre.localeCompare(b.nombre, 'es', { sensitivity: 'base' }));

  const seen = new Set();
  const dedup = [];

  for (const c of list) {
    const k = (c.nombre + '|' + c.email).toLowerCase();
    if (!seen.has(k)) {
      seen.add(k);
      dedup.push(c);
    }
  }

  return dedup;
}

function findCoordinadorForUser(coordinadores, user) {
  const email = (user.email || '').toLowerCase();
  const uid = user.uid;

  let c = coordinadores.find((x) => x.email && x.email.toLowerCase() === email);
  if (c) return c;

  if (uid) {
    c = coordinadores.find((x) => x.uid && x.uid === uid);
    if (c) return c;
  }

  return { id: 'self', nombre: user.displayName || email, email, uid };
}

/* ====== SELECTOR (CON "TODOS") ====== */
async function showSelector(coordinadores) {
  const bar = ensurePanel(
    'staffBar',
    `
      <label style="display:block;margin-bottom:6px;color:#cbd5e1">COORDINADOR(A):</label>
      <select id="coordSelect"></select>
    `,
  );

  const sel = bar.querySelector('#coordSelect');
  sel.innerHTML =
    '<option value="__ALL__">TODOS</option>' +
    coordinadores
      .map(
        (c) =>
          `<option value="${c.id}">${(c.nombre || '').toUpperCase()} — ${(c.email || '').toUpperCase()}</option>`,
      )
      .join('');

  sel.onchange = async () => {
    const id = sel.value || '';
    const elegido = id === '__ALL__' ? { id: '__ALL__' } : coordinadores.find((c) => c.id === id) || null;
    state.viewingCoordId = id || null;
    localStorage.setItem('rt__coord', id);
    await loadGruposForCoordinador(elegido, state.user);
    await window.renderGlobalAlertsV2();
  };

  const last = localStorage.getItem('rt__coord');
  if (last) {
    sel.value = last;
    const elegido =
      last === '__ALL__' ? { id: '__ALL__' } : coordinadores.find((c) => c.id === last) || null;
    state.viewingCoordId = last;
    await loadGruposForCoordinador(elegido, state.user);
  }
}

/* ====== GRUPOS PARA EL COORDINADOR EN CONTEXTO (O "TODOS") ====== */
async function loadGruposForCoordinador(coord, user) {
  const cont = document.getElementById('grupos');
  if (cont) cont.textContent = 'CARGANDO GRUPOS…';

  const allSnap = await getDocs(collection(db, 'grupos'));
  const wanted = [];
  const isAll = coord && coord.id === '__ALL__';

  const emailElegido = (coord?.email || '').toLowerCase();
  const docIdElegido = (coord?.id || '').toString();
  const isSelf =
    !coord || coord.id === 'self' || emailElegido === (user.email || '').toLowerCase();

  allSnap.forEach((d) => {
    const raw = { id: d.id, ...d.data() };
    const g = {
      ...raw,
      fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
      fechaFin: toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
      itinerario: normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
      serviciosEstado: raw.serviciosEstado || {},
      numeroNegocio: String(
        raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || d.id,
      ),
      identificador: String(raw.identificador || raw.codigo || ''),
    };

    if (isAll) {
      wanted.push(g);
      return;
    }

    const gEmails = emailsOf(raw);
    const gDocIds = coordDocIdsOf(raw);
    const match =
      (emailElegido && gEmails.includes(emailElegido)) ||
      (docIdElegido && gDocIds.includes(docIdElegido)) ||
      (isSelf && gEmails.includes((user.email || '').toLowerCase()));

    if (match) wanted.push(g);
  });

  // ORDENAR (FUTUROS → PASADOS)
  const hoy = toISO(new Date());
  const futuros = wanted
    .filter((g) => (g.fechaInicio || '') >= hoy)
    .sort((a, b) => (a.fechaInicio || '').localeCompare(b.fechaInicio || ''));
  const pasados = wanted
    .filter((g) => (g.fechaInicio || '') < hoy)
    .sort((a, b) => (a.fechaInicio || '').localeCompare(b.fechaInicio || ''));

  state.grupos = wanted;
  state.ordenados = [...futuros, ...pasados];

  state.filter = { type: 'all', value: null };
  state.groupQ = '';

  renderStatsFiltered();
  renderNavBar();

  const { g: qsG, f: qsF } = parseQS();
  let idx = 0;

  if (qsG) {
    const byNum = state.ordenados.findIndex((x) => String(x.numeroNegocio) === qsG);
    const byId = state.ordenados.findIndex((x) => String(x.id) === qsG);
    idx = byNum >= 0 ? byNum : byId >= 0 ? byId : 0;
  } else {
    const last = localStorage.getItem('rt_last_group');
    if (last) {
      const i = state.ordenados.findIndex((x) => x.id === last || x.numeroNegocio === last);
      if (i >= 0) idx = i;
    }
  }

  state.idx = Math.max(0, Math.min(idx, state.ordenados.length - 1));

  // garantizar itinerario antes de renderizar
  const target = state.ordenados[state.idx];
  await ensureItinerarioLoaded(target);

  await renderOneGroup(target, qsF);
}

/* ====== NORMALIZADOR DE ITINERARIO (multiesquema, robusto) ====== */
function normalizeItinerario(raw) {
  if (!raw) return {};

  // A) Array plano de actividades con .fecha → agrupar por fecha
  if (Array.isArray(raw)) {
    const map = {};
    for (const item of raw) {
      const f = toISO(item && item.fecha);
      if (!f || !item || typeof item !== 'object') continue;
      (map[f] ||= []).push({ ...item });
    }
    return map;
  }

  // B) Objeto { 'YYYY-MM-DD': [...] }  (OK)
  // C) Objeto { 'YYYY-MM-DD': { items | actividades | acts: [...] } }
  // D) Objeto { 'YYYY-MM-DD': { '0':act, '1':act, ... } }
  // E) Objeto { 'YYYY-MM-DD': { <timeId>: actividad } }
  if (raw && typeof raw === 'object') {
    const out = {};
    for (const [k, v] of Object.entries(raw)) {
      const f = toISO(k);
      if (!f) continue;

      let arr = [];
      if (Array.isArray(v)) {
        arr = v;
      } else if (v && typeof v === 'object') {
        if (Array.isArray(v.items)) arr = v.items;
        else if (Array.isArray(v.actividades)) arr = v.actividades;
        else if (Array.isArray(v.acts)) arr = v.acts;
        else {
          const keys = Object.keys(v);

          // D) objeto indexado tipo {"0":{...},"1":{...}} → ordénalo y pásalo a array
          if (keys.length && keys.every((x) => /^\d+$/.test(x))) {
            arr = keys
              .sort((a, b) => Number(a) - Number(b))
              .map((i) => v[i])
              .filter((x) => x && typeof x === 'object');
          } else {
            // E) mapa { timeId: actividad } si parecen actividades
            const vals = Object.values(v).filter((x) => x && typeof x === 'object');
            if (vals.some((x) => x.actividad || x.horaInicio || x.horaFin)) arr = vals;
          }
        }
      }

      if (Array.isArray(arr) && arr.length) {
        // Sanitiza: sólo objetos
        out[f] = arr.filter((x) => x && typeof x === 'object').map((x) => ({ ...x }));
      }
    }
    return out;
  }

  return {};
}

/* ====== ITINERARIO: carga desde subcolección (compat) ====== */
async function loadItinerarioFromSubcollections(grupoId) {
  const map = {};
  try {
    const coll = collection(db, 'grupos', grupoId, 'itinerario');
    const ds = await getDocs(coll);

    for (const d of ds.docs) {
      const iso = toISO(d.id);
      if (!iso) continue;

      const x = d.data() || {};

      // Prioriza arrays directos
      let items = null;
      if (Array.isArray(x.items)) items = x.items;
      else if (Array.isArray(x.actividades)) items = x.actividades;
      else if (Array.isArray(x.acts)) items = x.acts;

      // Si no hay arrays directos, intenta subcolección "items"
      if (!items) {
        try {
          const sub = await getDocs(collection(db, 'grupos', grupoId, 'itinerario', d.id, 'items'));
          const arr = [];
          sub.forEach((i) => arr.push({ id: i.id, ...(i.data() || {}) }));
          if (arr.length) items = arr;
        } catch (_) {}
      }

      // Si tampoco, convierte objeto {timeId:{...}}
      if (!items && x && typeof x === 'object') {
        const vals = Object.values(x).filter((z) => z && typeof z === 'object');
        if (vals.some((z) => z.actividad || z.horaInicio || z.horaFin)) items = vals;
      }

      if (Array.isArray(items) && items.length) map[iso] = items;
    }
  } catch (e) {
    console.warn('loadItinerarioFromSubcollections', e);
  }
  return map;
}

// Garantiza que el grupo tenga itinerario (si no viene en el doc, lo carga desde la subcolección)
async function ensureItinerarioLoaded(grupo) {
  try {
    if (grupo && grupo.itinerario && Object.keys(grupo.itinerario).length) return grupo;
    const map = await loadItinerarioFromSubcollections(grupo.id);
    if (map && Object.keys(map).length) {
      grupo.itinerario = map;
    }
  } catch (_) {}
  return grupo;
}

/* ====== STATS ====== */
function getFilteredList() {
  const base = state.ordenados.slice();
  const dest = state.filter.type === 'dest' && state.filter.value ? state.filter.value : null;
  return dest ? base.filter((g) => String(g.destino || '') === dest) : base;
}

function renderStatsFiltered() {
  renderStats(getFilteredList());
}

function renderStats(list) {
  const p = ensurePanel('statsPanel');
  if (!list.length) {
    p.innerHTML = '<div class="muted">SIN VIAJES ASIGNADOS.</div>';
    return;
  }

  const n = list.length;
  const minIni = list.map((g) => g.fechaInicio).filter(Boolean).sort()[0] || '';
  const maxFin = list.map((g) => g.fechaFin).filter(Boolean).sort().slice(-1)[0] || '';
  const totalDias = list.reduce((s, g) => s + daysInclusive(g.fechaInicio, g.fechaFin), 0);
  const paxTot = list.reduce((s, g) => s + paxOf(g), 0);
  const destinos = [...new Set(list.map((g) => String(g.destino || '')).filter(Boolean))].map((x) =>
    x.toUpperCase(),
  );

  p.innerHTML = `
    <div class="stats-wrap">
      <div><h4><strong>DESPACHO</strong></h4></div>
      <div class="meta-line meta">
        <span class="item nowrap">N° VIAJES: <strong>${n}</strong></span>
        <span class="item nowrap">DÍAS EN VIAJE: <strong>${totalDias}</strong></span>
        <span class="item">RANGO DE FECHAS: <strong>${minIni ? dmy(minIni) : '—'} — ${maxFin ? dmy(maxFin) : '—'}</strong></span>
        <span class="item">DESTINOS: <strong>${destinos.length ? destinos.join(' · ') : '—'}</strong></span>
      </div>
    </div>
  `;
}

/* ====== NAV ====== */
function renderNavBar() {
  // Crea el panel si no existe y sólo inyecta el HTML base una vez
  const p = ensurePanel('navPanel');
  if (!p.innerHTML.trim()) {
    p.innerHTML = `
      <div class="rowflex" style="gap:.5rem;align-items:center;flex-wrap:wrap">
        <button id="btnPrev" class="btn sec">◀</button>
        <select id="allTrips" style="flex:1;min-width:260px"></select>
        <button id="btnNext" class="btn sec">▶</button>
        <button id="btnPrintVch" class="btn sec">IMPRIMIR DESPACHO</button>
      </div>
    `;
  }

  const sel = p.querySelector('#allTrips');
  sel.textContent = '';

  // FILTRO TODOS (sólo UI del select de viajes)
  const ogFiltro = document.createElement('optgroup');
  ogFiltro.label = 'FILTRO';
  ogFiltro.appendChild(new Option('TODOS', 'all'));
  sel.appendChild(ogFiltro);

  // VIAJES
  const ogTrips = document.createElement('optgroup');
  ogTrips.label = 'VIAJES';
  state.ordenados.forEach((g, i) => {
    const name = g.nombreGrupo || g.aliasGrupo || g.id;
    const code = (g.numeroNegocio || '') + (g.identificador ? '-' + g.identificador : '');
    const opt = new Option(
      `${(g.destino || '').toUpperCase()} · ${(name || '').toUpperCase()} (${code}) | IDA: ${dmy(
        g.fechaInicio || '',
      )}  VUELTA: ${dmy(g.fechaFin || '')}`,
      `trip:${i}`,
    );
    ogTrips.appendChild(opt);
  });
  sel.appendChild(ogTrips);
  sel.value = `trip:${state.idx}`;

  p.querySelector('#btnPrev').onclick = async () => {
    const list = getFilteredList();
    if (!list.length) return;

    const cur = state.ordenados[state.idx]?.id;
    const j = list.findIndex((g) => g.id === cur);
    const j2 = Math.max(0, j - 1);
    const targetId = list[j2].id;

    state.idx = state.ordenados.findIndex((g) => g.id === targetId);
    await renderOneGroup(state.ordenados[state.idx]);
    sel.value = `trip:${state.idx}`;
  };

  p.querySelector('#btnNext').onclick = async () => {
    const list = getFilteredList();
    if (!list.length) return;

    const cur = state.ordenados[state.idx]?.id;
    const j = list.findIndex((g) => g.id === cur);
    const j2 = Math.min(list.length - 1, j + 1);
    const targetId = list[j2].id;

    state.idx = state.ordenados.findIndex((g) => g.id === targetId);
    await renderOneGroup(state.ordenados[state.idx]);
    sel.value = `trip:${state.idx}`;
  };

  sel.onchange = async () => {
    const v = sel.value || '';
    if (v === 'all') {
      state.filter = { type: 'all', value: null };
      renderStatsFiltered();
      sel.value = `trip:${state.idx}`;
    } else if (v.startsWith('trip:')) {
      state.idx = Number(v.slice(5)) || 0;
      await renderOneGroup(state.ordenados[state.idx]);
    }
  };

  // Botón imprimir sólo visible a STAFF
  if (state.is) {
    const btn = document.getElementById('btnPrintVch');
    if (btn) {
      btn.textContent = 'IMPRIMIR DESPACHO';
      btn.onclick = async () => {
        try {
          const g = state.ordenados[state.idx];
          await preparePrintForGroup(g); // deja listo #print-block
          window.print();
        } catch (e) {
          console.error('[PRINT] error', e);
          alert('No se pudo preparar el despacho para imprimir.');
        }
      };
    }
  } else {
    const btn = document.getElementById('btnPrintVch');
    if (btn) btn.style.display = 'none';
  }
}

/* ====== VISTA GRUPO ====== */
async function renderOneGroup(g, preferDate) {
  const cont = document.getElementById('grupos');
  if (!cont) return;
  cont.innerHTML = '';

  if (!g) {
    cont.innerHTML = '<p class="muted">NO HAY VIAJES.</p>';
    return;
  }

  localStorage.setItem('rt_last_group', g.id);

  const name = g.nombreGrupo || g.aliasGrupo || g.id;
  const code = (g.numeroNegocio || '') + (g.identificador ? '-' + g.identificador : '');
  const rango = `${dmy(g.fechaInicio || '')} — ${dmy(g.fechaFin || '')}`;

  /* ——— VIAJE / PAX REAL ——— */
  const paxPlan = paxOf(g);
  const real = paxRealOf(g);
  const { A: A_real, E: E_real } = paxBreakdown(g);
  const isStartDay = isToday(g.fechaInicio);
  const viaje = g.viaje || {};
  const viajeEstado =
    viaje.estado || (viaje.fin?.at ? 'FINALIZADO' : viaje.inicio?.at ? 'EN_CURSO' : 'PENDIENTE');
  const started = viajeEstado === 'EN_CURSO' || !!viaje.inicio?.at;
  const finished = viajeEstado === 'FINALIZADO' || !!viaje.fin?.at;

  const header = document.createElement('div');
  header.className = 'group-card';

  const topInfo = `
    <h3>${(name || '').toUpperCase()} · CÓDIGO: (${code})</h3>
    <div class="grid-mini">
      <div class="lab">DESTINO</div><div>${(g.destino || '—').toUpperCase()}</div>
      <div class="lab">GRUPO</div><div>${(name || '').toUpperCase()}</div>
      <div class="lab">PAX TOTAL</div>
      <div>${fmtPaxPlan(paxPlan, g)}${real ? ` <span class="muted">(A:${A_real} · E:${E_real})</span>` : ''}</div>
      <div class="lab">PROGRAMA</div><div>${(g.programa || '—').toUpperCase()}</div>
      <div class="lab">FECHAS</div><div>${rango}</div>
    </div>

    <div class="rowflex" style="margin-top:.6rem;gap:.5rem;flex-wrap:wrap">
      <input id="searchTrips" type="text" placeholder="BUSCADOR EN RESUMEN, ITINERARIO Y GASTOS..." style="flex:1"/>
    </div>
  `;

  // Botón INICIO (full width, verde)
  const btnInicioHtml = !started
    ? `<button id="btnInicioViaje" class="btn ok" style="width:100%;"${
        isStartDay ? '' : ' title="No es el día de inicio. Se pedirá confirmación."'
      }>INICIO DE VIAJE</button>`
    : '';

  // Botón RESTABLECER (STAFF + viaje iniciado) – gris, full width, debajo del inicio
  const btnResetInicioHtml =
    state.is && started && !finished
      ? `<button id="btnResetInicio" class="btn" style="width:100%;background:#64748b;color:#fff;">RESTABLECER</button>`
      : '';

  // Botón TERMINAR (si está en curso)
  const btnTerminarHtml =
    started && !finished
      ? `<button id="btnTerminoViaje" class="btn warn" style="width:100%;">TERMINAR VIAJE</button>`
      : '';

  // Info finalizado + botón reabrir cierre (STAFF)
  const finHtml = finished
    ? `
        <div class="muted">VIAJE FINALIZADO${
          viaje?.fin?.rendicionOk ? ' · RENDICIÓN HECHA' : ''
        }${viaje?.fin?.boletaOk ? ' · BOLETA ENTREGADA' : ''}</div>
        ${state.is ? `<button id="btnReabrirCierre" class="btn sec">RESTABLECER CIERRE</button>` : ''}
      `
    : '';

  header.innerHTML = `
    ${topInfo}
    <div class="rowflex" style="margin-top:.4rem;gap:.5rem;align-items:stretch;flex-wrap:wrap;flex-direction:column">
      ${btnInicioHtml}
      ${btnResetInicioHtml}
      ${btnTerminarHtml}
      ${finHtml}
    </div>

    <!-- HISTORIAL DEL VIAJE -->
    <div id="viajeHistoryBox" class="act" style="margin-top:.6rem">
      <h4>HISTORIAL DEL VIAJE</h4>
      <div class="muted">CARGANDO…</div>
    </div>
  `;

  cont.appendChild(header);

  // Handlers (únicos)
  const btnIV = header.querySelector('#btnInicioViaje');
  if (btnIV) btnIV.onclick = () => openInicioViajeModal(g);

  const btnTV = header.querySelector('#btnTerminoViaje');
  if (btnTV) btnTV.onclick = () => openTerminoViajeModal(g);

  const btnRY = header.querySelector('#btnReabrirCierre');
  if (btnRY) btnRY.onclick = () => staffReopenCierre(g);

  // RESTABLECER (antes "Restablecer inicio")
  const btnR0 = header.querySelector('#btnResetInicio');
  if (btnR0) btnR0.onclick = async () => { await staffResetInicio(g); };

  const histBox = header.querySelector('#viajeHistoryBox');
  renderViajeHistory(g, histBox);

  const tabs = document.createElement('div');
  tabs.innerHTML = `
    <div style="display:flex;gap:.5rem;margin:.6rem 0">
      <button id="tabResumen" class="btn sec">RESUMEN</button>
      <button id="tabItin"    class="btn sec">ITINERARIO</button>
      <button id="tabFin"     class="btn sec">FINANZAS</button>
    </div>
    <div id="paneResumen"></div>
    <div id="paneItin" style="display:none"></div>
    <div id="paneFin"  style="display:none"></div>
  `;
  cont.appendChild(tabs);

  const paneResumen = tabs.querySelector('#paneResumen');
  const paneItin = tabs.querySelector('#paneItin');
  const paneFin = tabs.querySelector('#paneFin');
  const btnResumen = tabs.querySelector('#tabResumen');
  const btnItin = tabs.querySelector('#tabItin');
  const btnFin = tabs.querySelector('#tabFin');

  const setTabLabel = (btn, base, n) => {
    const q = (state.groupQ || '').trim();
    btn.textContent = q && n > 0 ? `${base} (${n})` : base;
  };

  const show = (w) => {
    paneResumen.style.display = w === 'resumen' ? '' : 'none';
    paneItin.style.display = w === 'itin' ? '' : 'none';
    paneFin.style.display = w === 'fin' ? '' : 'none';
  };

  btnResumen.onclick = () => show('resumen');
  btnItin.onclick = () => show('itin');
  btnFin.onclick = () => show('fin');

  // Render y contadores
  const resumenHits = await renderResumen(g, paneResumen);
  const itinHits = renderItinerario(g, paneItin, preferDate);
  const finHits = await renderFinanzas(g, paneFin); // NUEVO

  setTabLabel(btnResumen, 'RESUMEN', resumenHits);
  setTabLabel(btnItin, 'ITINERARIO', itinHits);
  setTabLabel(btnFin, 'FINANZAS', finHits);

  show('resumen');

  // BÚSQUEDA INTERNA
  const input = header.querySelector('#searchTrips');
  input.value = state.groupQ || '';
  let tmr = null;

  input.oninput = () => {
    clearTimeout(tmr);
    tmr = setTimeout(async () => {
      state.groupQ = input.value || '';

      const active =
        paneItin.style.display !== 'none' ? 'itin' : paneFin.style.display !== 'none' ? 'fin' : 'resumen';

      const r = await renderResumen(g, paneResumen);
      const i = renderItinerario(
        g,
        paneItin,
        localStorage.getItem('rt_last_date_' + g.id) || preferDate,
      );
      const f = await renderFinanzas(g, paneFin);

      setTabLabel(btnResumen, 'RESUMEN', r);
      setTabLabel(btnItin, 'ITINERARIO', i);
      setTabLabel(btnFin, 'FINANZAS', f);

      show(active);
    }, 180);
  };

  ensurePrintDOM();
  await preparePrintForGroup(g);
}

async function renderViajeHistory(g, box) {
  try {
    const qs = await getDocs(
      query(collection(db, 'grupos', g.id, 'viajeLog'), orderBy('ts', 'desc'), limit(50)),
    );

    const head = '<h4>HISTORIAL DEL VIAJE</h4>';
    if (!qs.size) {
      box.innerHTML = head + '<div class="muted">SIN REGISTROS.</div>';
      return;
    }

    const frag = document.createDocumentFragment();
    const ttl = document.createElement('h4');
    ttl.textContent = 'HISTORIAL DEL VIAJE';
    frag.appendChild(ttl);

    qs.forEach((d) => {
      const x = d.data() || {};
      const quien = (x.by || x.byEmail || x.byUid || '').toString().toUpperCase();
      const cuando = x.ts?.seconds ? fmtChile(new Date(x.ts.seconds * 1000)) : '';
      const accion = (x.type || '').toString().replace(/_/g, ' ').toUpperCase();
      const txt = (x.text || '').toString().toUpperCase();

      const div = document.createElement('div');
      div.className = 'meta';
      div.textContent = `• ${accion}${txt ? ` — ${txt}` : ''} — ${quien}${
        cuando ? ` · ${cuando}` : ''
      }`;
      frag.appendChild(div);
    });

    box.innerHTML = '';
    box.appendChild(frag);
  } catch (e) {
    console.error(e);
    box.innerHTML = '<h4>HISTORIAL DEL VIAJE</h4><div class="muted">NO SE PUDO CARGAR.</div>';
  }
}

async function reloadGroupAndRender(groupId) {
  try {
    const snap = await getDoc(doc(db, 'grupos', groupId));
    if (!snap.exists()) {
      await renderOneGroup(null);
      return;
    }

    const raw = { id: snap.id, ...snap.data() };

    const g2 = {
      ...raw,
      fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
      fechaFin: toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
      itinerario: normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
      serviciosEstado: raw.serviciosEstado || {},
      numeroNegocio: String(
        raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || snap.id,
      ),
      identificador: String(raw.identificador || raw.codigo || ''),
    };

    // Si el campo 'itinerario' viene vacío, carga desde la subcolección
    await ensureItinerarioLoaded(g2);

    // Refresca en state (por si cambias de viaje luego)
    const idx = state.ordenados.findIndex((x) => x && x.id === g2.id);
    if (idx >= 0) {
      state.ordenados[idx] = g2;
      const j = state.grupos.findIndex((x) => x && x.id === g2.id);
      if (j >= 0) state.grupos[j] = g2;
      state.idx = idx;
    }

    await renderOneGroup(g2);
  } catch (e) {
    console.error('reloadGroupAndRender', e);
    await renderOneGroup(state.ordenados[state.idx] || null);
  }
}

/* ====== RESUMEN (HOTEL + VUELOS) ====== */
async function renderResumen(g, pane){
  pane.innerHTML = '<div class="muted">CARGANDO…</div>';

  const wrap = document.createElement('div');
  wrap.style.cssText = 'display:grid;gap:.8rem';
  pane.innerHTML = '';

  const qRaw = (state.groupQ || '').trim();
  const q = norm(qRaw);
  let hits = 0;

  // HOTEL(ES)
  const hotelBox = document.createElement('div');
  hotelBox.className = 'act';
  hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">BUSCANDO…</div>';
  wrap.appendChild(hotelBox);

  // VUELOS
  const vuelosBox = document.createElement('div');
  vuelosBox.className = 'act';
  vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">BUSCANDO…</div>';
  wrap.appendChild(vuelosBox);

  pane.appendChild(wrap);

  // ===== HOTELES (múltiples) =====
  try {
    const hoteles = await loadHotelesInfo(g);
    D_HOTEL('RENDERRESUMEN -> HOTELES[]', hoteles);

    if (!hoteles.length) {
      hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">SIN ASIGNACIÓN.</div>';
    } else {
      // FIX: sin backticks anidados
      hotelBox.innerHTML = `<h4>HOTELES ${hoteles.length > 1 ? '(' + hoteles.length + ')' : ''}</h4>`;

      const qn = norm((state.groupQ || '').trim());
      let rendered = 0;

      hoteles.forEach((h, idx) => {
        const H          = h.hotel || {};
        const nombre     = String(h.hotelNombre || H.nombre || '').toUpperCase();
        const direccion  = (H.direccion || h.direccion || '').toUpperCase();
        const cTelefono  = (H.contactoTelefono || '').toUpperCase();
        const status     = (h.status || '').toString().toUpperCase();
        const ciISO      = toISO(h.checkIn);
        const coISO      = toISO(h.checkOut);
        const noches     = (h.noches !== '' && h.noches != null) ? Number(h.noches) : '';

        const est        = h.estudiantes || { F:0, M:0, O:0 };
        const estTot     = Number(h.estudiantesTotal ?? (est.F + est.M + est.O));
        const adu        = h.adultos || { F:0, M:0, O:0 };
        const aduTot     = Number(h.adultosTotal ?? (adu.F + adu.M + adu.O));

        const hab        = h.habitaciones || {};
        const habLine    = (hab.singles != null || hab.dobles != null || hab.triples != null || hab.cuadruples != null)
          ? `HABITACIONES: ${[
              (hab.singles   != null ? `SINGLES: ${hab.singles}`     : ''),
              (hab.dobles    != null ? `DOBLES: ${hab.dobles}`       : ''),
              (hab.triples   != null ? `TRIPLES: ${hab.triples}`     : ''),
              (hab.cuadruples!= null ? `CUÁDRUPLES: ${hab.cuadruples}` : '')
            ].filter(Boolean).join(' · ')}`
          : '';

        const contactoLine = [cTelefono].filter(Boolean).join(' · ');

        const txtMatch = norm([
          nombre, direccion, contactoLine, status,
          dmy(ciISO), dmy(coISO),
          `estudiantes f ${est.F} m ${est.M} o ${est.O} total ${estTot}`,
          `adultos f ${adu.F} m ${adu.M} o ${adu.O} total ${aduTot}`,
          habLine
        ].join(' '));

        const matched = qn ? txtMatch.includes(qn) : true;
        if (qn && matched) hits += 1;
        if (!matched) return;

        const block = document.createElement('div');
        block.className = 'meta';

        // FIX: separar CHECK-IN/OUT y NOCHES en bloques distintos (evita </div> desbalanceado)
        block.innerHTML = `
          <div class="card" style="margin:.4rem 0;">
            ${nombre ? `<div class="meta"><strong>NOMBRE:</strong> ${nombre}</div>` : ''}
            <div class="meta"><strong>CHECK-IN/OUT:</strong> ${dmy(ciISO)} — ${dmy(coISO)}</div>
            ${noches !== '' ? `<div class="meta"><strong>NOCHES:</strong> ${noches}</div>` : ''}
            ${status ? `<div class="meta"><strong>ESTADO:</strong> ${status}</div>` : ''}
            <div class="meta"><strong>ESTUDIANTES:</strong> F: ${est.F || 0} · M: ${est.M || 0} · O: ${est.O || 0} (TOTAL ${estTot || 0}) · <strong>ADULTOS:</strong> F: ${adu.F || 0} · M: ${adu.M || 0} · O: ${adu.O || 0} (TOTAL ${aduTot || 0})</div>
            ${habLine ? `<div class="meta">${habLine}</div>` : ''}
            ${h.coordinadores != null ? `<div class="meta"><strong>COORDINADORES:</strong> ${String(h.coordinadores).toUpperCase()}</div>` : ''}
            ${h.conductores   != null ? `<div class="meta"><strong>CONDUCTORES:</strong> ${String(h.conductores).toUpperCase()}</div>`   : ''}
            ${direccion ? `<div class="meta"><strong>DIRECCIÓN:</strong> ${direccion}</div>` : ''}
            ${contactoLine ? `<div class="meta"><strong>TELÉFONO:</strong> ${contactoLine}</div>` : ''}
          </div>
        `;
        hotelBox.appendChild(block);

        if (idx < hoteles.length - 1) {
          const hr = document.createElement('div');
          hr.style.cssText = 'border-top:1px dashed var(--line);opacity:.55;margin:.5rem 0;';
          hotelBox.appendChild(hr);
        }
        rendered++;
      });

      if ((state.groupQ || '').trim() && rendered === 0) {
        hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">SIN COINCIDENCIAS.</div>';
      }
    }
  } catch (e) {
    console.error(e);
    D_HOTEL('ERROR RENDERRESUMEN HOTELES', e?.code || e, e?.message || '');
    hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">ERROR AL CARGAR.</div>';
  }

  // ===== VUELOS =====
  try {
    const vuelosRaw = await loadVuelosInfo(g);
    const vuelos = vuelosRaw.map(normalizeVuelo);

    // Filtro: incluye horarios top-level, horarios por tramo y bus
    const flt = (!q) ? vuelos : vuelos.filter(v => {
      const sTop = [
        v.numero, v.proveedor, v.origen, v.destino,
        toISO(v.fechaIda), toISO(v.fechaVuelta),
        v.presentacionIdaHora, v.vueloIdaHora, v.presentacionVueltaHora, v.vueloVueltaHora,
        v.idaHora, v.vueltaHora,
        v.tipoTransporte, v.tipoVuelo
      ].join(' ');
      const sTramos = (v.tramos || []).map(t => [
        t.aerolinea, t.numero, t.origen, t.destino,
        toISO(t.fechaIda), toISO(t.fechaVuelta),
        t.presentacionIdaHora, t.vueloIdaHora, t.presentacionVueltaHora, t.vueloVueltaHora
      ].join(' ')).join(' ');
      return norm(`${sTop} ${sTramos}`).includes(q);
    });
    if (q) hits += flt.length;

    if (!flt.length) {
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">SIN VUELOS.</div>';
    } else {
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4>';

      flt.forEach((v, i) => {
        const isAereo = (v.tipoTransporte || 'aereo') === 'aereo';
        const isRegMT = isAereo && v.tipoVuelo === 'regular' && Array.isArray(v.tramos) && v.tramos.length > 0;

        const numero  = (v.numero || (v.tramos?.[0]?.numero || '')).toString().toUpperCase();
        const empresa = (v.proveedor || (v.tramos?.[0]?.aerolinea || '')).toString().toUpperCase();
        const ruta    = [
          (v.origen || v.tramos?.[0]?.origen || ''),
          (v.destino || v.tramos?.slice(-1)?.[0]?.destino || '')
        ].map(x => (x || '').toUpperCase()).filter(Boolean).join(' — ');

        const ida    = dmy(toISO(v.fechaIda)    || toISO(v.tramos?.[0]?.fechaIda)               || '');
        const vuelta = dmy(toISO(v.fechaVuelta) || toISO(v.tramos?.slice(-1)?.[0]?.fechaVuelta) || '');

        const block = document.createElement('div');
        let extra = '';

        if (isRegMT) {
          // AÉREO REGULAR MULTITRAMO: listar tramos con horarios por tramo
          extra += `<div class="meta"><strong>TIPO:</strong> REGULAR · MULTITRAMO</div>`;
          extra += `<div class="card" style="margin:.4rem 0;">`;
          v.tramos.forEach((t, idxT) => {
            const idaLine = (t.presentacionIdaHora || t.vueloIdaHora)
              ? `IDA: ${dmy(toISO(t.fechaIda) || '')} ${t.presentacionIdaHora ? ' · PRESENTACIÓN ' + t.presentacionIdaHora : ''}${t.vueloIdaHora ? ' · VUELO ' + t.vueloIdaHora : ''}`
              : `IDA: ${dmy(toISO(t.fechaIda) || '')}`;

            const vtaLine = (t.presentacionVueltaHora || t.vueloVueltaHora)
              ? `REGRESO: ${dmy(toISO(t.fechaVuelta) || '')} ${t.presentacionVueltaHora ? ' · PRESENTACIÓN ' + t.presentacionVueltaHora : ''}${t.vueloVueltaHora ? ' · VUELO ' + t.vueloVueltaHora : ''}`
              : (toISO(t.fechaVuelta) ? `REGRESO: ${dmy(toISO(t.fechaVuelta))}` : '');

            extra += `
              <div class="meta" style="margin:.25rem 0;">
                <strong>TRAMO ${idxT + 1}:</strong> ${(t.aerolinea || '').toUpperCase()} ${(t.numero || '').toUpperCase()} — ${(t.origen || '').toUpperCase()} → ${(t.destino || '').toUpperCase()}
              </div>
              <div class="meta" style="margin-left:.5rem">${idaLine}</div>
              ${vtaLine ? `<div class="meta" style="margin-left:.5rem">${vtaLine}</div>` : ''}`;
          });
          extra += `</div>`;
        } else if (isAereo) {
          // AÉREO SIMPLE / CHARTER / REGULAR simple
          const l1 = (v.presentacionIdaHora || v.vueloIdaHora)
            ? `<div class="meta"><strong>IDA:</strong> ${v.presentacionIdaHora ? 'PRESENTACIÓN ' + v.presentacionIdaHora : ''}${v.vueloIdaHora ? (v.presentacionIdaHora ? ' · ' : '') + 'VUELO ' + v.vueloIdaHora : ''}</div>`
            : '';

          const l2 = (v.presentacionVueltaHora || v.vueloVueltaHora)
            ? `<div class="meta"><strong>REGRESO:</strong> ${v.presentacionVueltaHora ? 'PRESENTACIÓN ' + v.presentacionVueltaHora : ''}${v.vueloVueltaHora ? (v.presentacionVueltaHora ? ' · ' : '') + 'VUELO ' + v.vueloVueltaHora : ''}</div>`
            : '';

          const tipoTxt = v.tipoVuelo ? ` · ${(v.tipoVuelo || '').toString().toUpperCase()}` : '';
          extra += `<div class="meta"><strong>TIPO:</strong> AÉREO${tipoTxt}</div>${l1}${l2}`;
        } else {
          // TERRESTRE (BUS)
          extra += `<div class="meta"><strong>TIPO:</strong> TERRESTRE (BUS)</div>`;
          if (v.idaHora || v.vueltaHora){
            extra += `
              <div class="meta"><strong>SALIDA BUS (IDA):</strong> ${v.idaHora || '—'}</div>
              <div class="meta"><strong>REGRESO BUS:</strong> ${v.vueltaHora || '—'}</div>`;
          }
        }

        block.innerHTML = `
          <div class="meta"><strong>N° / SERVICIO:</strong> ${numero || '—'}</div>
          <div class="meta"><strong>EMPRESA:</strong> ${empresa || '—'}</div>
          <div class="meta"><strong>RUTA:</strong> ${ruta || '—'}</div>
          <div class="meta"><strong>IDA:</strong> ${ida || '—'}</div>
          <div class="meta"><strong>VUELTA:</strong> ${vuelta || '—'}</div>
          ${extra}
        `;
        vuelosBox.appendChild(block);

        if (i < flt.length - 1) {
          const hr = document.createElement('div');
          hr.style.cssText = 'border-top:1px dashed var(--line);opacity:.55;margin:.5rem 0;';
          vuelosBox.appendChild(hr);
        }
      });
    }
  } catch (e) {
    console.error(e);
    vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">ERROR AL CARGAR.</div>';
  }

  return hits;
}

/* ====== ÍNDICE DE HOTELES ====== */
async function ensureHotelesIndex(){
  if (state.cache.hoteles.loaded) return state.cache.hoteles;

  const byId   = new Map();
  const bySlug = new Map();
  const all    = [];

  const snap = await getDocs(collection(db,'hoteles'));
  snap.forEach(d => {
    const x    = d.data() || {};
    const docu = { id: d.id, ...x };
    const s    = norm(x.slug || x.nombre || d.id);
    byId.set(String(d.id), docu);
    if (s) bySlug.set(s, docu);
    all.push(docu);
  });

  state.cache.hoteles = { loaded: true, byId, bySlug, all };
  D_HOTEL('ÍNDICE HOTELES CARGADO', { count: all.length });
  return state.cache.hoteles;
}

/* ====== HOTELES: TODAS LAS ASIGNACIONES DE UN GRUPO ====== */
async function loadHotelesInfo(g){
  const groupDocId = String(g.id || '').trim();
  const groupNum   = String(g.numeroNegocio || '').trim();
  const cacheKey   = `hoteles:${groupDocId || groupNum}`;

  if (state.cache.hotel.has(cacheKey)) {
    D_HOTEL('CACHE HIT loadHotelesInfo', { cacheKey });
    return state.cache.hotel.get(cacheKey);
  }

  let cand = [];

  // Esquema “bueno”: grupoId === docId del grupo
  try {
    if (groupDocId) {
      const qs = await getDocs(query(collection(db,'hotelAssignments'), where('grupoId','==', groupDocId)));
      qs.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoId', e); }

  // Fallbacks legacy
  try {
    if (!cand.length && groupDocId) {
      const qs2 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoDocId','==', groupDocId)));
      qs2.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoDocId', e); }

  try {
    if (!cand.length && groupNum) {
      const qs3 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoNumero','==', groupNum)));
      qs3.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoNumero', e); }

  if (!cand.length){
    state.cache.hotel.set(cacheKey, []);
    return [];
  }

  // Orden por check-in asc
  cand.sort((a,b) => (toISO(a.checkIn) || '').localeCompare(toISO(b.checkIn) || ''));

  // Resolver docs hotel
  const { byId, bySlug, all } = await ensureHotelesIndex();

  function pickHotelDoc(asig){
    const tryIds = [];
    if (asig?.hotelId)   tryIds.push(String(asig.hotelId));
    if (asig?.hotelDocId)tryIds.push(String(asig.hotelDocId));
    if (asig?.hotel?.id) tryIds.push(String(asig.hotel.id));
    if (asig?.hotelRef && typeof asig.hotelRef === 'object' && 'id' in asig.hotelRef) {
      tryIds.push(String(asig.hotelRef.id));
    }
    if (asig?.hotelPath && typeof asig.hotelPath === 'string') {
      const m = asig.hotelPath.match(/hoteles\/([^/]+)/i);
      if (m) tryIds.push(m[1]);
    }
    for (const id of tryIds){
      if (byId.has(id)) return byId.get(id);
    }

    const s    = norm(asig?.nombre || asig?.hotelNombre || '');
    const dest = norm(g.destino || '');
    if (s && bySlug.has(s)) return bySlug.get(s);
    if (s){
      const candidatos = [];
      for (const [slugName, docu] of bySlug){
        if (slugName.includes(s) || s.includes(slugName)) candidatos.push(docu);
      }
      if (candidatos.length === 1) return candidatos[0];
      return candidatos.find(d => norm(d.destino || d.ciudad || '') === dest) || candidatos[0] || null;
    }

    const ci = toISO(asig.checkIn), co = toISO(asig.checkOut);
    const overlapDays = (A,B,C,D) => {
      if(!A||!B||!C||!D) return 0;
      const s = Math.max(new Date(A).getTime(), new Date(C).getTime());
      const e = Math.min(new Date(B).getTime(), new Date(D).getTime());
      return (e >= s) ? Math.round((e - s) / 86400000) + 1 : 0;
    };

    let candidatos = all.filter(h => norm(h.destino || h.ciudad || '') === dest);
    if (ci && co){
      candidatos = candidatos
        .map(h => ({ h, ov: overlapDays(ci, co, toISO(h.fechaInicio), toISO(h.fechaFin)) }))
        .sort((a,b)=> b.ov - a.ov)
        .map(x => x.h);
    }
    return candidatos[0] || null;
  }

  const out = cand.map(a => {
    const H      = pickHotelDoc(a);
    const ciISO  = toISO(a.checkIn);
    const coISO  = toISO(a.checkOut);
    const noches = (typeof a.noches === 'number')
      ? a.noches
      : (ciISO && coISO ? Math.max(0, (new Date(coISO) - new Date(ciISO)) / 86400000) : '');

    return {
      ...a,
      hotel: H,
      hotelNombre: a?.hotelNombre || a?.nombre || H?.nombre || '',
      checkIn: ciISO,
      checkOut: coISO,
      noches
    };
  });

  state.cache.hotel.set(cacheKey, out);
  if (groupNum) state.cache.hotel.set(groupNum, out);
  return out;
}

/* ====== (LEGACY) HOTEL: UNA ASIGNACIÓN MEJOR — compat ====== */
async function loadHotelInfo(g){
  const groupDocId = String(g.id || '').trim();
  const groupNum   = String(g.numeroNegocio || '').trim();
  const cacheKey   = groupDocId || groupNum || '';

  if (cacheKey && state.cache.hotel.has(cacheKey)) {
    D_HOTEL('CACHE HIT LOADHOTELINFO', { cacheKey, groupDocId, groupNum });
    return state.cache.hotel.get(cacheKey);
  }
  D_HOTEL('INI LOADHOTELINFO', { groupDocId, groupNum, grupoDoc: g.id, destino: g.destino });

  let cand = [];
  try {
    if (groupDocId) {
      const qs1 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoId', '==', groupDocId)));
      qs1.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERROR query grupoId', e); }

  try {
    if (!cand.length && groupDocId) {
      const qs2 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoDocId', '==', groupDocId)));
      qs2.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERROR query grupoDocId', e); }

  try {
    if (!cand.length && groupNum) {
      const qs3 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoNumero', '==', groupNum)));
      qs3.forEach(d => cand.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (e) { D_HOTEL('ERROR query grupoNumero', e); }

  if (!cand.length) {
    if (cacheKey) state.cache.hotel.set(cacheKey, null);
    D_HOTEL('SIN ASIGNACIÓN → NULL');
    return null;
  }

  let elegido = null, score = 1e15;
  const rangoIni = toISO(g.fechaInicio), rangoFin = toISO(g.fechaFin);
  for (const x of cand) {
    const ci = toISO(x.checkIn), co = toISO(x.checkOut);
    let s = 5e14;
    if (ci && co && rangoIni && rangoFin) {
      const overlap = !(co < rangoIni || ci > rangoFin);
      s = overlap ? 0 : Math.abs(new Date(ci) - new Date(rangoIni));
    }
    if (s < score) { score = s; elegido = x; }
  }
  D_HOTEL('ASIGNACIÓN ELEGIDA', elegido);

  const { byId, bySlug, all } = await ensureHotelesIndex();
  let hotelDoc = null;

  const tryIds = [];
  if (elegido?.hotelId)    tryIds.push(String(elegido.hotelId));
  if (elegido?.hotelDocId) tryIds.push(String(elegido.hotelDocId));
  if (elegido?.hotel?.id)  tryIds.push(String(elegido.hotel.id));
  if (elegido?.hotelRef && typeof elegido.hotelRef === 'object' && 'id' in elegido.hotelRef) {
    tryIds.push(String(elegido.hotelRef.id));
  }
  if (elegido?.hotelPath && typeof elegido.hotelPath === 'string') {
    const m = elegido.hotelPath.match(/hoteles\/([^/]+)/i);
    if (m) tryIds.push(m[1]);
  }
  for (const id of tryIds) {
    if (byId.has(id)) { hotelDoc = byId.get(id); D_HOTEL('MATCH ÍNDICE BYID', id); break; }
    try {
      const hd = await getDoc(doc(db,'hoteles', id));
      if (hd.exists()) { hotelDoc = { id: hd.id, ...(hd.data() || {}) }; break; }
    } catch (e) { D_HOTEL('ERROR GETDOC HOTELES por ID', id, e); }
  }

  if (!hotelDoc) {
    const s    = norm(elegido?.nombre || elegido?.hotelNombre || '');
    const dest = norm(g.destino || '');
    if (s && bySlug.has(s)) {
      hotelDoc = bySlug.get(s);
    } else if (s) {
      const candidatos = [];
      for (const [slugName, docu] of bySlug) {
        if (slugName.includes(s) || s.includes(slugName)) candidatos.push(docu);
      }
      hotelDoc = candidatos.length === 1
        ? candidatos[0]
        : (candidatos.find(d => norm(d.destino || d.ciudad || '') === dest) || candidatos[0] || null);
      D_HOTEL('MATCH FUZZY', { candidatos, elegido: hotelDoc });
    }
  }

  if (!hotelDoc) {
    const dest = norm(g.destino || '');
    const ci = toISO(elegido?.checkIn), co = toISO(elegido?.checkOut);
    const overlapDays = (A,B,C,D) => {
      if(!A||!B||!C||!D) return 0;
      const s = Math.max(new Date(A).getTime(), new Date(C).getTime());
      const e = Math.min(new Date(B).getTime(), new Date(D).getTime());
      return (e >= s) ? Math.round((e - s) / 86400000) + 1 : 0;
    };
    let candidatos = all.filter(h => norm(h.destino || h.ciudad || '') === dest);
    if (ci && co) {
      candidatos = candidatos
        .map(h => ({ h, ov: overlapDays(ci, co, toISO(h.fechaInicio), toISO(h.fechaFin)) }))
        .sort((a,b)=> b.ov - a.ov)
        .map(x => x.h);
    }
    hotelDoc = candidatos[0] || null;
    D_HOTEL('HEURÍSTICA DESTINO/FECHAS', { elegido: hotelDoc, ci, co });
  }

  const out = {
    ...elegido,
    hotel: hotelDoc,
    hotelNombre: elegido?.nombre || elegido?.hotelNombre || hotelDoc?.nombre || ''
  };

  if (groupDocId) state.cache.hotel.set(groupDocId, out);
  if (groupNum)   state.cache.hotel.set(groupNum,   out);

  D_HOTEL('OUT LOADHOTELINFO', out);
  return out;
}

function normalizeVuelo(v){
  const get = (...keys)=>{
    for (const k of keys){
      const val = k.split('.').reduce((acc, part)=> (acc && acc[part] !== undefined) ? acc[part] : undefined, v);
      if (val !== undefined && val !== null && val !== '') return val;
    }
    return '';
  };

  const numero    = get('numero','nro','numVuelo','vuelo','flightNumber','codigo','code');
  const proveedor = get('proveedor','empresa','aerolinea','compania');

  // NUEVO: tipo transporte / tipo vuelo
  const tipoTransporte = (String(get('tipoTransporte')) || 'aereo').toLowerCase() || 'aereo';
  const tipoVuelo      = (tipoTransporte === 'aereo')
    ? (String(get('tipoVuelo') || 'charter').toLowerCase())
    : '';

  // Top-level (aéreo simple/charter o regular simple)
  const presentacionIdaHora    = get('presentacionIdaHora');
  const vueloIdaHora           = get('vueloIdaHora');
  const presentacionVueltaHora = get('presentacionVueltaHora');
  const vueloVueltaHora        = get('vueloVueltaHora');

  // Terrestre (bus)
  const idaHora    = get('idaHora');
  const vueltaHora = get('vueltaHora');

  const origen      = get('origen','desde','from','salida.origen','salida.iata','origenIATA','origenSigla','origenCiudad');
  const destino     = get('destino','hasta','to','llegada.destino','llegada.iata','destinoIATA','destinoSigla','destinoCiudad');
  const fechaIda    = get('fechaIda','ida','salida.fecha','fechaSalida','fecha_ida','fecha');
  const fechaVuelta = get('fechaVuelta','vuelta','regreso.fecha','fechaRegreso','fecha_vuelta');

  // Tramos (aéreo regular multitramo) con horarios por tramo
  const trRaw  = Array.isArray(v.tramos) ? v.tramos : [];
  const tramos = trRaw.map(t => ({
    aerolinea:               String(t.aerolinea || '').toUpperCase(),
    numero:                  String(t.numero    || '').toUpperCase(),
    origen:                  String(t.origen    || '').toUpperCase(),
    destino:                 String(t.destino   || '').toUpperCase(),
    fechaIda:                t.fechaIda    || '',
    fechaVuelta:             t.fechaVuelta || '',
    presentacionIdaHora:     t.presentacionIdaHora     || '',
    vueloIdaHora:            t.vueloIdaHora            || '',
    presentacionVueltaHora:  t.presentacionVueltaHora  || '',
    vueloVueltaHora:         t.vueloVueltaHora         || '',
  }));

  // Reserva (si la manejas desde viajes.js)
  const reservaEstado      = (v.reservaEstado || '').toString().toLowerCase();
  const reservaFechaLimite = get('reservaFechaLimite');

  return {
    numero, proveedor,
    tipoTransporte, tipoVuelo,
    origen, destino, fechaIda, fechaVuelta,
    presentacionIdaHora, vueloIdaHora, presentacionVueltaHora, vueloVueltaHora,
    idaHora, vueltaHora,
    tramos,
    reservaEstado, reservaFechaLimite
  };
}

/* ====== VUELOS (BÚSQUEDA ROBUSTA POR DOCID Y NUM NEGOCIO) ====== */
async function loadVuelosInfo(g){
  const docId = String(g.id || '').trim();
  const num   = String(g.numeroNegocio || '').trim();

  const cacheKey = `vuelos:${docId || num}`;
  if (state.cache.vuelos.has(cacheKey)) return state.cache.vuelos.get(cacheKey);

  let found = [];

  // 1) Esquema: campo grupoIds = array de docIds
  try {
    if (docId) {
      const qs1 = await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains', docId)));
      qs1.forEach(d => found.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (_) {}

  // 2) Legacy: grupoIds = array de numeros de negocio
  try {
    if (!found.length && num) {
      const qs2 = await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains', num)));
      qs2.forEach(d => found.push({ id: d.id, ...(d.data() || {}) }));
    }
  } catch (_) {}

  // 3) Generalista: recorrer y chequear patrones frecuentes
  if (!found.length) {
    const ss = await getDocs(collection(db,'vuelos'));
    ss.forEach(d => {
      const v = d.data() || {};
      let match = false;

      // a) v.grupos: array de strings (docId o número)
      if (!match && Array.isArray(v.grupos)) {
        match = v.grupos.some(x => {
          if (typeof x === 'string') {
            return (docId && x === docId) || (num && x === num);
          }
          if (x && typeof x === 'object') {
            // b) v.grupos: array de objetos { id?, numeroNegocio?, grupoId? }
            const xid  = String(x.id || x.grupoId || '').trim();
            const xnum = String(x.numeroNegocio || x.numNegocio || '').trim();
            return (docId && xid && xid === docId) || (num && xnum && xnum === num);
          }
          return false;
        });
      }

      // c) campos sueltos: grupoId / grupoNumero en raíz
      if (!match) {
        const rootId  = String(v.grupoId || '').trim();
        const rootNum = String(v.grupoNumero || v.numeroNegocio || '').trim();
        match = (docId && rootId && rootId === docId) || (num && rootNum && rootNum === num);
      }

      if (match) found.push({ id: d.id, ...v });
    });
  }

  // Ordena por fecha de ida
  found.sort((a,b) => (toISO(a.fechaIda) || '').localeCompare(toISO(b.fechaIda) || ''));

  state.cache.vuelos.set(cacheKey, found);
  return found;
}

/* ====== ITINERARIO + BITÁCORA + VOUCHERS ====== */
function getSavedAsistencia(grupo, fechaISO, actividad){
  const byDate = grupo?.asistencias?.[fechaISO];
  if (!byDate) return null;

  const key = slug(actividad || 'actividad');
  if (Object.prototype.hasOwnProperty.call(byDate, key)) return byDate[key];

  for (const k of Object.keys(byDate)) if (slug(k) === key) return byDate[k];
  return null;
}

function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data){
  const key = slug(actividad || 'actividad');
  (grupo.asistencias ||= {});
  (grupo.asistencias[fechaISO] ||= {});
  grupo.asistencias[fechaISO][key] = data;
}

function calcPlan(actividad, grupo){
  const a  = actividad || {};
  const ad = Number(a.adultos || 0);
  const es = Number(a.estudiantes || 0);
  const s  = ad + es;
  if (s > 0) return s;
  const base = (grupo && (grupo.cantidadgrupo != null ? grupo.cantidadgrupo : grupo.pax));
  return Number(base || 0);
}

function countItinHits(g, qNorm){
  if (!qNorm) return 0;
  let c = 0;
  const map = g.itinerario || {};
  for (const f of Object.keys(map)){
    const arr = Array.isArray(map[f]) ? map[f] : [];
    c += arr.filter(a => norm([a.actividad, a.proveedor, a.horaInicio, a.horaFin].join(' ')).includes(qNorm)).length;
  }
  return c;
}

function renderItinerario(g, pane, preferDate){
  pane.innerHTML = '';
  const map = g?.itinerario || {};
  if (!map || typeof map !== 'object' || Object.keys(map).length === 0){
    pane.innerHTML = '<div class="muted">SIN ITINERARIO CARGADO.</div>';
    return 0;
  }

  const qNorm = norm(state.groupQ || '');
  const fechas = rangoFechas(g.fechaInicio, g.fechaFin);
  if (!fechas.length){
    pane.innerHTML = '<div class="muted">FECHAS NO DEFINIDAS.</div>';
    return 0;
  }

  const pillsWrap = document.createElement('div'); pillsWrap.className = 'date-pills'; pane.appendChild(pillsWrap);
  const actsWrap  = document.createElement('div'); actsWrap.className  = 'acts';       pane.appendChild(actsWrap);

  const hoy = toISO(new Date());
  let startDate = preferDate || ((hoy >= fechas[0] && hoy <= fechas.at(-1)) ? hoy : fechas[0]);

  const fechasMostrar = (!qNorm) ? fechas : fechas.filter(f => {
    const arr = (g.itinerario && g.itinerario[f]) ? g.itinerario[f] : [];
    return arr.some(a => norm([a.actividad, a.proveedor, a.horaInicio, a.horaFin].join(' ')).includes(qNorm));
  });

  if (!fechasMostrar.length){
    actsWrap.innerHTML = '<div class="muted">SIN COINCIDENCIAS PARA EL ITINERARIO.</div>';
    return 0;
  }
  if (!fechasMostrar.includes(startDate)) startDate = fechasMostrar[0];

  fechasMostrar.forEach(f => {
    const pill = document.createElement('div');
    pill.className = 'pill' + (f === startDate ? ' active' : '');
    pill.textContent = dmy(f);
    pill.title = f;
    pill.dataset.fecha = f;
    pill.onclick = () => {
      pillsWrap.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      renderActs(g, f, actsWrap);
      localStorage.setItem('rt_last_date_' + g.id, f);
    };
    pillsWrap.appendChild(pill);
  });

  const last = localStorage.getItem('rt_last_date_' + g.id);
  if (last && fechasMostrar.includes(last)) startDate = last;

  renderActs(g, startDate, actsWrap);

  // devolver cantidad de coincidencias totales en ITINERARIO
  return countItinHits(g, qNorm);
}

async function renderActs(grupo, fechaISO, cont){
  cont.innerHTML = '';

  // Banner superior: Alojamiento del día + aviso último día
  try {
    const top = document.createElement('div');
    top.className = 'act';

    const hoteles = await loadHotelesInfo(grupo) || [];
    const matchHotel = hoteles.find(h => {
      const ci = toISO(h.checkIn);
      const co = toISO(h.checkOut);
      return ci && co && (fechaISO >= ci) && (fechaISO < co);
    });

    const hotelName = (matchHotel?.hotelNombre || matchHotel?.hotel?.nombre || '').toString().toUpperCase();
    const isLastDay = (toISO(grupo.fechaFin) === fechaISO);

    let line = '';
    if (hotelName) line = `ALOJAMIENTO EN "${hotelName}"`;
    if (isLastDay) line = line ? `${line} · ÚLTIMO DÍA DEL VIAJE` : 'ÚLTIMO DÍA DEL VIAJE';
    if (line) { top.innerHTML = `<h4>${line}</h4>`; cont.appendChild(top); }
  } catch (e) { D_HOTEL('ERROR BANNER ALOJAMIENTO/ÚLTIMO DÍA', e); }

  const q = norm(state.groupQ || '');

  // Obtener actividades del día tolerando objeto indexado
  let acts = (grupo.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
  if (!Array.isArray(acts)) {
    acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');
  }

  // Orden por hora de inicio (temprano → tarde)
  acts = acts.slice().sort((a,b) => timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

  if (q) acts = acts.filter(a => norm([a.actividad, a.proveedor, a.horaInicio, a.horaFin].join(' ')).includes(q));
  if (!acts.length){
    cont.innerHTML = '<div class="muted">SIN ACTIVIDADES PARA ESTE DÍA.</div>';
    return;
  }

  // ===== Render inmediato, cargas asíncronas después =====
  for (const act of acts){
    const plan   = calcPlan(act, grupo);
    const saved  = getSavedAsistencia(grupo, fechaISO, act.actividad);
    const estado = (grupo.serviciosEstado?.[fechaISO]?.[slug(act.actividad || '')]?.estado) || '';

    const paxFinalInit = (saved?.paxFinal ?? '');
    const actName = act.actividad || 'ACTIVIDAD';
    const actKey  = slug(actName);

    const div = document.createElement('div');
    div.className = 'act';

    const estadoHtml = estado ? ('· <span class="muted">' + String(estado).toUpperCase() + '</span>') : '';

    // Botón de voucher: placeholder que reemplazamos cuando llegue el servicio
    const vchPlaceholder = '<span class="btnVchWrap"></span>';

    div.innerHTML =
      '<h4>' + (actName || '').toUpperCase() + ' ' + estadoHtml + '</h4>' +
      '<div class="meta">' +
      '<div class="rowflex" style="margin:.35rem 0">' +
      '<input type="number" min="0" inputmode="numeric" placeholder="N° ASISTENCIA" value="' + paxFinalInit + '"/>' +
      '<textarea placeholder="COMENTARIOS PARA BITÁCORA"></textarea>' +
      '<button class="btn ok btnSave">GUARDAR</button>' +
      vchPlaceholder +
      '<button class="btn sec btnActInfo">DETALLE / TIPS</button>' +
      '</div>' +
      '<div class="bitacora" style="margin-top:.4rem">' +
      '<div class="muted" style="margin-bottom:.25rem">BITÁCORA:</div>' +
      '<div class="bitItems" style="display:grid;gap:.35rem"><div class="muted">CARGANDO…</div></div>' +
      '</div>';

    cont.appendChild(div);

    // — Detalle/Comentarios
    const btnAI = div.querySelector('.btnActInfo');
    if (btnAI) btnAI.onclick = async () => {
      try {
        const servicio = await findServicio(grupo.destino, actName).catch(() => null);
        const tipoRaw  = (servicio?.voucher || 'No Aplica').toString();
        const tipo = /electron/i.test(tipoRaw) ? 'ELECTRONICO'
           : /fisic/i.test(tipoRaw)          ? 'FISICO'
           : /correo/i.test(tipoRaw)         ? 'CORREO'
           : 'NOAPLICA';
        await openActividadModal(grupo, fechaISO, act, servicio, tipo);
      } catch(e) {
        console.error('openActividadModal error', e);
      }
    };

    // — Guardar asistencia/nota
    div.querySelector('.btnSave').onclick = async ()=>{
      const btn = div.querySelector('.btnSave');
      btn.disabled = true;
      try{
        const pax  = Number(div.querySelector('input').value || 0);
        const nota = String(div.querySelector('textarea').value || '').trim();

        const refGrupo = doc(db,'grupos',grupo.id);
        const payload = {};
        payload[`asistencias.${fechaISO}.${actKey}`] = {
          paxFinal: pax,
          notas: nota,
          byUid: auth.currentUser.uid,
          byEmail: String(auth.currentUser.email || '').toLowerCase(),
          updatedAt: serverTimestamp()
        };
        await updateDoc(refGrupo, payload);
        setSavedAsistenciaLocal(grupo, fechaISO, actName, { paxFinal: pax, notas: nota });

        if (nota){
          const timeId = timeIdNowMs();
          const ref = doc(db,'grupos',grupo.id,'bitacora',actKey,fechaISO,timeId);
          await setDoc(ref, {
            texto: nota,
            byUid: auth.currentUser.uid,
            byEmail: (auth.currentUser.email || '').toLowerCase(),
            ts: serverTimestamp()
          });

          // Alerta para Operaciones
          await addDoc(collection(db,'alertas'),{
            audience: '',
            mensaje: `NOTA EN ${actName.toUpperCase()}: ${nota.toUpperCase()}`,
            createdAt: serverTimestamp(),
            createdBy: { uid: state.user.uid, email: (state.user.email || '').toLowerCase() },
            readBy: {},
            groupInfo: {
              grupoId: grupo.id,
              nombre: (grupo.nombreGrupo || grupo.aliasGrupo || grupo.id),
              code: (grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : ''),
              destino: (grupo.destino || null),
              programa: (grupo.programa || null),
              fechaActividad: fechaISO,
              actividad: actName
            }
          });

          await loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'));
          div.querySelector('textarea').value = '';
          await window.renderGlobalAlertsV2();
        }

        btn.textContent = 'GUARDADO';
        setTimeout(() => { btn.textContent = 'GUARDAR'; btn.disabled = false; }, 900);
      } catch(e) {
        console.error(e);
        btn.disabled = false;
        alert('NO SE PUDO GUARDAR.');
      }
    };

    // ===== CARGAS EN SEGUNDO PLANO =====
    // (1) Bitácora asíncrona
    loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'))
      .catch(e => {
        console.error(e);
        div.querySelector('.bitItems').innerHTML = '<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>';
      });

    // (2) Servicio / botón de voucher asíncrono
    (async () => {
      try {
        const servicio = await findServicio(grupo.destino, actName);
        const tipoRaw  = (servicio?.voucher || 'No Aplica').toString();
        const tipo = /electron/i.test(tipoRaw) ? 'ELECTRONICO'
                   : /fisic/i.test(tipoRaw)    ? 'FISICO'
                   : /correo/i.test(tipoRaw)   ? 'CORREO'
                   : 'NOAPLICA';

        if (tipo !== 'NOAPLICA') {
          const wrap = div.querySelector('.btnVchWrap');
          if (wrap) {
            const btn = document.createElement('button');
            btn.className = 'btn sec';
            btn.textContent = 'FINALIZAR…';
            btn.onclick = async () => { await openVoucherModal(grupo, fechaISO, act, servicio, tipo); };
            wrap.replaceWith(btn);
          }
        }
      } catch (e) {
        console.warn('findServicio falló', { destino: grupo.destino, act: actName, e });
      }
    })();
  }
}

async function loadBitacora(grupoId, fechaISO, actKey, wrap){
  wrap.innerHTML = '<div class="muted">CARGANDO…</div>';
  try {
    const coll = collection(db,'grupos',grupoId,'bitacora',actKey,fechaISO);
    const qs   = await getDocs(query(coll, orderBy('ts','desc'), limit(50)));

    const frag = document.createDocumentFragment();
    qs.forEach(d => {
      const x      = d.data() || {};
      const quien  = String(x.byEmail || x.byUid || 'USUARIO').toUpperCase();
      const cuando = x.ts?.seconds ? new Date(x.ts.seconds * 1000) : null;
      const hora   = cuando ? cuando.toLocaleString('es-CL').toUpperCase() : '';

      const div = document.createElement('div');
      div.className = 'meta';
      div.textContent = `• ${(x.texto || '').toString().toUpperCase()} — ${quien}${hora ? (' · ' + hora) : ''}`;
      frag.appendChild(div);
    });

    wrap.innerHTML = '';
    wrap.appendChild(frag);
    if (!qs.size) wrap.innerHTML = '<div class="muted">AÚN NO HAY NOTAS.</div>';
  } catch(e) {
    console.error(e);
    wrap.innerHTML = '<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>';
  }
}

/* ====== VIAJE: INICIO / TÉRMINO / REVERSIÓN ====== */
async function openInicioViajeModal(g) {
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = `INICIO DE VIAJE — ${dmy(g.fechaInicio)}`;

  const plan = paxOf(g);
  const preA = Number(g?.paxViajando?.A || 0);
  const preE = Number(g?.paxViajando?.E || 0);

  body.innerHTML = `
    <div class="meta">PLANIFICADO: <strong>${plan}</strong> PAX</div>
    <div class="rowflex" style="margin:.5rem 0">
      <input id="ivA" type="number" min="0" inputmode="numeric" placeholder="ADULTOS (A)" value="${preA || ''}" />
      <input id="ivE" type="number" min="0" inputmode="numeric" placeholder="ESTUDIANTES (E)" value="${preE || ''}" />
    </div>
    <div class="meta">TOTAL REAL: <strong id="ivTot">${(preA + preE) || 0}</strong></div>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="ivSave" class="btn ok">GUARDAR</button>
    </div>`;

  const $A = body.querySelector('#ivA');
  const $E = body.querySelector('#ivE');
  const $T = body.querySelector('#ivTot');

  const recalc = () => {
    const t = Number($A.value || 0) + Number($E.value || 0);
    $T.textContent = t;
  };
  $A.oninput = recalc;
  $E.oninput = recalc;

  body.querySelector('#ivSave').onclick = async () => {
    const A = Math.max(0, Number($A.value || 0));
    const E = Math.max(0, Number($E.value || 0));
    const total = A + E;

    // Si no es el día de inicio y NO es STAFF, pedimos confirmación
    if (!isToday(g.fechaInicio) && !state.is) {
      const ok = confirm('No es el día de inicio. ¿Confirmar de todas formas?');
      if (!ok) return;
    }

    const path = doc(db, 'grupos', g.id);

    // 1) Guardado principal (si falla, aborta)
    try {
      await updateDoc(path, {
        paxViajando: {
          A, E, total,
          by: (state.user.email || '').toLowerCase(),
          updatedAt: serverTimestamp()
        },
        viaje: {
          ...(g.viaje || {}),
          estado: 'EN_CURSO',
          inicio: { at: serverTimestamp(), by: (state.user.email || '').toLowerCase() }
        }
      });
    } catch (e) {
      console.error('INICIO: updateDoc FAILED', e?.code, e);
      alert('No fue posible guardar el inicio del viaje. ' + (e?.code || ''));
      return; // aborta si falló el guardado principal
    }

    // 2) Log inmutable (si falla, no bloquea)
    try {
      await appendViajeLog(
        g.id,
        'INICIO',
        `INICIO DE VIAJE — A:${A} · E:${E} · TOTAL:${total}`,
        { A, E, total }
      );
    } catch (e) {
      console.warn('appendViajeLog falló (no bloquea):', e?.code, e);
    }

    // 3) Refresco local + re-render (si falla, no bloquea)
    try {
      g.paxViajando = { A, E, total };
      g.viaje = {
        ...(g.viaje || {}),
        estado: 'EN_CURSO',
        inicio: { at: new Date(), by: (state.user.email || '').toLowerCase() }
      };
      document.getElementById('modalBack').style.display = 'none';
      await renderOneGroup(g);
    } catch (e) {
      console.warn('renderOneGroup después de inicio falló (no bloquea):', e?.code, e);
    }
  };

  document.getElementById('modalClose').onclick = () => {
    document.getElementById('modalBack').style.display = 'none';
  };
  back.style.display = 'flex';
}

async function ensureFinanzasSummary(groupId) {
  try {
    const d = await getDoc(doc(db, 'grupos', groupId, 'finanzas', 'summary'));
    return d.exists() ? (d.data() || {}) : null;
  } catch (_) {
    return null;
  }
}

async function openTerminoViajeModal(g) {
  // BLOQUEO: exige cierre financiero antes de permitir finalizar viaje
  const finSum = await ensureFinanzasSummary(g.id);
  if (!finSum || finSum.closed !== true) {
    alert('Debes cerrar FINANZAS (transferencia y boleta) antes de terminar el viaje.');
    // abrir pestaña FINANZAS si existe
    const paneFin = document.getElementById('paneFin');
    if (paneFin) {
      document.getElementById('paneResumen')?.style && (document.getElementById('paneResumen').style.display = 'none');
      document.getElementById('paneItin')?.style && (document.getElementById('paneItin').style.display = 'none');
      paneFin.style.display = '';
    }
    return;
  }

  if (!g?.viaje?.inicio?.at && !state.is) {
    alert('Aún no se ha registrado el inicio del viaje.');
    return;
  }

  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = `TERMINAR VIAJE — ${dmy(g.fechaFin)}`;

  body.innerHTML = `
    <div class="meta">¿Deseas cerrar el viaje? Esto pedirá confirmación de administración.</div>
    <label class="meta" style="display:flex;gap:.5rem;align-items:center">
      <input id="rvRend" type="checkbox"> RENDICIÓN HECHA
    </label>
    <label class="meta" style="display:flex;gap:.5rem;align-items:center">
      <input id="rvBol" type="checkbox"> BOLETA ENTREGADA
    </label>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="tvSave" class="btn warn">FINALIZAR VIAJE</button>
    </div>`;

  body.querySelector('#tvSave').onclick = async () => {
    const rend = !!body.querySelector('#rvRend').checked;
    const bol  = !!body.querySelector('#rvBol').checked;
    try {
      const path = doc(db, 'grupos', g.id);
      await updateDoc(path, {
        viaje: {
          ...(g.viaje || {}),
          estado: 'FINALIZADO',
          fin: {
            at: serverTimestamp(),
            by: (state.user.email || '').toLowerCase(),
            rendicionOk: rend,
            boletaOk: bol
          }
        }
      });
      await appendViajeLog(
        g.id,
        'FIN',
        `FINALIZAR VIAJE${rend ? ' · RENDICIÓN OK' : ''}${bol ? ' · BOLETA OK' : ''}`,
        { rend, bol }
      );
      g.viaje = {
        ...(g.viaje || {}),
        estado: 'FINALIZADO',
        fin: {
          at: new Date(),
          by: (state.user.email || '').toLowerCase(),
          rendicionOk: rend,
          boletaOk: bol
        }
      };
      document.getElementById('modalBack').style.display = 'none';
      await renderOneGroup(g);
    } catch (e) {
      console.error(e);
      alert('No fue posible finalizar el viaje.');
    }
  };

  document.getElementById('modalClose').onclick = () => {
    document.getElementById('modalBack').style.display = 'none';
  };
  back.style.display = 'flex';
}

// Reversión (solo STAFF)
async function staffReopenInicio(g) {
  if (!state.is) { alert('Solo staff puede reabrir el inicio.'); return; }
  const ok = confirm('¿Reabrir INICIO DE VIAJE? (se habilitará el botón de inicio para el coordinador)');
  if (!ok) return;
  try {
    const path = doc(db, 'grupos', g.id);
    await updateDoc(path, { 'viaje.inicio': deleteField(), 'viaje.estado': 'PENDIENTE' });
    await appendViajeLog(g.id, 'REABRIR_INICIO', 'SE REABRIÓ EL INICIO DEL VIAJE');
    if (g.viaje) { delete g.viaje.inicio; g.viaje.estado = 'PENDIENTE'; }
    await renderOneGroup(g);
  } catch (e) {
    console.error(e);
    alert('No fue posible reabrir el inicio.');
  }
}

async function staffReopenCierre(g) {
  if (!state.is) { alert('Solo staff puede reabrir el cierre.'); return; }
  const ok = confirm('¿Reabrir CIERRE DE VIAJE? (volverá a estado EN_CURSO)');
  if (!ok) return;
  try {
    const path = doc(db, 'grupos', g.id);
    await updateDoc(path, { 'viaje.fin': deleteField(), 'viaje.estado': 'EN_CURSO' });

    // Log correcto e inmutable + bitácora
    await appendViajeLog(g.id, 'REABRIR_CIERRE', 'SE REABRIÓ EL CIERRE DEL VIAJE');

    if (g.viaje) { delete g.viaje.fin; g.viaje.estado = 'EN_CURSO'; }
    await renderOneGroup(g);
  } catch (e) {
    console.error(e);
    alert('No fue posible reabrir el cierre.');
  }
}

/* ====== SERVICIOS / VOUCHERS ====== */
async function findServicio(destino, nombre) {
  if (!destino || !nombre) return null;
  const want = norm(nombre);
  const candidates = [['Servicios', destino, 'Listado'], [destino, 'Listado']];
  for (const path of candidates) {
    try {
      const snap = await getDocs(collection(db, path[0], path[1], path[2]));
      let best = null;
      snap.forEach(d => {
        const x = d.data() || {};
        const serv = String(x.servicio || x.nombre || d.id || '');
        if (norm(serv) === want) best = { id: d.id, ...x };
      });
      if (best) return best;
    } catch (_) {}
  }
  return null;
}

/* ====== HILOS GLOBALES (A/B/C) + CHEQ OUT ====== */

// A: actividad con proveedor  → DEST:{dest} | PROV:{proveedorId} | SRV:{servicioId}
// B: actividad sin proveedor  → DEST:{dest} | PROV:GENERAL      | ACT:{actKey}
// C: comidas de hotel         → HOTEL:{hotelId} | MEAL:{DESAYUNO|ALMUERZO|CENA}

function isMealAct(name = '') {
  const s = String(name || '').toUpperCase();
  if (/\bDESA(Y|LL)UNO\b|^DESA/i.test(s)) return 'DESAYUNO';
  if (/\bALMUERZO\b|^ALM/i.test(s))      return 'ALMUERZO';
  if (/\bCENA\b|^CEN/i.test(s))          return 'CENA';
  return null;
}

function isCheckoutAct(name = '') {
  const s = String(name || '').toUpperCase();
  // acepta variaciones: CHEQ OUT, CHECK OUT, CHECK-OUT, CHECKOUT
  return /\bCHE?CK[-\s]?OUT\b|\bCHEQ\s?OUT\b/.test(s);
}

function threadKeyForGeneral(destino, actName) {
  const dest = (destino || '').toString().toUpperCase().trim();
  const actKey = slug(actName || '');
  return `DEST:${dest}|PROV:GENERAL|ACT:${actKey}`;
}

function threadKeyForProv(destino, proveedorId, servicioId) {
  const dest = (destino || '').toString().toUpperCase().trim();
  return `DEST:${dest}|PROV:${String(proveedorId).toUpperCase()}|SRV:${String(servicioId).toUpperCase()}`;
}

function threadKeyForHotelMeal(hotelId, meal) {
  return `HOTEL:${String(hotelId).toUpperCase()}|MEAL:${String(meal).toUpperCase()}`;
}

// Colección única para hilos globales
function threadColl(threadKey) {
  return collection(db, 'threads', threadKey, 'msgs');
}

// Proveedor por DESTINO (coincide con tu fetchProveedorByDestino, pero local a este bloque)
async function findProveedorDocByDestino(destino, proveedorName) {
  if (!destino || !proveedorName) return null;
  try {
    const qs = await getDocs(collection(db, 'Proveedores', String(destino).toUpperCase(), 'Listado'));
    let hit = null;
    qs.forEach(d => {
      const x = d.data() || {};
      const nom = (x.proveedor || x.nombre || d.id || '').toString();
      if (norm(nom) === norm(proveedorName)) hit = { id: d.id, ...x };
    });
    return hit;
  } catch (_) {
    return null;
  }
}

// Alias global idempotente (no redeclara si ya existe en otra parte del bundle)
if (typeof window !== 'undefined' && !window.fetchProveedorByDestino) {
  window.fetchProveedorByDestino = (...args) => findProveedorDocByDestino(...args);
}

/* ====== Lógica de hotel por día con CHEQ OUT ====== */

// Devuelve las actividades del día como array (tolera objeto indexado)
function getDayActsArray(grupo, fechaISO) {
  let acts = (grupo?.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
  if (!Array.isArray(acts)) acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');
  // Orden por hora
  return acts.slice().sort((a, b) => timeVal(a?.horaInicio) - timeVal(b?.horaInicio));
}

// Encuentra el hotel "vigente" para ese día (checkIn <= día < checkOut)
// y, si hay cambio de hotel el MISMO día, detecta el "siguiente del mismo día".
function hotelContextForDay(hoteles, fechaISO) {
  const f = String(fechaISO || '');
  let current = null;
  let nextSameDay = null;

  for (const h of (hoteles || [])) {
    const ci = toISO(h.checkIn);
    const co = toISO(h.checkOut);
    if (ci && co && (f >= ci) && (f < co)) current = h;
  }
  // Si hay una asignación con checkIn EXACTO ese día, podría ser el nuevo hotel del mismo día
  for (const h of (hoteles || [])) {
    const ci = toISO(h.checkIn);
    if (ci && ci === f) {
      // sólo considera nextSameDay si NO es el mismo objeto que current
      if (!current || current.id !== h.id) nextSameDay = h;
    }
  }
  return { current, nextSameDay };
}

// ¿El ACT ocurre después del CHEQ OUT del día?
function isAfterCheckout(act, dayActs) {
  // Busca el primer CHEQ OUT con hora válida
  const co = dayActs.find(a => isCheckoutAct(a?.actividad) && timeVal(a?.horaInicio) < 1e9);
  if (!co) return false;
  const actT = timeVal(act?.horaInicio);
  const coT  = timeVal(co?.horaInicio);
  return (actT < 1e9) && (coT < 1e9) && (actT >= coT);
}

// Para comidas, decide HOTEL correcto considerando CHEQ OUT y posible cambio en el mismo día
function pickHotelForMeal(grupo, fechaISO, act, hoteles) {
  const { current, nextSameDay } = hotelContextForDay(hoteles, fechaISO);
  const meal = isMealAct(act?.actividad || '');
  if (!meal) return null;

  // DESAYUNO: se asume siempre en el hotel "current"
  if (meal === 'DESAYUNO') return current || nextSameDay || null;

  // ALMUERZO/CENA: si hubo CHEQ OUT antes de la hora de esta comida y existe nextSameDay → usar nextSameDay
  const dayActs = getDayActsArray(grupo, fechaISO);
  if (isAfterCheckout(act, dayActs) && nextSameDay) return nextSameDay;

  // Si no, se mantiene el hotel vigente
  return current || nextSameDay || null;
}

/* ====== Resolver ThreadKey global (A/B/C) ====== */
async function resolveThreadKey(grupo, fechaISO, act, servicioHint = null) {
  const actName = (act?.actividad || '').toString();
  const destino = (grupo?.destino || '').toString().toUpperCase().trim();
  const meal = isMealAct(actName);

  // (C) Comida de hotel → elegir hotel correcto según CHEQ OUT
  if (meal) {
    const hoteles = await loadHotelesInfo(grupo) || [];
    const h = pickHotelForMeal(grupo, fechaISO, act, hoteles);
    if (h && (h.hotel?.id || h.hotelId || h.id)) {
      const hId = h.hotel?.id || h.hotelId || h.id;
      return { key: threadKeyForHotelMeal(hId, meal), scope: 'C' };
    }
    // si no hay hotel identificable, cae a GENERAL (B)
  }

  // Intento (A) proveedor + servicio
  let servicio = servicioHint;
  if (!servicio) {
    try { servicio = await findServicio(destino, actName); } catch (_) {}
  }
  const servicioId = servicio?.id || null;

  // Proveedor (prioriza servicio.proveedor o act.proveedor)
  const proveedorName = (servicio?.proveedor || act?.proveedor || '').toString().trim();
  let proveedorId = null;
  if (proveedorName) {
    try {
      const provDoc = await findProveedorDocByDestino(destino, proveedorName);
      if (provDoc?.id) proveedorId = provDoc.id;
    } catch (_) {}
  }

  if (servicioId && proveedorId) {
    return { key: threadKeyForProv(destino, proveedorId, servicioId), scope: 'A' };
  }

  // (B) GENERAL
  return { key: threadKeyForGeneral(destino, actName), scope: 'B' };
}

function renderVoucherHTMLSync(g, fechaISO, act, proveedorDoc = null, compact = false) {
  const paxPlan = calcPlan(act, g);
  const asis = getSavedAsistencia(g, fechaISO, act.actividad);
  const paxAsist = asis?.paxFinal ?? '';
  const code = (g.numeroNegocio || '') + (g.identificador ? ('-' + g.identificador) : '');
  const provTexto = proveedorDoc
    ? `${(proveedorDoc.nombre || '').toString().toUpperCase()}${proveedorDoc.rut ? (' · ' + String(proveedorDoc.rut).toUpperCase()) : ''}${proveedorDoc.direccion ? (' · ' + String(proveedorDoc.direccion).toUpperCase()) : ''}`
    : (String(act.proveedor || '').toUpperCase());

  return `
    <div class="card">
      <h3>${(act.actividad || 'SERVICIO').toString().toUpperCase()}</h3>
      <div class="meta">PROVEEDOR: ${provTexto || '—'}</div>
      <div class="meta">GRUPO: ${(g.nombreGrupo || g.aliasGrupo || g.id).toString().toUpperCase()} (${code})</div>
      <div class="meta">FECHA: ${dmy(fechaISO)}</div>
      <div class="meta">PAX PLAN: ${paxPlan} · PAX ASISTENTES: ${paxAsist}</div>
      ${compact ? '' : '<hr><div class="meta">FIRMA COORDINADOR: ________________________________</div>'}
    </div>`;
}

async function openVoucherModal(g, fechaISO, act, servicio, tipo) {
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = `VOUCHER — ${(act.actividad || '').toString().toUpperCase()} — ${dmy(fechaISO)}`;

  let proveedorDoc = null;
  try {
    if (servicio?.proveedor) {
      proveedorDoc = await findProveedorDocByDestino(
        (g.destino || '').toString().toUpperCase(),
        servicio.proveedor
      );
    }
  } catch (_) {}

  const voucherHTML = renderVoucherHTMLSync(g, fechaISO, act, proveedorDoc, false);

  if (tipo === 'FISICO') {
    body.innerHTML = `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem">
        <button id="vchPrint" class="btn sec">IMPRIMIR</button>
        <button id="vchOk" class="btn ok">FINALIZAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>`;

    document.getElementById('vchPrint').onclick = () => {
      const w = window.open('', '_blank');
      w.document.write(`<!doctype html><html><body>${voucherHTML}</body></html>`);
      w.document.close();
      w.print();
    };
    document.getElementById('vchOk').onclick    = () => setEstadoServicio(g, fechaISO, act, 'FINALIZADA', true);
    document.getElementById('vchPend').onclick  = () => setEstadoServicio(g, fechaISO, act, 'PENDIENTE',  true);

  } else if (tipo === 'CORREO') {
    // ——— 1) Exige asistencia guardada ———
    const asis = getSavedAsistencia(g, fechaISO, act.actividad);
    if (asis?.paxFinal == null) { alert('PRIMERO GUARDA LA ASISTENCIA (PAX).'); return; }

    // ——— 2) Intentar prellenar correo del proveedor ———
    let provEmail = String(servicio?.correoProveedor || '').trim();
    if (!provEmail) {
      try {
        const prov = await findProveedorDocByDestino(
          (g.destino || '').toString().toUpperCase(),
          (servicio?.proveedor || act.proveedor || '').toString()
        );
        provEmail = String(prov?.correo || prov?.email || '').trim();
      } catch (_) {}
    }

    // ——— 3) Datos base ———
    const code        = (g.numeroNegocio || '') + (g.identificador ? ('-' + g.identificador) : '');
    const actividadTX = (act.actividad || '').toString().toUpperCase();
    const grupoTX     = (g.nombreGrupo || g.aliasGrupo || g.id).toString().toUpperCase();
    const destinoTX   = (g.destino || '—').toString().toUpperCase();
    const programaTX  = (g.programa || '—').toString().toUpperCase();
    const fechaTX     = dmy(fechaISO);
    const paxTX       = (asis?.paxFinal ?? '—');
    const coordTX     = (g.coordinadorNombre || '—').toString().toUpperCase();
    const subject     = `CONFIRMACIÓN DE ASISTENCIA — ${actividadTX} — ${fechaTX} — ${grupoTX} (${code})`;

    // cuerpo por defecto (texto plano, editable)
    const defaultBody =
`ESTIMADOS ${(act.proveedor || 'PROVEEDOR').toString().toUpperCase()}:

CONFIRMAMOS LA ASISTENCIA PARA EL SERVICIO INDICADO:

• ACTIVIDAD: ${actividadTX}
• FECHA: ${fechaTX}
• GRUPO: ${grupoTX} (${code})
• DESTINO / PROGRAMA: ${destinoTX} / ${programaTX}
• PAX ASISTENTES: ${paxTX}
• COORDINADOR(A): ${coordTX}

OBSERVACIONES:
—`;

    // estado inicial (si alguien ya lo marcó antes)
    const actKey = slug(act.actividad || 'actividad');
    const correoYaEnviado = (g?.serviciosEstado?.[fechaISO]?.[actKey]?.correo?.estado === 'ENVIADA');

    // ——— 4) UI ———
    body.innerHTML = `
      ${voucherHTML}
      <div class="meta" style="margin-top:.5rem">
        <strong>PASOS:</strong> 1) ABRIR CORREO · 2) ENVIAR DESDE TU APP · 3) VOLVER Y <u>MARCAR COMO ENVIADA</u> · 4) FINALIZAR ACTIVIDAD.
      </div>
      <div class="rowflex" style="gap:.4rem;align-items:center;margin:.25rem 0 .25rem 0">
        <input id="rtMailTo" type="email" placeholder="PARA" value="${(provEmail || '')}" style="flex:1"/>
        <input id="rtMailCc" type="email" placeholder="CC" value="operaciones@raitrai.cl" style="flex:1"/>
      </div>
      <input id="rtMailSubj" type="text" placeholder="ASUNTO" value="${subject.replace(/"/g, '&quot;')}" />
      <textarea id="rtMailBody" placeholder="CUERPO (SE PUEDE EDITAR ANTES DE ENVIAR)" style="margin-top:.4rem;height:160px">${defaultBody}</textarea>

      <div class="rowflex" style="margin-top:.6rem;gap:.5rem;flex-wrap:wrap">
        <button id="rtOpenMail" class="btn ok">ABRIR CORREO</button>
        <button id="rtMarkSent" class="btn sec">MARCAR COMO ENVIADA</button>
        <button id="rtFinalizar" class="btn warn" ${correoYaEnviado ? '' : 'disabled title="PRIMERO MARCA EL CORREO COMO ENVIADO"'}>
          FINALIZAR ACTIVIDAD
        </button>
        <button id="vchPend" class="btn">DEJAR PENDIENTE</button>
      </div>
      <div class="meta muted">Se abrirá tu cliente de correo (Gmail/Mail) con el mensaje prellenado. Luego vuelve y marca “ENVIADA”.</div>
    `;

    // ——— 5) Handlers ———
    const $to   = document.getElementById('rtMailTo');
    const $cc   = document.getElementById('rtMailCc');
    const $subj = document.getElementById('rtMailSubj');
    const $txt  = document.getElementById('rtMailBody');
    const $fin  = document.getElementById('rtFinalizar');

    // Habilitar "Finalizar" si ya estaba ENVIADA
    (() => {
      const actKey = slug(act.actividad || 'actividad');
      const ya = (g?.serviciosEstado?.[fechaISO]?.[actKey]?.correo?.estado === 'ENVIADA');
      if (ya && $fin) { $fin.disabled = false; $fin.removeAttribute('title'); }
    })();

    // 5.a) Abrir mailto (no cambia estado)
    document.getElementById('rtOpenMail').onclick = () => {
      const to = ($to.value || '').trim();
      if (!to) { alert('INGRESA UN DESTINATARIO (PARA).'); return; }
      const mailto = buildMailto({
        to,
        cc: ($cc.value || '').trim(),
        subject: ($subj.value || '').trim(),
        htmlBody: ($txt.value || '').trim()
      });
      const a = document.createElement('a');
      a.href = mailto;
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      a.remove();
      showFlash('ABRÍ TU CORREO, LUEGO MARCA “ENVIADA”');
    };

    // 5.b) Marcar como ENVIADA → guarda en Firestore + log + alerta
    document.getElementById('rtMarkSent').onclick = async () => {
      const to = ($to.value || '').trim();
      if (!to) { alert('COMPLETA EL CORREO DEL PROVEEDOR.'); return; }
      try {
        const refGrupo = doc(db, 'grupos', g.id);
        const payload  = {};
        payload[`serviciosEstado.${fechaISO}.${actKey}.correo`] = {
          estado: 'ENVIADA',
          enviadaAt: serverTimestamp()
        };
        await updateDoc(refGrupo, payload);

        // espejo local
        (g.serviciosEstado ||= {});
        (g.serviciosEstado[fechaISO] ||= {});
        (g.serviciosEstado[fechaISO][actKey] ||= {});
        g.serviciosEstado[fechaISO][actKey].correo = { estado: 'ENVIADA', enviadaAt: new Date() };

        // log + alerta
        try {
          await appendViajeLog(
            g.id,
            'DECLARACION_CORREO',
            `DECLARACIÓN ENVIADA — ${actividadTX} — ${fechaTX} — PAX:${paxTX}`,
            { actividad: actividadTX, fecha: fechaTX, pax: paxTX }
          );
          await addDoc(collection(db, 'alertas'), {
            audience: '',
            mensaje: `DECLARACIÓN ENVIADA — ${actividadTX} — ${fechaTX} — PAX:${paxTX}`,
            createdAt: serverTimestamp(),
            createdBy: { uid: state.user.uid, email: (state.user.email || '').toLowerCase() },
            readBy: {},
            groupInfo: {
              grupoId: g.id,
              nombre: (g.nombreGrupo || g.aliasGrupo || g.id),
              code: (g.numeroNegocio || '') + (g.identificador ? ('-' + g.identificador) : ''),
              destino: (g.destino || null),
              programa: (g.programa || null),
              fechaActividad: fechaISO,
              actividad: actividadTX
            }
          });
          await window.renderGlobalAlertsV2();
        } catch (_) {}

        showFlash('CORREO MARCADO COMO ENVIADO');
        if ($fin) { $fin.disabled = false; $fin.removeAttribute('title'); }
      } catch (e) {
        console.error('Error al marcar correo ENVIADA', e);
        alert('NO SE PUDO GUARDAR EL ESTADO DEL CORREO.');
      }
    };

    // 5.c) Finalizar actividad (requiere PAX y correo ENVIADA)
    document.getElementById('rtFinalizar').onclick = async () => {
      const paxOk    = (getSavedAsistencia(g, fechaISO, act.actividad)?.paxFinal ?? null) != null;
      const correoOk = (g?.serviciosEstado?.[fechaISO]?.[actKey]?.correo?.estado === 'ENVIADA');
      if (!paxOk)  { alert('FALTA DECLARAR LA ASISTENCIA (PAX).'); return; }
      if (!correoOk) { alert('PRIMERO MARCA EL CORREO COMO ENVIADO.'); return; }
      await setEstadoServicio(g, fechaISO, act, 'FINALIZADA', true);
      document.getElementById('modalBack').style.display = 'none';
    };

    // 5.d) Dejar PENDIENTE explícitamente
    document.getElementById('vchPend').onclick = () =>
      setEstadoServicio(g, fechaISO, act, 'PENDIENTE', true);

  } else {
    // ELECTRÓNICO (clave / NFC)
    const clave = (servicio?.clave || '').toString();
    body.innerHTML = `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem">
        <div style="display:flex;gap:.4rem;align-items:center;width:100%">
          <input id="vchClave" type="password" placeholder="CLAVE (O ACERQUE TARJETA NFC)" style="flex:1"/>
          <button id="vchEye" class="btn sec" title="MOSTRAR/OCULTAR">👁</button>
        </div>
        <button id="vchFirmar" class="btn ok">FIRMAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>
      <div class="meta">TIP: SI TU MÓVIL SOPORTA NFC, PUEDES ACERCAR LA TARJETA PARA LEER LA CLAVE AUTOMÁTICAMENTE.</div>`;

    document.getElementById('vchEye').onclick = () => {
      const inp = document.getElementById('vchClave');
      inp.type = (inp.type === 'password' ? 'text' : 'password');
    };

    document.getElementById('vchFirmar').onclick = async () => {
      const val = (document.getElementById('vchClave').value || '').trim();
      if (!val) { alert('INGRESA LA CLAVE.'); return; }
      if (norm(val) !== norm(clave || '')) { alert('CLAVE INCORRECTA.'); return; }
      await setEstadoServicio(g, fechaISO, act, 'FINALIZADA', true);
    };

    document.getElementById('vchPend').onclick = () =>
      setEstadoServicio(g, fechaISO, act, 'PENDIENTE', true);

    if ('NDEFReader' in window) {
      try {
        const reader = new window.NDEFReader();
        await reader.scan();
        reader.onreading = (ev) => {
          const rec = ev.message.records[0];
          let text = '';
          try { text = (new TextDecoder().decode(rec.data) || '').trim(); } catch (_) {}
          if (text) {
            const inp = document.getElementById('vchClave');
            inp.value = text;
          }
        };
      } catch (_) {}
    }
  }

  document.getElementById('modalClose').onclick = () => {
    document.getElementById('modalBack').style.display = 'none';
  };
  back.style.display = 'flex';
}

async function openCorreoConfirmModal(grupo, fechaISO, act, proveedorEmail) {
  // exige asistencia guardada
  const asis = getSavedAsistencia(grupo, fechaISO, act.actividad);
  if (asis?.paxFinal == null) { alert('Primero guarda la ASISTENCIA (PAX).'); return; }

  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  const code = (grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '');
  const asunto =
    `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad || '').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id).toString().toUpperCase()} (${code})`;

  title.textContent = 'ENVIAR CONFIRMACIÓN POR CORREO';

  body.innerHTML = `
    <div class="meta"><strong>PARA:</strong> ${(proveedorEmail || '—').toUpperCase()}</div>
    <div class="meta"><strong>CC:</strong> OPERACIONES@RAITRAI.CL</div>
    <div class="meta"><strong>ASUNTO:</strong> ${asunto}</div>
    <div class="meta">NOTA ADICIONAL (opcional):</div>
    <textarea id="rt-nota-extra" placeholder="Escribe una nota corta…"></textarea>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="rtSendMail" class="btn ok">ENVIAR</button>
    </div>
  `;

  document.getElementById('rtSendMail').onclick = async () => {
    console.group('[MAIL] Enviar');
    const btn = document.getElementById('rtSendMail');
    if (btn.dataset.busy === '1') return; // anti doble-click
    btn.dataset.busy = '1';
    btn.disabled = true;

    try {
      const to = (proveedorEmail || '').trim().toLowerCase();
      console.log('[MAIL] to:', to);
      if (!to) { alert('No hay correo del proveedor. Completa su ficha.'); return; }

      const coordNom = (grupo.coordinadorNombre || '').toString().toUpperCase();
      const nota = (document.getElementById('rt-nota-extra').value || '').trim();

      const asunto =
        `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad || '').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id).toString().toUpperCase()} (${(grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '')})`;

      const htmlBody =
        `<p>Estimados ${(act.proveedor || 'PROVEEDOR').toString().toUpperCase()}:</p>
         <p>Confirmamos la asistencia para el servicio indicado:</p>
         <ul>
           <li><b>Actividad:</b> ${(act.actividad || '').toString().toUpperCase()}</li>
           <li><b>Fecha:</b> ${dmy(fechaISO)}</li>
           <li><b>Grupo:</b> ${(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id).toString().toUpperCase()} (${(grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '')})</li>
           <li><b>Destino / Programa:</b> ${(grupo.destino || '—').toString().toUpperCase()} / ${(grupo.programa || '—').toString().toUpperCase()}</li>
           <li><b>Pax asistentes:</b> ${getSavedAsistencia(grupo, fechaISO, act.actividad)?.paxFinal ?? '—'}</li>
           <li><b>Coordinador(a):</b> ${coordNom || '—'}</li>
         </ul>
         <p><b>Observaciones:</b><br>${nota ? nota.replace(/\n/g, '<br>') : '—'}</p>
         <p>— Enviado por Administración RT.</p>`;

      const oldTxt = btn.textContent;
      btn.textContent = 'ENVIANDO…';

      // — Intento vía GAS (servidor)
      console.time('[MAIL] fetch');
      const out = await sendMailViaGAS({
        key: GAS_KEY,
        to,
        cc: 'operaciones@raitrai.cl',
        subject: asunto,
        htmlBody,
        replyTo: 'operaciones@raitrai.cl'
      }, { retries: 0 });
      console.timeEnd('[MAIL] fetch');
      console.log('[MAIL] OK:', out);

      showFlash('CORREO ENVIADO');
      document.getElementById('modalBack').style.display = 'none';
    } catch (e) {
      console.error('[MAIL] ERROR', e);

      // — Fallback: abre cliente de correo del usuario SIEMPRE con asunto/cuerpo correctos
      const nota = (document.getElementById('rt-nota-extra').value || '').trim();
      const fallbackSubject =
        `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad || '').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id).toString().toUpperCase()} (${(grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '')})`;

      const fallbackBody =
`ESTIMADOS ${(act.proveedor || 'PROVEEDOR').toString().toUpperCase()}:

CONFIRMAMOS LA ASISTENCIA PARA EL SERVICIO INDICADO:

• ACTIVIDAD: ${(act.actividad || '').toString().toUpperCase()}
• FECHA: ${dmy(fechaISO)}
• GRUPO: ${(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id).toString().toUpperCase()} (${(grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '')})
• DESTINO / PROGRAMA: ${(grupo.destino || '—').toString().toUpperCase()} / ${(grupo.programa || '—').toString().toUpperCase()}
• PAX ASISTENTES: ${getSavedAsistencia(grupo, fechaISO, act.actividad)?.paxFinal ?? '—'}
• COORDINADOR(A): ${(grupo.coordinadorNombre || '—').toString().toUpperCase()}

OBSERVACIONES:
${nota || '—'}

— ENVIADO POR ADMINISTRACIÓN RT.`;

      const mailto = buildMailto({
        to: (proveedorEmail || '').trim(),
        cc: 'operaciones@raitrai.cl',
        subject: fallbackSubject,
        htmlBody: fallbackBody
      });

      // evitar que SPA intercepte: usamos un <a> temporal
      const a = document.createElement('a');
      a.href = mailto;
      a.style.display = 'none';
      document.body.appendChild(a);
      a.click();
      a.remove();

      showFlash('ABRÍ TU CORREO PARA ENVIARLO');
    } finally {
      btn.textContent = 'ENVIAR';
      btn.disabled = false;
      btn.dataset.busy = '0';
      console.groupEnd();
    }
  };
}
/* === IMPRESIÓN — helpers de formato === */
function formatDateReadable(isoStr) {
  if (!isoStr) return '—';
  const [yyyy, mm, dd] = isoStr.split('-').map(Number);
  const d = new Date(yyyy, (mm || 1) - 1, dd || 1);
  const wd = d.toLocaleDateString('es-CL', { weekday: 'long' });
  const name = wd.charAt(0).toUpperCase() + wd.slice(1);
  const ddp = String(dd || '').padStart(2, '0');
  const mmp = String(mm || '').padStart(2, '0');
  return `${name} ${ddp}/${mmp}`;
}

/* === DESPACHO (texto estilo Word) — con HOTELES, VUELOS, CONTACTOS, FINANZAS === */
/* Construye el texto “simple y elegante” del despacho */
function buildPrintTextDespacho(grupo, opts) {
  // opts: { itinLines, hoteles, vuelos, contactos, finanzas }
  const { itinLines = [], hoteles = [], vuelos = [], contactos = [], finanzas = null } = (opts || {});
  const up = s => (s || '').toString().toUpperCase();

  const code    = (grupo.numeroNegocio || '') + (grupo.identificador ? ('-' + grupo.identificador) : '');
  const paxPlan = paxOf(grupo);
  const paxReal = paxRealOf(grupo);
  const { A: A_real, E: E_real } = paxBreakdown(grupo);

  // ===== Encabezado (HTML) =====
  let out = '';
  out += '<div class="h1">DESPACHO DE VIAJE</div>\n';
  out += `<div><span class="b">GRUPO:</span> ${up(grupo.nombreGrupo || grupo.aliasGrupo || grupo.id)}  ·  <span class="b">CÓDIGO:</span> ${code}</div>\n`;
  out += `<div><span class="b">DESTINO:</span> ${up(grupo.destino || '—')}  ·  <span class="b">PROGRAMA:</span> ${up(grupo.programa || '—')}</div>\n`;
  out += `<div><span class="b">FECHAS:</span> ${dmy(grupo.fechaInicio || '')} — ${dmy(grupo.fechaFin || '')}  ·  <span class="b">PAX:</span> ${paxPlan}${paxReal ? `  <span class="muted">(REAL ${paxReal} · A:${A_real} · E:${E_real})</span>` : ''}</div>\n`;
  out += '<div>────────────────────────────────────────────────────────</div>\n\n';

  // ===== HOTELES =====
  out += `HOTELES (${hoteles.length || 0}):\n\n`;
  if (!hoteles.length) {
    out += '— SIN ASIGNACIÓN DE HOTELES —\n\n\n';
  } else {
    hoteles.forEach((h) => {
      const nombre  = up(h.hotelNombre || h.hotel?.nombre || '');
      const ci      = dmy(toISO(h.checkIn));
      const co      = dmy(toISO(h.checkOut));
      const noches  = (h.noches !== '' && h.noches != null) ? String(h.noches) : '';
      const estado  = up(h.status || '');

      const est     = h.estudiantes || { F: 0, M: 0, O: 0 };
      const estTot  = Number(h.estudiantesTotal ?? (est.F + est.M + est.O));
      const adu     = h.adultos || { F: 0, M: 0, O: 0 };
      const aduTot  = Number(h.adultosTotal ?? (adu.F + adu.M + adu.O));

      const hhab    = h.habitaciones || {};
      const habLine = (hhab.singles != null || hhab.dobles != null || hhab.triples != null || hhab.cuadruples != null)
        ? `HABITACIONES: ${[
            (hhab.singles   != null ? `SINGLES: ${hhab.singles}`   : ''),
            (hhab.dobles    != null ? `DOBLES: ${hhab.dobles}`     : ''),
            (hhab.triples   != null ? `TRIPLES: ${hhab.triples}`   : ''),
            (hhab.cuadruples!= null ? `CUÁDRUPLES: ${hhab.cuadruples}` : '')
          ].filter(Boolean).join(' · ')}`
        : '';

      const dir = up(h.hotel?.direccion || h.direccion || '');
      const tel = up(h.hotel?.contactoTelefono || h.contactoTelefono || '');

      out += `NOMBRE: ${nombre}  CHECK-IN/OUT: ${ci} — ${co}${noches ? `  NOCHES: ${noches}` : ''}\n`;
      if (estado) out += `ESTADO: ${estado}  `;
      out += `ESTUDIANTES: F: ${est.F || 0} · M: ${est.M || 0} · O: ${est.O || 0} (TOTAL ${estTot || 0}) · ` +
             `ADULTOS: F: ${adu.F || 0} · M: ${adu.M || 0} · O: ${adu.O || 0} (TOTAL ${aduTot || 0})\n`;
      if (habLine) out += `${habLine}\n`;
      if (dir)     out += `DIRECCIÓN: ${dir}\n`;
      if (tel)     out += `TELÉFONO: ${tel}\n`;
      out += '\n';
    });
    out += '\n';
  }

  // ===== TRANSPORTE / VUELOS =====
  out += 'TRANSPORTE / VUELOS:\n\n';
  if (!vuelos.length) {
    out += '— SIN VUELOS/TRANSPORTE —\n\n\n';
  } else {
    vuelos.forEach((v) => {
      const numero  = up(v.numero || v.tramos?.[0]?.numero || '');
      const empresa = up(v.proveedor || v.tramos?.[0]?.aerolinea || '');
      const ruta    = [
        up(v.origen  || v.tramos?.[0]?.origen  || ''),
        up(v.destino || v.tramos?.slice(-1)?.[0]?.destino || '')
      ].filter(Boolean).join(' — ');

      const ida = dmy(toISO(v.fechaIda)    || toISO(v.tramos?.[0]?.fechaIda)             || '');
      const vta = dmy(toISO(v.fechaVuelta) || toISO(v.tramos?.slice(-1)?.[0]?.fechaVuelta) || '');

      out += `N° / SERVICIO: ${numero || '—'}  EMPRESA: ${empresa || '—'}  RUTA: ${ruta || '—'}\n`;
      out += `IDA: ${ida || '—'}  VUELTA: ${vta || '—'}\n`;

      const isAereo = (v.tipoTransporte || 'aereo') === 'aereo';
      const isMulti = isAereo && v.tipoVuelo === 'regular' && Array.isArray(v.tramos) && v.tramos.length > 0;

      if (isMulti) {
        out += 'TIPO: REGULAR · MULTITRAMO\n';
        (v.tramos || []).forEach((t, i) => {
          const idaL = [
            dmy(toISO(t.fechaIda) || ''),
            t.presentacionIdaHora ? `PRESENTACIÓN ${t.presentacionIdaHora}` : '',
            t.vueloIdaHora        ? `VUELO ${t.vueloIdaHora}`               : ''
          ].filter(Boolean).join(' · ');

          const vtaL = toISO(t.fechaVuelta)
            ? [
                dmy(toISO(t.fechaVuelta) || ''),
                t.presentacionVueltaHora ? `PRESENTACIÓN ${t.presentacionVueltaHora}` : '',
                t.vueloVueltaHora        ? `VUELO ${t.vueloVueltaHora}`               : ''
              ].filter(Boolean).join(' · ')
            : '';

          out += `TRAMO ${i + 1}: ${up(t.aerolinea || '')} ${up(t.numero || '')} — ${up(t.origen || '')} → ${up(t.destino || '')}\n`;
          out += `IDA: ${idaL}\n`;
          if (vtaL) out += `REGRESO: ${vtaL}\n`;
        });
      } else if (isAereo) {
        const l1 = [
          v.presentacionIdaHora    ? `PRESENTACIÓN ${v.presentacionIdaHora}` : '',
          v.vueloIdaHora           ? `VUELO ${v.vueloIdaHora}`               : ''
        ].filter(Boolean).join(' · ');
        const l2 = [
          v.presentacionVueltaHora ? `PRESENTACIÓN ${v.presentacionVueltaHora}` : '',
          v.vueloVueltaHora        ? `VUELO ${v.vueloVueltaHora}`               : ''
        ].filter(Boolean).join(' · ');

        out += `TIPO: AÉREO${v.tipoVuelo ? ` · ${up(v.tipoVuelo)}` : ''}\n`;
        if (l1) out += `IDA: ${l1}\n`;
        if (l2) out += `REGRESO: ${l2}\n`;
      } else {
        out += 'TIPO: TERRESTRE (BUS)\n';
        if (v.idaHora || v.vueltaHora) {
          if (v.idaHora)    out += `SALIDA BUS (IDA): ${v.idaHora}\n`;
          if (v.vueltaHora) out += `REGRESO BUS: ${v.vueltaHora}\n`;
        }
      }
      out += '\n';
    });
    out += '\n';
  }

  // ===== ITINERARIO =====
  out += 'ITINERARIO:  (ORDEN Y HORARIOS DE ACTIVIDADES PUEDEN SER MODIFICADOS)\n\n';
  const byDate = new Map();
  (itinLines || []).forEach((x) => {
    if (!byDate.has(x.fechaISO)) byDate.set(x.fechaISO, []);
    byDate.get(x.fechaISO).push(x);
  });

  const fechas = Array.from(byDate.keys()).sort();
  fechas.forEach((f, idx) => {
    out += `<div class="h2">DÍA ${idx + 1} – ${formatDateReadable(f)}</div>\n`;
    const items = (byDate.get(f) || []).slice().sort((a, b) => (a.hora || '').localeCompare(b.hora || ''));
    items.forEach((a) => { out += `${a.actividad}\n`; /* sin hora */ });
    out += '\n';
  });
  out += '\n';

  // ===== CONTACTOS =====
  out += 'CONTACTOS IMPORTANTES:\n\n';
  if (!contactos.length) {
    out += '— SIN CONTACTOS —\n\n';
  } else {
    contactos.forEach((c) => {
      const line = [
        up(c.etiqueta || c.nombre || ''),
        up(c.persona || ''),
        up(c.telefono || ''),
        up(c.email   || '')
      ].filter(Boolean).join(' · ');
      out += `${line}\n`;
    });
    out += '\n';
  }

  // ===== FINANZAS =====
  out += 'FINANZAS:\n\n';
  if (!finanzas || !Array.isArray(finanzas.rows)) {
    out += '— SIN ABONOS REGISTRADOS —\n';
  } else {
    out += `ABONOS CLP ${ (finanzas.totales?.CLP || 0).toLocaleString('es-CL') } · USD ${ (finanzas.totales?.USD || 0) } · BRL ${ (finanzas.totales?.BRL || 0) } · ARS ${ (finanzas.totales?.ARS || 0) } · TOTAL CLP: ${ (finanzas.totalCLP || 0).toLocaleString('es-CL') }\n\n`;
    finanzas.rows.forEach((r) => {
      out += `${up(r.asunto || '')} \n`;
      out += `FECHA: ${r.fecha || '—'}\n`;
      out += `MONEDA/VALOR: ${r.moneda || ''} ${r.valor != null ? Number(r.valor).toLocaleString('es-CL') : ''}\n`;
      if (r.medio)   out += `MEDIO: ${up(r.medio)}\n`;
      if (r.detalle) out += `${up(r.detalle)}\n`;
      out += '\n';
    });
  }

  return out.trimEnd();
}

/* ====== IMPRESIÓN DE DESPACHO (PDF por print) ====== */

// Reúne una línea por actividad del itinerario con contacto de proveedor
async function collectItinLines(grupo) {
  const out = [];
  const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);

  for (const f of fechas) {
    // tolera objeto indexado
    let acts = (grupo.itinerario && grupo.itinerario[f]) ? grupo.itinerario[f] : [];
    if (!Array.isArray(acts)) acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');

    // ordenar por hora
    acts = acts.slice().sort((a, b) => timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

    for (const a of acts) {
      try {
        const actName  = (a?.actividad || '').toString();
        const servicio = await findServicio(grupo.destino, actName).catch(() => null);
        const provNom  = (servicio?.proveedor || a?.proveedor || '').toString();

        let provDoc = null;
        if (provNom) {
          provDoc = await fetchProveedorByDestino(
            (grupo.destino || '').toString().toUpperCase(),
            provNom
          ).catch(() => null);
        }

        const telefono = (provDoc?.telefono || '').toString().toUpperCase();
        const correo   = (provDoc?.correo   || '').toString().toUpperCase();
        const contacto = (provDoc?.contacto || '').toString().toUpperCase();
        const provTxt  = (provDoc?.proveedor || provNom || '—').toString().toUpperCase();
        const estado   = (grupo?.serviciosEstado?.[f]?.[slug(actName)]?.estado || '').toString().toUpperCase();

        out.push({
          fechaISO: f,
          hora: (a?.horaInicio || '--:--'),
          actividad: actName.toUpperCase(),
          proveedor: provTxt,
          contacto: [contacto, telefono, correo].filter(Boolean).join(' · '),
          estado
        });
      } catch (_) {}
    }
  }
  return out;
}

/* Recolector RÁPIDO: sin consultas, usa lo que ya está en g.itinerario */
async function collectItinLinesFast(grupo) {
  const out = [];
  const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);

  for (const f of fechas) {
    let acts = (grupo.itinerario && grupo.itinerario[f]) ? grupo.itinerario[f] : [];
    if (!Array.isArray(acts)) acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');

    acts = acts.slice().sort((a, b) => timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

    for (const a of acts) {
      const actName = (a?.actividad || '').toString().toUpperCase();
      const estado  = (grupo?.serviciosEstado?.[f]?.[slug(actName)]?.estado || '').toString().toUpperCase();

      out.push({
        fechaISO: f,
        hora: a?.horaInicio || '--:--',
        actividad: actName,
        proveedor: (a?.proveedor || '').toString().toUpperCase(),
        contacto: '',
        estado
      });
    }
  }
  return out;
}

async function openPrintDespacho(g, w) {
  if (!g) { alert('No hay viaje activo.'); return; }
  console.log('[PRINT] Inicia generación', { grupoId: g.id });

  // ==== DATOS BASE ====
  const code    = (g.numeroNegocio || '') + (g.identificador ? ('-' + g.identificador) : '');
  const paxPlan = paxOf(g);
  const paxReal = paxRealOf(g);
  const { A: A_real, E: E_real } = paxBreakdown(g);
  const fechasTxt = `${dmy(g.fechaInicio || '')} — ${dmy(g.fechaFin || '')}`;

  // 1) Itinerario (líneas con proveedor/contacto)
  console.time('[PRINT] collectItinLines');
  const itin = await collectItinLines(g).catch((e) => { console.error(e); return []; });
  console.timeEnd('[PRINT] collectItinLines');

  // 2) Finanzas: ABONOS (defensivo si faltan helpers externos)
  const haveLoadAbonos = (typeof loadAbonos === 'function');
  const haveConv       = (typeof convertirMoneda === 'function');

  let abonos = [];
  if (haveLoadAbonos) {
    try {
      console.time('[PRINT] loadAbonos');
      abonos = await loadAbonos(g.id);
      console.timeEnd('[PRINT] loadAbonos');
    } catch (e) {
      console.warn('[PRINT] loadAbonos falló:', e);
      abonos = [];
    }
  } else {
    console.warn('[PRINT] loadAbonos no definido. Se omiten abonos.');
  }

  const toCLP = async (valor, moneda) => {
    if (!haveConv) return 0;
    try { return await convertirMoneda(Number(valor || 0), String(moneda || 'CLP'), 'CLP'); }
    catch (e) { console.warn('[PRINT] convertirMoneda falló:', e); return 0; }
  };

  const abonosRows = [];
  for (const a of (abonos || [])) {
    const fecha  = dmy(toISO(a.fecha || ''));
    const moneda = (a.moneda || '').toString().toUpperCase();
    const valor  = Number(a.valor || 0);
    const clpEq  = await toCLP(valor, moneda);
    abonosRows.push({
      fecha,
      asunto: (a.asunto || '').toString().toUpperCase(),
      moneda,
      valor,
      clp: clpEq
    });
  }
  const totalCLP = Math.round(abonosRows.reduce((s, x) => s + Number(x.clp || 0), 0));

  // ==== HTML IMPRESIÓN ====
  const css = `
  <style>
    @page { size: A4; margin: 14mm; }
    body { font-family: system-ui, Arial, sans-serif; font-size: 12px; color: #0a0a0a; }
    .head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 10px; }
    .logo { height: 40px; object-fit: contain; }
    h1 { font-size: 18px; margin: 0 0 6px; }
    h2 { font-size: 14px; margin: 12px 0 6px; border-bottom: 1px solid #ddd; padding-bottom: 4px; }
    .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 6px 16px; }
    .muted { color: #555; }
    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 4px 6px; border-bottom: 1px solid #eee; vertical-align: top; }
    th { text-align: left; font-size: 11px; color: #444; }
    .t-tight td { padding: 3px 4px; }
    .right { text-align: right; }
    .badge { font-weight: 700; }
    .small { font-size: 11px; }
    .cut { page-break-inside: avoid; }
    .footer { margin-top: 8px; font-size: 10px; color: #666; }
  </style>`;

  const infoGeneral = `
    <div class="head">
      <div>
        <h1>DESPACHO DE VIAJE</h1>
        <div class="small muted">GENERADO: ${new Date().toLocaleString('es-CL').toUpperCase()}</div>
      </div>
      <img src="RaitraiLogo.png" class="logo" alt="RAITRAI"/>
    </div>

    <div class="grid2">
      <div><strong>GRUPO:</strong> ${(g.nombreGrupo || g.aliasGrupo || g.id).toString().toUpperCase()}</div>
      <div><strong>CÓDIGO:</strong> ${code.toUpperCase()}</div>
      <div><strong>DESTINO:</strong> ${(g.destino || '—').toString().toUpperCase()}</div>
      <div><strong>PROGRAMA:</strong> ${(g.programa || '—').toString().toUpperCase()}</div>
      <div><strong>FECHAS:</strong> ${fechasTxt}</div>
      <div><strong>PAX:</strong> PLAN ${paxPlan}${paxReal ? ` · REAL ${paxReal} (A:${A_real} · E:${E_real})` : ''}</div>
    </div>
  `;

  const resumen = `
    <h2>RESUMEN</h2>
    <div class="small">VIAJE: ${fechasTxt} · DESTINO: ${(g.destino || '—').toString().toUpperCase()} · PROGRAMA: ${(g.programa || '—').toString().toUpperCase()}</div>
  `;

  const itinRows = (itin || []).map(x => `
    <tr>
      <td>${dmy(x.fechaISO)}</td>
      <td>${(x.hora || '--:--')}</td>
      <td>${(x.actividad || '').toString().toUpperCase()}</td>
      <td>${(x.proveedor || '').toString().toUpperCase()}</td>
      <td>${x.contacto || '—'}</td>
      <td>${(x.estado || '').toString().toUpperCase()}</td>
    </tr>`).join('');

  const itinerario = `
    <h2>ITINERARIO</h2>
    <table class="t-tight">
      <thead>
        <tr>
          <th>FECHA</th><th>HORA</th><th>ACTIVIDAD</th><th>PROVEEDOR</th><th>CONTACTO</th><th>ESTADO</th>
        </tr>
      </thead>
      <tbody>${itinRows || '<tr><td colspan="6" class="muted">SIN ACTIVIDADES.</td></tr>'}</tbody>
    </table>
  `;

  const finRows = (abonosRows || []).map(r => `
    <tr>
      <td>${r.fecha || '—'}</td>
      <td>${r.asunto || '—'}</td>
      <td class="right">${(r.valor || 0).toLocaleString('es-CL')}</td>
      <td>${r.moneda || ''}</td>
      <td class="right">${Math.round(r.clp || 0).toLocaleString('es-CL')}</td>
    </tr>`).join('');

  const finanzas = haveLoadAbonos ? `
    <h2>FINANZAS — ABONOS</h2>
    <table class="t-tight">
      <thead>
        <tr><th>FECHA</th><th>ASUNTO</th><th class="right">MONTO</th><th>MONEDA</th><th class="right">EQUIV. CLP</th></tr>
      </thead>
      <tbody>${finRows || '<tr><td colspan="5" class="muted">SIN ABONOS REGISTRADOS.</td></tr>'}</tbody>
      <tfoot>
        <tr><th colspan="4" class="right">TOTAL CLP</th><th class="right">${totalCLP.toLocaleString('es-CL')}</th></tr>
      </tfoot>
    </table>
  ` : '';

  const html = `
    <!doctype html><html><head><meta charset="utf-8">${css}</head>
    <body>
      ${infoGeneral}
      ${resumen}
      <div class="cut">${itinerario}</div>
      ${finanzas ? `<div class="cut">${finanzas}</div>` : ''}
      <div class="footer">RAITRAI — Despacho de Viaje. Para PDF usa “Guardar como PDF”.</div>
      <script>
        window.addEventListener('load', () => {
          try { window.print(); } catch (_) {}
          setTimeout(() => { try { window.close(); } catch (_) {} }, 600);
        });
      </script>
    </body></html>
  `;

  // 3) Escribir en la ventana ya abierta
  try {
    w.document.open('text/html');
    w.document.write(html);
    w.document.close();
    console.log('[PRINT] HTML escrito en ventana.');
  } catch (e) {
    console.error('[PRINT] No se pudo escribir el HTML final', e);
    alert('No se pudo escribir el documento de impresión.');
    try { w.close(); } catch (_) {}
  }
}

// Busca proveedor en la ruta por DESTINO
async function fetchProveedorByDestino(destino, proveedorName) {
  if (!destino || !proveedorName) return null;
  try {
    const qs = await getDocs(collection(db, 'Proveedores', String(destino).toUpperCase(), 'Listado'));
    let hit = null;
    qs.forEach((d) => {
      const x = d.data() || {};
      const nom = (x.proveedor || d.id || '').toString();
      if (norm(nom) === norm(proveedorName)) hit = { id: d.id, ...x };
    });
    return hit;
  } catch (_) { return null; }
}

async function openActividadModal(g, fechaISO, act, servicio = null, tipoVoucher = 'NOAPLICA') {
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  const actName = (act?.actividad || 'ACTIVIDAD').toString();
  const actKey  = slug(actName);
  const destino = (g?.destino || '').toString().toUpperCase();

  // NUEVO: resolver hilo global (A/B/C) considerando CHEQ OUT
  let thread = { key: '', scope: 'B' };
  try { thread = await resolveThreadKey(g, fechaISO, act, servicio); } catch (_) {}

  // Servicio (para indicaciones/voucher)
  let indicaciones = '';
  let voucherLabel = (tipoVoucher || 'NOAPLICA').toString().toUpperCase();
  try {
    if (servicio) {
      indicaciones  = String(
        servicio.indicaciones || servicio.instrucciones || act?.indicaciones || act?.instrucciones || ''
      ).trim();
      const vRaw    = (servicio.voucher || voucherLabel || '').toString();
      voucherLabel  = /electron/i.test(vRaw) ? 'ELECTRONICO' : (/fisic/i.test(vRaw) ? 'FISICO' : 'NOAPLICA');
    }
  } catch (_) {}

  // Proveedor por DESTINO
  let proveedorDoc = null;
  try {
    const provNom = (servicio?.proveedor || act?.proveedor || '').toString();
    proveedorDoc  = await fetchProveedorByDestino(destino, provNom);
  } catch (_) {}

  const nombreProv  = (proveedorDoc?.proveedor || act?.proveedor || '—').toString().toUpperCase();
  const contactoNom = (proveedorDoc?.contacto  || '').toString().toUpperCase();
  const contactoTel = (proveedorDoc?.telefono  || '').toString().toUpperCase();
  const contactoMail= (proveedorDoc?.correo    || '').toString().toUpperCase();

  title.textContent = `DETALLE — ${actName.toUpperCase()} — ${dmy(fechaISO)}`;

  const scopeBadge = (thread.scope === 'A' ? 'PROVEEDOR' : (thread.scope === 'C' ? 'HOTEL/COMIDA' : 'GENERAL'));
  const scopeLine  = `<div class="meta"><strong>HILO:</strong> ${scopeBadge} · ${thread.key}</div>`;

  body.innerHTML = `
    ${scopeLine}
    <div class="card">
      <div class="meta"><strong>PROVEEDOR:</strong> ${nombreProv}</div>
      ${contactoNom ? `<div class="meta"><strong>CONTACTO:</strong> ${contactoNom}</div>` : ''}
      ${contactoTel ? `<div class="meta"><strong>TELÉFONO:</strong> ${contactoTel}</div>` : ''}
      ${contactoMail ? `<div class="meta"><strong>CORREO:</strong> ${contactoMail}</div>` : ''}
      <div class="meta"><strong>VOUCHER:</strong> ${voucherLabel}</div>
      <div class="meta"><strong>HORARIO:</strong> ${(act.horaInicio || '--:--')}–${(act.horaFin || '--:--')}</div>
    </div>
    <div class="act">
      <h4>INDICACIONES</h4>
      ${indicaciones ? `<div class="meta" style="white-space:pre-wrap">${indicaciones.toUpperCase()}</div>` : '<div class="muted">SIN INDICACIONES.</div>'}
    </div>
    <div class="act" id="foroBox">
      <h4>TIPS O COMENTARIOS</h4>
      <div class="rowflex" style="margin:.35rem 0">
        <textarea id="foroText" placeholder="ESCRIBE UN COMENTARIO (SE PUBLICA CON TU CORREO)"></textarea>
        <button id="foroSend" class="btn ok">PUBLICAR</button>
      </div>
      <div class="muted">-------</div>
      <div id="foroList" style="display:grid;gap:.4rem;margin-top:.5rem"></div>
      <div class="rowflex" style="justify-content:center;margin-top:.4rem">
        <button id="foroMore" class="btn sec" style="display:none">CARGAR MÁS</button>
      </div>
    </div>
  `;

  // ===== Paginación (STAFF arriba, 10 por página) =====
  const paging = { cursor: null, exhausted: false, loading: false, pageSize: 10, items: [] };

  const renderForo = () => {
    const wrap = body.querySelector('#foroList');
    wrap.innerHTML = '';

    const staff = paging.items.filter(x => x.isStaff).sort((a, b) => b.tsMs - a.tsMs);
    const resto = paging.items.filter(x => !x.isStaff).sort((a, b) => b.tsMs - a.tsMs);
    const ordered = [...staff, ...resto];

    if (!ordered.length) {
      wrap.innerHTML = '<div class="muted">AÚN NO HAY COMENTARIOS.</div>';
      body.querySelector('#foroMore').style.display = 'none';
      return;
    }

    ordered.forEach((x) => {
      const div = document.createElement('div');
      div.className = 'card';
      div.innerHTML = `
        <div class="meta" style="display:flex;gap:.5rem;align-items:center">
          ${x.isStaff ? '<span class="badge" style="background:#1d4ed8;color:#fff">STAFF</span>' : ''}
          <strong>${(x.byEmail || '').toUpperCase()}</strong> · ${formatCL(new Date(x.tsMs || Date.now()))}
        </div>
        <div style="margin-top:.25rem;white-space:pre-wrap">${(x.texto || '').toString().toUpperCase()}</div>
      `;
      wrap.appendChild(div);
    });

    const moreBtn = body.querySelector('#foroMore');
    moreBtn.style.display = paging.exhausted ? 'none' : '';
  };

  const loadPage = async () => {
    if (paging.loading || paging.exhausted) return;
    paging.loading = true;

    try {
      let qy = query(threadColl(thread.key), orderBy('ts', 'desc'), limit(paging.pageSize + 1));
      if (paging.cursor) {
        qy = query(threadColl(thread.key), orderBy('ts', 'desc'), startAfter(paging.cursor), limit(paging.pageSize + 1));
      }
      const snap = await getDocs(qy);
      const docs = snap.docs;

      if (docs.length > paging.pageSize) {
        paging.cursor = docs[paging.pageSize - 1];
      } else {
        paging.cursor  = docs[docs.length - 1] || paging.cursor;
        paging.exhausted = true;
      }

      const add = docs.slice(0, paging.pageSize).map((d) => {
        const x = d.data() || {};
        const tsMs = x.ts?.seconds ? x.ts.seconds * 1000 : Date.now();
        return {
          id: d.id,
          texto: String(x.texto || ''),
          byEmail: String(x.byEmail || x.by || '').toLowerCase(),
          isStaff: !!x.isStaff,
          tsMs
        };
      });

      const seen = new Set(paging.items.map(z => z.id));
      add.forEach(z => { if (!seen.has(z.id)) paging.items.push(z); });

      renderForo();
    } catch (e) {
      console.error('FORO loadPage', e);
      alert('NO SE PUDO CARGAR COMENTARIOS.');
    } finally {
      paging.loading = false;
    }
  };

  body.querySelector('#foroMore').onclick = loadPage;

  body.querySelector('#foroSend').onclick = async () => {
    const ta = body.querySelector('#foroText');
    const texto = (ta.value || '').trim();
    if (!texto) { alert('ESCRIBE UN COMENTARIO.'); return; }

    try {
      await addDoc(threadColl(thread.key), {
        texto,
        byUid: state.user.uid,
        byEmail: (state.user.email || '').toLowerCase(),
        isStaff: !!state.is,
        ts: serverTimestamp()
      });
      ta.value = '';
      paging.cursor = null; paging.exhausted = false; paging.items = [];
      await loadPage();
    } catch (e) {
      console.error('FORO send', e);
      alert('NO SE PUDO PUBLICAR.');
    }
  };

  document.getElementById('modalClose').onclick = () => {
    document.getElementById('modalBack').style.display = 'none';
  };

  back.style.display = 'flex';
  await loadPage();
}

async function setEstadoServicio(g, fechaISO, act, estado, logBitacora = false) {
  try {
    const key   = slug(act.actividad || '');
    const path  = doc(db, 'grupos', g.id);
    const payload = {};
    payload[`serviciosEstado.${fechaISO}.${key}`] = {
      estado,
      updatedAt: serverTimestamp(),
      by: (state.user.email || '').toLowerCase()
    };
    await updateDoc(path, payload);

    (g.serviciosEstado ||= {});
    (g.serviciosEstado[fechaISO] ||= {});
    g.serviciosEstado[fechaISO][key] = { estado };

    document.getElementById('modalBack').style.display = 'none';
    renderItinerario(g, document.getElementById('paneItin'), fechaISO);

    if (logBitacora) {
      const timeId = timeIdNowMs();
      const ref = doc(db, 'grupos', g.id, 'bitacora', key, fechaISO, timeId);
      await setDoc(ref, {
        texto: `ACTIVIDAD ${estado.toLowerCase()}`,
        byUid: state.user.uid,
        byEmail: (state.user.email || '').toLowerCase(),
        ts: serverTimestamp()
      });
    }
  } catch (e) {
    console.error(e);
    alert('NO FUE POSIBLE ACTUALIZAR EL ESTADO.');
  }
}

/* ====== VIAJE: RESTABLECER (STAFF) ====== */
async function resetInicioFinViaje(grupo) {
  if (!state.is) { alert('Solo el STAFF puede restablecer el inicio/fin de viaje.'); return; }
  if (!confirm('¿Restablecer INICIO/FIN DE VIAJE y borrar PAX VIAJANDO?')) return;

  try {
    const ref = doc(db, 'grupos', grupo.id);

    // Borra override y marcas de inicio/fin (cubrimos nombres posibles)
    await updateDoc(ref, {
      paxViajando: deleteField(),
      trip: deleteField(),
      viaje: deleteField(),
      viajeInicioAt: deleteField(),
      viajeFinAt: deleteField(),
      viajeInicioBy: deleteField(),
      viajeFinBy: deleteField()
    });

    // Limpia en memoria/local
    delete grupo.paxViajando;
    delete grupo.trip;
    delete grupo.viaje;
    delete grupo.viajeInicioAt;
    delete grupo.viajeFinAt;
    delete grupo.viajeInicioBy;
    delete grupo.viajeFinBy;

    try { localStorage.removeItem('rt__paxStart_' + grupo.id); } catch (_) {}

    // Re-render para quitar “tachado”
    await renderOneGroup(grupo);
  } catch (e) {
    console.error(e);
    alert('No se pudo restablecer el viaje.');
  }
}

/* ====== ALERTAS ====== */
/** AYUDA: OBTENER NOMBRE POR EMAIL (MAYÚSCULAS) */
function upperNameByEmail(email) {
  const e = (email || '').toLowerCase();
  const c = state.coordinadores.find(x => (x.email || '').toLowerCase() === e);
  const n = (c?.nombre || '').toString().toUpperCase();
  return n || e.toUpperCase();
}
/* ═══════════════════ ALERTAS + FINANZAS + VOUCHERS + RESET (BLOQUE COMPLETO) ═══════════════════ */

/** DESTINATARIOS POR FILTROS (DESTINOS, RANGO/FECHA) ESCANEANDO TODOS LOS GRUPOS */
async function recipientsFromFilters(destinosList, rangoStr){
  const wantedDest = destinosList.map(d=>norm(d)).filter(Boolean);
  let A=null,B=null;
  const rtrim = (rangoStr||'').trim();
  if(/^\d{2}-\d{2}-\d{4}\.\.\d{2}-\d{2}-\d{4}$/.test(rtrim)){
    const [a,b]=rtrim.split('..'); A=ymdFromDMY(a); B=ymdFromDMY(b);
  } else if(/^\d{2}-\d{2}-\d{4}$/.test(rtrim)){
    const d=ymdFromDMY(rtrim); A=d; B=d;
  }

  if(!wantedDest.length && !A) return new Set();

  const r = new Set();
  const mapEmailToId = new Map((state.coordinadores||[]).map(c=>[(c.email||'').toLowerCase(), c.id]));
  const snap=await getDocs(collection(db,'grupos'));
  snap.forEach(d=>{
    const g={id:d.id, ...(d.data()||{})};
    const destOk = !wantedDest.length
      || wantedDest.includes(norm(g.destino||''))
      || wantedDest.includes(norm(g.Destino||''));
    let dateOk = true;
    if(A){
      const ini=toISO(g.fechaInicio||g.inicio||g.fecha_ini), fin=toISO(g.fechaFin||g.fin||g.fecha_fin);
      dateOk = !( (fin && fin < A) || (ini && ini > B) );
    }
    if(destOk && dateOk){
      const ids = (typeof coordDocIdsOf==='function') ? coordDocIdsOf(g) : [];
      ids.forEach(id=>r.add(String(id)));
      if (typeof emailsOf==='function'){
        emailsOf(g).forEach(e=>{ const k=(e||'').toLowerCase(); if(mapEmailToId.has(k)) r.add(mapEmailToId.get(k)); });
      }
    }
  });
  return r;
}

/** MODAL: CREAR ALERTA */
async function openCreateAlertModal(){
  const back=document.getElementById('modalBack'), body=document.getElementById('modalBody'), title=document.getElementById('modalTitle');
  title.textContent='CREAR ALERTA';
  const coordOpts=(state.coordinadores||[])
    .map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()} — ${(c.email||'').toUpperCase()}</option>`)
    .join('');
  body.innerHTML=`
   <div class="rowflex" style="gap:.5rem;flex-wrap:wrap">
      <input id="alertDestinos" type="text" placeholder="DESTINOS (SEPARADOS POR COMA, OPCIONAL)"/>
      <input id="alertRango" type="text" placeholder="RANGO DD-MM-AAAA..DD-MM-AAAA O FECHA ÚNICA"/>
    </div>
    <div class="rowflex">
      <label>DESTINATARIOS (COORDINADORES)</label>
      <select id="alertCoords" multiple size="8" style="width:100%">${coordOpts}</select>
    </div>
    <div class="rowflex"><textarea id="alertMsg" placeholder="MENSAJE" style="width:100%"></textarea></div>
    <div class="rowflex"><button id="alertSave" class="btn ok">ENVIAR</button></div>`;

  document.getElementById('alertSave').onclick=async ()=>{
    const msg=(document.getElementById('alertMsg').value||'').trim();
    const sel=Array.from(document.getElementById('alertCoords').selectedOptions).map(o=>o.value);
    const destinos=(document.getElementById('alertDestinos').value||'').split(',').map(x=>x.trim()).filter(Boolean);
    const rango=(document.getElementById('alertRango').value||'').trim();

    if(!msg && !destinos.length){ alert('ESCRIBE UN MENSAJE O USA FILTROS.'); return; }

    const set=new Set(sel);
    try{
      const fromFilters = await recipientsFromFilters(destinos, rango);
      fromFilters.forEach(id=>set.add(id));
    }catch(e){ console.error(e); }

    const forCoordIds=[...set];
    if(!forCoordIds.length){ alert('NO HAY DESTINATARIOS. REVISA FILTROS/SELECCIÓN.'); return; }

    await addDoc(collection(db,'alertas'),{
      audience:'coord',
      mensaje: msg.toUpperCase(),
      forCoordIds,
      meta:{ filtros:{ destinos, rango } },
      createdAt:serverTimestamp(),
      createdBy:{ uid:(state.user&&state.user.uid)||null, email:(state.user?.email||'').toLowerCase() },
      readBy:{}
    });
    back.style.display='none';
    if (window.renderGlobalAlertsV2) await window.renderGlobalAlertsV2();
  };
  document.getElementById('modalClose').onclick=()=>{ back.style.display='none'; };
  back.style.display='flex';
}

// ====== PANEL GLOBAL DE ALERTAS (COMPLETO) ======
// Requisitos externos que ya tienes en el file:
// - Firestore: db, collection, getDocs, getDoc, doc, setDoc, addDoc, updateDoc, serverTimestamp, query, orderBy, limit
// - Helpers que ya existen en tu app: norm(str), ymdFromDMY('DD-MM-AAAA'), toISO(any), emailsOf(grupo), coordDocIdsOf(grupo)
// - Estado global: state.user {uid,email}, state.coordinadores[], state.is (STAFF? true/false), state.viewingCoordId seleccionado
// - En el HTML existe <div id="alertsPanelV2" class="panel"></div>

// ------------------------
// Utilitarios locales
// ------------------------
function _byCreatedAtDesc(a, b){
  const ax = a?.createdAt?.seconds || a?.createdAt?._seconds || 0;
  const bx = b?.createdAt?.seconds || b?.createdAt?._seconds || 0;
  return bx - ax;
}
function _fmtWhen(ts){
  try{
    const d = ts?.seconds ? new Date(ts.seconds*1000) :
              ts?._seconds ? new Date(ts._seconds*1000) : null;
    if(!d) return '';
    return d.toLocaleString('es-CL').toUpperCase();
  }catch(_){ return ''; }
}

// ------------------------
// Filtrado por destinos/fechas escaneando grupos
// Devuelve Set de coordIds destino del mensaje
// ------------------------
async function recipientsFromFilters(destinosList, rangoStr){
  const wantedDest = (destinosList||[]).map(d=>norm(d)).filter(Boolean);
  let A=null,B=null;
  const raw = (rangoStr||'').trim();
  if(/^\d{2}-\d{2}-\d{4}\.\.\d{2}-\d{2}-\d{4}$/.test(raw)){
    const [a,b]=raw.split('..'); A=ymdFromDMY(a); B=ymdFromDMY(b);
  }else if(/^\d{2}-\d{2}-\d{4}$/.test(raw)){
    const d=ymdFromDMY(raw); A=d; B=d;
  }

  if(!wantedDest.length && !A) return new Set();

  const r = new Set();
  const mapEmailToId = new Map((state.coordinadores||[]).map(c=>[(c.email||'').toLowerCase(), c.id]));

  const snap = await getDocs(collection(db,'grupos'));
  snap.forEach(d=>{
    const g = { id:d.id, ...(d.data()||{}) };
    const destOk = !wantedDest.length
      || wantedDest.includes(norm(g.destino||''))
      || wantedDest.includes(norm(g.Destino||''));
    let dateOk = true;
    if(A){
      const ini = toISO(g.fechaInicio||g.inicio||g.fecha_ini);
      const fin = toISO(g.fechaFin||g.fin||g.fecha_fin);
      // fuera del rango si fin < A  o ini > B
      dateOk = !( (fin && fin < A) || (ini && ini > B) );
    }
    if(destOk && dateOk){
      // ids de coordinadores por documento
      if (typeof coordDocIdsOf === 'function'){
        const ids = coordDocIdsOf(g) || [];
        ids.forEach(id=> r.add(String(id)));
      }
      // emails → id
      const emails = (typeof emailsOf === 'function') ? emailsOf(g) : [];
      emails.forEach(e=>{
        const id = mapEmailToId.get((e||'').toLowerCase());
        if(id) r.add(id);
      });
    }
  });
  return r;
}

// ------------------------
// Modal: Crear Alerta (STAFF)
// ------------------------
async function openCreateAlertModal(){
  const back  = document.getElementById('modalBack');
  const body  = document.getElementById('modalBody');
  const title = document.getElementById('modalTitle');

  title.textContent = 'CREAR ALERTA';
  const coordOpts = (state.coordinadores||[])
    .map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()} — ${(c.email||'').toUpperCase()}</option>`)
    .join('');

  body.innerHTML = `
    <div class="rowflex">
      <input id="alertDestinos" type="text" placeholder="DESTINOS (SEPARADOS POR COMA, OPCIONAL)"/>
      <input id="alertRango" type="text" placeholder="RANGO DD-MM-AAAA..DD-MM-AAAA O FECHA ÚNICA"/>
    </div>
    <div class="rowflex">
      <label>DESTINATARIOS (COORDINADORES)</label>
      <select id="alertCoords" multiple size="8" style="width:100%">${coordOpts}</select>
    </div>
    <div class="rowflex"><textarea id="alertMsg" placeholder="MENSAJE" style="width:100%"></textarea></div>
    <div class="rowflex"><button id="alertSave" class="btn ok">ENVIAR</button></div>
  `;

  const onClose = ()=>{ document.getElementById('modalBack').style.display='none'; };
  document.getElementById('modalClose').onclick = onClose;

  document.getElementById('alertSave').onclick = async ()=>{
    const msg = (document.getElementById('alertMsg').value||'').trim();
    const sel = Array.from(document.getElementById('alertCoords').selectedOptions).map(o=>o.value);
    const destinos = (document.getElementById('alertDestinos').value||'').split(',').map(x=>x.trim()).filter(Boolean);
    const rango = (document.getElementById('alertRango').value||'').trim();

    if(!msg && !destinos.length){
      alert('ESCRIBE UN MENSAJE O USA FILTROS.'); return;
    }

    const set = new Set(sel);
    try{
      const fromFilters = await recipientsFromFilters(destinos, rango);
      fromFilters.forEach(id => set.add(id));
    }catch(e){ console.error('recipientsFromFilters', e); }

    const forCoordIds = [...set];
    if(!forCoordIds.length){
      alert('NO HAY DESTINATARIOS. REVISA FILTROS/SELECCIÓN.');
      return;
    }

    await addDoc(collection(db,'alertas'),{
      audience:'coord',           // ámbito coordinadores
      mensaje: msg.toUpperCase(), // consistente con UI mayúsculas
      forCoordIds,
      meta:{ filtros:{ destinos, rango } },
      createdAt: serverTimestamp(),
      createdBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() },
      readBy:{}                   // mapa uid→true
    });

    onClose();
    if (typeof window.renderGlobalAlertsV2 === 'function'){
      await window.renderGlobalAlertsV2();
    }
  };

  back.style.display = 'flex';
}

// ------------------------
// Renderizador de lista (unread/read) — retorna {ui, unreadCount, readCount}
// ------------------------
function _renderList(list, scopeKey){
  const wrap = document.createElement('div');
  let unread = 0, read = 0;

  if(!Array.isArray(list) || !list.length){
    wrap.innerHTML = `<div class="muted">SIN ALERTAS.</div>`;
    return { ui: wrap, unreadCount:0, readCount:0 };
  }

  const userId = state?.user?.uid || 'anon';
  const byTab = {
    unread: list.filter(x => !(x.readBy && x.readBy[userId])),
    read:   list.filter(x =>  (x.readBy && x.readBy[userId]))
  };
  unread = byTab.unread.length;
  read   = byTab.read.length;

  // Tabs
  const tabs = document.createElement('div');
  tabs.className = 'tabs';

  const tabUnread = document.createElement('div');
  tabUnread.className = 'tab active';
  tabUnread.innerHTML = `NO LEÍDAS <span class="badge">${unread}</span>`;

  const tabRead = document.createElement('div');
  tabRead.className = 'tab';
  tabRead.innerHTML = `LEÍDAS <span class="badge">${read}</span>`;

  tabs.appendChild(tabUnread);
  tabs.appendChild(tabRead);

  const content = document.createElement('div');

  const paint = (which)=>{
    content.innerHTML = '';
    const arr = which==='read' ? byTab.read : byTab.unread;
    (arr||[]).forEach(a=>{
      const card = document.createElement('div');
      card.className = 'alert-card';
      const when = _fmtWhen(a.createdAt);
      const metaFiltro = a?.meta?.filtros;
      card.innerHTML = `
        <div class="alert-title">${(a.mensaje||'').toString().toUpperCase()}</div>
        ${metaFiltro ? `<div class="meta">FILTROS: ${(metaFiltro.destinos||[]).join(', ')} ${metaFiltro.rango?('· ' + metaFiltro.rango):''}</div>` : ''}
        ${when ? `<div class="meta">${when}</div>` : ''}
        ${state.is ? `<div class="meta">PARA: ${(a.forCoordIds||[]).length} COORD.</div>` : ''}
      `;

      // Marcar como leído al hacer click (simple)
      card.onclick = async ()=>{
        try{
          a.readBy = a.readBy || {};
          if(!a.readBy[userId]){
            a.readBy[userId] = true;
            await setDoc(doc(db,'alertas', a.id), { readBy: a.readBy }, { merge:true });
            // refresca rápido sin reseleccionar pestaña
            paint(which);
            // y actualiza contadores de tabs
            const nu = Math.max(0, (which==='unread'?arr.length-1:byTab.unread.length));
            const nr = (which==='unread'?byTab.read.length+1:arr.length);
            tabUnread.innerHTML = `NO LEÍDAS <span class="badge">${nu}</span>`;
            tabRead.innerHTML   = `LEÍDAS <span class="badge">${nr}</span>`;
          }
        }catch(e){ console.warn('mark read', e); }
      };

      content.appendChild(card);
      const sep = document.createElement('div');
      sep.className = 'alert-sep';
      content.appendChild(sep);
    });
  };

  tabUnread.onclick = ()=>{
    tabUnread.classList.add('active');
    tabRead.classList.remove('active');
    paint('unread');
  };
  tabRead.onclick = ()=>{
    tabRead.classList.add('active');
    tabUnread.classList.remove('active');
    paint('read');
  };

  wrap.appendChild(tabs);
  wrap.appendChild(content);
  paint('unread');

  return { ui: wrap, unreadCount: unread, readCount: read };
}

// ------------------------
// Tirita para plegar/desplegar y recordar estado
// ------------------------
function ensureAlertsFoldStrip(panel){
  if (!panel) return;
  if (document.getElementById('alertsFoldStrip')) return;

  const strip = document.createElement('div');
  strip.id = 'alertsFoldStrip';
  strip.style.cssText = 'display:flex;align-items:center;justify-content:flex-start;margin:.35rem 0;';
  strip.innerHTML = `<button id="btnFoldAlerts" class="btn sec" aria-expanded="true">▼ OCULTAR ALERTAS</button>`;

  panel.parentNode.insertBefore(strip, panel);

  const btn = strip.querySelector('#btnFoldAlerts');
  const loadPref = localStorage.getItem('rt__alerts_fold') === '1';

  const apply = (fold) => {
    panel.style.display = fold ? 'none' : '';
    btn.textContent = fold ? '► MOSTRAR ALERTAS' : '▼ OCULTAR ALERTAS';
    btn.setAttribute('aria-expanded', String(!fold));
  };
  apply(loadPref);

  btn.onclick = () => {
    const willFold = panel.style.display !== 'none';
    localStorage.setItem('rt__alerts_fold', willFold ? '1' : '0');
    apply(willFold);
  };
}

// ------------------------
// Carga y render principal (V2)
// ------------------------
async function renderGlobalAlertsV2(){
  // Panel destino (V2)
  const box = document.getElementById('alertsPanelV2');
  if (!box) return;

  // Señal de inicialización (para fallback del HTML)
  window.state = window.state || {};
  window.state.alertsUI = window.state.alertsUI || {};
  window.state.alertsUI.inited = true;

  box.innerHTML = `<div class="muted">CARGANDO…</div>`;

  // Trae alertas
  // Por ahora leemos las más recientes; ajusta el límite si lo necesitas.
  let list = [];
  try{
    const qs = await getDocs(query(collection(db,'alertas'), orderBy('createdAt','desc'), limit(200)));
    qs.forEach(d => list.push({ id:d.id, ...(d.data()||{}) }));
  }catch(e){ console.error('load alertas', e); }

  list.sort(_byCreatedAtDesc);

  // Ambito visible para el usuario:
  // - STAFF: ve todo (sección OPERACIONES muestra todas; "PARA MÍ" solo las dirigidas a su coord activo)
  // - COORDINADOR: solo lo dirigido a su coord id (o por email mapeado a id)
  const userId = state?.user?.uid || 'anon';
  const myCoordId = (()=>{
    // priorizamos selector activo; si no, buscamos por email mapeado
    if (state?.viewingCoordId && state.viewingCoordId !== '__ALL__') return state.viewingCoordId;
    const email = (state?.user?.email||'').toLowerCase();
    const hit = (state.coordinadores||[]).find(c => (c.email||'').toLowerCase() === email);
    return hit?.id || '__NONE__';
  })();

  // filtra "para mí"
  const paraMi = list.filter(a => Array.isArray(a.forCoordIds) && a.forCoordIds.includes(myCoordId));

  // si es staff, sección "operaciones" muestra todo el set
  const ops = state.is ? list.slice() : [];

  // HEADER
  box.innerHTML = '';
  const head = document.createElement('div');
  head.className = 'alert-head';

  const left = document.createElement('div');
  left.className = 'alert-title-row';
  left.innerHTML = `<h4>ALERTAS</h4>`;

  const right = document.createElement('div');
  if (state.is){
    const btn = document.createElement('button');
    btn.id = 'btnCreateAlert';
    btn.className = 'btn ok';
    btn.textContent = 'CREAR ALERTA';
    btn.onclick = openCreateAlertModal;
    right.appendChild(btn);
  }

  head.appendChild(left);
  head.appendChild(right);
  box.appendChild(head);

  // CONTENEDOR
  const area = document.createElement('div');
  box.appendChild(area);

  // RENDER SECCIÓN PARA MÍ
  const mi = _renderList(paraMi, 'mi');
  const secMi = document.createElement('div');
  secMi.className = 'act';
  secMi.innerHTML = `<h4>PARA MÍ</h4>`;
  secMi.appendChild(mi.ui);
  area.appendChild(secMi);

  // RENDER SECCIÓN OPERACIONES (solo STAFF)
  if (state.is){
    const op = _renderList(ops, 'ops');
    const secOp = document.createElement('div');
    secOp.className = 'act';
    secOp.innerHTML = `<h4>OPERACIONES</h4>`;
    secOp.appendChild(op.ui);
    area.appendChild(secOp);
  }

  // Tira plegable (arriba) con persistencia
  ensureAlertsFoldStrip(box);

  // Auto-refresco suave cada 60s (sin duplicar timers)
  if (!window.state.alertsUI.timer){
    window.state.alertsUI.timer = setInterval(()=>{
      // refresh sin bloquear UI
      renderGlobalAlertsV2().catch(()=>{});
    }, 60000);
  }
}

// ------------------------
// Compatibilidad: deja el nombre antiguo apuntando a V2
// (wrapper sin "return" suelto, dentro de la función)
// ------------------------
async function renderGlobalAlerts(){
  return await renderGlobalAlertsV2();
}

// expone para otros bloques (ej. openCreateAlertModal)
window.renderGlobalAlertsV2 = renderGlobalAlertsV2;

// (Opcional) inicializar en load si quieres:
document.addEventListener('DOMContentLoaded', ()=> { renderGlobalAlertsV2().catch(()=>{}); });


  // GASTOS
  const paneGastos=document.createElement('div');
  const ghits = await renderGastos(g, paneGastos);
  wrap.appendChild(paneGastos);

  // CIERRE FINANCIERO
  const cierre=document.createElement('div'); cierre.className='act';
  cierre.innerHTML=`
    <h4>CIERRE FINANCIERO</h4>
    <div class="card">
      <div class="meta"><strong>DATOS DE TRANSFERENCIA</strong></div>
      <div class="meta">CUENTA CORRIENTE N° 03398-07 · BANCO DE CHILE</div>
      <div class="meta">TURISMO RAITRAI LIMITADA · RUT 78.384.230-0</div>
      <div class="meta">aleoperaciones@raitrai.cl</div>
    </div>
    <div class="rowflex" style="margin:.5rem 0; flex-wrap:wrap; gap:.5rem">
      <label class="meta" style="display:flex;gap:.4rem;align-items:center">
        <input id="chTransf" type="checkbox"/> TRANSFERENCIA REALIZADA
      </label>
      <input id="upComp" type="file" accept="image/*,application/pdf"/>
      <button id="btnUpComp" class="btn sec">SUBIR COMPROBANTE</button>
    </div>
    <div class="rowflex" style="margin:.5rem 0; flex-wrap:wrap; gap:.5rem; align-items:center">
      <a href="https://www.sii.cl" target="_blank" class="btn sec">IR A SII.CL</a>
      <input id="upBoleta" type="file" accept="image/*,application/pdf"/>
      <button id="btnUpBoleta" class="btn sec">SUBIR BOLETA</button>
    </div>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="btnCloseFin" class="btn ok" disabled>CERRAR FINANZAS</button>
    </div>
  `;
  wrap.appendChild(cierre);

  const sumPrev = (typeof ensureFinanzasSummary==='function') ? (await ensureFinanzasSummary(g.id) || {}) : {};
  const ch = cierre.querySelector('#chTransf');
  if (sumPrev?.transfer?.done && ch) ch.checked = true;

  const checkReady = ()=>{
    const transfOk = !!(ch && ch.checked);
    const boletaOk = !!sumPrev?.boleta?.uploaded;
    const btn = cierre.querySelector('#btnCloseFin');
    if (btn) btn.disabled = !(transfOk && boletaOk);
  };
  checkReady();

  const upCompBtn = cierre.querySelector('#btnUpComp');
  if (upCompBtn) upCompBtn.onclick = async ()=>{
    const file = cierre.querySelector('#upComp').files[0]||null;
    if (!file){ alert('Selecciona el comprobante.'); return; }
    if (file.size > 15*1024*1024){ alert('Archivo supera 15MB.'); return; }
    const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
    const path = `finanzas/${g.id}/comprobantes/${Date.now()}_${safe}`;
    const r = sRef(storage, path);
    await uploadBytes(r, file, { contentType: file.type || 'application/octet-stream' });
    const url = await getDownloadURL(r);
    await updateFinanzasSummary(g.id, { transfer:{ done:true, fecha: todayISO(), medio:'TRANSFERENCIA', comprobanteUrl:url } });
    sumPrev.transfer = { done:true, fecha: todayISO(), medio:'TRANSFERENCIA', comprobanteUrl:url };
    if (ch) ch.checked = true;
    checkReady();
    if (typeof showFlash==='function') showFlash('COMPROBANTE SUBIDO', 'ok');
  };

  const upBolBtn = cierre.querySelector('#btnUpBoleta');
  if (upBolBtn) upBolBtn.onclick = async ()=>{
    const file = cierre.querySelector('#upBoleta').files[0]||null;
    if (!file){ alert('Selecciona la boleta (imagen o PDF).'); return; }
    if (file.size > 15*1024*1024){ alert('Archivo supera 15MB.'); return; }
    const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
    const path = `finanzas/${g.id}/boletas/${Date.now()}_${safe}`;
    const r = sRef(storage, path);
    await uploadBytes(r, file, { contentType: file.type || 'application/pdf' });
    const url = await getDownloadURL(r);
    await updateFinanzasSummary(g.id, { boleta:{ uploaded:true, url, filename:safe } });
    sumPrev.boleta = { uploaded:true, url, filename:safe };
    checkReady();
    if (typeof showFlash==='function') showFlash('BOLETA SUBIDA', 'ok');
  };

  const closeBtn = cierre.querySelector('#btnCloseFin');
  if (closeBtn) closeBtn.onclick = async ()=>{
    if (!(ch && ch.checked)){ alert('Marca transferencia realizada / sube comprobante.'); return; }
    if (!sumPrev?.boleta?.uploaded){ alert('Debes subir boleta para cerrar.'); return; }
    await closeFinanzas(g);
    await renderFinanzas(g, pane);
  };

  await updateFinanzasSummary(g.id, {
    totals:{
      abonos: totAb, gastos: totGa,
      tasas: tasas, abonosCLP: abCLP.CLPconv, gastosCLP: gaCLP.CLPconv, saldoCLP
    }
  });

  const hitsAb = qNorm ? abonos
    .filter(a => norm([a.asunto,a.comentarios,a.medio,String(a.valor||0)].join(' ')).includes(qNorm))
    .length : 0;
  return hitsAb + (ghits||0);
}

/* ===== TASAS DESDE FIRESTORE: Config/Finanzas (USD como pivote) =====
   ADICIÓN: helpers que usa getTasas()
   Lee Config/Finanzas y arma perUSD (CLP/BRL/ARS por 1 USD). Guarda en cache.
*/
async function loadTasasFinanzas(){
  // Usa cache si ya se cargó desde Config/Finanzas
  if (state.cache && state.cache.tasas && state.cache.tasas.__from === 'Config/Finanzas') {
    return state.cache.tasas;
  }

  try{
    const snap = await getDoc(doc(db,'Config','Finanzas'));
    if (snap.exists()){
      const x = snap.data() || {};
      const perUSD = {
        USD: 1,
        CLP: Number(x.tcUSD || 945),   // CLP por USD
        BRL: Number(x.tcBRL || 5.5),   // BRL por USD
        ARS: Number(x.tcARS || 1370),  // ARS por USD
      };
      state.cache = state.cache || {};
      state.cache.tasas = { __from:'Config/Finanzas', perUSD };
      return state.cache.tasas;
    }
  }catch(_e){ /* noop: usaremos fallback abajo */ }

  // Fallback razonable si no hay doc Config/Finanzas
  const perUSD = { USD:1, CLP:945, BRL:5.5, ARS:1370 };
  state.cache = state.cache || {};
  state.cache.tasas = { __from:'fallback', perUSD };
  return state.cache.tasas;
}

/* ===== Tasas “simples” para UI: CLP por USD/BRL/ARS (derivadas de perUSD) =====
   Se usa en loadGastosList() para el EQUIV. CLP
*/
async function getTasas(){
  if (state.cache?.tasasCLP) return state.cache.tasasCLP;
  const { perUSD } = await loadTasasFinanzas();
  const clpPerUSD = perUSD.CLP;
  const clpPerBRL = perUSD.CLP / perUSD.BRL;
  const clpPerARS = perUSD.CLP / perUSD.ARS;
  state.cache = state.cache || {};
  state.cache.tasasCLP = { USD: clpPerUSD, BRL: clpPerBRL, ARS: clpPerARS };
  return state.cache.tasasCLP;
}

// -------- Modal editor de ABONO (STAFF) ----------
async function openAbonoEditor(g, abono, onSaved){
  const isEdit = !!abono;
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = (isEdit?'EDITAR ABONO':'NUEVO ABONO');

  const seed = abono || {
    asunto:'', comentarios:'', moneda:'CLP', valor:'', fecha: todayISO(), medio:'CTA CTE', autoCalc:false, provWhitelistHit:null, refActs:[]
  };

  body.innerHTML = `
    <div class="rowflex" style="gap:.5rem;flex-wrap:wrap">
      <input id="abAsunto" type="text" placeholder="ASUNTO" value="${(seed.asunto||'')}"/>
      <select id="abMon">
        <option value="CLP"${seed.moneda==='CLP'?' selected':''}>CLP</option>
        <option value="USD"${seed.moneda==='USD'?' selected':''}>USD</option>
        <option value="BRL"${seed.moneda==='BRL'?' selected':''}>BRL</option>
        <option value="ARS"${seed.moneda==='ARS'?' selected':''}>ARS</option>
      </select>
      <input id="abVal" type="number" min="0" inputmode="numeric" placeholder="VALOR" value="${seed.valor||''}"/>
      <input id="abFec" type="date" value="${toISO(seed.fecha)||todayISO()}"/>
      <input id="abMed" type="text" placeholder="MEDIO (CTA CTE / EFECTIVO / ...)" value="${(seed.medio||'')}"/>
    </div>
    <div class="rowflex" style="margin-top:.5rem">
      <textarea id="abCom" placeholder="COMENTARIOS" style="width:100%">${seed.comentarios||''}</textarea>
    </div>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="abSave" class="btn ok">${isEdit?'GUARDAR':'CREAR'}</button>
    </div>
  `;

  body.querySelector('#abSave').onclick = async ()=>{
    const data = {
      id: seed.id,
      asunto: (body.querySelector('#abAsunto').value||'').trim(),
      comentarios: (body.querySelector('#abCom').value||'').trim(),
      moneda: (body.querySelector('#abMon').value||'CLP').toUpperCase(),
      valor: Number(body.querySelector('#abVal').value||0),
      fecha: toISO(body.querySelector('#abFec').value||todayISO()),
      medio: (body.querySelector('#abMed').value||'').trim() || 'CTA CTE',
      autoCalc: !!seed.autoCalc,
      provWhitelistHit: seed.provWhitelistHit || null,
      refActs: Array.isArray(seed.refActs)? seed.refActs : []
    };
    if (!data.asunto || !data.valor){ alert('ASUNTO y VALOR son obligatorios.'); return; }
    const id = await saveAbono(g.id, data);
    const saved = { id: id || data.id, ...data };
    document.getElementById('modalBack').style.display='none';
    onSaved && onSaved(saved);
  };

  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}

async function loadGastosList(g, box, coordId){
  const qs=await getDocs(query(collection(db,'coordinadores',coordId,'gastos'), orderBy('createdAt','desc')));
  let list=[]; qs.forEach(d=>{ const x=d.data()||{}; if(x.grupoId===g.id) list.push({id:d.id,...x}); });

  const q = norm(state.groupQ||''); let hits=0;
  if(q){ list = list.filter(x => norm([x.asunto,x.byEmail,x.moneda,String(x.valor||0)].join(' ')).includes(q)); hits = list.length; }

  const tasas=await getTasas(); // CLP por unidad
  const tot={ CLP:0, USD:0, BRL:0, ARS:0, CLPconv:0 };

  const table=document.createElement('table'); table.className='table';
  table.innerHTML='<thead><tr><th>ASUNTO</th><th>AUTOR</th><th>MONEDA</th><th>VALOR</th><th>COMPROBANTE</th></tr></thead><tbody></tbody>';
  const tb=table.querySelector('tbody');

  list.forEach(x=>{
     const tr=document.createElement('tr');
     tr.innerHTML = `
       <td data-label="ASUNTO">${(x.asunto||'').toString().toUpperCase()}</td>
       <td data-label="AUTOR">${(x.byEmail||'').toString().toUpperCase()}</td>
       <td data-label="MONEDA">${(x.moneda||'').toString().toUpperCase()}</td>
       <td data-label="VALOR">${Number(x.valor||0).toLocaleString('es-CL')}</td>
       <td data-label="COMPROBANTE">${x.imgUrl?`<a href="${x.imgUrl}" target="_blank">VER</a>`:'—'}</td>
     `;
     tb.appendChild(tr);

     const m = String(x.moneda||'CLP').toUpperCase();
     if(m==='CLP') tot.CLP+=Number(x.valor||0);
     if(m==='USD') tot.USD+=Number(x.valor||0);
     if(m==='BRL') tot.BRL+=Number(x.valor||0);
     if(m==='ARS') tot.ARS+=Number(x.valor||0);
  });

  // Equivalente CLP usando tasas CLP por unidad
  tot.CLPconv = tot.CLP
              + (tot.USD * (tasas.USD||0))
              + (tot.BRL * (tasas.BRL||0))
              + (tot.ARS * (tasas.ARS||0));

  box.innerHTML='<h4>GASTOS DEL GRUPO</h4>'; box.appendChild(table);

  const totDiv=document.createElement('div'); totDiv.className='totline';
  totDiv.textContent=`TOTAL CLP: ${tot.CLP.toLocaleString('es-CL')} · USD: ${tot.USD.toLocaleString('es-CL')} · BRL: ${tot.BRL.toLocaleString('es-CL')} · ARS: ${tot.ARS.toLocaleString('es-CL')} · EQUIV. CLP: ${Math.round(tot.CLPconv).toLocaleString('es-CL')}`;
  box.appendChild(totDiv);

  return hits;
}

/* ====== IMPRIMIR VOUCHERS (STAFF) ====== */
function openPrintVouchersModal(){
  const back=document.getElementById('modalBack'); const body=document.getElementById('modalBody'); const title=document.getElementById('modalTitle');
  title.textContent='IMPRIMIR VOUCHERS (STAFF)';
  const coordOpts=[`<option value="__ALL__">TODOS</option>`].concat((state.coordinadores||[]).map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()}</option>`)).join('');
  body.innerHTML=`
    <div class="rowflex"><label>COORDINADOR</label><select id="pvCoord">${coordOpts}</select></div>
    <div class="rowflex"><input type="text" id="pvDestino" placeholder="DESTINO (OPCIONAL)"/><input type="text" id="pvRango" placeholder="RANGO DD-MM-AAAA..DD-MM-AAAA (OPCIONAL)"/></div>
    <div class="rowflex"><button id="pvGo" class="btn ok">GENERAR</button></div>`;
  document.getElementById('pvGo').onclick=async ()=>{
    const coordSel=document.getElementById('pvCoord').value;
    const dest=(document.getElementById('pvDestino').value||'').trim();
    const rango=(document.getElementById('pvRango').value||'').trim();
    let list=(state.grupos||[]).slice();
    if(coordSel!=='__ALL__'){
      const emailElegido=((state.coordinadores||[]).find(c=>c.id===coordSel)?.email || '').toLowerCase();
      if (typeof emailsOf==='function') list=list.filter(g=> emailsOf(g).includes(emailElegido));
    }
    if(dest) list=list.filter(g=> norm(g.destino||'').includes(norm(dest)));
    if(/^\d{2}-\d{2}-\d{4}\.\.\d{2}-\d{2}-\d{4}$/.test(rango)){
      const [a,b]=rango.split('..'); const A=ymdFromDMY(a), B=ymdFromDMY(b);
      list=list.filter(g=> !( (g.fechaFin && g.fechaFin < A) || (g.fechaInicio && g.fechaInicio > B) ));
    }
    const html=await buildPrintableVouchers(list);
    const w=window.open('','_blank','width=900,height=700'); w.document.write(html); w.document.close(); w.focus(); w.print();
  };
  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}
async function buildPrintableVouchers(list){
  let rows='';
  for(const g of list){
    const fechas=rangoFechas(g.fechaInicio,g.fechaFin);
    for(const f of fechas){
      const acts = Array.isArray(g.itinerario?.[f]) ? g.itinerario[f] : [];
      for(const a of acts){
        const servicio=(typeof findServicio==='function') ? await findServicio(g.destino, a.actividad) : null;
        const tRaw=(servicio?.voucher||'No Aplica').toString();
        const t = /electron/i.test(tRaw)?'ELECTRONICO':(/fisic/i.test(tRaw)?'FISICO':'NOAPLICA');
        if(t==='NOAPLICA') continue;
        rows += (typeof renderVoucherHTMLSync==='function')
          ? renderVoucherHTMLSync(g,f,a,null,true)
          : `<div class="card"><h3>${(g.nombreGrupo||g.aliasGrupo||g.id||'GRUPO').toString().toUpperCase()}</h3><div class="meta">${dmy(f)} — ${(a.actividad||'').toString().toUpperCase()}</div><hr/></div>`;
      }
    }
  }
  return `<!doctype html><html><head><meta charset="utf-8"><title>VOUCHERS</title>
<style>body{font-family:system-ui,Segoe UI,Roboto,Arial;color:#111;padding:20px}
.card{border:1px solid #999;border-radius:8px;padding:12px;margin:10px 0}
h3{margin:.2rem 0 .4rem}.meta{color:#333;font-size:14px}hr{border:0;border-top:1px dashed #999;margin:.4rem 0}</style>
</head><body><h2>VOUCHERS</h2>${rows || '<div>SIN ACTIVIDADES.</div>'}</body></html>`;
}

// RESTABLECER (STAFF): reset instantáneo en UI y purgas en background
async function staffResetInicio(grupo){
  if (!state.is){ alert('Solo el STAFF puede restablecer.'); return; }
  const ok = confirm('Esto eliminará Bitácora y Gastos del grupo. ¿Continuar?');
  if(!ok) return;

  try{
    // 1) Persistir flags mínimos (limpia inicio/fin y deja estado PENDIENTE)
    const ref = doc(db,'grupos',grupo.id);
    await updateDoc(ref, {
      paxViajando: deleteField(),
      'viaje.inicio': deleteField(),
      'viaje.fin': deleteField(),
      'viaje.estado': 'PENDIENTE',
      // legacy
      viajeInicioAt: deleteField(),
      viajeFinAt: deleteField(),
      viajeInicioBy: deleteField(),
      viajeFinBy: deleteField(),
      trip: deleteField()
    });

    // Log inmutable (si falla, NO bloquea el flujo)
    try {
      if (typeof appendViajeLog==='function'){
        await appendViajeLog(
          grupo.id,
          'RESTABLECER_INICIO',
          'SE RESTABLECIERON INICIO/FIN Y PAX VIAJANDO (LIMPIEZA)'
        );
      }
    } catch (e) {
      console.warn('appendViajeLog falló (no bloquea):', e?.code || e);
    }

    // 2) Actualizar objeto en memoria (para que started = false ya mismo)
    delete grupo.paxViajando;
    if (grupo.viaje){
      delete grupo.viaje.inicio;
      delete grupo.viaje.fin;
      grupo.viaje.estado = 'PENDIENTE';
    } else {
      grupo.viaje = { estado:'PENDIENTE' };
    }
    delete grupo.viajeInicioAt; delete grupo.viajeFinAt;
    delete grupo.viajeInicioBy; delete grupo.viajeFinBy;
    delete grupo.trip;

    // 3) Reemplazar en los arrays de estado (por seguridad)
    const replaceIn = (arr)=>{
      if (!Array.isArray(arr)) return;
      const i = arr.findIndex(x => x && x.id === grupo.id);
      if (i >= 0) arr[i] = grupo;
    };
    replaceIn(state.grupos);
    replaceIn(state.ordenados);

    // 4) Re-render INMEDIATO → aparece el botón verde
    if (typeof renderOneGroup==='function') await renderOneGroup(grupo);
    window.scrollTo({ top: 0, behavior: 'smooth' });
    if (typeof showFlash === 'function') showFlash('INICIO RESTABLECIDO', 'ok');
    setTimeout(()=> document.getElementById('btnInicioViaje')?.focus?.(), 80);

    // 5) Purgas SIN bloquear la UI
    purgeBitacoraForGroup(grupo).catch(e=>console.warn('purgeBitacora', e));
    purgeGastosForGroup(grupo.id).catch(e=>console.warn('purgeGastos', e));

  }catch(e){
    console.error(e);
    alert('No se pudo restablecer el inicio del viaje.');
  }
}

// Elimina todas las notas de bitácora del rango del viaje, para cada actividad del itinerario
async function purgeBitacoraForGroup(grupo){
  try{
    const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);
    const map = grupo.itinerario || {};
    for (const fecha of fechas){
      const acts = Array.isArray(map[fecha]) ? map[fecha] : [];
      for (const act of acts){
        const actKey = slug(act.actividad || 'actividad');
        try{
          const coll = collection(db,'grupos',grupo.id,'bitacora',actKey,fecha);
          const qs = await getDocs(coll);
          const dels = [];
          qs.forEach(d => dels.push(deleteDoc(d.ref)));
          if (dels.length) await Promise.all(dels);
        }catch(err){ console.warn('purgeBitacora error', fecha, actKey, err); }
      }
    }
  }catch(e){ console.error('purgeBitacoraForGroup', e); }
}

// Elimina todos los gastos que apunten a este grupo en todos los coordinadores
async function purgeGastosForGroup(grupoId){
  try{
    const coords = state.coordinadores || [];
    for (const c of coords){
      try{
        const qs = await getDocs(collection(db,'coordinadores',c.id,'gastos'));
        const dels = [];
        qs.forEach(d => { const x = d.data() || {}; if (x.grupoId === grupoId) dels.push(deleteDoc(d.ref)); });
        if (dels.length) await Promise.all(dels);
      }catch(err){ console.warn('purgeGastos coord', c.id, err); }
    }
  }catch(e){ console.error('purgeGastosForGroup', e); }
}
/* ══════════════════════════════════════════════════════════════════════════ */
