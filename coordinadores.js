/* COORDINADORES.JS — PORTAL COORDINADORES RT */

import { app, db, auth, storage } from './firebase-init-portal.js';
import { onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, addDoc, setDoc,
  serverTimestamp, query, where, orderBy, limit, deleteField, deleteDoc, startAfter
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import { ref as sRef, uploadBytes, getDownloadURL, listAll, deleteObject }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

// === CORREO POR GAS (CONFIG) ===
const GAS_URL = 'https://script.google.com/macros/s/AKfycbwRMaUfZ0gJq015HIJ0yKyqu6_rkfmBkOp3oQH0wh4RSpjNxYTxmAf55Pv9pXQ64fUy/exec';
const GAS_KEY = '1GN4C10P4ST0RP1N0-P1N0P4ST0R1GN4C10';   // misma KEY que en Apps Script

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
  const qp  = encodeURIComponent;
  const url = `${GAS_URL}?origin=${qp(location.origin)}&key=${qp(GAS_KEY)}`;

  const finalPayload = {
    key: GAS_KEY,
    origin: location.origin,                 // 👈 IMPORTANTE para tu whitelist del GAS
    replyTo: payload.replyTo ?? 'operaciones@raitrai.cl',
    ...payload
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
      signal: ctrl.signal
    });

    clearTimeout(t);

    const raw = await res.text().catch(() => '');
    let json = {};
    try { json = raw ? JSON.parse(raw) : {}; } catch (_) {}
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
        cache: 'no-store'
      });
      return { ok: true, opaque: true };
    } catch (e2) {
      if (retries > 0) return sendMailViaGAS(payload, { retries: retries - 1 });
      throw e2;
    }
  }
}

// === Colapsa listas a los últimos 5 elementos con botón VER MÁS / VER MENOS ===
function applyCollapse5(container, toggleBtn){
  if (!container) return;
  const items = Array.from(container.children || []);
  const MAX = 5;
  if (items.length <= MAX){
    if (toggleBtn) toggleBtn.style.display = 'none';
    return;
  }
  let expanded = false;
  const sync = () => {
    const start = Math.max(0, items.length - MAX);
    items.forEach((el, idx) => {
      el.style.display = expanded || idx >= start ? '' : 'none';
    });
    if (toggleBtn){
      const ocultos = items.length - MAX;
      toggleBtn.style.display = '';
      toggleBtn.textContent = expanded ? 'VER MENOS' : `VER MÁS (${ocultos})`;
    }
  };
  sync();
  if (toggleBtn){
    toggleBtn.onclick = () => { expanded = !expanded; sync(); };
  }
}

// ==== Compat: algunos módulos usan "formatCL" en vez de "fmtCL" ====
if (typeof formatCL === 'undefined') {
  var formatCL = (v) => (typeof fmtCL === 'function'
    ? fmtCL(v)
    : Number(v || 0).toLocaleString('es-CL'));
}

// Fecha y hora legible desde milisegundos (ms)
function fmtFechaHoraMs(ms){
  try{
    return new Date(ms).toLocaleString('es-CL', { hour12: false }).toUpperCase();
  }catch(_){
    return '';
  }
}

// ====== DEDUP HOTELES (último asignado gana) ======
function collapseHotelAssignments(assigns){
  if (!Array.isArray(assigns) || !assigns.length) return [];

  const toISOday = (v)=>{
    if (!v) return null;
    const d = (v instanceof Date) ? v : new Date(v);
    if (isNaN(d)) return null;
    const y = d.getFullYear();
    const m = String(d.getMonth()+1).padStart(2,'0');
    const dd= String(d.getDate()).padStart(2,'0');
    return `${y}-${m}-${dd}`;
  };
  const addDays = (iso, n)=>{
    const d = new Date(iso+'T00:00:00');
    d.setDate(d.getDate()+n);
    return toISOday(d);
  };

  // preferimos updatedAt; si no hay, createdAt; si tampoco, 0 (y usamos el índice como desempate)
  const tsOf = (x)=>{
    const u = x?.updatedAt?.seconds ? x.updatedAt.seconds*1000 :
              x?.updatedAt?.toMillis ? x.updatedAt.toMillis() :
              (x?.updatedAt instanceof Date ? x.updatedAt.getTime() : null);
    const c = x?.createdAt?.seconds ? x.createdAt.seconds*1000 :
              x?.createdAt?.toMillis ? x.createdAt.toMillis() :
              (x?.createdAt instanceof Date ? x.createdAt.getTime() : null);
    return Number(u ?? c ?? 0);
  };
  const getStart = (x)=> toISOday(x.checkIn || x.fechaInicio || x.inicio || x.start);
  const getEnd   = (x)=> toISOday(x.checkOut|| x.fechaFin    || x.fin    || x.end);

  // orden estable: por timestamp asc, y en empate por índice original asc
  const ordered = assigns
    .map((a,i)=>({a,i,ts:tsOf(a)}))
    .sort((p,q)=> (p.ts - q.ts) || (p.i - q.i))
    .map(z=>z.a);

  // “pintamos” noche por noche: el ÚLTIMO que pase por un día lo sobrescribe
  const dayMap = new Map(); // ISO -> asignación final
  for (const a of ordered){
    const ini = getStart(a), out = getEnd(a);
    if (!ini || !out) continue;
    let d = ini;
    while (d && d < out){
      dayMap.set(d, a);        // ← último gana
      d = addDays(d, 1);
    }
  }

  // compactamos días consecutivos con la misma asignación final
  const days = [...dayMap.keys()].sort();
  const blocks = [];
  for (let i=0; i<days.length; ){
    const start = days[i];
    const ref   = dayMap.get(start);
    let j = i+1;
    while (j<days.length){
      const prev = days[j-1], curr = days[j];
      if (addDays(prev,1) === curr && dayMap.get(curr) === ref) j++;
      else break;
    }
    const endExcl = addDays(days[j-1],1);
    blocks.push({
      ...ref,
      checkIn:     getStart(ref) || start,
      checkOut:    getEnd(ref)   || endExcl,
      fechaInicio: getStart(ref) || start,  // alias por compatibilidad
      fechaFin:    getEnd(ref)   || endExcl
    });
    i = j;
  }

  return blocks.sort((a,b)=> (getStart(a) > getStart(b)) ? 1 : -1);
}


/* ====== UTILS TEXTO/FECHAS ====== */
// Convierte un SOLO valor de teléfono en link WhatsApp
const fmtTelWhats = (raw='')=>{
  const s = String(raw || '').trim();
  if (!s) return '';

  // limpiamos: dejamos solo dígitos y +
  const cleaned = s.replace(/[^\d+]/g,'');
  if (!cleaned) return s;

  // quitamos el + para wa.me
  const digits = cleaned.replace('+','');

  // si es muy corto, no lo tratamos como teléfono
  if (digits.length < 8) return s;

  const wa = `https://wa.me/${digits}`;
  return `<a href="${wa}" target="_blank" rel="noopener">${s}</a>`;
};

// Reemplaza TODOS los teléfonos tipo +54 / +55 / +56 etc. dentro de un texto
const autoLinkPhones = (txt='')=>{
  const s = String(txt || '');
  if (!s) return '';

  // Busca secuencias con + y al menos 8 caracteres numéricos (permitiendo espacios/guiones)
  return s.replace(/(\+?\d[\d\s-]{7,})/g, (match)=>{
    const digits = match.replace(/[^\d]/g,''); // solo dígitos
    if (digits.length < 8) return match;      // muy corto, lo dejamos tal cual

    const wa = `https://wa.me/${digits}`;
    return `<a href="${wa}" target="_blank" rel="noopener">${match}</a>`;
  });
};

const norm = (s='') => s.toString()
  .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
  .toLowerCase().replace(/[^a-z0-9]+/g,'');

const slug = s => norm(s).slice(0,60);
const toISO = (x) => {
  if (!x) return '';
  if (typeof x === 'string') {
    const t = x.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;              // YYYY-MM-DD
    if (/^\d{2}-\d{2}-\d{4}$/.test(t)) {                      // DD-MM-AAAA
      const [dd, mm, yy] = t.split('-');
      return `${yy}-${mm}-${dd}`;
    }
    const d = new Date(t);
    return isNaN(d) ? '' : d.toISOString().slice(0, 10);
  }
  if (x && typeof x === 'object' && 'seconds' in x)
    return new Date(x.seconds * 1000).toISOString().slice(0, 10);
  if (x instanceof Date) return x.toISOString().slice(0, 10);
  return '';
};

const dmy=(iso)=>{ const m=/^(\d{4})-(\d{2})-(\d{2})$/.exec(iso||''); return m?`${m[3]}-${m[2]}-${m[1]}`:''; };
const ymdFromDMY=(s)=>{ const t=(s||'').trim(); if(/^\d{2}-\d{2}-\d{4}$/.test(t)){ const [dd,mm,yy]=t.split('-'); return `${yy}-${mm}-${dd}`;} return ''; };
const daysInclusive=(ini,fin)=>{ const a=toISO(ini), b=toISO(fin); if(!a||!b) return 0; return Math.max(1,Math.round((new Date(b)-new Date(a))/86400000)+1); };
const rangoFechas=(ini,fin)=>{ const out=[]; const A=toISO(ini), B=toISO(fin); if(!A||!B) return out; for(let d=new Date(A+'T00:00:00'); d<=new Date(B+'T00:00:00'); d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10)); return out; };
const parseQS=()=>{ const p=new URLSearchParams(location.search); return { g:p.get('g')||'', f:p.get('f')||'' }; };
const pad = n => String(n).padStart(2,'0');
const timeIdNowMs = () => {
  const d = new Date();
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${String(d.getMilliseconds()).padStart(3,'0')}`;
};

/* ====== IMPRESIÓN: HOJA OCULTA EN ESTA MISMA PÁGINA ====== */
function ensurePrintDOM(){
  if (document.getElementById('printSheet')) return;

  const css = document.createElement('style');
  css.id = 'printStyles';
   css.textContent = `
     /* PANTALLA: hoja oculta */
     #printSheet { display:none; }
   
     /* IMPRESIÓN */
     @media print {
       @page { size: A4; margin: 20mm; }
   
       /* Muestra sólo la hoja de impresión; ocultamos la app por display, no por visibility */
       #printSheet { display:block !important; }
       .wrap, #alertsPanel, #navPanel, #statsPanel, #gruposPanel, #modalBack { display:none !important; }
   
       /* Encabezado y pie (fijos) */
       #printSheet .print-head {
         position: fixed; top: 10mm; left: 0; right: 0;
         display: grid; grid-template-columns: 1fr auto; align-items: start; gap: 8px;
         font-family: Calibri, Arial, sans-serif; font-size: 11px; color:#444;
       }
       #printSheet .ph-left { text-transform: uppercase; }
       #printSheet .ph-left strong { font-size: 12px; }
       #printSheet .ph-right img { height: 36px; object-fit: contain; }
   
       #printSheet .print-foot {
         position: fixed; bottom: 10mm; left: 0; right: 0;
         text-align: center; font-family: Calibri, Arial, sans-serif; font-size: 10px; color:#666;
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
      #printSheet .print-doc .h1 { font-size: 16px; font-weight: 700; letter-spacing:.3px; }
      #printSheet .print-doc .h2 { font-size: 14px; font-weight: 700; margin-top: 8px; }
      #printSheet .print-doc .b  { font-weight: 700; }
      #printSheet .print-doc .big{ font-size: 14px; }
      #printSheet .print-doc .muted { color:#666; }
      #printSheet .print-doc .mono { font-family: ui-monospace, Menlo, Consolas, monospace; }
     }
   `;
  document.head.appendChild(css);

  const sheet = document.createElement('div');
  sheet.id = 'printSheet';
  sheet.innerHTML = `
    <div class="print-head">
      <div class="ph-left">
        <div><strong id="ph-title">DESPACHO DE VIAJE</strong></div>
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

async function preparePrintForGroup(g){
  // Mantén tu estructura de impresión ya creada
  ensurePrintDOM();

  // Referencias a los placeholders del encabezado (como ya tenías)
  const $doc   = document.getElementById('print-block');
  const $grp   = document.getElementById('ph-grupo');
  const $m1    = document.getElementById('ph-meta1');
  const $m2    = document.getElementById('ph-meta2');
  const $fech  = document.getElementById('ph-fechas');
  const $pax   = document.getElementById('ph-pax');

  // Helpers locales (sin depender de otros nombres)
  const norm = s => String(s||'').trim().toUpperCase();
  const dmySafe = v => {
    try{
      if (!v) return '';
      const d = (v instanceof Date) ? v : new Date(v);
      return isNaN(d) ? String(v).toUpperCase() : d.toLocaleDateString('es-CL').toUpperCase();
    }catch{ return String(v||'').toUpperCase(); }
  };
  const tVal = (hhmm)=>{
    const m = String(hhmm||'').match(/^(\d{1,2}):(\d{2})$/);
    if (!m) return 99_999;
    return (+m[1])*60 + (+m[2]);
  };

  // ===== Encabezado =====
  const nombre = (nombreOperativoGrupo(g))||'';
  const code   = (g.numeroNegocio||'') + (g.identificador?('-'+g.identificador):'');
  const rango  = `${dmySafe(g.fechaInicio||'')} — ${dmySafe(g.fechaFin||'')}`;
  const destino= norm(g.destino||'');
  const programa = norm(g.programa||'');

  $grp.textContent  = `GRUPO: ${norm(nombre)} (${code})`;
  $m1.textContent   = `DESTINO: ${destino}`;
  $m2.textContent   = `PROGRAMA: ${programa || '—'}`;
  $fech.textContent = `FECHAS: ${rango}`;
  const real  = paxRealOf ? paxRealOf(g) : null;
  const plan  = paxOf ? paxOf(g) : null;
  $pax.innerHTML    = `PAX: ${real && plan && real!==plan ? `${plan} → ${real}` : (plan || real || '—')}`;


  // ===== Cargas seguras (si existen loaders; si no, continuamos sin romper)
  let vuelos = [];
  try{ if (typeof loadVuelosInfo === 'function') vuelos = await loadVuelosInfo(g) || []; }catch{}
  
  let hoteles = [];
  try{
    if (typeof loadHotelAssignmentsForGroup === 'function'){
      hoteles = collapseHotelAssignments(await loadHotelAssignmentsForGroup(g) || []);
    }
  }catch{}
  
  let vouchersSet = new Set();
  try{ if (typeof loadVouchersForGroup === 'function') vouchersSet = await loadVouchersForGroup(g) || new Set(); }catch{}


  // ===== Sección: TRANSPORTE =====
  let htmlTransp = `<h2>TRANSPORTE</h2>`;
  if (Array.isArray(vuelos) && vuelos.length){
    htmlTransp += vuelos.map(v=>{
      const tipo = norm(v.tipo||'');
      const code = String(v.codigo||'').toUpperCase();
      const sl   = v.salida  ? ` · ${v.salida}`  : '';
      const ll   = v.llegada ? ` → ${v.llegada}` : '';
      const ob   = v.obs     ? ` · ${norm(v.obs)}` : '';
      return `<div class="meta">• ${tipo}${code?(' '+code):''}${sl}${ll}${ob}</div>`;
    }).join('');
  } else {
    htmlTransp += `<div class="muted">SIN REGISTROS.</div>`;
  }

  // ===== Sección: HOTELES =====
  let htmlHoteles = `<h2>HOTELES</h2>`;
  if (Array.isArray(hoteles) && hoteles.length){
    htmlHoteles += hoteles.map(h=>`
      <div class="card">
        <div class="meta"><strong>${norm(h.nombre||'HOTEL')}</strong>${h.ciudad?(' · '+norm(h.ciudad)) : ''}</div>
        ${(h.in||h.checkIn||h.fechaInicio) || (h.out||h.checkOut||h.fechaFin) ? 
          `<div class="meta">CHECK-IN: ${dmySafe(h.in||h.checkIn||h.fechaInicio)} · CHECK-OUT: ${dmySafe(h.out||h.checkOut||h.fechaFin)}</div>` : ''
        }
        ${h.direccion ? `<div class="meta">${norm(h.direccion)}</div>` : ''}
        ${(h.contacto||h.telefono||h.email) ? 
          `<div class="meta">CONTACTO: ${norm(h.contacto||'')}${h.telefono?(' · '+fmtTelWhats(h.telefono)) : ''}${h.email?(' · '+String(h.email).toLowerCase()) : ''}</div>`
          : ''
        }
      </div>
    `).join('');
  } else {
    htmlHoteles += `<div class="muted">SIN ASIGNACIONES.</div>`;
  }

  // ===== Sección: ITINERARIO (oculta "DESAYUNO HOTEL", proveedor/contacto/dirección + VOUCHER + indicaciones del item) =====
  let htmlItin = `<h2>ITINERARIO</h2>`;
  const byDate = g.itinerario || {};
  const fechas = Object.keys(byDate).sort();
  if (!fechas.length){
    htmlItin += `<div class="muted">SIN ITINERARIO.</div>`;
  } else {
    for (const f of fechas){
      let acts = byDate[f];
      if (!Array.isArray(acts)) acts = Object.values(acts||{}).filter(x=>x && typeof x==='object');

      // ocultar "DESAYUNO HOTEL"
      acts = acts.filter(a => norm(a.actividad) !== 'DESAYUNO HOTEL')
                 .sort((a,b)=> tVal(a?.horaInicio) - tVal(b?.horaInicio));
      if (!acts.length) continue;

      htmlItin += `<h3>${dmySafe(f)}</h3>`;

      for (const a of acts){
        const actName = norm(a.actividad||'');
        const horaL   = a.horaInicio ? ` · ${a.horaInicio}${a.horaFin?('—'+a.horaFin):''}` : '';

        // Resolver datos de servicio/proveedor si existe helper; si no, usa lo que venga en el item
        let provLines = '';
        let markVoucher = !!a.requiereVoucher;

        try{
          if (typeof resolveServicioYProveedor === 'function'){
            const { servicio, proveedor } = await resolveServicioYProveedor(g.destino || a.destino, a.actividad, a.servicioId);
            const nombreProv = proveedor?.proveedor || servicio?.proveedor || a.proveedor || '';
            const dir        = proveedor?.direccion || a.direccion || '';
            const contacto   = proveedor?.contacto || a.contacto || '';
            const tel        = proveedor?.telefono || proveedor?.fono || a.telefono || '';

            if (nombreProv) provLines += `<div class="meta">${norm(nombreProv)}</div>`;
            if (dir)        provLines += `<div class="meta">${norm(dir)}</div>`;
            if (contacto || tel) provLines += `<div class="meta">CONTACTO: ${norm(contacto||'')}${tel?(' · '+fmtTelWhats(tel)) : ''}</div>`;

            // VOUCHER: por servicio o por nombre de actividad
            markVoucher = markVoucher
              || (servicio?.requiereVoucher ? true : false)
              || (servicio?.id && vouchersSet && typeof vouchersSet.has==='function' && vouchersSet.has('S:'+servicio.id))
              || (vouchersSet && typeof vouchersSet.has==='function' && vouchersSet.has('A:'+actName));
          } else {
            // Fallback: sólo con lo que traiga la actividad
            if (a.proveedor) provLines += `<div class="meta">${norm(a.proveedor)}</div>`;
            if (a.direccion) provLines += `<div class="meta">${norm(a.direccion)}</div>`;
            if (a.contacto || a.telefono){
              provLines += `<div class="meta">CONTACTO: ${norm(a.contacto||'')}${a.telefono?(' · '+fmtTelWhats(a.telefono)) : ''}</div>`;
            }
          }
        }catch{}

        // Indicaciones puntuales del item (si existen en el itinerario)
        const indic = (a.indicaciones || a.nota || '').toString().trim();
        const indicLine = indic ? `<div class="meta">INDICACIONES: ${norm(indic)}</div>` : '';

        // Render del ítem
        htmlItin += `
          <div class="meta">• ${actName}${horaL}${markVoucher ? ' · <span class="badge">VOUCHER</span>' : ''}</div>
          ${provLines}
          ${indicLine}
        `;
      }
    }
  }

  // ===== Sección: FINANZAS — SOLO ABONOS =====
  let htmlAbonos = `<h2>FINANZAS — ABONOS</h2>`;
  try{
    let abonos = [];
    if (typeof loadAbonos === 'function') abonos = await loadAbonos(g.id) || [];
    if (abonos.length){
      abonos.sort((a,b)=> {
        const da = new Date(a.fecha || a.ts?.seconds*1000 || 0).getTime();
        const db = new Date(b.fecha || b.ts?.seconds*1000 || 0).getTime();
        return da - db;
      });
      htmlAbonos += `<table class="t-tight" style="width:100%;border-collapse:collapse">
        <thead>
          <tr>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #eee">TIPO/MEDIO</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #eee">ASUNTO</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #eee">FECHA</th>
            <th style="text-align:left;padding:4px 6px;border-bottom:1px solid #eee">MONEDA</th>
            <th style="text-align:right;padding:4px 6px;border-bottom:1px solid #eee">MONTO</th>
          </tr>
        </thead>
        <tbody>
          ${abonos.map(a=>{
            const medio  = (a.medio||a.tipo||'').toString().toUpperCase();
            const asunto = (a.asunto||'ABONO').toString().toUpperCase();
            const fecha  = a.fecha ? dmySafe(a.fecha) : (a.ts?.seconds ? dmySafe(new Date(a.ts.seconds*1000)) : '');
            const mon    = (a.moneda||'CLP').toString().toUpperCase();
            const val    = Number(a.valor||0);
            return `
              <tr>
                <td style="padding:4px 6px;border-bottom:1px solid #eee">${medio||'—'}</td>
                <td style="padding:4px 6px;border-bottom:1px solid #eee">${asunto}</td>
                <td style="padding:4px 6px;border-bottom:1px solid #eee">${fecha||'—'}</td>
                <td style="padding:4px 6px;border-bottom:1px solid #eee">${mon}</td>
                <td style="padding:4px 6px;border-bottom:1px solid #eee;text-align:right">${val.toLocaleString('es-CL')}</td>
              </tr>
            `;
          }).join('')}
        </tbody>
      </table>`;
    } else {
      htmlAbonos += `<div class="muted">SIN ABONOS REGISTRADOS.</div>`;
    }
  }catch{
    htmlAbonos += `<div class="muted">SIN ABONOS.</div>`;
  }

  // ===== Ensamble final en el contenedor del print =====
  $doc.innerHTML = `
    ${htmlTransp}
    ${htmlHoteles}
    ${htmlItin}
    ${htmlAbonos}
  `;
}

/* ====== HISTORIAL VIAJE (utils) ====== */
const HIST_ACTKEY = '_viaje_'; // o 'viaje_hist', cualquier cosa que NO sea __...__
const fmtChile = (date) =>
  new Intl.DateTimeFormat('es-CL', {
    timeZone: 'America/Santiago',
    year:'numeric', month:'2-digit', day:'2-digit',
    hour:'2-digit', minute:'2-digit'
  }).format(date).toUpperCase();

async function appendViajeLog(grupoId, kind, text='', meta=null){
  // Registro inmutable en subcolección viajeLog
  await addDoc(collection(db,'grupos',grupoId,'viajeLog'),{
    type: kind, text, meta,
    by: (state.user.email||'').toLowerCase(),
    byUid: state.user.uid,
    ts: serverTimestamp()
  });
  // Copia visible en Bitácora bajo actividad especial __viaje__
  const dateIso = todayISO();
  const timeId  = timeIdNowMs();
  await setDoc(doc(db,'grupos',grupoId,'bitacora',HIST_ACTKEY,dateIso,timeId),{
    texto: (text || kind).toString().toUpperCase(),
    byUid: state.user.uid,
    byEmail: (state.user.email||'').toLowerCase(),
    ts: serverTimestamp()
  });
}

/* ====== UTILS PAX/VIAJE (NUEVOS) ====== */
const todayISO = () => new Date().toISOString().slice(0,10);
const isToday = (iso) => (toISO(iso) === todayISO());
const paxOf = g => Number(g?.cantidadgrupo ?? g?.pax ?? 0);
const paxRealOf = (g) => Number(g?.paxViajando?.total || 0);
const paxBreakdown = (g) => ({ A: Number(g?.paxViajando?.A || 0), E: Number(g?.paxViajando?.E || 0) });
const fmtPaxPlan = (plan, g) => {
  const real = paxRealOf(g);
  const nPlan = Number(plan || 0);
  if (real && real !== nPlan){
    return `<span style="text-decoration:line-through;opacity:.7">${nPlan}</span> → <strong>${real}</strong>`;
  }
  return `<strong>${nPlan}</strong>`;
};

/* Tiempo: HH:MM → minutos (sin hora => muy grande para que quede al final) */
const timeVal = (t) => {
  const m = /^(\d{1,2}):(\d{2})/.exec(String(t||'').trim());
  if (!m) return 1e9;
  const h = Math.max(0, Math.min(23, parseInt(m[1],10)));
  const mi = Math.max(0, Math.min(59, parseInt(m[2],10)));
  return h*60 + mi;
};

/* ===== DEBUG HOTEL ===== */
const DEBUG_HOTEL = true;
const D_HOTEL = (...args)=> { if (DEBUG_HOTEL) console.log('%c[HOTEL]', 'color:#0ff', ...args); };

/* ===== DEBUG FINANZAS ===== */
const DEBUG_FIN = true;
const D_FIN = (...args)=> { if (DEBUG_FIN) console.log('%c[FIN]', 'color:#22c55e', ...args); };

/* ====== EXTRACCIÓN TOLERANTE DESDE GRUPOS ====== */
const arrify=v=>Array.isArray(v)?v:(v&&typeof v==='object'?Object.values(v):(v?[v]:[]));
function emailsOf(g){ const out=new Set(), push=e=>{if(e) out.add(String(e).toLowerCase());};
  push(g?.coordinadorEmail); push(g?.coordinador?.email); arrify(g?.coordinadoresEmails).forEach(push);
  if(g?.coordinadoresEmailsObj) Object.keys(g.coordinadoresEmailsObj).forEach(push);
  arrify(g?.coordinadores).forEach(x=>{ if(x?.email) push(x.email); else if(typeof x==='string'&&x.includes('@')) push(x); });
  return [...out];
}
function coordDocIdsOf(g) {
  const out = new Set();

  const push = valor => {
    if (valor) {
      out.add(String(valor));
    }
  };

  push(g?.coordinadorId);

  arrify(
    g?.coordinadorIds
  ).forEach(push);

  arrify(
    g?.coordinadoresIds
  ).forEach(push);

  arrify(
    g?.coordinadores
  ).forEach(item => {
    if (
      item &&
      typeof item === 'object'
    ) {
      push(
        item.id ||
        item.coordinadorId
      );
    }
  });

  const mapEmailToId =
    new Map(
      (state.coordinadores || [])
        .map(coordinador => [
          String(
            coordinador.email ||
            ''
          ).toLowerCase(),

          coordinador.id
        ])
    );

  emailsOf(g).forEach(email => {
    if (mapEmailToId.has(email)) {
      out.add(
        mapEmailToId.get(email)
      );
    }
  });

  return [...out];
}

/* ====== ESTADO APP ====== */
const STAFF_EMAILS = new Set(['aleoperaciones@raitrai.cl','operaciones@raitrai.cl','anamaria@raitrai.cl','tomas@raitrai.cl','sistemas@raitrai.cl', 'administracion@raitrai.cl', 'administracion@hotelbordeandino.cl'].map(x=>x.toLowerCase()));
const state = {
  user:null,
  is:false,
  coordinadores:[],
  viewingCoordId:null,              // STAFF: ID SELECCIONADO · COORD: SU PROPIO ID
  grupos:[], ados:[], idx:0,
  filter:{ type:'all', value:null },
  groupQ:'',
  lastTab:'resumen',                // ⬅️ NUEVO: recuerda la pestaña activa
  alertsTimer:null,                 // AUTO-REFRESCO DE ALERTAS (60S)
  anoViajeActivo:
    obtenerAnoViajeActivoChile(),
  
  coordinadorActual: null,

  cache:{
    hotel:new Map(),
    vuelos:new Map(),

    // NUEVO: catálogos en memoria
    servicios:new Map(),    // key: 'Servicios/BRASIL/Listado' → [servicios...]
    proveedores:new Map(),  // key: 'BRASIL::proveedor-normalizado' → doc proveedor

    resumenesPorAno:
      new Map(),
    
    gruposDetalle:
      new Map(),

    gruposVentas:
      new Map(),

    tasas:null,
    hoteles:{ loaded:false, byId:new Map(), bySlug:new Map(), all:[] },
    documentosViaje:
      new Map(),
    
    nominasViaje:
      new Map(),
    
    encuestasViaje:
      new Map()
  }
};

// ——— GASTOS: resolver coordinador activo evitando "__ALL__"
function getActiveCoordIdForGastos(){
  // Si el selector tiene un coordinador concreto, úsalo
  if (state.viewingCoordId && state.viewingCoordId !== '__ALL__') return state.viewingCoordId;

  // Si no, usa mi propio coordinador (por email) o 'self'
  const me = state.coordinadores.find(
    c => (c.email||'').toLowerCase() === (state.user.email||'').toLowerCase()
  );
  return me?.id || 'self';
}

// ====== HELPERS UI ======
function ensurePanel(id, html=''){
  let p=document.getElementById(id);
  if(!p){ p=document.createElement('div'); p.id=id; p.className='panel'; document.querySelector('.wrap').prepend(p); }
  if(html) p.innerHTML=html;
  enforceOrder();
  return p;
}

function enforceOrder(){
  const wrap=document.querySelector('.wrap');
  // ORDEN CORRECTO: STAFF -> ALERTAS -> STATS -> NAV -> GRUPOS
  ['staffBar','alertsFoldStrip','alertsPanelV2','statsPanel','navPanel','gruposPanel'].forEach(id=>{
    const n=document.getElementById(id);
    if(n) wrap.appendChild(n);
  });
}

function getTodayLocalISO() {
  const ahora =
    new Date();

  const year =
    ahora.getFullYear();

  const month =
    String(
      ahora.getMonth() + 1
    ).padStart(
      2,
      "0"
    );

  const day =
    String(
      ahora.getDate()
    ).padStart(
      2,
      "0"
    );

  return `${year}-${month}-${day}`;
}

function getIndiceProximoViaje(
  grupos = []
) {
  if (
    !Array.isArray(grupos) ||
    !grupos.length
  ) {
    return 0;
  }

  const hoy =
    getTodayLocalISO();

  const indice =
    grupos.findIndex(
      grupo => {
        const fechaFin =
          toISO(
            grupo.fechaFin
          );

        return (
          fechaFin &&
          fechaFin >= hoy
        );
      }
    );

  return indice >= 0
    ? indice
    : Math.max(
        0,
        grupos.length - 1
      );
}

function showFlash(msg, kind='ok'){
  const colors = {
    ok:   { bg:'#16a34a', fg:'#fff' },
    warn: { bg:'#ea580c', fg:'#fff' },
    err:  { bg:'#dc2626', fg:'#fff' },
    info: { bg:'#64748b', fg:'#fff' }
  };
  const c = colors[kind] || colors.ok;
  const n = document.createElement('div');
  n.textContent = String(msg || '').toUpperCase();
  n.style.cssText = 'position:fixed;right:16px;bottom:16px;z-index:9999;padding:10px 12px;border-radius:10px;font-weight:700;letter-spacing:.5px;'+
                    `background:${c.bg};color:${c.fg};box-shadow:0 10px 20px rgba(0,0,0,.15);opacity:0;transform:translateY(8px);`+
                    'transition:opacity .2s ease, transform .2s ease';
  document.body.appendChild(n);
  requestAnimationFrame(()=>{ n.style.opacity='1'; n.style.transform='translateY(0)'; });
  setTimeout(()=>{
    n.style.opacity='0'; n.style.transform='translateY(6px)';
    n.addEventListener('transitionend', ()=> n.remove(), { once:true });
  }, 4000);
}

// === LOGS GLOBALES (poner una sola vez) ===
if (typeof window !== 'undefined') {
  window.addEventListener('error',  ev => console.error('[GLOBAL ERROR]', ev.message, ev.error));
  window.addEventListener('unhandledrejection', ev => console.error('[PROMISE REJECTION]', ev.reason));
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
    'tomas@raitrai.cl',
    'administracion@raitrai.cl',
    'administracion@hotelbordeandino.cl',    
  ]);

  const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();

  
   // Estado UI (reutiliza si ya existe)
   state.alertsUI ||= {
     items: [],
     lastDoc: null,
     totalLoaded: 0,
     q: '',
     // sin "sort": siempre nuevas→antiguas
     filter: 'all',         // se ajusta luego según sea STAFF o no
     loading: false,
     inited: false,
   };

  function ensureAlertsPanel(){
    const host = ensurePanel('alertsPanelV2');
  
    // Anti-parpadeo: si está plegado, ocultar de inmediato
    const _folded = (localStorage.getItem('rt__alerts_fold') ?? '1') !== '0';
    host.style.display = _folded ? 'none' : '';
  
    host.innerHTML = `
      <div class="rowflex" style="gap:.5rem;align-items:center;flex-wrap:wrap;margin-bottom:.5rem">
        <input id="alQ" type="text" placeholder="BUSCAR EN NOTIFICACIONES..." style="flex:1;min-width:240px"/>
  
        <select id="alState" title="ESTADO">
          <option value="unread" selected>NO LEÍDAS</option>
          <option value="read">LEÍDAS</option>
        </select>

        ${state.is ? `
          <select id="alScope" title="ÁMBITO">
            <option value="ops" selected>OPERACIONES</option>
            <option value="mine">DEL COORDINADOR(A)</option>
          </select>
        ` : ``}
  
        ${state.is ? `<button id="btnCreateAlert" class="btn ok" style="width:100%;display:block">CREAR NOTIFICACIÓN</button>` : ''}
  
        <button id="alRefresh" class="btn sec" style="width:100%;display:block">ACTUALIZAR</button>
      </div>
  
      <div id="alList" class="acts"></div>
      <div class="rowflex" style="margin-top:.6rem;gap:.5rem;justify-content:center">
        <button id="alMore" class="btn sec" style="width:100%;display:block">VER MÁS</button>
      </div>
      <div class="meta muted" id="alMeta" style="margin-top:.25rem"></div>
    `;
  
    // BUSCADOR
    const $q = host.querySelector('#alQ');
    let t=null;
    $q.oninput = () => {
      clearTimeout(t);
      t = setTimeout(() => {
        state.alertsUI.q = norm($q.value||'');
        renderAlertsPanel();
      }, 150);
    };
  
    // CREAR (solo staff)
    const $btnCreate = host.querySelector('#btnCreateAlert');
    if ($btnCreate) $btnCreate.onclick = openCreateAlertModal;
  
    // HOOKS: estado + ámbito
    const $state = host.querySelector('#alState');
    $state.value = (state.alertsUI.state === 'read') ? 'read' : 'unread';
    $state.onchange = () => { state.alertsUI.state = $state.value; renderAlertsPanel(); };
    
    // ÁMBITO solo existe para staff; si no existe, fuerza 'mine' internamente
    const $scope = host.querySelector('#alScope');
    if ($scope) {
      $scope.value = (state.alertsUI.scope === 'mine') ? 'mine' : 'ops';
      $scope.onchange = () => { state.alertsUI.scope = $scope.value; renderAlertsPanel(); };
    } else {
      state.alertsUI.scope = 'mine'; // no-staff: ver solo sus notificaciones
    }
    
    host.querySelector('#alRefresh').onclick = async () => { resetAlertsCache(); await fetchAlertsPage(true); };
    host.querySelector('#alMore').onclick    = async () => { await fetchAlertsPage(false); };

  
    return host;
  }

  // === Tira de plegado/desplegado para el panel de alertas (por defecto PLEGADO) ===
  function ensureAlertsFoldStrip(){
    const panel = document.getElementById('alertsPanelV2');
    if (!panel) return;
  
    // Si ya existe, solo resincroniza textos/estado/badge y sal
    if (document.getElementById('alertsFoldStrip')) {
      const btn = document.getElementById('btnFoldAlerts');
      const badge = document.getElementById('alBadge');
      if (btn) {
        const folded = (localStorage.getItem('rt__alerts_fold') ?? '1') !== '0';
        panel.style.display = folded ? 'none' : '';
        btn.innerHTML = folded
          ? 'DESPLEGAR NOTIFICACIONES<span id="alBadge" class="badge"></span>'
          : 'CONTRAER NOTIFICACIONES<span id="alBadge" class="badge"></span>';
        // Badge se actualizará desde renderAlertsPanel() con setAlertsBadge()
      }
      return;
    }
  
    // Crea tira (botón full-width + hint)
    const strip = document.createElement('div');
    strip.id = 'alertsFoldStrip';
    strip.style.cssText = 'display:flex;flex-direction:column;gap:.35rem;margin:.35rem 0 .25rem 0;';
  
    strip.innerHTML = `
      <button id="btnFoldAlerts" class="btn sec" style="width:100%;display:block">
        DESPLEGAR NOTIFICACIONES<span id="alBadge" class="badge"></span>
      </button>
      <div class="muted" id="alFoldHint" style="font-size:.85rem"></div>
    `;
  
    // Inserta la tira antes del panel
    const wrap = document.querySelector('.wrap') || panel.parentElement || document.body;
    wrap.insertBefore(strip, panel);
  
    const btn   = strip.querySelector('#btnFoldAlerts');
    const hint  = strip.querySelector('#alFoldHint');
  
    const getFolded = () => {
      const v = localStorage.getItem('rt__alerts_fold');
      return v === null ? true : v === '1'; // por defecto plegado
    };
  
    const applyUI = () => {
      const folded = getFolded();
      panel.style.display = folded ? 'none' : '';
      btn.innerHTML = folded
        ? 'DESPLEGAR NOTIFICACIONES<span id="alBadge" class="badge"></span>'
        : 'CONTRAER NOTIFICACIONES<span id="alBadge" class="badge"></span>';
    };
  
    btn.onclick = () => {
      const folded = getFolded();
      localStorage.setItem('rt__alerts_fold', folded ? '0' : '1'); // toggle
      applyUI();
    };
  
    // Estado inicial: PLEGADO
    if (!localStorage.getItem('rt__alerts_fold')) {
      localStorage.setItem('rt__alerts_fold', '1');
    }
    applyUI();
  }
  
  // Helper para actualizar el badge desde el render
  function setAlertsBadge(n){
    const badge = document.getElementById('alBadge');
    if (!badge) return;
    const v = Number(n||0);
    badge.textContent = v > 0 ? `(${v})` : '';
  }


  function resetAlertsCache(){
    state.alertsUI.items = [];
    state.alertsUI.totalLoaded = 0;
    state.alertsUI.lastDoc = null;
  }

  // --- Carga paginada (Firestore)
  async function fetchAlertsPage(initial){
    if (state.alertsUI.loading) return;
    state.alertsUI.loading = true;
    try{
      const base = collection(db,'alertas');
      let qFs = query(base, orderBy('createdAt','desc'), limit(initial ? PAGE_1 : PAGE_MORE));
      if (!initial && state.alertsUI.lastDoc){
        qFs = query(base, orderBy('createdAt','desc'), startAfter(state.alertsUI.lastDoc), limit(PAGE_MORE));
      }
      const snap = await getDocs(qFs);
      if (!snap.size){ showFlash('NO HAY MÁS ALERTAS', 'info'); return; }

      const batch = [];
      snap.forEach(d => {
        const x = d.data() || {};
        const ts = x.createdAt?.seconds ? new Date(x.createdAt.seconds*1000) : (x.createdAt?.toDate?.() || null);
        const byEmail = (x?.createdBy?.email || '').toLowerCase();
        const readBy  = x.readBy || {};       // { uid: true, ... }
        batch.push({
          id: d.id,
          mensaje: String(x.mensaje || ''),
          audience: String(x.audience || ''),
          createdAt: ts,
          createdByEmail: byEmail,
          readBy,
          groupInfo: x.groupInfo || null,
          forCoordIds: Array.isArray(x.forCoordIds) ? x.forCoordIds.slice() : [],
          _q: ''
        });
      });

      state.alertsUI.items.push(...batch);
      state.alertsUI.totalLoaded += batch.length;
      state.alertsUI.lastDoc = snap.docs[snap.docs.length - 1];

      renderAlertsPanel();
    } catch (e){
      console.error('[ALERTAS] fetch', e);
      showFlash('ERROR AL CARGAR ALERTAS', 'err');
    } finally {
      state.alertsUI.loading = false;
    }
  }

  function getEmailByCoordId(id){
    if (!id || id==='__ALL__') return null;
    const c = (state.coordinadores||[]).find(x => x.id===id);
    return (c?.email || '').toLowerCase() || null;
  }

  // --- Render list + filtros + orden + marcar leído
  function renderAlertsPanel(){
    ensureAlertsPanel();
    ensureAlertsFoldStrip(); // ← NUEVO: crea/actualiza la tira plegable y aplica el estado (oculto/visible)
  
    const p    = document.getElementById('alertsPanelV2');
    const list = p.querySelector('#alList');
    const meta = p.querySelector('#alMeta');

    const meUid   = state?.user?.uid || '';
    const meEmail = (state?.user?.email || '').toLowerCase();
    
    // coordId global (selector arriba). En staff: '__ALL__' = Todos; en no-staff ya es su propio id.
    const coordId    = state.viewingCoordId || '__ALL__';
    const coordAll   = (coordId==='__ALL__' || !coordId);
    const coordEmail = getEmailByCoordId(coordId);
    
    const q = (state.alertsUI.q || '').trim();
    let arr = state.alertsUI.items.slice();

    // Construye _q solo si hay texto en el buscador
    if (q) {
      for (const a of arr) {
        if (!a._q) {
          a._q = norm([
            a.mensaje,
            a.audience,
            a.createdByEmail,
            a?.groupInfo?.actividad,
            a?.groupInfo?.destino,
            a?.groupInfo?.nombre,
          ].filter(Boolean).join(' '));
        }
      }
    }
    
    // 0) BUSCADOR (normalizado en _q)
    if (q) arr = arr.filter(a => a._q && a._q.includes(q));
    
    // 1) STAFF vs NO-STAFF (dataset base)
    if (!state.is){
      // COORDINADOR: solo mensajes dirigidos a él/ella
      arr = arr.filter(a =>
        a.audience==='coord' &&
        Array.isArray(a.forCoordIds) &&
        a.forCoordIds.includes(coordId)
      );
    } else {
      // STAFF: combinamos selector global + scope (OPERACIONES / DEL COORDINADOR(A))
      const scope = state.alertsUI.scope || 'ops';
    
      if (scope === 'ops'){
        // OPERACIONES: notas de itinerario (audience:'')
        arr = arr.filter(a => (a.audience || '') === '');
        if (!coordAll && coordEmail){
          // Global = Coord X → solo notas creadas por X
          arr = arr.filter(a => (a.createdByEmail || '') === coordEmail);
        }
      } else {
        // DEL COORDINADOR(A): mensajes enviados a coordinadores (audience:'coord')
        arr = arr.filter(a => (a.audience || '') === 'coord');
        if (!coordAll){
          // Global = Coord X → solo dirigidas a X
          arr = arr.filter(a => Array.isArray(a.forCoordIds) && a.forCoordIds.includes(coordId));
        }
      }
    }
    
    // 2) BADGE de NO LEÍDAS (según dataset ya filtrado por scope + global)
    const unreadCountForBadge = arr.reduce((n,a)=> n + (a.readBy?.[meUid] ? 0 : 1), 0);
    setAlertsBadge(unreadCountForBadge);
    
    // 3) ESTADO (NO LEÍDAS / LEÍDAS) — SIEMPRE PRIMERO “unread” por defecto
    if ((state.alertsUI.state||'unread') === 'unread'){
      arr = arr.filter(a => !a.readBy?.[meUid]);
    } else {
      arr = arr.filter(a =>  a.readBy?.[meUid]);
    }
    
        

    // ORDEN (fallback si falta fecha)
      arr.sort((a,b) => {
        const ta = a.createdAt ? a.createdAt.getTime() : 0;
        const tb = b.createdAt ? b.createdAt.getTime() : 0;
        return tb - ta; // SIEMPRE NUEVAS → ANTIGUAS
      });

    // DIBUJO
    if (!arr.length){
      list.innerHTML = '<div class="muted">SIN NOTIFICACIONES POR EL MOMENTO.</div>';
    } else {
      const frag = document.createDocumentFragment();
      arr.forEach(a => {
        const box = document.createElement('div');
        const unread = !a.readBy?.[meUid];
        box.className = 'act';
        const cuando = a.createdAt
          ? new Intl.DateTimeFormat('es-CL',{ dateStyle:'short', timeStyle:'short' }).format(a.createdAt).toUpperCase()
          : '—';

        // puntito si no leída
        const dot = unread ? '<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:#10b981;margin-right:6px;vertical-align:middle"></span>' : '';

        box.innerHTML = `
          <div class="meta"><strong>${dot}${(a.mensaje||'').toUpperCase()}</strong></div>
          <div class="meta muted">FECHA: ${cuando}${a.audience ? ' · AUDIENCIA: '+a.audience.toUpperCase() : ''}${a.createdByEmail ? ' · POR: '+a.createdByEmail : ''}</div>
          ${a.groupInfo ? `<div class="meta">GRUPO: ${(a.groupInfo.nombre||'—').toString().toUpperCase()} · ACT: ${(a.groupInfo.actividad||'—').toString().toUpperCase()} · DEST: ${(a.groupInfo.destino||'—').toString().toUpperCase()}</div>` : ''}
          <div class="rowflex" style="gap:.4rem;margin-top:.35rem">
            <button class="btn sec btnMark">${unread ? 'MARCAR LEÍDA' : 'MARCAR NO LEÍDA'}</button>
          </div>
        `;

        // toggle leído
        box.querySelector('.btnMark').onclick = async () => {
          try{
            const path = doc(db,'alertas', a.id);
            const payload = {};
            if (unread){
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
          } catch(e){
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
      `MOSTRANDO ${arr.length} `;
  }

  // API pública
  window.renderGlobalAlertsV2 = async () => {
    ensureAlertsPanel();
    if (!state.alertsUI.inited){
      state.alertsUI.inited = true;
      resetAlertsCache();
      await fetchAlertsPage(true);
    } else {
      resetAlertsCache();
      await fetchAlertsPage(true);
    }
      // (defensa) OCULTAR PANELES LEGACY DE ALERTAS (cubre varias variantes)
      ['alerts','alertas','panel-alertas','alertasPanel','alertasWrap'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.style.display = 'none';
      });
      document.querySelectorAll('[data-section="alertas"], .alertas-wrap, .alertasTabs').forEach(n => {
        n.style.display = 'none';
      });
  };
})();

// ===== Helpers financieros por moneda (sin conversión) =====
function _safeNum(x){ const n = Number(x||0); return isFinite(n) ? n : 0; }

// Días inclusivos (ej: 1–5 = 5 días). Acepta 'YYYY-MM-DD' o Date.
function daysBetweenInclusive(a,b){
  const toD = (v)=> (v instanceof Date) ? v : new Date(String(v));
  const d1 = toD(a), d2 = toD(b);
  if (isNaN(d1) || isNaN(d2)) return 0;
  const ONE = 24*60*60*1000;
  const z1 = new Date(d1.getFullYear(), d1.getMonth(), d1.getDate());
  const z2 = new Date(d2.getFullYear(), d2.getMonth(), d2.getDate());
  return Math.round((z2 - z1)/ONE) + 1;
}

// Carga GASTOS APROBADOS del grupo (colección estándar).
// Lee de: grupos/{id}/gastos  (fallback: finanzas_gastos/{id}/items si existiera)
// Verifica si existen gastos PENDIENTES (bloquea cierre)
window.renderFinanzas ??= async ()=>0;
window.setEstadoServicio ??= async ()=> showFlash('ESTADO ACTUALIZADO');
window.openActividadModal ??= async ()=>{};
window.staffResetInicio ??= async ()=>{};

/* ====== ARRANQUE ====== */
onAuthStateChanged(auth, async (user) => {
  if (!user){ location.href='index.html'; return; }
  state.user = user;
  state.is = STAFF_EMAILS.has((user.email||'').toLowerCase());

  const coords = await loadCoordinadores(); state.coordinadores = coords;

  // : SELECTOR CON "TODOS"
  if (state.is){ await showSelector(coords); }
  else {
    const mine = findCoordinadorForUser(coords, user);
    state.viewingCoordId = mine.id || 'self';
    await loadGruposForCoordinador(mine, user);
  }

   // BOTONES SOLO PARA STAFF (en NAV solo queda imprimir; crear alerta va en Alertas)
   const btnPrint = document.getElementById('btnPrintVch');
   // if (btnPrint){
     // btnPrint.style.display = state.is ? '' : 'none';
     // if (state.is) btnPrint.textContent = 'IMPRIMIR DESPACHO';
   // } 
   const legacyNewAlert = document.getElementById('btnNewAlert');
   if (legacyNewAlert) legacyNewAlert.style.display = 'none';

     // 🔽 crea hoja de impresión oculta
     ensurePrintDOM();

     // === Preferencias iniciales del panel de notificaciones ===
     state.alertsUI ||= {};
     state.alertsUI.state = 'unread'; // NO LEÍDAS por defecto
     state.alertsUI.scope = 'ops';    // OPERACIONES por defecto (notas de itinerario)

     // Fuerza panel de notificaciones contraído en cada carga
     try { localStorage.setItem('rt__alerts_fold','1'); } catch(_) {}

     // PANEL ALERTAS
     await window.renderGlobalAlertsV2();

      // matar cualquier timer viejo de versiones anteriores
      try { if (state.alertsTimer)     { clearInterval(state.alertsTimer);     state.alertsTimer = null; } } catch(_) {}
      try { if (window.rtAlertsTimer)  { clearInterval(window.rtAlertsTimer);  window.rtAlertsTimer  = null; } } catch(_) {}
      try { if (window.alertsTimer)    { clearInterval(window.alertsTimer);    window.alertsTimer    = null; } } catch(_) {}
      
      // AUTO-REFRESCO CADA 60S (solo alertas, sin reordenar paneles)
      if (!state.alertsTimer){
        state.alertsTimer = setInterval(window.renderGlobalAlertsV2, 3600000); // 60 min
        // opcional: espejo para legacy que miraba window.*
        window.rtAlertsTimer = state.alertsTimer;
      }
});

function obtenerFechaChile(
  fecha = new Date()
) {
  const partes =
    new Intl.DateTimeFormat(
      'en-CA',
      {
        timeZone:
          'America/Santiago',

        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      }
    ).formatToParts(fecha);

  const valores = {};

  partes.forEach(parte => {
    if (parte.type !== 'literal') {
      valores[parte.type] =
        parte.value;
    }
  });

  return {
    ano: Number(valores.year),
    mes: Number(valores.month),
    dia: Number(valores.day)
  };
}

function obtenerAnoViajeActivoChile(
  fecha = new Date()
) {
  const {
    ano,
    mes
  } = obtenerFechaChile(fecha);

  return mes >= 7
    ? ano
    : ano - 1;
}

function nombreOperativoGrupo(g) {
  return String(
    g?.aliasGrupo ||
    g?.nombreGrupo ||
    g?.grupoId ||
    g?.id ||
    ''
  ).trim();
}

function normalizarResumenCoordinadores(
  datos
) {
  const coordinadores =
    Array.isArray(datos?.coordinadores)
      ? datos.coordinadores
          .filter(
            coordinador =>
              coordinador &&
              typeof coordinador ===
                'object'
          )
          .map(coordinador => ({
            id: String(
              coordinador.id ||
              coordinador.coordinadorId ||
              ''
            ).trim(),

            nombre: String(
              coordinador.nombre ||
              ''
            ).trim(),

            email: String(
              coordinador.email ||
              coordinador.correo ||
              ''
            )
              .trim()
              .toLowerCase(),

            telefono: String(
              coordinador.telefono ||
              coordinador.fono ||
              coordinador.celular ||
              ''
            ).trim()
          }))
      : [];

  const coordinadorIds = [
    ...new Set([
      ...(Array.isArray(
        datos?.coordinadorIds
      )
        ? datos.coordinadorIds
        : []),

      ...(Array.isArray(
        datos?.coordinadoresIds
      )
        ? datos.coordinadoresIds
        : []),

      ...coordinadores.map(
        coordinador =>
          coordinador.id
      )
    ]
      .map(String)
      .filter(Boolean))
  ];

  const coordinadoresEmails = [
    ...new Set([
      ...(Array.isArray(
        datos?.coordinadoresEmails
      )
        ? datos.coordinadoresEmails
        : []),

      ...coordinadores.map(
        coordinador =>
          coordinador.email
      )
    ]
      .map(email =>
        String(email)
          .trim()
          .toLowerCase()
      )
      .filter(Boolean))
  ];

  return {
    coordinadores,
    coordinadorIds,
    coordinadoresIds:
      coordinadorIds,
    coordinadoresEmails
  };
}

function prepararGrupoDesdeResumen(
  docSnap
) {
  const datos =
    docSnap.data() || {};

  const grupoId = String(
    datos.grupoId ||
    docSnap.id
  ).trim();

  const numeroNegocio = String(
    datos.numeroNegocio ||
    ''
  ).trim();

  const coordinacion =
    normalizarResumenCoordinadores(
      datos
    );

  return {
    id: grupoId,
    grupoId,

    numeroNegocio,

    identificador: String(
      datos.identificador ||
      (
        numeroNegocio &&
        grupoId.startsWith(
          `${numeroNegocio}-`
        )
          ? grupoId.slice(
              numeroNegocio.length + 1
            )
          : ''
      )
    ).trim(),

    nombreGrupo: String(
      datos.nombreGrupo ||
      ''
    ).trim(),

    aliasGrupo: String(
      datos.aliasGrupo ||
      datos.nombreGrupo ||
      ''
    ).trim(),

    colegio: String(
      datos.colegio ||
      ''
    ).trim(),

    curso: String(
      datos.curso ||
      ''
    ).trim(),

    anoViaje: Number(
      datos.anoViaje ||
      0
    ),

    destino: String(
      datos.destino ||
      ''
    ).trim(),

    programa: String(
      datos.programa ||
      ''
    ).trim(),

    fechaInicio:
      toISO(datos.fechaInicio),

    fechaFin:
      toISO(datos.fechaFin),

    cantidadgrupo:
      Number(
        datos?.pax?.total ||
        0
      ),

    adultos:
      Number(
        datos?.pax?.adultos ||
        0
      ),

    estudiantes:
      Number(
        datos?.pax?.estudiantes ||
        0
      ),

    paxResumen:
      datos.pax || {},

    hotelesResumen:
      Array.isArray(datos.hoteles)
        ? datos.hoteles
        : [],

    vuelosResumen:
      Array.isArray(datos.vuelos)
        ? datos.vuelos
        : [],

    itinerario:
      normalizeItinerario(
        datos.itinerario || {}
      ),

    fechasItinerario:
      Array.isArray(
        datos.fechasItinerario
      )
        ? datos.fechasItinerario
        : [],

    ...coordinacion,

    _desdeResumen: true,
    _detalleCargado: false
  };
}

async function cargarResumenesAno(
  anoViaje,
  {
    force = false
  } = {}
) {
  const ano = Number(anoViaje);

  if (
    !force &&
    state.cache
      .resumenesPorAno
      .has(ano)
  ) {
    return state.cache
      .resumenesPorAno
      .get(ano);
  }

  const snapshot =
    await getDocs(
      query(
        collection(
          db,
          'operaciones_calendario_resumen'
        ),

        where(
          'anoViaje',
          '==',
          ano
        )
      )
    );

  const grupos =
    snapshot.docs.map(
      prepararGrupoDesdeResumen
    );

  state.cache
    .resumenesPorAno
    .set(
      ano,
      grupos
    );

  return grupos;
}

function grupoPerteneceCoordinador(
  grupo,
  coordinador,
  user
) {
  if (
    coordinador?.id ===
    '__ALL__'
  ) {
    return true;
  }

  const coordinadorId =
    String(
      coordinador?.id ||
      ''
    ).trim();

  const email = String(
    coordinador?.email ||
    user?.email ||
    ''
  )
    .trim()
    .toLowerCase();

  const ids = new Set([
    ...(grupo.coordinadorIds || []),
    ...(grupo.coordinadoresIds || [])
  ].map(String));

  const emails = new Set(
    (grupo.coordinadoresEmails || [])
      .map(valor =>
        String(valor)
          .trim()
          .toLowerCase()
      )
  );

  return (
    (
      coordinadorId &&
      ids.has(coordinadorId)
    ) ||
    (
      email &&
      emails.has(email)
    )
  );
}

async function ensureGrupoDetalleLoaded(
  grupo
) {
  if (!grupo) {
    return grupo;
  }

  if (grupo._detalleCargado) {
    return grupo;
  }

  if (
    state.cache.gruposDetalle.has(
      grupo.id
    )
  ) {
    return state.cache
      .gruposDetalle
      .get(grupo.id);
  }

  try {
    const snapshot =
      await getDoc(
        doc(
          db,
          'grupos',
          grupo.id
        )
      );

    if (!snapshot.exists()) {
      grupo._detalleCargado = true;

      state.cache
        .gruposDetalle
        .set(
          grupo.id,
          grupo
        );

      return grupo;
    }

    const raw =
      snapshot.data() || {};

    const coordinadoresResumen =
      grupo.coordinadores || [];

    const idsResumen =
      grupo.coordinadorIds || [];

    const emailsResumen =
      grupo.coordinadoresEmails || [];

    const itinerarioResumen =
      grupo.itinerario || {};

    const fusionado = {
      ...grupo,
      ...raw,

      id: grupo.id,
      grupoId: grupo.id,

      aliasGrupo:
        grupo.aliasGrupo ||
        raw.aliasGrupo ||
        raw.nombreGrupo ||
        grupo.id,

      numeroNegocio: String(
        raw.numeroNegocio ||
        grupo.numeroNegocio ||
        ''
      ),

      identificador: String(
        raw.identificador ||
        grupo.identificador ||
        ''
      ),

      fechaInicio:
        toISO(
          raw.fechaInicio ||
          grupo.fechaInicio
        ),

      fechaFin:
        toISO(
          raw.fechaFin ||
          grupo.fechaFin
        ),

      itinerario:
        Object.keys(
          itinerarioResumen
        ).length
          ? itinerarioResumen
          : normalizeItinerario(
              raw.itinerario || {}
            ),

      asistencias:
        raw.asistencias || {},

      serviciosEstado:
        raw.serviciosEstado || {},

      coordinadores:
        coordinadoresResumen.length
          ? coordinadoresResumen
          : (
              Array.isArray(
                raw.coordinadores
              )
                ? raw.coordinadores
                : []
            ),

      coordinadorIds:
        idsResumen.length
          ? idsResumen
          : (
              Array.isArray(
                raw.coordinadorIds
              )
                ? raw.coordinadorIds
                : []
            ),

      coordinadoresIds:
        idsResumen.length
          ? idsResumen
          : (
              Array.isArray(
                raw.coordinadoresIds
              )
                ? raw.coordinadoresIds
                : []
            ),

      coordinadoresEmails:
        emailsResumen.length
          ? emailsResumen
          : (
              Array.isArray(
                raw.coordinadoresEmails
              )
                ? raw.coordinadoresEmails
                : []
            ),

      _desdeResumen: true,
      _detalleCargado: true
    };

    state.cache
      .gruposDetalle
      .set(
        grupo.id,
        fusionado
      );

    return fusionado;
  } catch (error) {
    console.error(
      '[COORDINADORES] No se pudo cargar detalle',
      {
        grupoId: grupo.id,
        error
      }
    );

    grupo._detalleCargado = true;
    return grupo;
  }
}

function reemplazarGrupoEnEstado(
  grupo
) {
  const reemplazar = lista => {
    if (!Array.isArray(lista)) {
      return;
    }

    const indice =
      lista.findIndex(
        item =>
          item?.id === grupo.id
      );

    if (indice >= 0) {
      lista[indice] = grupo;
    }
  };

  reemplazar(state.grupos);
  reemplazar(state.ordenados);
}

/* ====== CARGAS FIRESTORE ====== */
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list=[]; snap.forEach(d=>{ const x=d.data()||{}; list.push({
    id:d.id, nombre:String(x.nombre||x.Nombre||x.coordinador||''), email:String(x.email||x.correo||x.mail||'').toLowerCase(), uid:String(x.uid||x.userId||'')
  });});
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre,'es',{sensitivity:'base'}));
  const seen=new Set(), dedup=[];
  for(const c of list){ const k=(c.nombre+'|'+c.email).toLowerCase(); if(!seen.has(k)){ seen.add(k); dedup.push(c); } }
  return dedup;
}
function findCoordinadorForUser(coordinadores, user){
  const email=(user.email||'').toLowerCase(), uid=user.uid;
  let c = coordinadores.find(x=> x.email && x.email.toLowerCase()===email); if(c) return c;
  if (uid){ c=coordinadores.find(x=>x.uid && x.uid===uid); if(c) return c; }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

/* ====== SELECTOR  (CON "TODOS") ====== */
async function showSelector(
  coordinadores
) {
  const anoActivo =
    obtenerAnoViajeActivoChile();

  const anos = [
    anoActivo - 1,
    anoActivo,
    anoActivo + 1,
    anoActivo + 2
  ];

  const bar = ensurePanel(
    'staffBar',

    `
      <div
        style="
          display:grid;
          grid-template-columns:
            minmax(0,2fr)
            minmax(150px,1fr);
          gap:.65rem;
          align-items:end
        "
      >
        <label>
          <span
            style="
              display:block;
              margin-bottom:6px;
              color:var(--muted)
            "
          >
            COORDINADOR(A):
          </span>

          <select id="coordSelect">
          </select>
        </label>

        <label>
          <span
            style="
              display:block;
              margin-bottom:6px;
              color:var(--muted)
            "
          >
            AÑO DE VIAJE:
          </span>

          <select id="coordAnoSelect">
          </select>
        </label>
      </div>
    `
  );

  const selectorCoord =
    bar.querySelector(
      '#coordSelect'
    );

  const selectorAno =
    bar.querySelector(
      '#coordAnoSelect'
    );

  selectorCoord.innerHTML = [
    '<option value="__ALL__">TODOS</option>',

    ...coordinadores.map(
      coordinador =>
        `
          <option
            value="${coordinador.id}"
          >
            ${(
              coordinador.nombre ||
              ''
            ).toUpperCase()}
            —
            ${(
              coordinador.email ||
              ''
            ).toUpperCase()}
          </option>
        `
    )
  ].join('');

  selectorAno.innerHTML =
    anos.map(
      ano =>
        `
          <option value="${ano}">
            ${ano}${
              ano === anoActivo
                ? ' · ACTIVO'
                : ''
            }
          </option>
        `
    ).join('');

  const coordinadorGuardado =
    localStorage.getItem(
      'rt__coord'
    );

  const anoGuardado = Number(
    localStorage.getItem(
      'rt__ano_viaje'
    )
  );

  selectorCoord.value =
    coordinadores.some(
      coordinador =>
        coordinador.id ===
        coordinadorGuardado
    ) ||
    coordinadorGuardado ===
      '__ALL__'
      ? coordinadorGuardado
      : '__ALL__';

  selectorAno.value = String(
    anos.includes(anoGuardado)
      ? anoGuardado
      : anoActivo
  );

  const obtenerCoordinador =
    () => {
      const id =
        selectorCoord.value ||
        '__ALL__';

      if (id === '__ALL__') {
        return {
          id: '__ALL__',
          nombre: 'TODOS',
          email: ''
        };
      }

      return (
        coordinadores.find(
          coordinador =>
            coordinador.id === id
        ) ||
        {
          id,
          nombre: '',
          email: ''
        }
      );
    };

  const recargar = async ({
    force = false
  } = {}) => {
    const coordinador =
      obtenerCoordinador();

    const ano = Number(
      selectorAno.value ||
      anoActivo
    );

    state.viewingCoordId =
      coordinador.id;

    state.coordinadorActual =
      coordinador;

    state.anoViajeActivo =
      ano;

    localStorage.setItem(
      'rt__coord',
      coordinador.id
    );

    localStorage.setItem(
      'rt__ano_viaje',
      String(ano)
    );

    if (force) {
      state.cache
        .resumenesPorAno
        .delete(ano);
    }

    await loadGruposForCoordinador(
      coordinador,
      state.user
    );

    await window
      .renderGlobalAlertsV2();
  };

  selectorCoord.onchange =
    () => recargar();

  selectorAno.onchange =
    () => recargar();

  await recargar();
}

/* ====== GRUPOS PARA EL COORDINADOR EN CONTEXTO (O "TODOS") ====== */
async function loadGruposForCoordinador(
  coord,
  user
) {
  const cont =
    document.getElementById(
      'grupos'
    );

  if (cont) {
    cont.textContent =
      'CARGANDO GRUPOS…';
  }

  const anoViaje = Number(
    state.anoViajeActivo ||
    obtenerAnoViajeActivoChile()
  );

  state.anoViajeActivo =
    anoViaje;

  state.coordinadorActual =
    coord || null;

  let resumenes = [];

  try {
    resumenes =
      await cargarResumenesAno(
        anoViaje
      );
  } catch (error) {
    console.error(
      '[COORDINADORES] Error leyendo resumen',
      error
    );

    if (cont) {
      cont.innerHTML =
        `
          <div class="muted">
            NO SE PUDO CARGAR EL
            RESUMEN OPERATIVO.
          </div>
        `;
    }

    return;
  }

  const isAll =
    coord?.id === '__ALL__';

  const wanted = isAll
    ? resumenes.slice()
    : resumenes.filter(
        grupo =>
          grupoPerteneceCoordinador(
            grupo,
            coord,
            user
          )
      );

  const hoy =
    obtenerFechaChile();

  const hoyISO = [
    hoy.ano,
    String(hoy.mes)
      .padStart(2, '0'),
    String(hoy.dia)
      .padStart(2, '0')
  ].join('-');

  state.grupos =
    wanted;
  
  state.ordenados =
    wanted
      .slice()
      .sort(
        (a, b) => {
          const inicioA =
            toISO(
              a.fechaInicio
            ) ||
            "9999-12-31";
  
          const inicioB =
            toISO(
              b.fechaInicio
            ) ||
            "9999-12-31";
  
          const porInicio =
            inicioA.localeCompare(
              inicioB
            );
  
          if (porInicio !== 0) {
            return porInicio;
          }
  
          return String(
            a.numeroNegocio ||
            a.id ||
            ""
          ).localeCompare(
            String(
              b.numeroNegocio ||
              b.id ||
              ""
            ),
            "es",
            {
              numeric:
                true
            }
          );
        }
      );

  state.filter = {
    type: 'all',
    value: null
  };

  state.groupQ = '';

  renderStatsFiltered();

  if (!state.ordenados.length) {
    state.idx = 0;

    if (cont) {
      cont.innerHTML =
        `
          <div class="muted">
            NO HAY VIAJES ASIGNADOS
            PARA EL AÑO ${anoViaje}.
          </div>
        `;
    }

    return;
  }

  const {
    g: qsG,
    f: qsF
  } = parseQS();

  let idx =
    getIndiceProximoViaje(
      state.ordenados
    );
  
  if (qsG) {
    const byNum =
      state.ordenados.findIndex(
        grupo =>
          String(
            grupo.numeroNegocio
          ) ===
          qsG
      );
  
    const byId =
      state.ordenados.findIndex(
        grupo =>
          String(
            grupo.id
          ) ===
          qsG
      );
  
    if (byNum >= 0) {
      idx = byNum;
    } else if (byId >= 0) {
      idx = byId;
    }
  }
  state.idx =
    Math.max(
      0,
      Math.min(
        idx,
        state.ordenados.length - 1
      )
    );
  
  // El selector se construye después de conocer
  // el índice definitivo.
  renderNavBar();
  
  let target =
    state.ordenados[
      state.idx
    ];

  target =
    await ensureGrupoDetalleLoaded(
      target
    );

  reemplazarGrupoEnEstado(target);

  await ensureItinerarioLoaded(
    target
  );

  await renderOneGroup(
    target,
    qsF
  );
}

/* ====== NORMALIZADOR DE ITINERARIO (multiesquema, robusto) ====== */
function normalizeItinerario(raw){
  if (!raw) return {};

  // A) Array plano de actividades con .fecha → agrupar por fecha
  if (Array.isArray(raw)){
    const map = {};
    for (const item of raw){
      const f = toISO(item && item.fecha);
      if (!f || !item || typeof item !== 'object') continue;
      (map[f] ||= []).push({ ...item });
    }
    return map;
  }

  // B) Objeto { 'YYYY-MM-DD': [...] }  (OK)
  // C) Objeto { 'YYYY-MM-DD': { items | actividades | acts: [...] } }
  // D) Objeto { 'YYYY-MM-DD': { '0':act, '1':act, ... } }  ← NUEVO
  // E) Objeto { 'YYYY-MM-DD': { <timeId>: actividad } }
  if (raw && typeof raw === 'object'){
    const out = {};
    for (const [k,v] of Object.entries(raw)){
      const f = toISO(k);
      if (!f) continue;

      let arr = [];
      if (Array.isArray(v)){
        arr = v;
      } else if (v && typeof v === 'object'){
        if (Array.isArray(v.items))        arr = v.items;
        else if (Array.isArray(v.actividades)) arr = v.actividades;
        else if (Array.isArray(v.acts))    arr = v.acts;
        else {
          const keys = Object.keys(v);

          // D) objeto indexado tipo {"0":{...},"1":{...}} → ordénalo y pásalo a array
          if (keys.length && keys.every(x => /^\d+$/.test(x))){
            arr = keys.sort((a,b)=>Number(a)-Number(b)).map(i => v[i]).filter(x => x && typeof x==='object');
          } else {
            // E) mapa { timeId: actividad } si parecen actividades
            const vals = Object.values(v).filter(x => x && typeof x==='object');
            if (vals.some(x => x.actividad || x.horaInicio || x.horaFin)) arr = vals;
          }
        }
      }

      if (Array.isArray(arr) && arr.length){
        // Sanitiza: sólo objetos
        out[f] = arr.filter(x => x && typeof x==='object').map(x => ({ ...x }));
      }
    }
    return out;
  }

  return {};
}

/* ====== ITINERARIO: carga desde subcolección (compat) ====== */
async function loadItinerarioFromSubcollections(grupoId){
  const map = {};
  try{
    const coll = collection(db, 'grupos', grupoId, 'itinerario');
    const ds   = await getDocs(coll);
    for (const d of ds.docs){
      const iso = toISO(d.id);
      if (!iso) continue;
      const x = d.data() || {};

      // Prioriza arrays directos
      let items = null;
      if (Array.isArray(x.items))        items = x.items;
      else if (Array.isArray(x.actividades)) items = x.actividades;
      else if (Array.isArray(x.acts))    items = x.acts;

      // Si no hay arrays directos, intenta subcolección "items"
      if (!items){
        try{
          const sub = await getDocs(collection(db,'grupos',grupoId,'itinerario',d.id,'items'));
          const arr = [];
          sub.forEach(i => arr.push({ id:i.id, ...(i.data()||{}) }));
          if (arr.length) items = arr;
        }catch(_){}
      }

      // Si tampoco, convierte objeto {timeId:{...}}
      if (!items && x && typeof x === 'object'){
        const vals = Object.values(x).filter(z => z && typeof z === 'object');
        if (vals.some(z => z.actividad || z.horaInicio || z.horaFin)) items = vals;
      }

      if (Array.isArray(items) && items.length) map[iso] = items;
    }
  }catch(e){ console.warn('loadItinerarioFromSubcollections', e); }
  return map;
}

// Garantiza que el grupo tenga itinerario (si no viene en el doc, lo carga desde la subcolección)
async function ensureItinerarioLoaded(grupo){
  try{
    if (grupo && grupo.itinerario && Object.keys(grupo.itinerario).length) return grupo;
    const map = await loadItinerarioFromSubcollections(grupo.id);
    if (map && Object.keys(map).length){
      grupo.itinerario = map;
    }
  }catch(_){}
  return grupo;
}

/* ====== STATS ====== */
function getFilteredList(){ const base=state.ordenados.slice();
  const dest = (state.filter.type==='dest' && state.filter.value) ? state.filter.value : null;
  return dest ? base.filter(g=> String(g.destino||'')===dest) : base;
}
function renderStatsFiltered(){ renderStats(getFilteredList()); }
function renderStats(list){
  const p = ensurePanel('statsPanel');
  if (!list.length){
    p.innerHTML = '<div class="muted">SIN VIAJES ASIGNADOS.</div>';
    return;
  }

  const n        = list.length;
  const minIni   = list.map(g=>g.fechaInicio).filter(Boolean).sort()[0] || '';
  const maxFin   = list.map(g=>g.fechaFin).filter(Boolean).sort().slice(-1)[0] || '';
  const totalDias= list.reduce((s,g)=> s + daysInclusive(g.fechaInicio,g.fechaFin), 0);
  const paxTot   = list.reduce((s,g)=> s + paxOf(g), 0);
  const destinos = [...new Set(list.map(g=>String(g.destino||'')).filter(Boolean))]
                    .map(x=>x.toUpperCase());

  p.innerHTML = `
    <div class="stats-wrap">
      <div><strong><h4>DESPACHO</h4></strong></span></div>    
      <div class="meta-line meta">
        <span class="item nowrap">N° VIAJES: <strong>${n}</strong></span>
        <span class="item nowrap">DÍAS EN VIAJE: <strong>${totalDias}</strong></span>
        <span class="item">RANGO DE FECHAS: <strong>${minIni?dmy(minIni):'—'} — ${maxFin?dmy(maxFin):'—'}</strong></span>
        <span class="item">DESTINOS: <strong>${destinos.length?destinos.join(' · '):'—'}</strong></span>
      </div>
    </div>`;
}

/* ====== NAV ====== */
function renderNavBar(){
  // Crea el panel si no existe y sólo inyecta el HTML base una vez
  const p = ensurePanel('navPanel');
  if (!p.innerHTML.trim()) {
    p.innerHTML = `
      <div class="rowflex" style="gap:.5rem;align-items:center;flex-wrap:wrap">
        <button id="btnPrev" class="btn sec">◀</button>
        <select id="allTrips" style="flex:1;min-width:260px"></select>
        <button id="btnNext" class="btn sec">▶</button>
        <button id="btnPrintVch" class="btn sec">IMPRIMIR DESPACHO</button>
      </div>`;
  }

  const sel = p.querySelector('#allTrips');
  sel.textContent = '';

  // FILTRO TODOS (sólo UI del select de viajes)
  const ogFiltro = document.createElement('optgroup'); ogFiltro.label = 'FILTRO';
  ogFiltro.appendChild(new Option('TODOS','all')); sel.appendChild(ogFiltro);

  // VIAJES
  const ogTrips = document.createElement('optgroup'); ogTrips.label = 'VIAJES';
  state.ordenados.forEach((g,i)=>{
    const name=(nombreOperativoGrupo(g));
    const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
    const opt=new Option(`${(g.destino||'').toUpperCase()} · ${(name||'').toUpperCase()} (${code}) | IDA: ${dmy(g.fechaInicio||'')}  VUELTA: ${dmy(g.fechaFin||'')}`, `trip:${i}`);
    ogTrips.appendChild(opt);
  });
  sel.appendChild(ogTrips);
  sel.value = `trip:${state.idx}`;

  p.querySelector('#btnPrev').onclick = async ()=>{
    const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.max(0,j-1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId);
    await renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`;
  };
  p.querySelector('#btnNext').onclick = async ()=>{
    const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.min(list.length-1,j+1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId);
    await renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`;
  };
  sel.onchange = async ()=>{
    const v=sel.value||'';
    if(v==='all'){ state.filter={type:'all',value:null}; renderStatsFiltered(); sel.value=`trip:${state.idx}`; }
    else if(v.startsWith('trip:')){ state.idx=Number(v.slice(5))||0; await renderOneGroup(state.ordenados[state.idx]); }
  };

  // Botón imprimir sólo visible a STAFF
  if (state.is){
    const btn = document.getElementById('btnPrintVch');
    if (btn){
      btn.textContent = 'IMPRIMIR DESPACHO';
      btn.onclick = async () => {
        try{
          const g = state.ordenados[state.idx];
          await preparePrintForGroup(g);   // deja listo #print-block
          window.print();
        }catch(e){
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

// ====== ACTA DE CIERRE DE FINANZAS (usa un snapshot) ======
async function preparePrintActaFinanzas(g, snap){
  // Reutilizamos la misma hoja oculta de impresión
  ensurePrintDOM();

  const $doc   = document.getElementById('print-block');
  const $title = document.getElementById('ph-title');
  const $grp   = document.getElementById('ph-grupo');
  const $m1    = document.getElementById('ph-meta1');
  const $m2    = document.getElementById('ph-meta2');
  const $fech  = document.getElementById('ph-fechas');
  const $pax   = document.getElementById('ph-pax');

  const norm = s => String(s||'').trim().toUpperCase();
  const dmySafe = v => {
    try{
      if (!v) return '';
      const d = (v instanceof Date) ? v : new Date(v);
      return isNaN(d)
        ? String(v).toUpperCase()
        : d.toLocaleDateString('es-CL').toUpperCase();
    }catch{
      return String(v||'').toUpperCase();
    }
  };
  const fmtMon = v => {
    const n = Number(v||0);
    if (!isFinite(n)) return String(v||'');
    return n.toLocaleString('es-CL',{ minimumFractionDigits: 0 });
  };

  const s = snap || {};
  const resumen    = s.resumen || {};
  const saldos     = resumen.saldos || {};
  const totAb      = resumen.totalesAbonos || {};
  const totGas     = resumen.totalesGastos || {};
  const cierrePrev = s.cierrePrevio || {};

  const nn    = s.numeroNegocio ?? g.numeroNegocio ?? '';
  const ident = s.identificador ?? g.identificador ?? '';
  const code  = nn + (ident ? '-' + ident : '');
  const nombre = s.nombreGrupo
    || g.nombreGrupo
    || g.aliasGrupo
    || code
    || g.id
    || '';

  const destino    = s.destino || g.destino || '';
  const anoViaje   = s.anoViaje || g.anoViaje || '';
  const coordName  = g.coordinadorNombre || g.coordinador || '';
  const programa   = g.programa || '';
  const rangoViaje = `${dmySafe(g.fechaInicio||'')} — ${dmySafe(g.fechaFin||'')}`;

  // Encabezado HTML del acta (bloque arriba del PRE)
  if ($title) $title.textContent = 'ACTA DE CIERRE FINANCIERO';
  if ($grp)   $grp.textContent   = `GRUPO: ${norm(nombre)} (${code || 'SIN CÓDIGO'})`;
  if ($m1)    $m1.textContent    = `DESTINO: ${norm(destino || '')}`;
  if ($m2)    $m2.textContent    = `SNAPSHOT POR: ${norm(s.createdBy || '')} · MOTIVO: ${norm(s.motivo || 'MANUAL')}`;
  if ($fech){
    let fechaSnap = '';
    try{
      const raw = s.createdAt?.toDate ? s.createdAt.toDate() : s.createdAt;
      if (raw) fechaSnap = dmySafe(raw);
    }catch{}
    $fech.textContent = `FECHA SNAPSHOT: ${fechaSnap || '—'}`;
  }

  if ($pax){
    let plan = null, real = null;
    try{
      if (typeof paxOf === 'function')     plan = paxOf(g);
      if (typeof paxRealOf === 'function') real = paxRealOf(g);
    }catch{}
    let txt = 'PAX: ';
    if (plan!=null && real!=null && plan!==real){
      txt += `${plan} → ${real}`;
    }else if (real!=null){
      txt += `${real}`;
    }else if (plan!=null){
      txt += `${plan}`;
    }else{
      txt += '—';
    }
    $pax.textContent = txt;
  }

  const lines = [];

  // ────────────────────────────────
  // Título
  // ────────────────────────────────
  lines.push('ACTA DE CIERRE FINANCIERO');
  lines.push('');

  // 0) Datos generales del grupo
  lines.push('0) DATOS GENERALES DEL GRUPO');
  lines.push(`   - GRUPO: ${norm(nombre)} (${code || 'SIN CÓDIGO'})`);
  if (coordName) lines.push(`   - COORDINADOR/A PRINCIPAL: ${norm(coordName)}`);
  if (destino)   lines.push(`   - DESTINO: ${norm(destino)}`);
  if (programa)  lines.push(`   - PROGRAMA: ${norm(programa)}`);
  if (anoViaje)  lines.push(`   - AÑO DE VIAJE: ${anoViaje}`);
  if (rangoViaje.trim()) lines.push(`   - FECHAS DE VIAJE: ${rangoViaje}`);
  lines.push('');

  // 1) Resumen por moneda
  lines.push('1) RESUMEN POR MONEDA');
  const monedas = new Set([
    ...Object.keys(totAb || {}),
    ...Object.keys(totGas || {}),
    ...Object.keys(saldos || {})
  ]);
  if (monedas.size){
    monedas.forEach(mon=>{
      const m  = String(mon||'CLP').toUpperCase();
      const ab = totAb[m]  ?? totAb[mon]  ?? 0;
      const ga = totGas[m] ?? totGas[mon] ?? 0;
      const sd = saldos[m] ?? saldos[mon] ?? 0;
      lines.push(
        `   - ${m}: ABONOS ${fmtMon(ab)} · GASTOS ${fmtMon(ga)} · SALDO ${fmtMon(sd)}`
      );
    });
  }else{
    lines.push('   (sin totales registrados)');
  }

  // 2) Detalle de abonos
  lines.push('');
  lines.push('2) DETALLE DE ABONOS');
  if (Array.isArray(s.abonos) && s.abonos.length){
    s.abonos.forEach(a=>{
      const f   = a.fecha ? dmySafe(a.fecha) : 'S/F';
      const mon = String(a.moneda||'CLP').toUpperCase();
      const v   = fmtMon(a.valor);
      const medio  = norm(a.medio || '');
      const asunto = (a.asunto || '').toString().toUpperCase();
      const parts = [`${f}`, `${mon} ${v}`];
      if (medio)  parts.push(medio);
      if (asunto) parts.push(asunto);
      lines.push('   - ' + parts.join(' · '));
    });
  }else{
    lines.push('   (sin abonos registrados)');
  }

  // 3) Detalle de gastos aprobados
  lines.push('');
  lines.push('3) DETALLE DE GASTOS APROBADOS');
  if (Array.isArray(s.gastosAprobados) && s.gastosAprobados.length){
    s.gastosAprobados.forEach(x=>{
      const f   = x.fecha ? dmySafe(x.fecha) : 'S/F';
      const mon = String(x.moneda||'CLP').toUpperCase();
      const v   = fmtMon(x.valor);
      const prov = (x.proveedor || '').toString().toUpperCase();
      const act  = (x.actividad || '').toString().toUpperCase();
      const asu  = (x.asunto || '').toString().toUpperCase();
      const parts = [`${f}`, `${mon} ${v}`];
      if (prov) parts.push(prov);
      if (act)  parts.push(act);
      if (asu)  parts.push(asu);
      lines.push('   - ' + parts.join(' · '));
    });
  }else{
    lines.push('   (sin gastos aprobados)');
  }

  // 4) Respaldos cargados
  lines.push('');
  lines.push('4) RESPALDOS CARGADOS');
  const t = cierrePrev.transfer || {};
  const c = cierrePrev.cashUsd || {};
  const b = cierrePrev.boleta  || {};
  if (t.comprobanteUrl) lines.push('   - COMPROBANTE TRANSFERENCIA: ' + t.comprobanteUrl);
  if (c.comprobanteUrl) lines.push('   - CONSTANCIA EFECTIVO USD: ' + c.comprobanteUrl);
  if (b.url)            lines.push('   - BOLETA: ' + b.url);
  if (!t.comprobanteUrl && !c.comprobanteUrl && !b.url){
    lines.push('   (sin URLs de respaldo registradas en este snapshot)');
  }

  // 5) Firmas / observaciones
  lines.push('');
  lines.push('5) OBSERVACIONES / FIRMAS INTERNAS');
  lines.push('   _________________________________');
  lines.push('   _________________________________');
  lines.push('   _________________________________');

  // 6) Resumen logístico del viaje (hoteles + vuelos + resumen de itinerario)
  lines.push('');
  lines.push('6) RESUMEN LOGÍSTICO DEL VIAJE');

  // 6.a) Hoteles
  try{
    const hoteles = await loadHotelesInfo(g);
    if (Array.isArray(hoteles) && hoteles.length){
      lines.push('   · HOTELES ASIGNADOS:');
      hoteles.forEach(h=>{
        const nombreHotel = norm(h.hotelNombre || (h.hotel && h.hotel.nombre) || '');
        const ci = dmySafe(h.checkIn || h.fechaCheckIn || h.fechaIngreso || '');
        const co = dmySafe(h.checkOut || h.fechaCheckOut || h.fechaSalida || '');
        const noches = (h.noches!=='' && h.noches!=null) ? ` · ${h.noches} NOCHES` : '';
        const ciudad = norm(h.ciudad || h.ciudadHotel || '');
        let linea = `     - ${nombreHotel}`;
        const rangoH = [ci, co].filter(Boolean).join(' — ');
        if (rangoH) linea += ` · ${rangoH}`;
        if (noches) linea += noches;
        if (ciudad) linea += ` · ${ciudad}`;
        lines.push(linea);
      });
    }else{
      lines.push('   · HOTELES ASIGNADOS: SIN REGISTROS.');
    }
  }catch(e){
    console.warn('preparePrintActaFinanzas: error al cargar hoteles', e);
    lines.push('   · HOTELES ASIGNADOS: error al cargar información.');
  }

  // 6.b) Vuelos
  try{
    const vuelos = await loadVuelosInfo(g);
    if (Array.isArray(vuelos) && vuelos.length){
      lines.push('   · VUELOS ASIGNADOS:');
      vuelos.forEach(v=>{
        const f = v.fecha || v.fechaISO || v.fechaVuelo || null;
        const fTxt = f ? dmySafe(f) : '';
        const tramo = [v.origen, v.destino].filter(Boolean).join(' → ');
        const vueloCod = v.codigo || v.vuelo || v.codVuelo || '';
        const aerolinea = v.aerolinea || v.compania || v.linea || '';
        let linea = '     - ';
        if (fTxt) linea += fTxt + ' · ';
        if (tramo) linea += tramo + ' · ';
        if (aerolinea) linea += aerolinea + ' ';
        if (vueloCod) linea += vueloCod;
        lines.push(linea.trim());
      });
    }else{
      lines.push('   · VUELOS ASIGNADOS: SIN REGISTROS.');
    }
  }catch(e){
    console.warn('preparePrintActaFinanzas: error al cargar vuelos', e);
    lines.push('   · VUELOS ASIGNADOS: error al cargar información.');
  }

  // 6.c) Itinerario (detalle por día y actividad)
  try{
    const gIt = await ensureItinerarioLoaded(g);
    const it = (gIt && gIt.itinerario) || {};
    const fechas = Object.keys(it || {}).sort();

    if (fechas.length){
      lines.push('   · ITINERARIO (DETALLE POR DÍA):');
      fechas.forEach(fechaISO=>{
        let acts = it[fechaISO];
        if (!acts) return;

        if (Array.isArray(acts)){
          // ok
        }else if (Array.isArray(acts.items)){
          acts = acts.items;
        }else if (Array.isArray(acts.actividades)){
          acts = acts.actividades;
        }else if (Array.isArray(acts.acts)){
          acts = acts.acts;
        }else{
          return;
        }

        if (!acts.length) return;

        const fTxt = dmySafe(fechaISO);
        // encabezado del día
        lines.push(`     - ${fTxt}:`);

        // TODAS las actividades de ese día, una por línea
        acts.forEach(a=>{
          const h   = a.horaInicio || a.hora || '';
          const nom = a.actividad || a.nombre || a.titulo || '';
          const prov = (a.proveedor || a.proveedorNombre || a.provNombre || '');
          let linea = '       · ';
          if (h)   linea += `${h} `;
          if (nom) linea += nom.toString().toUpperCase();
          if (prov) linea += ` · ${prov.toString().toUpperCase()}`;
          lines.push(linea);
        });
      });
    }else{
      lines.push('   · ITINERARIO: SIN ACTIVIDADES REGISTRADAS.');
    }
  }catch(e){
    console.warn('preparePrintActaFinanzas: error al cargar itinerario', e);
    lines.push('   · ITINERARIO: error al cargar información.');
  }


  // 7) Actividades y bitácora detallada
  lines.push('');
  lines.push('7) ACTIVIDADES Y BITÁCORA DE COORDINACIÓN');

  try{
    // Itinerario normalizado para recorrer fechas y actos
    const gIt = await ensureItinerarioLoaded(g);
    const normIt = normalizeItinerario((gIt && gIt.itinerario) || g.itinerario || {});
    const fechas = Object.keys(normIt || {}).sort();

    if (!fechas.length){
      lines.push('   (sin itinerario cargado)');
    }else{
      for (const fechaISO of fechas){
        const acts = normIt[fechaISO] || [];
        if (!Array.isArray(acts) || !acts.length) continue;

        const fechaLabel = dmySafe(fechaISO) || fechaISO;
        lines.push(`   • ${fechaLabel}`);

        for (const a of acts){
          const actName = (a.actividad || a.nombre || a.titulo || '').toString().toUpperCase();
          const hora    = (a.horaInicio || a.hora || '').toString();
          const prov    = (a.proveedor || a.proveedorNombre || a.provNombre || '').toString().toUpperCase();

          const headParts = [];
          if (hora)    headParts.push(hora);
          if (actName) headParts.push(actName);
          if (prov)    headParts.push(prov);
          lines.push('      - ' + headParts.join(' · '));

          const actKey = slugActKey(a);
          if (!actKey){
            lines.push('           · (sin clave de actividad para bitácora)');
            continue;
          }

          try{
            const coll = collection(db,'grupos', g.id, 'bitacora', actKey, fechaISO);
            const qs = await getDocs(coll);

            if (qs.empty){
              lines.push('           · (sin notas de bitácora)');
            }else{
              const notas = [];
              qs.forEach(d=>{
                const x = d.data() || {};
                notas.push(x);
              });

              // Ordena por timestamp si existe
              notas.sort((a,b)=>{
                const ta = a.ts?.seconds || a.ts?.toMillis?.() || 0;
                const tb = b.ts?.seconds || b.ts?.toMillis?.() || 0;
                return ta - tb;
              });

              notas.forEach(note=>{
                const quien = String(note.byEmail || note.byUid || 'USUARIO').toUpperCase();
                let cuandoTxt = '';
                try{
                  const tv = note.ts?.seconds
                    ? new Date(note.ts.seconds*1000)
                    : (note.ts?.toDate ? note.ts.toDate() : null);
                  if (tv) cuandoTxt = tv.toLocaleString('es-CL').toUpperCase();
                }catch{}
                const txt = String(note.texto || note.text || '').trim().toUpperCase();
                if (!txt) return;

                let linea = `           · ${txt} — ${quien}`;
                if (cuandoTxt) linea += ` · ${cuandoTxt}`;
                lines.push(linea);
              });
            }
          }catch(eBit){
            console.error('[ACTA] Error cargando bitácora', {grupoId:g.id, fechaISO, actKey, e:eBit});
            lines.push('           · (no se pudo cargar la bitácora)');
          }
        }
      }
    }
  }catch(e){
    console.error('[ACTA] Error al preparar sección de actividades/bitácora', e);
    lines.push('   (no se pudo cargar el itinerario / bitácora)');
  }

  // OJO: acá va '\n' (salto real), no '\\n'
  if ($doc) $doc.textContent = lines.join('\n');
}

const URL_SEGUIMIENTO_ENCUESTA =
  "https://obtenergestionencuestaviaje-r3llfis4wa-tl.a.run.app";

function escapePortalHTML(
  value = ""
) {
  return String(
    value ?? ""
  )
    .replace(
      /&/g,
      "&amp;"
    )
    .replace(
      /</g,
      "&lt;"
    )
    .replace(
      />/g,
      "&gt;"
    )
    .replace(
      /"/g,
      "&quot;"
    )
    .replace(
      /'/g,
      "&#039;"
    );
}

function fechaHoraPortal(
  value
) {
  if (!value) {
    return "—";
  }

  try {
    const fecha =
      new Date(value);

    if (
      Number.isNaN(
        fecha.getTime()
      )
    ) {
      return "—";
    }

    return fecha
      .toLocaleString(
        "es-CL",
        {
          dateStyle:
            "short",

          timeStyle:
            "short",

          hour12:
            false
        }
      )
      .toUpperCase();
  } catch (_) {
    return "—";
  }
}

function etiquetaTipoPasajero(
  value = ""
) {
  const tipo =
    norm(value);

  if (
    tipo.includes(
      "profesor"
    )
  ) {
    return "PROFESOR/A";
  }

  if (
    tipo.includes(
      "adult"
    )
  ) {
    return "ADULTO ACOMPAÑANTE";
  }

  return "ESTUDIANTE";
}

async function postSeguimientoEncuesta(
  grupoDocId
) {
  const user =
    auth.currentUser;

  if (!user) {
    throw new Error(
      "SESIÓN NO DISPONIBLE"
    );
  }

  const token =
    await user.getIdToken();

  const response =
    await fetch(
      URL_SEGUIMIENTO_ENCUESTA,
      {
        method:
          "POST",

        headers: {
          "Content-Type":
            "application/json",

          Authorization:
            `Bearer ${token}`
        },

        body:
          JSON.stringify({
            modo:
              "seguimiento_coordinador",
        
            grupoDocId
          }),

        cache:
          "no-store"
      }
    );

  const data =
    await response
      .json()
      .catch(
        () => ({})
      );

  if (
    !response.ok ||
    data.ok !== true
  ) {
    throw new Error(
      data.message ||
      "NO SE PUDO CARGAR LA ENCUESTA"
    );
  }

  return data;
}

async function renderAccesoNFC(
  g,
  pane,
  {
    force = false
  } = {}
) {
  if (!g || !pane) {
    return;
  }

  pane.innerHTML = `
    <div class="act">
      <h4>NFC / PULSERAS</h4>

      <div class="muted">
        CARGANDO DATOS DE ACCESO…
      </div>
    </div>
  `;

  try {
    const grupoVentas =
      await buscarGrupoVentasParaNomina(
        g,
        {
          force
        }
      );

    if (!grupoVentas) {
      throw new Error(
        "NO SE ENCONTRÓ EL GRUPO CORRESPONDIENTE EN VENTAS."
      );
    }

    const usuario =
      String(
        grupoVentas.idGrupo ||
        grupoVentas.id ||
        ""
      )
        .trim()
        .replace(
          /\D/g,
          ""
        );

    const claveRaw =
      grupoVentas.numeroNegocio ??
      grupoVentas.negocio_id ??
      g.numeroNegocio ??
      "";

    const clave =
      Array.isArray(
        claveRaw
      )
        ? claveRaw
            .map(
              value =>
                String(
                  value
                ).trim()
            )
            .filter(Boolean)
            .join(" ")
        : String(
            claveRaw
          ).trim();

    const grupoNombre =
      nombreOperativoGrupo(g) ||
      g.nombreGrupo ||
      g.aliasGrupo ||
      "";

    if (!usuario) {
      throw new Error(
        "EL GRUPO NO TIENE UN ID DE ACCESO VÁLIDO."
      );
    }

    if (!clave) {
      throw new Error(
        "EL GRUPO NO TIENE NÚMERO DE NEGOCIO."
      );
    }

    const mensaje =
      construirMensajeAccesoNFC({
        grupoNombre:
          String(
            grupoNombre
          ).toUpperCase(),

        usuario,

        clave
      });

    pane.innerHTML = `
      <div class="act">
        <h4>
          LECTOR DE PULSERAS NFC
        </h4>

        <div class="meta">
          ACCESO AL SISTEMA DE FICHAS MÉDICAS
          Y CONTROL DE ASISTENCIA.
        </div>

        <div
          class="nfc-access-grid"
          style="
            display:grid;
            gap:.6rem;
            margin-top:.8rem;
          "
        >
          <div class="card">
            <div class="lab">
              USUARIO / ID GRUPO
            </div>

            <div class="nfc-credential">
              <strong id="nfcUsuario">
                ${escapePortalHTML(
                  usuario
                )}
              </strong>

              <button
                id="btnCopyNfcUsuario"
                class="btn sec"
                type="button"
              >
                COPIAR
              </button>
            </div>
          </div>

          <div class="card">
            <div class="lab">
              CLAVE / N° NEGOCIO
            </div>

            <div class="nfc-credential">
              <strong id="nfcClave">
                ${escapePortalHTML(
                  clave
                )}
              </strong>

              <button
                id="btnCopyNfcClave"
                class="btn sec"
                type="button"
              >
                COPIAR
              </button>
            </div>
          </div>
        </div>

        <div
          class="nfc-warning"
          style="margin-top:.8rem"
        >
          <strong>
            ACCESO CONFIDENCIAL
          </strong>

          <div class="meta">
            COMPARTIR SOLAMENTE CON COORDINADORES,
            PROFESORES O ADULTOS RESPONSABLES
            AUTORIZADOS DEL VIAJE.
          </div>
        </div>

        <div
          class="nfc-actions"
          style="
            display:grid;
            gap:.5rem;
            margin-top:.8rem;
          "
        >
          <a
            id="btnOpenNfc"
            class="btn ok"
            href="https://comunicaciones-raitrai.vercel.app/"
            target="_blank"
            rel="noopener"
          >
            ABRIR LECTOR NFC
          </a>

          <button
            id="btnCopyNfcCompleto"
            class="btn sec"
            type="button"
          >
            COPIAR ACCESO COMPLETO
          </button>

          <button
            id="btnShareNfc"
            class="btn sec"
            type="button"
          >
            COMPARTIR ACCESO
          </button>

          <button
            id="btnRefreshNfc"
            class="btn sec"
            type="button"
          >
            ACTUALIZAR DATOS
          </button>
        </div>
      </div>
    `;

    pane
      .querySelector(
        "#btnCopyNfcUsuario"
      )
      .onclick =
        () =>
          copiarTextoPortal(
            usuario,
            "USUARIO COPIADO"
          );

    pane
      .querySelector(
        "#btnCopyNfcClave"
      )
      .onclick =
        () =>
          copiarTextoPortal(
            clave,
            "CLAVE COPIADA"
          );

    pane
      .querySelector(
        "#btnCopyNfcCompleto"
      )
      .onclick =
        () =>
          copiarTextoPortal(
            mensaje,
            "ACCESO COPIADO"
          );

    pane
      .querySelector(
        "#btnShareNfc"
      )
      .onclick =
        () =>
          compartirAccesoNFC(
            mensaje
          );

    pane
      .querySelector(
        "#btnRefreshNfc"
      )
      .onclick =
        () =>
          renderAccesoNFC(
            g,
            pane,
            {
              force: true
            }
          );
  } catch (error) {
    console.error(
      "[NFC PULSERAS]",
      error
    );

    pane.innerHTML = `
      <div class="act">
        <h4>NFC / PULSERAS</h4>

        <div class="muted">
          ${escapePortalHTML(
            error.message ||
            "NO SE PUDIERON CARGAR LOS DATOS DE ACCESO."
          )}
        </div>

        <button
          id="btnRetryNfc"
          class="btn sec"
          style="
            width:100%;
            margin-top:.7rem;
          "
          type="button"
        >
          REINTENTAR
        </button>
      </div>
    `;

    pane
      .querySelector(
        "#btnRetryNfc"
      )
      .onclick =
        () =>
          renderAccesoNFC(
            g,
            pane,
            {
              force: true
            }
          );
  }
}

async function renderEncuestaCoordinador(
  g,
  pane,
  {
    force = false
  } = {}
) {
  if (!g || !pane) {
    return;
  }

  const cacheKey =
    String(g.id);

  pane.innerHTML = `
    <div class="act">
      <h4>ENCUESTA DEL VIAJE</h4>

      <div class="muted">
        CARGANDO SEGUIMIENTO…
      </div>
    </div>
  `;

  try {
    let data =
      !force
        ? state.cache
            .encuestasViaje
            .get(cacheKey)
        : null;

    if (!data) {
      data =
        await postSeguimientoEncuesta(
          g.id
        );

      state.cache
        .encuestasViaje
        .set(
          cacheKey,
          data
        );
    }

    if (data.existe !== true) {
      pane.innerHTML = `
        <div class="act">
          <h4>ENCUESTA DEL VIAJE</h4>

          <div class="muted">
            TODAVÍA NO EXISTE UNA ENCUESTA
            PROGRAMADA PARA ESTE VIAJE.
          </div>

          <button
            id="btnRefreshEncuesta"
            class="btn sec"
            style="width:100%;margin-top:.6rem"
          >
            ACTUALIZAR
          </button>
        </div>
      `;

      pane
        .querySelector(
          "#btnRefreshEncuesta"
        )
        .onclick =
          () =>
            renderEncuestaCoordinador(
              g,
              pane,
              {
                force: true
              }
            );

      return;
    }

    const encuesta =
      data.encuesta || {};

    const seguimiento =
      data.seguimiento || {};

    const respondieron =
      Array.isArray(
        seguimiento.respondieronLista
      )
        ? seguimiento.respondieronLista
        : [];

    const pendientes =
      Array.isArray(
        seguimiento.pendientesLista
      )
        ? seguimiento.pendientesLista
        : [];

    const preguntas =
      encuesta.preguntas || {};

    const tieneLink =
      !!String(
        encuesta.linkPublico ||
        ""
      ).trim();

    pane.innerHTML = `
      <div class="act">
        <h4>ENCUESTA DEL VIAJE</h4>

        <div class="grid-mini">
          <div class="lab">
            ESTADO
          </div>

          <div>
            <strong>
              ${escapePortalHTML(
                encuesta.estadoEfectivo ||
                encuesta.estado ||
                "—"
              )}
            </strong>
          </div>

          <div class="lab">
            APERTURA
          </div>

          <div>
            ${fechaHoraPortal(
              encuesta.disponibleDesde
            )}
          </div>

          <div class="lab">
            CIERRE
          </div>

          <div>
            ${fechaHoraPortal(
              encuesta.disponibleHasta
            )}
          </div>
        </div>

        <div
          class="survey-kpis"
          style="
            display:grid;
            grid-template-columns:
              repeat(2,minmax(0,1fr));
            gap:.5rem;
            margin-top:.8rem;
          "
        >
          <div class="card">
            <div class="lab">
              HABILITADOS
            </div>

            <strong>
              ${Number(
                seguimiento.total || 0
              )}
            </strong>
          </div>

          <div class="card">
            <div class="lab">
              RESPONDIERON
            </div>

            <strong>
              ${Number(
                seguimiento.respondieron ||
                0
              )}
            </strong>
          </div>

          <div class="card">
            <div class="lab">
              PENDIENTES
            </div>

            <strong>
              ${Number(
                seguimiento.pendientes ||
                0
              )}
            </strong>
          </div>

          <div class="card">
            <div class="lab">
              AVANCE
            </div>

            <strong>
              ${Number(
                seguimiento.porcentaje ||
                0
              )}%
            </strong>
          </div>
        </div>

        <div
          class="rowflex"
          style="
            gap:.5rem;
            margin-top:.7rem;
            flex-wrap:wrap;
          "
        >
          <button
            id="btnRefreshEncuesta"
            class="btn sec"
          >
            ACTUALIZAR
          </button>

          ${
            tieneLink
              ? `
                <a
                  id="btnOpenEncuesta"
                  class="btn sec"
                  href="${escapePortalHTML(
                    encuesta.linkPublico
                  )}"
                  target="_blank"
                  rel="noopener"
                >
                  ABRIR ENCUESTA
                </a>

                <button
                  id="btnCopyEncuesta"
                  class="btn ok"
                >
                  COPIAR ENLACE
                </button>

                <button
                  id="btnQrEncuesta"
                  class="btn sec"
                >
                  MOSTRAR QR
                </button>
              `
              : ""
          }
        </div>

        ${
          tieneLink
            ? `
              <div
                id="encuestaQrBox"
                class="encuesta-qr-box"
                style="display:none"
              >
                <div id="encuestaQr"></div>

                <div class="meta muted">
                  ESCANEA ESTE CÓDIGO
                  DESDE OTRO CELULAR
                </div>
              </div>
            `
            : ""
        }
      </div>

      ${
        encuesta.comentarioOperativo
          ? `
            <div class="act">
              <h4>
                COMENTARIO OPERATIVO
              </h4>

              <div
                class="meta"
                style="white-space:pre-wrap"
              >
                ${escapePortalHTML(
                  encuesta.comentarioOperativo
                )}
              </div>
            </div>
          `
          : ""
      }

      <div class="act">
        <h4>SEGUIMIENTO</h4>

        <div
          class="rowflex"
          style="
            gap:.5rem;
            flex-wrap:wrap;
          "
        >
          <button
            id="btnEncuestaPendientes"
            class="btn warn"
          >
            PENDIENTES
            (${pendientes.length})
          </button>

          <button
            id="btnEncuestaRespondieron"
            class="btn sec"
          >
            RESPONDIERON
            (${respondieron.length})
          </button>
        </div>

        <div
          id="encuestaPersonas"
          style="
            display:grid;
            gap:.45rem;
            margin-top:.7rem;
          "
        ></div>
      </div>

      <div class="act">
        <h4>
          PREGUNTAS PROGRAMADAS
        </h4>

        <div class="grid-mini">
          <div class="lab">
            GENERALES
          </div>

          <div>
            ${Number(
              preguntas.generales || 0
            )}
          </div>

          <div class="lab">
            ACTIVIDADES
          </div>

          <div>
            ${Number(
              preguntas.actividades ||
              0
            )}
          </div>

          <div class="lab">
            HOTELES
          </div>

          <div>
            ${Number(
              preguntas.hoteles || 0
            )}
          </div>

          <div class="lab">
            BUSES EN DESTINO
          </div>

          <div>
            ${Number(
              preguntas.transportes ||
              0
            )}
          </div>

          <div class="lab">
            COORDINADORES
          </div>

          <div>
            ${Number(
              preguntas.coordinadores ||
              0
            )}
          </div>

          <div class="lab">
            ORGANIZACIÓN
          </div>

          <div>
            ${Number(
              preguntas.organizacion ||
              0
            )}
          </div>

          <div class="lab">
            ASISTENCIA MÉDICA
          </div>

          <div>
            ${
              preguntas.asistenciaMedica
                ? "SÍ"
                : "NO"
            }
          </div>

          <div class="lab">
            TOTAL
          </div>

          <div>
            <strong>
              ${Number(
                preguntas.total || 0
              )}
            </strong>
          </div>
        </div>
      </div>
    `;

    const personas =
      pane.querySelector(
        "#encuestaPersonas"
      );

    function pintarPersonas(
      items,
      respondidas
    ) {
      if (!items.length) {
        personas.innerHTML = `
          <div class="muted">
            SIN PERSONAS EN ESTA LISTA.
          </div>
        `;

        return;
      }

      personas.innerHTML =
        items.map(
          item => `
            <div class="card">
              <div>
                <strong>
                  ${escapePortalHTML(
                    item.nombre
                  )}
                </strong>
              </div>

              <div class="meta muted">
                ${etiquetaTipoPasajero(
                  item.tipoPasajero
                )}

                ${
                  respondidas &&
                  item.respondidoEn
                    ? ` · ${fechaHoraPortal(
                        item.respondidoEn
                      )}`
                    : ""
                }
              </div>
            </div>
          `
        ).join("");
    }

    pane
      .querySelector(
        "#btnEncuestaPendientes"
      )
      .onclick =
        () =>
          pintarPersonas(
            pendientes,
            false
          );

    pane
      .querySelector(
        "#btnEncuestaRespondieron"
      )
      .onclick =
        () =>
          pintarPersonas(
            respondieron,
            true
          );

    pane
      .querySelector(
        "#btnRefreshEncuesta"
      )
      .onclick =
        () =>
          renderEncuestaCoordinador(
            g,
            pane,
            {
              force: true
            }
          );

    const btnCopy =
      pane.querySelector(
        "#btnCopyEncuesta"
      );

    if (btnCopy) {
      btnCopy.onclick =
        async () => {
          try {
            await navigator.clipboard
              .writeText(
                encuesta.linkPublico
              );

            showFlash(
              "ENLACE COPIADO",
              "ok"
            );
          } catch (_) {
            prompt(
              "COPIA EL ENLACE:",
              encuesta.linkPublico
            );
          }
        };
    }

    const btnQr =
      pane.querySelector(
        "#btnQrEncuesta"
      );

    const qrBox =
      pane.querySelector(
        "#encuestaQrBox"
      );

    const qrContainer =
      pane.querySelector(
        "#encuestaQr"
      );

    if (
      btnQr &&
      qrBox &&
      qrContainer
    ) {
      let qrCreado = false;

      btnQr.onclick =
        () => {
          const estaVisible =
            qrBox.style.display !==
            "none";

          if (estaVisible) {
            qrBox.style.display =
              "none";

            btnQr.textContent =
              "MOSTRAR QR";

            return;
          }

          qrBox.style.display =
            "grid";

          btnQr.textContent =
            "OCULTAR QR";

          if (qrCreado) {
            return;
          }

          qrContainer.innerHTML =
            "";

          if (
            typeof window.QRCode !==
            "function"
          ) {
            qrContainer.innerHTML = `
              <div class="muted">
                NO SE PUDO CARGAR
                EL GENERADOR QR.
              </div>
            `;

            return;
          }

          new window.QRCode(
            qrContainer,
            {
              text:
                encuesta.linkPublico,

              width: 220,

              height: 220,

              colorDark:
                "#151c40",

              colorLight:
                "#ffffff",

              correctLevel:
                window.QRCode
                  .CorrectLevel
                  .H
            }
          );

          qrCreado = true;
        };
    }

    // Por defecto mostramos las personas pendientes.
    pintarPersonas(
      pendientes,
      false
    );
  } catch (error) {
    console.error(
      "[ENCUESTA COORDINADOR]",
      error
    );

    pane.innerHTML = `
      <div class="act">
        <h4>ENCUESTA DEL VIAJE</h4>

        <div class="muted">
          ${escapePortalHTML(
            error.message ||
            "NO SE PUDO CARGAR."
          )}
        </div>

        <button
          id="btnRetryEncuesta"
          class="btn sec"
          style="
            width:100%;
            margin-top:.6rem;
          "
        >
          REINTENTAR
        </button>
      </div>
    `;

    pane
      .querySelector(
        "#btnRetryEncuesta"
      )
      .onclick =
        () =>
          renderEncuestaCoordinador(
            g,
            pane,
            {
              force: true
            }
          );
  }
}

async function renderDocumentosViaje(
  g,
  pane,
  {
    force =
      false
  } = {}
) {
  if (
    !g ||
    !pane
  ) {
    return;
  }

  pane.innerHTML = `
    <div class="act">
      <h4>DOCUMENTOS DEL VIAJE</h4>
      <div class="muted">
        CARGANDO DOCUMENTOS…
      </div>
    </div>
  `;

  try {
    const cacheKey =
      String(g.id);

    let documentos =
      !force
        ? state.cache
            .documentosViaje
            .get(cacheKey)
        : null;

    if (!documentos) {
      const snap =
        await getDocs(
          collection(
            db,
            "grupos",
            g.id,
            "documentosViaje"
          )
        );

      documentos =
        snap.docs
          .map(
            documento => ({
              id:
                documento.id,

              ...documento.data()
            })
          )
          .sort(
            (a, b) => {
              const fechaA =
                a.createdAt
                  ?.seconds ||
                0;

              const fechaB =
                b.createdAt
                  ?.seconds ||
                0;

              return (
                fechaB -
                fechaA
              );
            }
          );

      state.cache
        .documentosViaje
        .set(
          cacheKey,
          documentos
        );
    }

    pane.innerHTML = `
      ${
        state.is
          ? `
            <div class="act">
              <h4>
                CARGAR DOCUMENTO
              </h4>

              <div
                style="
                  display:grid;
                  gap:.5rem
                "
              >
                <select id="docTipo">
                  <option value="">
                    SELECCIONA EL TIPO
                  </option>

                  <option value="CARTA">
                    CARTA
                  </option>

                  <option value="NOMINA">
                    NÓMINA
                  </option>

                  <option value="CONTRATO">
                    CONTRATO
                  </option>

                  <option value="VOUCHER">
                    VOUCHER
                  </option>

                  <option value="INSTRUCTIVO">
                    INSTRUCTIVO
                  </option>

                  <option value="OTRO">
                    OTRO
                  </option>
                </select>

                <input
                  id="docTitulo"
                  type="text"
                  placeholder="¿A QUÉ CORRESPONDE?"
                />

                <textarea
                  id="docDescripcion"
                  placeholder="DESCRIPCIÓN OPCIONAL"
                ></textarea>

                <input
                  id="docArchivo"
                  type="file"
                  accept="application/pdf,image/*"
                />

                <button
                  id="btnSubirDocumento"
                  class="btn ok"
                >
                  SUBIR DOCUMENTO
                </button>
              </div>
            </div>
          `
          : ""
      }

      <div class="act">
        <div
          class="rowflex"
          style="
            justify-content:
              space-between;
            gap:.5rem
          "
        >
          <h4>
            DOCUMENTOS DISPONIBLES
          </h4>

          <button
            id="btnRefreshDocumentos"
            class="btn sec"
          >
            ACTUALIZAR
          </button>
        </div>

        <div
          id="documentosViajeList"
          style="
            display:grid;
            gap:.5rem
          "
        ></div>
      </div>
    `;

    const list =
      pane.querySelector(
        "#documentosViajeList"
      );

    if (
      !documentos.length
    ) {
      list.innerHTML = `
        <div class="muted">
          NO HAY DOCUMENTOS DISPONIBLES.
        </div>
      `;
    } else {
      list.innerHTML =
        documentos.map(
          documento => `
            <div class="card">
              <div>
                <strong>
                  ${escapePortalHTML(
                    documento.tipo ||
                    "DOCUMENTO"
                  )}
                </strong>
              </div>

              <div class="meta">
                ${escapePortalHTML(
                  documento.titulo ||
                  documento.nombreArchivo ||
                  ""
                )}
              </div>

              ${
                documento.descripcion
                  ? `
                    <div
                      class="meta muted"
                      style="
                        white-space:
                          pre-wrap
                      "
                    >
                      ${escapePortalHTML(
                        documento.descripcion
                      )}
                    </div>
                  `
                  : ""
              }

              <div
                class="rowflex"
                style="
                  gap:.4rem;
                  margin-top:.5rem
                "
              >
                <a
                  class="btn sec"
                  href="${escapePortalHTML(
                    documento.url ||
                    "#"
                  )}"
                  target="_blank"
                  rel="noopener"
                >
                  VER ARCHIVO
                </a>

                ${
                  state.is
                    ? `
                      <button
                        class="btn warn btnDeleteDocumento"
                        data-id="${escapePortalHTML(
                          documento.id
                        )}"
                        data-path="${escapePortalHTML(
                          documento.storagePath ||
                          ""
                        )}"
                      >
                        ELIMINAR
                      </button>
                    `
                    : ""
                }
              </div>
            </div>
          `
        ).join("");
    }

    pane
      .querySelector(
        "#btnRefreshDocumentos"
      )
      .onclick =
        () =>
          renderDocumentosViaje(
            g,
            pane,
            {
              force:
                true
            }
          );

    if (state.is) {
      const btnSubir =
        pane.querySelector(
          "#btnSubirDocumento"
        );

      btnSubir.onclick =
        async () => {
          const tipo =
            pane
              .querySelector(
                "#docTipo"
              )
              .value;

          const titulo =
            pane
              .querySelector(
                "#docTitulo"
              )
              .value
              .trim();

          const descripcion =
            pane
              .querySelector(
                "#docDescripcion"
              )
              .value
              .trim();

          const file =
            pane
              .querySelector(
                "#docArchivo"
              )
              .files[0];

          if (
            !tipo ||
            !titulo ||
            !file
          ) {
            alert(
              "TIPO, DESCRIPCIÓN DEL DOCUMENTO Y ARCHIVO SON OBLIGATORIOS."
            );

            return;
          }

          if (
            file.size >
            15 * 1024 * 1024
          ) {
            alert(
              "EL ARCHIVO SUPERA 15 MB."
            );

            return;
          }

          btnSubir.disabled =
            true;

          try {
            const documentoRef =
              doc(
                collection(
                  db,
                  "grupos",
                  g.id,
                  "documentosViaje"
                )
              );

            const safeName =
              file.name.replace(
                /[^a-z0-9._-]/gi,
                "_"
              );

            const storagePath =
              `documentos-viaje/${g.id}/${documentoRef.id}/${safeName}`;

            const archivoRef =
              sRef(
                storage,
                storagePath
              );

            await uploadBytes(
              archivoRef,
              file,
              {
                contentType:
                  file.type ||
                  "application/octet-stream"
              }
            );

            const url =
              await getDownloadURL(
                archivoRef
              );

            await setDoc(
              documentoRef,
              {
                tipo,
                titulo,
                descripcion,

                nombreArchivo:
                  file.name,

                contentType:
                  file.type ||
                  "",

                size:
                  file.size,

                url,
                storagePath,

                visibleCoordinador:
                  true,

                createdAt:
                  serverTimestamp(),

                createdBy: {
                  uid:
                    state.user.uid,

                  email:
                    (
                      state.user.email ||
                      ""
                    ).toLowerCase()
                }
              }
            );

            state.cache
              .documentosViaje
              .delete(
                String(g.id)
              );

            showFlash(
              "DOCUMENTO CARGADO",
              "ok"
            );

            await renderDocumentosViaje(
              g,
              pane,
              {
                force:
                  true
              }
            );
          } catch (error) {
            console.error(
              "[DOCUMENTOS VIAJE]",
              error
            );

            alert(
              "NO SE PUDO CARGAR EL DOCUMENTO."
            );
          } finally {
            btnSubir.disabled =
              false;
          }
        };

      pane
        .querySelectorAll(
          ".btnDeleteDocumento"
        )
        .forEach(
          boton => {
            boton.onclick =
              async () => {
                if (
                  !confirm(
                    "¿ELIMINAR ESTE DOCUMENTO?"
                  )
                ) {
                  return;
                }

                try {
                  const id =
                    boton.dataset.id;

                  const path =
                    boton.dataset.path;

                  if (path) {
                    await deleteObject(
                      sRef(
                        storage,
                        path
                      )
                    ).catch(
                      () => {}
                    );
                  }

                  await deleteDoc(
                    doc(
                      db,
                      "grupos",
                      g.id,
                      "documentosViaje",
                      id
                    )
                  );

                  state.cache
                    .documentosViaje
                    .delete(
                      String(g.id)
                    );

                  await renderDocumentosViaje(
                    g,
                    pane,
                    {
                      force:
                        true
                    }
                  );
                } catch (error) {
                  console.error(
                    error
                  );

                  alert(
                    "NO SE PUDO ELIMINAR EL DOCUMENTO."
                  );
                }
              };
          }
        );
    }
  } catch (error) {
    console.error(
      "[DOCUMENTOS VIAJE]",
      error
    );

    pane.innerHTML = `
      <div class="act">
        <h4>DOCUMENTOS DEL VIAJE</h4>

        <div class="muted">
          NO SE PUDIERON CARGAR LOS DOCUMENTOS.
        </div>
      </div>
    `;
  }
}

async function buscarGrupoVentasParaNomina(
  g,
  {
    force = false
  } = {}
) {
  if (!g) {
    return null;
  }

  const cacheKey =
    String(
      g.id ||
      g.numeroNegocio ||
      ""
    );

  if (
    !force &&
    state.cache
      .gruposVentas
      .has(cacheKey)
  ) {
    return state.cache
      .gruposVentas
      .get(cacheKey);
  }

  const numeroTexto =
    String(
      g.numeroNegocio ||
      ""
    ).trim();

  const numeroNumerico =
    Number(
      numeroTexto
    );

  if (!numeroTexto) {
    return null;
  }

  const condiciones = [
    [
      "numeroNegocio",
      numeroTexto
    ],

    [
      "negocio_id",
      numeroTexto
    ]
  ];

  if (
    Number.isFinite(
      numeroNumerico
    )
  ) {
    condiciones.push(
      [
        "numeroNegocio",
        numeroNumerico
      ],

      [
        "negocio_id",
        numeroNumerico
      ]
    );
  }

  const encontrados =
    new Map();

  for (
    const [
      campo,
      valor
    ] of condiciones
  ) {
    try {
      const snapshot =
        await getDocs(
          query(
            collection(
              db,
              "ventas_cotizaciones"
            ),

            where(
              campo,
              "==",
              valor
            ),

            limit(10)
          )
        );

      snapshot.docs.forEach(
        documento => {
          encontrados.set(
            documento.id,
            {
              id:
                documento.id,

              ...documento.data()
            }
          );
        }
      );
    } catch (error) {
      console.warn(
        "[VENTAS] Consulta no disponible",
        campo,
        valor,
        error
      );
    }
  }

  const candidatos = [
    ...encontrados.values()
  ];

  const anoViaje =
    Number(
      g.anoViaje ||
      0
    );

  const identificador =
    String(
      g.identificador ||
      ""
    ).trim();

  const grupoVentas =
    candidatos.find(
      item =>
        Number(
          item.anoViaje ||
          0
        ) ===
          anoViaje &&
        (
          !identificador ||
          String(
            item.identificador ||
            ""
          ).trim() ===
            identificador
        )
    ) ||

    candidatos.find(
      item =>
        Number(
          item.anoViaje ||
          0
        ) ===
        anoViaje
    ) ||

    candidatos[0] ||
    null;

  if (grupoVentas) {
    state.cache
      .gruposVentas
      .set(
        cacheKey,
        grupoVentas
      );
  }

  return grupoVentas;
}

async function copiarTextoPortal(
  texto,
  mensaje = "COPIADO"
) {
  const valor =
    String(
      texto ||
      ""
    ).trim();

  if (!valor) {
    return false;
  }

  try {
    await navigator.clipboard
      .writeText(valor);

    showFlash(
      mensaje,
      "ok"
    );

    return true;
  } catch (_) {
    window.prompt(
      "COPIA EL TEXTO:",
      valor
    );

    return false;
  }
}

function construirMensajeAccesoNFC({
  grupoNombre,
  usuario,
  clave
}) {
  return [
    "ACCESO COMUNICACIONES RAI TRAI",
    "",
    grupoNombre
      ? `GRUPO: ${grupoNombre}`
      : "",

    `USUARIO / ID GRUPO: ${usuario}`,
    `CLAVE / N° NEGOCIO: ${clave}`,
    "",
    "INGRESAR EN:",
    "https://comunicaciones-raitrai.vercel.app/",
    "",
    "ACCESO CONFIDENCIAL. COMPARTIR SOLO CON ADULTOS RESPONSABLES AUTORIZADOS DEL VIAJE."
  ]
    .filter(
      linea =>
        linea !== null &&
        linea !== undefined
    )
    .join("\n");
}

async function compartirAccesoNFC(
  mensaje
) {
  if (
    navigator.share
  ) {
    try {
      await navigator.share({
        title:
          "ACCESO COMUNICACIONES RAI TRAI",

        text:
          mensaje
      });

      return;
    } catch (error) {
      if (
        error?.name ===
        "AbortError"
      ) {
        return;
      }
    }
  }

  const whatsappUrl =
    `https://wa.me/?text=${encodeURIComponent(
      mensaje
    )}`;

  window.open(
    whatsappUrl,
    "_blank",
    "noopener"
  );
}

async function renderNominaCoordinador(
  g,
  pane,
  {
    force =
      false
  } = {}
) {
  if (
    !g ||
    !pane
  ) {
    return;
  }

  pane.innerHTML = `
    <div class="act">
      <h4>NÓMINA DEL GRUPO</h4>
      <div class="muted">
        CARGANDO NÓMINA…
      </div>
    </div>
  `;

  try {
    const cacheKey =
      String(g.id);

    let data =
      !force
        ? state.cache
            .nominasViaje
            .get(cacheKey)
        : null;

    if (!data) {
      const grupoVentas =
        await buscarGrupoVentasParaNomina(
          g
        );

      if (!grupoVentas) {
        throw new Error(
          "NO SE ENCONTRÓ EL GRUPO EN VENTAS."
        );
      }

      const nominaSnap =
        await getDocs(
          query(
            collection(
              db,
              "nominas_publicas"
            ),
            where(
              "groupDocId",
              "==",
              grupoVentas.id
            )
          )
        );

      const documentos =
        nominaSnap.docs
          .map(
            documento => ({
              token:
                documento.id,

              ...documento.data()
            })
          )
          .filter(
            item =>
              item.activo !==
              false
          );

      const nomina =
        documentos.find(
          item =>
            item.esTokenPrincipal ===
            true
        ) ||
        documentos[0] ||
        null;

      data = {
        existe:
          !!nomina,

        nomina
      };

      state.cache
        .nominasViaje
        .set(
          cacheKey,
          data
        );
    }

    if (
      !data.existe ||
      !data.nomina
    ) {
      pane.innerHTML = `
        <div class="act">
          <h4>NÓMINA DEL GRUPO</h4>

          <div class="muted">
            TODAVÍA NO EXISTE UNA NÓMINA
            PÚBLICA DISPONIBLE PARA ESTE GRUPO.
          </div>

          <button
            id="btnRefreshNomina"
            class="btn sec"
            style="width:100%;margin-top:.6rem"
          >
            ACTUALIZAR
          </button>
        </div>
      `;

      pane
        .querySelector(
          "#btnRefreshNomina"
        )
        .onclick =
          () =>
            renderNominaCoordinador(
              g,
              pane,
              {
                force:
                  true
              }
            );

      return;
    }

    const nomina =
      data.nomina;

    const pasajeros =
      Array.isArray(
        nomina.pasajeros
      )
        ? nomina.pasajeros
            .filter(
              item =>
                item?.nombre
            )
            .sort(
              (a, b) =>
                String(
                  a.nombre
                ).localeCompare(
                  String(
                    b.nombre
                  ),
                  "es",
                  {
                    sensitivity:
                      "base"
                  }
                )
            )
        : [];

    const resumen =
      nomina.resumen ||
      {};

    pane.innerHTML = `
      <div class="act">
        <div
          class="rowflex"
          style="
            justify-content:
              space-between;
            gap:.5rem
          "
        >
          <h4>
            NÓMINA DEL GRUPO
          </h4>

          <button
            id="btnRefreshNomina"
            class="btn sec"
          >
            ACTUALIZAR
          </button>
        </div>

        <div class="grid-mini">
          <div class="lab">
            VIAJAN
          </div>

          <div>
            <strong>
              ${Number(
                resumen.viajan ||
                pasajeros.length ||
                0
              )}
            </strong>
          </div>

          <div class="lab">
            FICHAS PENDIENTES
          </div>

          <div>
            ${Number(
              resumen.fichasPendientes ||
              0
            )}
          </div>

          <div class="lab">
            LISTA DE ESPERA
          </div>

          <div>
            ${Number(
              resumen.listaEsperaPendiente ||
              0
            )}
          </div>
        </div>
      </div>

      <div class="act">
        <h4>
          INTEGRANTES
          (${pasajeros.length})
        </h4>

        <div
          style="
            display:grid;
            gap:.4rem
          "
        >
          ${
            pasajeros.length
              ? pasajeros.map(
                  pasajero => `
                    <div class="card">
                      <strong>
                        ${escapePortalHTML(
                          pasajero.nombre
                        )}
                      </strong>

                      ${
                        pasajero.tipo
                          ? `
                            <div class="meta muted">
                              ${escapePortalHTML(
                                pasajero.tipo
                              )}
                            </div>
                          `
                          : ""
                      }
                    </div>
                  `
                ).join("")
              : `
                <div class="muted">
                  SIN INTEGRANTES DISPONIBLES.
                </div>
              `
          }
        </div>
      </div>
    `;

    pane
      .querySelector(
        "#btnRefreshNomina"
      )
      .onclick =
        () =>
          renderNominaCoordinador(
            g,
            pane,
            {
              force:
                true
            }
          );
  } catch (error) {
    console.error(
      "[NÓMINA COORDINADOR]",
      error
    );

    pane.innerHTML = `
      <div class="act">
        <h4>NÓMINA DEL GRUPO</h4>

        <div class="muted">
          ${escapePortalHTML(
            error.message ||
            "NO SE PUDO CARGAR."
          )}
        </div>
      </div>
    `;
  }
}

/* ====== VISTA GRUPO ====== */
async function renderOneGroup(
  g,
  preferDate
) {
  const cont =
    document.getElementById(
      'grupos'
    );

  if (!cont) {
    return;
  }

  cont.innerHTML = '';

  if (!g) {
    cont.innerHTML =
      `
        <p class="muted">
          NO HAY VIAJES.
        </p>
      `;

    return;
  }

  g =
    await ensureGrupoDetalleLoaded(
      g
    );

  reemplazarGrupoEnEstado(g);
  localStorage.setItem('rt_last_group', g.id);

  const name =
   nombreOperativoGrupo(g);
  const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
  const rango = `${dmy(g.fechaInicio||'')} — ${dmy(g.fechaFin||'')}`;

  /* ——— VIAJE / PAX REAL ——— */
  const paxPlan = paxOf(g);
  const real = paxRealOf(g);
  const { A: A_real, E: E_real } = paxBreakdown(g);
  const isStartDay = isToday(g.fechaInicio);
  const viaje = g.viaje || {};
  const viajeEstado = viaje.estado || (viaje.fin?.at ? 'FINALIZADO' : (viaje.inicio?.at ? 'EN_CURSO' : 'PENDIENTE'));
  const started  = (viajeEstado === 'EN_CURSO') || !!viaje.inicio?.at;
  const finished = (viajeEstado === 'FINALIZADO') || !!viaje.fin?.at;

  const header = document.createElement('div');
  header.className = 'group-card';
   
  const topInfo = `
    <h3>${(name||'').toUpperCase()} · CÓDIGO: (${code})</h3>
    <div class="grid-mini">
      <div class="lab">DESTINO</div><div>${(g.destino||'—').toUpperCase()}</div>
      <div class="lab">GRUPO</div><div>${(name||'').toUpperCase()}</div>
      <div class="lab">PAX TOTAL</div>
      <div>${fmtPaxPlan(paxPlan, g)}${real ? ` <span class="muted">(A:${A_real} · E:${E_real})</span>` : ''}</div>
      <div class="lab">PROGRAMA</div><div>${(g.programa||'—').toUpperCase()}</div>
      <div class="lab">FECHAS</div><div>${rango}</div>
    </div>
   
    <div class="rowflex" style="margin-top:.6rem;gap:.5rem;flex-wrap:wrap">
      <input id="searchTrips" type="text" placeholder="BUSCADOR EN RESUMEN, ITINERARIO Y GASTOS..." style="flex:1"/>
    </div>
  `;
   
  // Botón INICIO (full width, verde)
  const btnInicioHtml = (!started)
    ? `<button id="btnInicioViaje" class="btn ok" style="width:100%;"${isStartDay ? '' : ' title="No es el día de inicio. Se pedirá confirmación."'}>INICIO DE VIAJE</button>`
    : '';
  
  // Botón RESTABLECER (solo STAFF) – gris, full width, debajo del inicio
  const btnResetInicioHtml = state.is
    ? `<button id="btnResetInicio" class="btn" style="width:100%;background:#64748b;color:#fff;">RESTABLECER VIAJE COMPLETO</button>`
    : '';

   
  // Botón TERMINAR (si está en curso)
  const btnTerminarHtml = (started && !finished)
    ? `<button id="btnTerminoViaje" class="btn warn" style="width:100%;">TERMINAR VIAJE</button>`
    : '';
   
  // Info finalizado + botón reabrir cierre (STAFF)
  const finHtml = (finished)
    ? `
      <div class="muted">VIAJE FINALIZADO${viaje?.fin?.rendicionOk ? ' · RENDICIÓN HECHA' : ''}${viaje?.fin?.boletaOk ? ' · BOLETA ENTREGADA' : ''}</div>
      ${state.is ? `<button id="btnReabrirCierre" class="btn sec">RESTABLECER CIERRE</button>` : ''}`
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
  if (btnR0) btnR0.onclick = async () => { await resetViajeCompleto(g); };

  const histBox = header.querySelector('#viajeHistoryBox');
  renderViajeHistory(g, histBox);

  const tabs =
    document.createElement(
      "div"
    );
  
  tabs.innerHTML = `
    <div class="group-tabs">
      <button
        id="tabResumen"
        class="btn sec"
      >
        RESUMEN
      </button>
  
      <button
        id="tabNomina"
        class="btn sec"
      >
        NÓMINA
      </button>
  
      <button
        id="tabDocs"
        class="btn sec"
      >
        DOCUMENTOS
      </button>
  
      <button
        id="tabItin"
        class="btn sec"
      >
        ITINERARIO
      </button>
  
      <button
        id="tabNfc"
        class="btn sec"
      >
        NFC-PULSERAS
      </button>
  
      <button
        id="tabEncuesta"
        class="btn sec"
      >
        ENCUESTA
      </button>
  
      <button
        id="tabFin"
        class="btn sec"
      >
        FINANZAS
      </button>
    </div>
  
    <div id="paneResumen"></div>
  
    <div
      id="paneNomina"
      style="display:none"
    ></div>
  
    <div
      id="paneDocs"
      style="display:none"
    ></div>
  
    <div
      id="paneItin"
      style="display:none"
    ></div>
  
    <div
      id="paneNfc"
      style="display:none"
    ></div>
  
    <div
      id="paneEncuesta"
      style="display:none"
    ></div>
  
    <div
      id="paneFin"
      style="display:none"
    ></div>
  `;
  
  cont.appendChild(
    tabs
  );
  
  const panes = {
    resumen:
      tabs.querySelector(
        "#paneResumen"
      ),
  
    nomina:
      tabs.querySelector(
        "#paneNomina"
      ),
  
    docs:
      tabs.querySelector(
        "#paneDocs"
      ),
  
    itin:
      tabs.querySelector(
        "#paneItin"
      ),
  
    nfc:
      tabs.querySelector(
        "#paneNfc"
      ),
  
    encuesta:
      tabs.querySelector(
        "#paneEncuesta"
      ),
  
    fin:
      tabs.querySelector(
        "#paneFin"
      )
  };
  
  const buttons = {
    resumen:
      tabs.querySelector(
        "#tabResumen"
      ),
  
    nomina:
      tabs.querySelector(
        "#tabNomina"
      ),
  
    docs:
      tabs.querySelector(
        "#tabDocs"
      ),
  
    itin:
      tabs.querySelector(
        "#tabItin"
      ),
  
    nfc:
      tabs.querySelector(
        "#tabNfc"
      ),
  
    encuesta:
      tabs.querySelector(
        "#tabEncuesta"
      ),
  
    fin:
      tabs.querySelector(
        "#tabFin"
      )
  };
  
  // Alias para conservar el código existente.
  const paneResumen =
    panes.resumen;
  
  const paneItin =
    panes.itin;
  
  const paneFin =
    panes.fin;
  
  const paneDocs =
    panes.docs;
  
  const paneNomina =
    panes.nomina;
  
  const paneNfc =
    panes.nfc;
  
  const paneEncuesta =
    panes.encuesta;
  
  const btnResumen =
    buttons.resumen;
  
  const btnItin =
    buttons.itin;
  
  const btnFin =
    buttons.fin;
  
  const btnDocs =
    buttons.docs;
  
  const btnNomina =
    buttons.nomina;
  
  const btnNfc =
    buttons.nfc;
  
  const btnEncuesta =
    buttons.encuesta;
  
  const lazyLoaded = {
    docs: false,
    nomina: false,
    nfc: false,
    encuesta: false
  };
  
  const setTabLabel =
    (
      btn,
      base,
      n
    ) => {
      const q =
        String(
          state.groupQ ||
          ""
        ).trim();
  
      btn.textContent =
        q && n > 0
          ? `${base} (${n})`
          : base;
    };
  
  const show =
    async tabName => {
      const selected =
        tabName ||
        "resumen";
  
      state.lastTab =
        selected;
  
      Object.entries(
        panes
      ).forEach(
        ([
          nombre,
          panel
        ]) => {
          panel.style.display =
            nombre === selected
              ? ""
              : "none";
        }
      );
  
      Object.entries(
        buttons
      ).forEach(
        ([
          nombre,
          button
        ]) => {
          button.classList.toggle(
            "active",
            nombre === selected
          );
        }
      );
  
      if (
        selected === "docs" &&
        !lazyLoaded.docs
      ) {
        lazyLoaded.docs = true;
  
        await renderDocumentosViaje(
          g,
          paneDocs
        );
      }
  
      if (
        selected === "nomina" &&
        !lazyLoaded.nomina
      ) {
        lazyLoaded.nomina = true;
  
        await renderNominaCoordinador(
          g,
          paneNomina
        );
      }
  
      if (
        selected === "nfc" &&
        !lazyLoaded.nfc
      ) {
        lazyLoaded.nfc = true;
  
        await renderAccesoNFC(
          g,
          paneNfc
        );
      }
  
      if (
        selected === "encuesta" &&
        !lazyLoaded.encuesta
      ) {
        lazyLoaded.encuesta = true;
  
        await renderEncuestaCoordinador(
          g,
          paneEncuesta
        );
      }
    };
  
  Object.entries(
    buttons
  ).forEach(
    ([
      nombre,
      button
    ]) => {
      button.onclick =
        () =>
          show(nombre);
    }
  );

  // Render inicial de las pestañas principales.
  const resumenHits =
    await renderResumen(
      g,
      paneResumen
    );
  
  const itinHits =
    renderItinerario(
      g,
      paneItin,
      preferDate
    );
  
  const finHits =
    await renderFinanzas(
      g,
      paneFin
    );
  
  // Contadores del buscador.
  setTabLabel(
    btnResumen,
    "RESUMEN",
    resumenHits
  );
  
  setTabLabel(
    btnItin,
    "ITINERARIO",
    itinHits
  );
  
  setTabLabel(
    btnFin,
    "FINANZAS",
    finHits
  );
  // si viene desde una fecha (ej. click en día), priorizamos ITINERARIO
  // si no, usamos la última pestaña usada; fallback: RESUMEN
  const initialTab = preferDate ? 'itin' : (state.lastTab || 'resumen');
  await show(
    initialTab
  );

  // BÚSQUEDA INTERNA
  const input=header.querySelector('#searchTrips');
  input.value = state.groupQ || '';
  let tmr=null;
  input.oninput=()=>{ clearTimeout(tmr); tmr=setTimeout(async ()=>{
    state.groupQ=input.value||'';
    const active = state.lastTab || 'resumen';
   
    const r = await renderResumen(g, paneResumen);
    const i = renderItinerario(g, paneItin, localStorage.getItem('rt_last_date_'+g.id) || preferDate);
    const f = await renderFinanzas(g, paneFin);
   
    setTabLabel(btnResumen, 'RESUMEN', r);
    setTabLabel(btnItin,    'ITINERARIO', i);
    setTabLabel(btnFin,     'FINANZAS', f);
   
    show(active);
  
  },180); };

ensurePrintDOM();
await preparePrintForGroup(g);
}

async function renderViajeHistory(g, box){
  try{
    const qs = await getDocs(query(
      collection(db,'grupos',g.id,'viajeLog'),
      orderBy('ts','desc'), limit(50)
    ));
    const head = '<h4>HISTORIAL DEL VIAJE</h4>';
    if(!qs.size){ box.innerHTML = head + '<div class="muted">SIN REGISTROS.</div>'; return; }

    const frag = document.createDocumentFragment();
    const ttl  = document.createElement('h4'); ttl.textContent = 'HISTORIAL DEL VIAJE'; frag.appendChild(ttl);

    qs.forEach(d=>{
      const x = d.data()||{};
      const quien  = (x.by || x.byEmail || x.byUid || '').toString().toUpperCase();
      const cuando = x.ts?.seconds ? fmtChile(new Date(x.ts.seconds*1000)) : '';
      const accion = (x.type||'').toString().replace(/_/g,' ').toUpperCase();
      const txt    = (x.text||'').toString().toUpperCase();
      const div = document.createElement('div'); div.className='meta';
      div.textContent = `• ${accion}${txt?` — ${txt}`:''} — ${quien}${cuando?` · ${cuando}`:''}`;
      frag.appendChild(div);
    });
    box.innerHTML=''; box.appendChild(frag);
  }catch(e){
    console.error(e);
    box.innerHTML = '<h4>HISTORIAL DEL VIAJE</h4><div class="muted">NO SE PUDO CARGAR.</div>';
  }
}

async function reloadGroupAndRender(groupId){
  try{
    const snap = await getDoc(doc(db,'grupos', groupId));
    if (!snap.exists()) { await renderOneGroup(null); return; }
    const raw = { id: snap.id, ...snap.data() };

    const g2 = {
      ...raw,
      fechaInicio: toISO(raw.fechaInicio||raw.inicio||raw.fecha_ini),
      fechaFin:    toISO(raw.fechaFin||raw.fin||raw.fecha_fin),
      itinerario:  normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
      serviciosEstado: raw.serviciosEstado || {},
      numeroNegocio: String(raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || snap.id),
      identificador:  String(raw.identificador || raw.codigo || '')
    };

    // 👇 AÑADIR: si el campo 'itinerario' viene vacío, carga desde la subcolección
    await ensureItinerarioLoaded(g2);

    // Refresca en state (por si cambias de viaje luego)
    const idx = state.ordenados.findIndex(x => x && x.id === g2.id);
    if (idx >= 0) {
      state.ordenados[idx] = g2;
      const j = state.grupos.findIndex(x => x && x.id === g2.id);
      if (j >= 0) state.grupos[j] = g2;
      state.idx = idx;
    }

    await renderOneGroup(g2);
  }catch(e){
    console.error('reloadGroupAndRender', e);
    await renderOneGroup(state.ordenados[state.idx] || null);
  }
}

/* =========================================================
   INFORMACIÓN ADICIONAL DEL GRUPO
   Solo STAFF puede crear, editar o eliminar.
   Coordinadores pueden verla.
========================================================= */

function obtenerFechaMsInformacionAdicional(valor){
  if (!valor) return 0;

  if (typeof valor.toMillis === 'function'){
    return valor.toMillis();
  }

  if (valor.seconds){
    return valor.seconds * 1000;
  }

  if (valor instanceof Date){
    return valor.getTime();
  }

  const fecha = new Date(valor);
  return Number.isNaN(fecha.getTime())
    ? 0
    : fecha.getTime();
}


function normalizarAsuntoInformacionAdicional(valor = ''){
  return String(valor || '')
    .trim()
    .replace(/:+\s*$/, '');
}


async function cargarInformacionAdicionalGrupo(g){
  if (!g?.id){
    return [];
  }

  const snap = await getDocs(
    collection(
      db,
      'grupos',
      String(g.id),
      'informacionAdicional'
    )
  );

  const registros = [];

  snap.forEach(documento => {
    const datos = documento.data() || {};

    registros.push({
      id: documento.id,
      ...datos
    });
  });

  registros.sort((a, b) => {
    const ordenA = Number(a.orden ?? 999999);
    const ordenB = Number(b.orden ?? 999999);

    if (ordenA !== ordenB){
      return ordenA - ordenB;
    }

    const fechaA =
      obtenerFechaMsInformacionAdicional(
        a.createdAt || a.updatedAt
      );

    const fechaB =
      obtenerFechaMsInformacionAdicional(
        b.createdAt || b.updatedAt
      );

    return fechaA - fechaB;
  });

  return registros;
}


function abrirFormularioInformacionAdicional({
  titulo = 'AGREGAR INFORMACIÓN',
  asunto = '',
  informacion = ''
} = {}){
  return new Promise(resolve => {
    const modalAnterior =
      document.getElementById(
        'modalInformacionAdicional'
      );

    if (modalAnterior){
      modalAnterior.remove();
    }

    const overlay =
      document.createElement('div');

    overlay.id =
      'modalInformacionAdicional';

    overlay.style.cssText = `
      position: fixed;
      inset: 0;
      z-index: 10000;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 16px;
      background: rgba(15, 23, 42, .62);
    `;

    const dialogo =
      document.createElement('div');

    dialogo.style.cssText = `
      width: min(560px, 100%);
      max-height: calc(100vh - 32px);
      overflow-y: auto;
      background: #ffffff;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 24px 70px rgba(0, 0, 0, .28);
    `;

    const encabezado =
      document.createElement('h3');

    encabezado.textContent = titulo;

    encabezado.style.cssText = `
      margin: 0 0 16px;
      color: #0f2d66;
      font-size: 1rem;
    `;

    const labelAsunto =
      document.createElement('label');

    labelAsunto.textContent = 'ASUNTO';

    labelAsunto.style.cssText = `
      display: block;
      margin-bottom: 6px;
      font-weight: 800;
      color: #0f172a;
    `;

    const inputAsunto =
      document.createElement('input');

    inputAsunto.type = 'text';
    inputAsunto.value = asunto;
    inputAsunto.placeholder =
      'EJ.: NOMBRE DE CONDUCTORES';

    inputAsunto.maxLength = 120;

    inputAsunto.style.cssText = `
      width: 100%;
      box-sizing: border-box;
      margin-bottom: 14px;
      padding: 10px 12px;
      border: 1px solid #cbd5e1;
      border-radius: 9px;
      font: inherit;
      text-transform: uppercase;
    `;

    const labelInformacion =
      document.createElement('label');

    labelInformacion.textContent = 'INFORMACIÓN';

    labelInformacion.style.cssText = `
      display: block;
      margin-bottom: 6px;
      font-weight: 800;
      color: #0f172a;
    `;

    const textareaInformacion =
      document.createElement('textarea');

    textareaInformacion.value = informacion;
    textareaInformacion.placeholder =
      'EJ.: PEDRO MARTÍNEZ E IGNACIO GONZÁLEZ';

    textareaInformacion.rows = 5;
    textareaInformacion.maxLength = 2000;

    textareaInformacion.style.cssText = `
      width: 100%;
      min-height: 110px;
      box-sizing: border-box;
      resize: vertical;
      padding: 10px 12px;
      border: 1px solid #cbd5e1;
      border-radius: 9px;
      font: inherit;
      text-transform: uppercase;
    `;

    const mensaje =
      document.createElement('div');

    mensaje.style.cssText = `
      display: none;
      margin-top: 10px;
      color: #b91c1c;
      font-weight: 700;
      font-size: .85rem;
    `;

    const botones =
      document.createElement('div');

    botones.style.cssText = `
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 16px;
      flex-wrap: wrap;
    `;

    const btnCancelar =
      document.createElement('button');

    btnCancelar.type = 'button';
    btnCancelar.className = 'btn sec';
    btnCancelar.textContent = 'CANCELAR';

    const btnGuardar =
      document.createElement('button');

    btnGuardar.type = 'button';
    btnGuardar.className = 'btn ok';
    btnGuardar.textContent = 'GUARDAR';

    botones.append(
      btnCancelar,
      btnGuardar
    );

    dialogo.append(
      encabezado,
      labelAsunto,
      inputAsunto,
      labelInformacion,
      textareaInformacion,
      mensaje,
      botones
    );

    overlay.appendChild(dialogo);
    document.body.appendChild(overlay);

    let cerrado = false;

    const cerrar = resultado => {
      if (cerrado){
        return;
      }

      cerrado = true;
      document.removeEventListener(
        'keydown',
        cerrarConEscape
      );

      overlay.remove();
      resolve(resultado);
    };

    const cerrarConEscape = evento => {
      if (evento.key === 'Escape'){
        cerrar(null);
      }
    };

    btnCancelar.onclick = () => {
      cerrar(null);
    };

    overlay.onclick = evento => {
      if (evento.target === overlay){
        cerrar(null);
      }
    };

    btnGuardar.onclick = () => {
      const asuntoLimpio =
        normalizarAsuntoInformacionAdicional(
          inputAsunto.value
        );

      const informacionLimpia =
        String(
          textareaInformacion.value || ''
        ).trim();

      if (!asuntoLimpio){
        mensaje.textContent =
          'DEBES INGRESAR EL ASUNTO.';

        mensaje.style.display = 'block';
        inputAsunto.focus();
        return;
      }

      if (!informacionLimpia){
        mensaje.textContent =
          'DEBES INGRESAR LA INFORMACIÓN.';

        mensaje.style.display = 'block';
        textareaInformacion.focus();
        return;
      }

      cerrar({
        asunto: asuntoLimpio,
        informacion: informacionLimpia
      });
    };

    document.addEventListener(
      'keydown',
      cerrarConEscape
    );

    setTimeout(
      () => inputAsunto.focus(),
      0
    );
  });
}


async function agregarInformacionAdicionalGrupo(
  g,
  contenedor
){
  if (!state.is){
    alert(
      'SOLO EL STAFF PUEDE AGREGAR INFORMACIÓN.'
    );

    return;
  }

  const resultado =
    await abrirFormularioInformacionAdicional({
      titulo: 'AGREGAR INFORMACIÓN ADICIONAL'
    });

  if (!resultado){
    return;
  }

  try{
    await addDoc(
      collection(
        db,
        'grupos',
        String(g.id),
        'informacionAdicional'
      ),
      {
        asunto: resultado.asunto,
        informacion: resultado.informacion,

        createdAt: serverTimestamp(),
        updatedAt: serverTimestamp(),

        createdByUid:
          state.user?.uid || '',

        createdByEmail:
          String(
            state.user?.email || ''
          ).toLowerCase()
      }
    );

    showFlash(
      'INFORMACIÓN AGREGADA',
      'ok'
    );

    await renderInformacionAdicional(
      g,
      contenedor
    );
  }catch(error){
    console.error(
      'agregarInformacionAdicionalGrupo',
      error
    );

    alert(
      'NO SE PUDO GUARDAR LA INFORMACIÓN ADICIONAL.'
    );
  }
}


async function editarInformacionAdicionalGrupo(
  g,
  registro,
  contenedor
){
  if (!state.is){
    alert(
      'SOLO EL STAFF PUEDE EDITAR INFORMACIÓN.'
    );

    return;
  }

  const resultado =
    await abrirFormularioInformacionAdicional({
      titulo: 'EDITAR INFORMACIÓN ADICIONAL',
      asunto: registro.asunto || '',
      informacion: registro.informacion || ''
    });

  if (!resultado){
    return;
  }

  try{
    await updateDoc(
      doc(
        db,
        'grupos',
        String(g.id),
        'informacionAdicional',
        String(registro.id)
      ),
      {
        asunto: resultado.asunto,
        informacion: resultado.informacion,

        updatedAt: serverTimestamp(),

        updatedByUid:
          state.user?.uid || '',

        updatedByEmail:
          String(
            state.user?.email || ''
          ).toLowerCase()
      }
    );

    showFlash(
      'INFORMACIÓN ACTUALIZADA',
      'ok'
    );

    await renderInformacionAdicional(
      g,
      contenedor
    );
  }catch(error){
    console.error(
      'editarInformacionAdicionalGrupo',
      error
    );

    alert(
      'NO SE PUDO ACTUALIZAR LA INFORMACIÓN.'
    );
  }
}


async function eliminarInformacionAdicionalGrupo(
  g,
  registro,
  contenedor
){
  if (!state.is){
    alert(
      'SOLO EL STAFF PUEDE ELIMINAR INFORMACIÓN.'
    );

    return;
  }

  const asunto =
    normalizarAsuntoInformacionAdicional(
      registro.asunto || 'ESTE REGISTRO'
    ).toUpperCase();

  const confirmado =
    confirm(
      `¿ELIMINAR "${asunto}"?\n\nESTA ACCIÓN NO SE PUEDE DESHACER.`
    );

  if (!confirmado){
    return;
  }

  try{
    await deleteDoc(
      doc(
        db,
        'grupos',
        String(g.id),
        'informacionAdicional',
        String(registro.id)
      )
    );

    showFlash(
      'INFORMACIÓN ELIMINADA',
      'ok'
    );

    await renderInformacionAdicional(
      g,
      contenedor
    );
  }catch(error){
    console.error(
      'eliminarInformacionAdicionalGrupo',
      error
    );

    alert(
      'NO SE PUDO ELIMINAR LA INFORMACIÓN.'
    );
  }
}


async function renderInformacionAdicional(
  g,
  contenedor
){
  if (!contenedor){
    return 0;
  }

  contenedor.style.display = '';
  contenedor.innerHTML = `
    <h4>INFORMACIÓN ADICIONAL</h4>
    <div class="muted">BUSCANDO…</div>
  `;

  try{
    const registros =
      await cargarInformacionAdicionalGrupo(g);

    const busqueda =
      norm(
        String(
          state.groupQ || ''
        ).trim()
      );

    const registrosFiltrados =
      busqueda
        ? registros.filter(registro => {
            const texto =
              norm([
                registro.asunto || '',
                registro.informacion || ''
              ].join(' '));

            return texto.includes(busqueda);
          })
        : registros;

    /*
      Para coordinadores:
      si no hay información, el bloque no aparece.

      Para STAFF:
      el bloque aparece siempre para poder mostrar
      el botón AGREGAR INFORMACIÓN.
    */
    if (
      !state.is &&
      registros.length === 0
    ){
      contenedor.style.display = 'none';
      contenedor.innerHTML = '';
      return 0;
    }

    contenedor.innerHTML = '';

    const cabecera =
      document.createElement('div');

    cabecera.style.cssText = `
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      flex-wrap: wrap;
      margin-bottom: .4rem;
    `;

    const titulo =
      document.createElement('h4');

    titulo.textContent =
      registros.length > 0
        ? `INFORMACIÓN ADICIONAL (${registros.length})`
        : 'INFORMACIÓN ADICIONAL';

    titulo.style.margin = '0';

    cabecera.appendChild(titulo);

    if (state.is){
      const btnAgregar =
        document.createElement('button');

      btnAgregar.type = 'button';
      btnAgregar.className = 'btn ok';
      btnAgregar.textContent =
        'AGREGAR INFORMACIÓN';

      btnAgregar.onclick = () => {
        agregarInformacionAdicionalGrupo(
          g,
          contenedor
        );
      };

      cabecera.appendChild(btnAgregar);
    }

    contenedor.appendChild(cabecera);

    if (
      busqueda &&
      registrosFiltrados.length === 0
    ){
      const sinCoincidencias =
        document.createElement('div');

      sinCoincidencias.className = 'muted';
      sinCoincidencias.textContent =
        'SIN COINCIDENCIAS.';

      contenedor.appendChild(
        sinCoincidencias
      );

      return 0;
    }

    if (registrosFiltrados.length === 0){
      const sinInformacion =
        document.createElement('div');

      sinInformacion.className = 'muted';
      sinInformacion.textContent =
        'SIN INFORMACIÓN ADICIONAL.';

      contenedor.appendChild(
        sinInformacion
      );

      return 0;
    }

    registrosFiltrados.forEach(
      (registro, indice) => {
        const fila =
          document.createElement('div');

        fila.className = 'card';

        fila.style.cssText = `
          margin: .4rem 0;
          display: flex;
          align-items: flex-start;
          justify-content: space-between;
          gap: 12px;
          flex-wrap: wrap;
        `;

        const contenido =
          document.createElement('div');

        contenido.className = 'meta';
        contenido.style.cssText = `
          flex: 1;
          min-width: 220px;
          white-space: pre-wrap;
          overflow-wrap: anywhere;
        `;

        const asunto =
          document.createElement('strong');

        asunto.textContent =
          `${normalizarAsuntoInformacionAdicional(
            registro.asunto
          ).toUpperCase()}:`;

        const texto =
          document.createTextNode(
            ` ${String(
              registro.informacion || ''
            ).toUpperCase()}`
          );

        contenido.append(
          asunto,
          texto
        );

        fila.appendChild(contenido);

        if (state.is){
          const acciones =
            document.createElement('div');

          acciones.style.cssText = `
            display: flex;
            gap: 6px;
            flex-wrap: wrap;
          `;

          const btnEditar =
            document.createElement('button');

          btnEditar.type = 'button';
          btnEditar.className = 'btn sec';
          btnEditar.textContent = 'EDITAR';

          btnEditar.onclick = () => {
            editarInformacionAdicionalGrupo(
              g,
              registro,
              contenedor
            );
          };

          const btnEliminar =
            document.createElement('button');

          btnEliminar.type = 'button';
          btnEliminar.className = 'btn sec';
          btnEliminar.textContent = 'ELIMINAR';

          btnEliminar.onclick = () => {
            eliminarInformacionAdicionalGrupo(
              g,
              registro,
              contenedor
            );
          };

          acciones.append(
            btnEditar,
            btnEliminar
          );

          fila.appendChild(acciones);
        }

        contenedor.appendChild(fila);

        if (
          indice <
          registrosFiltrados.length - 1
        ){
          const separador =
            document.createElement('div');

          separador.style.cssText = `
            border-top: 1px dashed var(--line);
            opacity: .55;
            margin: .5rem 0;
          `;

          contenedor.appendChild(
            separador
          );
        }
      }
    );

    return busqueda
      ? registrosFiltrados.length
      : 0;
  }catch(error){
    console.error(
      'renderInformacionAdicional',
      error
    );

    /*
      Si el coordinador no tiene permisos de lectura,
      mostramos el error para poder detectarlo.
    */
    contenedor.style.display = '';
    contenedor.innerHTML = `
      <h4>INFORMACIÓN ADICIONAL</h4>
      <div class="muted">
        ERROR AL CARGAR.
      </div>
    `;

    return 0;
  }
}

/* ====== RESUMEN (HOTEL + VUELOS) ====== */
async function renderResumen(g, pane){
  pane.innerHTML='<div class="muted">CARGANDO…</div>';
  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem'; pane.innerHTML='';
  const qRaw = (state.groupQ||'').trim();
  const q = norm(qRaw);
  let hits = 0;

  // HOTEL(ES)
  const hotelBox=document.createElement('div'); hotelBox.className='act';
  hotelBox.innerHTML='<h4>HOTELES</h4><div class="muted">BUSCANDO…</div>'; wrap.appendChild(hotelBox);

  // VUELOS
  // VUELOS
  const vuelosBox = document.createElement('div');
  vuelosBox.className = 'act';
  vuelosBox.innerHTML = `
    <h4>TRANSPORTE / VUELOS</h4>
    <div class="muted">BUSCANDO…</div>
  `;
  wrap.appendChild(vuelosBox);

  // INFORMACIÓN ADICIONAL
  const informacionAdicionalBox =
    document.createElement('div');

  informacionAdicionalBox.className = 'act';

  informacionAdicionalBox.innerHTML = `
    <h4>INFORMACIÓN ADICIONAL</h4>
    <div class="muted">BUSCANDO…</div>
  `;

  /*
    Al principio se oculta para evitar que el coordinador
    vea un bloque vacío mientras se consulta Firestore.
  */
  informacionAdicionalBox.style.display = 'none';

  wrap.appendChild(
    informacionAdicionalBox
  );

  pane.appendChild(wrap);

  // ===== HOTELES (múltiples) =====
  try{
    const hoteles = await loadHotelesInfo(g);
    D_HOTEL('RENDERRESUMEN -> HOTELES[]', hoteles);

    if (!hoteles.length){
      hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">SIN ASIGNACIÓN.</div>';
    } else {
      hotelBox.innerHTML = `<h4>HOTELES ${hoteles.length>1?`(${hoteles.length})`:''}</h4>`;
      const qn = norm((state.groupQ||'').trim());
      let rendered = 0;

      hoteles.forEach((h, idx) => {
        const H = h.hotel || {};
        const nombre    = String(h.hotelNombre || H.nombre || '').toUpperCase();
        const direccion = (H.direccion || h.direccion || '').toUpperCase();
        const cTelefono = (H.contactoTelefono || '').toUpperCase();
        const status    = (h.status || '').toString().toUpperCase();
        const ciISO     = toISO(h.checkIn);
        const coISO     = toISO(h.checkOut);
        const noches    = (h.noches !== '' && h.noches != null) ? Number(h.noches) : '';

        const est = h.estudiantes || {F:0,M:0,O:0};
        const estTot = Number(h.estudiantesTotal ?? (est.F+est.M+est.O));
        const adu = h.adultos || {F:0,M:0,O:0};
        const aduTot = Number(h.adultosTotal ?? (adu.F+adu.M+adu.O));

        const hab = h.habitaciones || {};
        const habLine = (hab.singles!=null || hab.dobles!=null || hab.triples!=null || hab.cuadruples!=null)
          ? `HABITACIONES: ${[
              (hab.singles!=null?`SINGLES: ${hab.singles}`:''),
              (hab.dobles!=null?`DOBLES: ${hab.dobles}`:''),
              (hab.triples!=null?`TRIPLES: ${hab.triples}`:''),
              (hab.cuadruples!=null?`CUÁDRUPLES: ${hab.cuadruples}`:'')
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
        block.innerHTML = `
          <div class="card" style="margin:.4rem 0;">
            ${nombre ? `<div class="meta"><strong>NOMBRE:</strong> ${nombre}</div>` : ''}
            <div class="meta"><strong>CHECK-IN/OUT:</strong> ${dmy(ciISO)} — ${dmy(coISO)}${(noches!==''?`</div>
            <div class="meta"><strong>NOCHES:</strong> ${noches}`:'')}</div>
            ${status ? `<div class="meta"><strong>ESTADO:</strong> ${status}</div>` : ''}
            <div class="meta"><strong>ESTUDIANTES:</strong> F: ${est.F||0} · M: ${est.M||0} · O: ${est.O||0} (TOTAL ${estTot||0}) · <strong>ADULTOS:</strong> F: ${adu.F||0} · M: ${adu.M||0} · O: ${adu.O||0} (TOTAL ${aduTot||0})</div>
            ${habLine ? `<div class="meta">${habLine}</div>` : ''}
            ${h.coordinadores!=null ? `<div class="meta"><strong>COORDINADORES:</strong> ${String(h.coordinadores).toUpperCase()}</div>` : ''}
            ${h.conductores!=null ? `<div class="meta"><strong>CONDUCTORES:</strong> ${String(h.conductores).toUpperCase()}</div>` : ''}
            ${direccion ? `<div class="meta"><strong>DIRECCIÓN:</strong> ${direccion}</div>` : ''}
            ${contactoLine ? `<div class="meta"><strong>TELÉFONO:</strong> ${contactoLine}</div>` : ''}
          </div>`;
        hotelBox.appendChild(block);

        if (idx < hoteles.length-1) {
          const hr = document.createElement('div');
          hr.style.cssText = 'border-top:1px dashed var(--line);opacity:.55;margin:.5rem 0;';
          hotelBox.appendChild(hr);
        }
        rendered++;
      });

      if ((state.groupQ||'').trim() && rendered === 0){
        hotelBox.innerHTML = '<h4>HOTELES</h4><div class="muted">SIN COINCIDENCIAS.</div>';
      }
    }
  }catch(e){
    console.error(e);
    D_HOTEL('ERROR RENDERRESUMEN HOTELES', e?.code || e, e?.message || '');
    hotelBox.innerHTML='<h4>HOTELES</h4><div class="muted">ERROR AL CARGAR.</div>';
  }

  // ===== VUELOS =====
  // ===== VUELOS =====
  try{
    const vuelosRaw = await loadVuelosInfo(g);
    const vuelos = vuelosRaw.map(normalizeVuelo);

    // Filtro: incluye horarios top-level, horarios por tramo y bus
    const flt = (!q) ? vuelos : vuelos.filter(v=>{
      const sTop = [
        v.numero, v.proveedor, v.origen, v.destino,
        toISO(v.fechaIda), toISO(v.fechaVuelta),
        v.presentacionIdaHora, v.vueloIdaHora, v.presentacionVueltaHora, v.vueloVueltaHora,
        v.idaHora, v.vueltaHora,
        v.tipoTransporte, v.tipoVuelo
      ].join(' ');
      const sTramos = (v.tramos||[]).map(t => [
        t.aerolinea, t.numero, t.origen, t.destino,
        toISO(t.fechaIda), toISO(t.fechaVuelta),
        t.presentacionIdaHora, t.vueloIdaHora, t.presentacionVueltaHora, t.vueloVueltaHora
      ].join(' ')).join(' ');
      return norm(`${sTop} ${sTramos}`).includes(q);
    });
    if (q) hits += flt.length;

    if(!flt.length){
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">SIN VUELOS.</div>';
    }else{
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4>';

      flt.forEach((v, i) => {
        const isAereo = (v.tipoTransporte || 'aereo') === 'aereo';
        const isRegMT = isAereo && v.tipoVuelo === 'regular' && Array.isArray(v.tramos) && v.tramos.length > 0;

        const numero   = (v.numero || (v.tramos?.[0]?.numero || '')).toString().toUpperCase();
        const empresa  = (v.proveedor || (v.tramos?.[0]?.aerolinea || '')).toString().toUpperCase();
        const ruta     = [ (v.origen || v.tramos?.[0]?.origen || ''), (v.destino || v.tramos?.slice(-1)?.[0]?.destino || '') ]
                         .map(x => (x||'').toUpperCase()).filter(Boolean).join(' — ');
        const ida      = dmy(toISO(v.fechaIda)    || toISO(v.tramos?.[0]?.fechaIda)    || '');
        const vuelta   = dmy(toISO(v.fechaVuelta) || toISO(v.tramos?.slice(-1)?.[0]?.fechaVuelta) || '');

        const block = document.createElement('div');
        let extra = '';

        if (isRegMT){
          // AÉREO REGULAR MULTITRAMO: listar tramos con horarios por tramo
          extra += `<div class="meta"><strong>TIPO:</strong> REGULAR · MULTITRAMO</div>`;
          extra += `<div class="card" style="margin:.4rem 0;">`;
          v.tramos.forEach((t, idxT) => {
            const idaLine = (t.presentacionIdaHora || t.vueloIdaHora)
              ? `IDA: ${dmy(toISO(t.fechaIda)||'')} ${t.presentacionIdaHora ? ' · PRESENTACIÓN '+t.presentacionIdaHora : ''}${t.vueloIdaHora ? ' · VUELO '+t.vueloIdaHora : ''}`
              : `IDA: ${dmy(toISO(t.fechaIda)||'')}`;
            const vtaLine = (t.presentacionVueltaHora || t.vueloVueltaHora)
              ? `REGRESO: ${dmy(toISO(t.fechaVuelta)||'')} ${t.presentacionVueltaHora ? ' · PRESENTACIÓN '+t.presentacionVueltaHora : ''}${t.vueloVueltaHora ? ' · VUELO '+t.vueloVueltaHora : ''}`
              : (toISO(t.fechaVuelta) ? `REGRESO: ${dmy(toISO(t.fechaVuelta))}` : '');

            extra += `
              <div class="meta" style="margin:.25rem 0;">
                <strong>TRAMO ${idxT+1}:</strong> ${(t.aerolinea||'').toUpperCase()} ${(t.numero||'').toUpperCase()} — ${(t.origen||'').toUpperCase()} → ${(t.destino||'').toUpperCase()}
              </div>
              <div class="meta" style="margin-left:.5rem">${idaLine}</div>
              ${vtaLine ? `<div class="meta" style="margin-left:.5rem">${vtaLine}</div>` : ''}`;
          });
          extra += `</div>`;
        } else if (isAereo) {
          // AÉREO SIMPLE / CHARTER / REGULAR simple: usa horarios top-level
          const l1 = (v.presentacionIdaHora || v.vueloIdaHora)
            ? `<div class="meta"><strong>IDA:</strong> ${v.presentacionIdaHora ? 'PRESENTACIÓN '+v.presentacionIdaHora : ''}${v.vueloIdaHora ? (v.presentacionIdaHora ? ' · ' : '') + 'VUELO ' + v.vueloIdaHora : ''}</div>` : '';
          const l2 = (v.presentacionVueltaHora || v.vueloVueltaHora)
            ? `<div class="meta"><strong>REGRESO:</strong> ${v.presentacionVueltaHora ? 'PRESENTACIÓN '+v.presentacionVueltaHora : ''}${v.vueloVueltaHora ? (v.presentacionVueltaHora ? ' · ' : '') + 'VUELO ' + v.vueloVueltaHora : ''}</div>` : '';
          const tipoTxt = v.tipoVuelo ? ` · ${(v.tipoVuelo||'').toString().toUpperCase()}` : '';
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

        if (i < flt.length - 1){
          const hr = document.createElement('div');
          hr.style.cssText = 'border-top:1px dashed var(--line);opacity:.55;margin:.5rem 0;';
          vuelosBox.appendChild(hr);
        }
      });
    }
  }catch(e){
    console.error(e);
    vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">ERROR AL CARGAR.</div>';
  }

  // ===== INFORMACIÓN ADICIONAL =====
  hits += await renderInformacionAdicional(
    g,
    informacionAdicionalBox
  );

  return hits;
}

/* ====== ÍNDICE DE HOTELES ====== */
async function ensureHotelesIndex(){
  if (state.cache.hoteles.loaded) return state.cache.hoteles;
  const byId  = new Map();
  const bySlug= new Map();
  const all   = [];
  const snap = await getDocs(collection(db,'hoteles'));
   snap.forEach(d=>{
     const x = d.data() || {};
     const docu = { id:d.id, ...x };
     const s = norm(x.slug || x.nombre || d.id);
     byId.set(String(d.id), docu);
     if (s) bySlug.set(s, docu);
     all.push(docu);
   });
  state.cache.hoteles = { loaded:true, byId, bySlug, all };
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
      qs.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoId', e); }

  // Fallbacks legacy
  try {
    if (!cand.length && groupDocId) {
      const qs2 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoDocId','==', groupDocId)));
      qs2.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoDocId', e); }

  try {
    if (!cand.length && groupNum) {
      const qs3 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoNumero','==', groupNum)));
      qs3.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (e) { D_HOTEL('ERR hotelAssignments.grupoNumero', e); }

  if (!cand.length){
    state.cache.hotel.set(cacheKey, []);
    return [];
  }

  // Orden por check-in asc
  cand.sort((a,b) => (toISO(a.checkIn)||'').localeCompare(toISO(b.checkIn)||''));

  // Resolver docs hotel
  const { byId, bySlug, all } = await ensureHotelesIndex();
  function pickHotelDoc(asig){
    const tryIds = [];
    if (asig?.hotelId) tryIds.push(String(asig.hotelId));
    if (asig?.hotelDocId) tryIds.push(String(asig.hotelDocId));
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

    const s = norm(asig?.nombre || asig?.hotelNombre || '');
    const dest = norm(g.destino || '');
    if (s && bySlug.has(s)) return bySlug.get(s);
    if (s){
      const candidatos = [];
      for (const [slugName, docu] of bySlug){
        if (slugName.includes(s) || s.includes(slugName)) candidatos.push(docu);
      }
      if (candidatos.length === 1) return candidatos[0];
      return candidatos.find(d => norm(d.destino||d.ciudad||'') === dest) || candidatos[0] || null;
    }

    const ci = toISO(asig.checkIn), co = toISO(asig.checkOut);
    const overlapDays = (A,B,C,D)=>{ if(!A||!B||!C||!D) return 0;
      const s = Math.max(new Date(A).getTime(), new Date(C).getTime());
      const e = Math.min(new Date(B).getTime(), new Date(D).getTime());
      return (e>=s) ? Math.round((e - s)/86400000) + 1 : 0;
    };
    let candidatos = all.filter(h => norm(h.destino||h.ciudad||'') === dest);
    if (ci && co){
      candidatos = candidatos
        .map(h => ({ h, ov: overlapDays(ci, co, toISO(h.fechaInicio), toISO(h.fechaFin)) }))
        .sort((a,b)=> b.ov - a.ov).map(x=>x.h);
    }
    return candidatos[0] || null;
  }

  const out = cand.map(a => {
    const H = pickHotelDoc(a);
    const ciISO = toISO(a.checkIn);
    const coISO = toISO(a.checkOut);
    const noches = (typeof a.noches === 'number')
      ? a.noches
      : (ciISO && coISO ? Math.max(0, (new Date(coISO)-new Date(ciISO))/86400000) : '');
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

/* ====== (LEGACY) HOTEL: UNA ASIGNACIÓN MEJOR — se mantiene por compatibilidad ====== */
async function loadHotelInfo(g){
  const groupDocId = String(g.id || '').trim();
  const groupNum   = String(g.numeroNegocio || '').trim();
  const cacheKey = groupDocId || groupNum || '';
  if (cacheKey && state.cache.hotel.has(cacheKey)) {
    D_HOTEL('CACHE HIT LOADHOTELINFO', { cacheKey, groupDocId, groupNum });
    return state.cache.hotel.get(cacheKey);
  }
  D_HOTEL('INI LOADHOTELINFO', { groupDocId, groupNum, grupoDoc: g.id, destino: g.destino });

  let cand = [];
  try {
    if (groupDocId) {
      const qs1 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoId', '==', groupDocId)));
      qs1.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (e) { D_HOTEL('ERROR query grupoId', e); }

  try {
    if (!cand.length && groupDocId) {
      const qs2 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoDocId', '==', groupDocId)));
      qs2.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (e) { D_HOTEL('ERROR query grupoDocId', e); }

  try {
    if (!cand.length && groupNum) {
      const qs3 = await getDocs(query(collection(db,'hotelAssignments'), where('grupoNumero', '==', groupNum)));
      qs3.forEach(d => cand.push({ id:d.id, ...(d.data()||{}) }));
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
  if (elegido?.hotelId)     tryIds.push(String(elegido.hotelId));
  if (elegido?.hotelDocId)  tryIds.push(String(elegido.hotelDocId));
  if (elegido?.hotel?.id)   tryIds.push(String(elegido.hotel.id));
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
      if (hd.exists()) { hotelDoc = { id:hd.id, ...(hd.data()||{}) }; break; }
    } catch (e) { D_HOTEL('ERROR GETDOC HOTELES por ID', id, e); }
  }

  if (!hotelDoc) {
    const s = norm(elegido?.nombre || elegido?.hotelNombre || '');
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
        : (candidatos.find(d => norm(d.destino||d.ciudad||'') === dest) || candidatos[0] || null);
      D_HOTEL('MATCH FUZZY', { candidatos, elegido: hotelDoc });
    }
  }

  if (!hotelDoc) {
    const dest = norm(g.destino || '');
    const ci = toISO(elegido?.checkIn), co = toISO(elegido?.checkOut);
    const overlapDays = (A,B,C,D)=>{ if(!A||!B||!C||!D) return 0;
      const s = Math.max(new Date(A).getTime(), new Date(C).getTime());
      const e = Math.min(new Date(B).getTime(), new Date(D).getTime());
      return (e>=s) ? Math.round((e - s)/86400000) + 1 : 0;
    };
    let candidatos = all.filter(h => norm(h.destino||h.ciudad||'') === dest);
    if (ci && co) {
      candidatos = candidatos
        .map(h => ({ h, ov: overlapDays(ci, co, toISO(h.fechaInicio), toISO(h.fechaFin)) }))
        .sort((a,b)=> b.ov - a.ov)
        .map(x=>x.h);
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
      const val = k.split('.').reduce((acc, part)=> (acc && acc[part]!==undefined)? acc[part] : undefined, v);
      if (val!==undefined && val!==null && val!=='') return val;
    }
    return '';
  };

  const numero      = get('numero','nro','numVuelo','vuelo','flightNumber','codigo','code');
  const proveedor   = get('proveedor','empresa','aerolinea','compania');

  // NUEVO: tipo transporte / tipo vuelo
  const tipoTransporte = (String(get('tipoTransporte')) || 'aereo').toLowerCase() || 'aereo';
  const tipoVuelo      = (tipoTransporte==='aereo')
    ? (String(get('tipoVuelo')||'charter').toLowerCase())
    : '';

  // Top-level (aéreo simple/charter o regular simple)
  const presentacionIdaHora     = get('presentacionIdaHora');
  const vueloIdaHora            = get('vueloIdaHora');
  const presentacionVueltaHora  = get('presentacionVueltaHora');
  const vueloVueltaHora         = get('vueloVueltaHora');

  // Terrestre (bus)
  const idaHora    = get('idaHora');
  const vueltaHora = get('vueltaHora');

  const origen      = get('origen','desde','from','salida.origen','salida.iata','origenIATA','origenSigla','origenCiudad');
  const destino     = get('destino','hasta','to','llegada.destino','llegada.iata','destinoIATA','destinoSigla','destinoCiudad');
  const fechaIda    = get('fechaIda','ida','salida.fecha','fechaSalida','fecha_ida','fecha');
  const fechaVuelta = get('fechaVuelta','vuelta','regreso.fecha','fechaRegreso','fecha_vuelta');

  // Tramos (aéreo regular multitramo) con horarios por tramo
  const trRaw = Array.isArray(v.tramos) ? v.tramos : [];
  const tramos = trRaw.map(t => ({
    aerolinea: String(t.aerolinea || '').toUpperCase(),
    numero:    String(t.numero    || '').toUpperCase(),
    origen:    String(t.origen    || '').toUpperCase(),
    destino:   String(t.destino   || '').toUpperCase(),
    fechaIda:  t.fechaIda    || '',
    fechaVuelta: t.fechaVuelta || '',
    presentacionIdaHora:     t.presentacionIdaHora     || '',
    vueloIdaHora:            t.vueloIdaHora            || '',
    presentacionVueltaHora:  t.presentacionVueltaHora  || '',
    vueloVueltaHora:         t.vueloVueltaHora         || '',
  }));

  // Reserva (si la manejas desde viajes.js)
  const reservaEstado       = (v.reservaEstado || '').toString().toLowerCase();
  const reservaFechaLimite  = get('reservaFechaLimite');

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
      qs1.forEach(d => found.push({ id:d.id, ...(d.data()||{}) }));
    }
  } catch (_) {}

  // 2) Legacy: grupoIds = array de numeros de negocio
  try {
    if (!found.length && num) {
      const qs2 = await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains', num)));
      qs2.forEach(d => found.push({ id:d.id, ...(d.data()||{}) }));
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

      if (match) found.push({ id:d.id, ...v });
    });
  }

  // Ordena por fecha de ida
  found.sort((a,b) => (toISO(a.fechaIda) || '').localeCompare(toISO(b.fechaIda) || ''));

  state.cache.vuelos.set(cacheKey, found);
  return found;
}

/* ====== ITINERARIO + BITÁCORA + VOUCHERS ====== */
function getSavedAsistencia(grupo, fechaISO, actividad){
  const byDate=grupo?.asistencias?.[fechaISO]; if(!byDate) return null;
  const key=slug(actividad||'actividad');
  if(Object.prototype.hasOwnProperty.call(byDate,key)) return byDate[key];
  for(const k of Object.keys(byDate)) if(slug(k)===key) return byDate[k];
  return null;
}
function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data){
  const key=slug(actividad||'actividad'); (grupo.asistencias||={}); (grupo.asistencias[fechaISO]||={}); grupo.asistencias[fechaISO][key]=data;
}
function calcPlan(actividad, grupo){
  const a=actividad||{}; const ad=Number(a.adultos||0), es=Number(a.estudiantes||0); const s=ad+es;
  if(s>0) return s; const base=(grupo && (grupo.cantidadgrupo!=null?grupo.cantidadgrupo:grupo.pax)); return Number(base||0);
}

function countItinHits(g, qNorm){
  if(!qNorm) return 0;
  let c=0;
  const map=g.itinerario||{};
  for(const f of Object.keys(map)){
    const arr = Array.isArray(map[f]) ? map[f] : [];
    c += arr.filter(a => norm([a.actividad,a.proveedor,a.horaInicio,a.horaFin].join(' ')).includes(qNorm)).length;
  }
  return c;
}

function renderItinerario(g, pane, preferDate){
  pane.innerHTML='';
  const map = g?.itinerario || {};
  if (!map || typeof map !== 'object' || Object.keys(map).length === 0){
    pane.innerHTML = '<div class="muted">SIN ITINERARIO CARGADO.</div>';
    return 0;
  }
  const qNorm = norm(state.groupQ||'');
  const fechas=rangoFechas(g.fechaInicio,g.fechaFin);
  if(!fechas.length){ pane.innerHTML='<div class="muted">FECHAS NO DEFINIDAS.</div>'; return 0; }

  const pillsWrap=document.createElement('div'); pillsWrap.className='date-pills'; pane.appendChild(pillsWrap);
  const actsWrap=document.createElement('div'); actsWrap.className='acts'; pane.appendChild(actsWrap);

  const hoy=toISO(new Date());
  let startDate=preferDate || ((hoy>=fechas[0] && hoy<=fechas.at(-1))?hoy:fechas[0]);

  const fechasMostrar = (!qNorm) ? fechas : fechas.filter(f=>{
    const arr=(g.itinerario && g.itinerario[f])? g.itinerario[f] : [];
    return arr.some(a => norm([a.actividad,a.proveedor,a.horaInicio,a.horaFin].join(' ')).includes(qNorm));
  });
  if(!fechasMostrar.length){ actsWrap.innerHTML='<div class="muted">SIN COINCIDENCIAS PARA EL ITINERARIO.</div>'; return 0; }
  if(!fechasMostrar.includes(startDate)) startDate=fechasMostrar[0];

  fechasMostrar.forEach(f=>{
    const pill=document.createElement('div'); pill.className='pill'+(f===startDate?' active':''); pill.textContent=dmy(f); pill.title=f; pill.dataset.fecha=f;
    pill.onclick=()=>{ pillsWrap.querySelectorAll('.pill').forEach(p=>p.classList.remove('active')); pill.classList.add('active'); renderActs(g,f,actsWrap); localStorage.setItem('rt_last_date_'+g.id,f); };
    pillsWrap.appendChild(pill);
  });

  const last=localStorage.getItem('rt_last_date_'+g.id); if(last && fechasMostrar.includes(last)) startDate=last;
  renderActs(g,startDate,actsWrap);

  // devolver cantidad de coincidencias totales en ITINERARIO
  return countItinHits(g, qNorm);
}

async function renderActs(grupo, fechaISO, cont){
  cont.innerHTML='';

  // Banner superior: Alojamiento del día + aviso último día (igual que antes)
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

  const q = norm(state.groupQ||'');
  // Obtener actividades del día tolerando objeto indexado
  let acts = (grupo.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
  if (!Array.isArray(acts)) {
    acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');
  }

  // Ocultar "Desayuno Hotel" en la vista de itinerario
  acts = acts.filter(a => String(a?.actividad || '').toUpperCase() !== 'DESAYUNO HOTEL');

  // Orden por hora de inicio (temprano → tarde)
  acts = acts.slice().sort((a,b)=> timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

  if(q) acts = acts.filter(a => norm([a.actividad,a.proveedor,a.horaInicio,a.horaFin].join(' ')).includes(q));
  if (!acts.length){ cont.innerHTML='<div class="muted">SIN ACTIVIDADES PARA ESTE DÍA.</div>'; return; }

  // ===== Render inmediato, cargas asíncronas después =====
  for (const act of acts){
    const plan  = calcPlan(act, grupo);
    const saved = getSavedAsistencia(grupo, fechaISO, act.actividad);
    const estado = (grupo.serviciosEstado?.[fechaISO]?.[slug(act.actividad||'')]?.estado) || '';

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

    // — Detalle/Comentarios (servicio se resuelve dentro del modal si es necesario)
    const btnAI = div.querySelector('.btnActInfo');
    
    if (btnAI) {
      btnAI.onclick = async () => {
        try {
          const servicio = await findServicio({
            destino:
              act.servicioDestino ||
              grupo.destino,
    
            anoViaje:
              grupo.anoViaje ||
              state.anoViajeActivo,
    
            servicioId:
              act.servicioId ||
              '',
    
            nombre:
              actName
          }).catch(() => null);
    
          const tipoRaw = (
            servicio?.voucher ||
            'No Aplica'
          ).toString();
    
          const tipo =
            /electron/i.test(tipoRaw)
              ? 'ELECTRONICO'
              : /fisic/i.test(tipoRaw)
                ? 'FISICO'
                : /correo/i.test(tipoRaw)
                  ? 'CORREO'
                  : 'NOAPLICA';
    
          await openActividadModal(
            grupo,
            fechaISO,
            act,
            servicio,
            tipo
          );
        } catch (error) {
          console.error(
            'openActividadModal error',
            error
          );
        }
      };
    }

    // — Guardar asistencia/nota (igual que antes)
    div.querySelector('.btnSave').onclick = async ()=>{
      const btn = div.querySelector('.btnSave'); btn.disabled = true;
      try{
        const pax  = Number(div.querySelector('input').value || 0);
        const nota = String(div.querySelector('textarea').value || '').trim();
        const refGrupo = doc(db,'grupos',grupo.id);
        const payload = {};
        payload[`asistencias.${fechaISO}.${actKey}`] = {
          paxFinal:pax, notas:nota, byUid:auth.currentUser.uid,
          byEmail:String(auth.currentUser.email||'').toLowerCase(), updatedAt:serverTimestamp()
        };
        await updateDoc(refGrupo, payload);
        setSavedAsistenciaLocal(grupo, fechaISO, actName, { paxFinal:pax, notas:nota });

        if(nota){
          const timeId = timeIdNowMs();
          const ref = doc(db,'grupos',grupo.id,'bitacora',actKey,fechaISO,timeId);
          await setDoc(ref, {
            texto: nota,
            byUid: auth.currentUser.uid,
            byEmail: (auth.currentUser.email||'').toLowerCase(),
            ts: serverTimestamp()
          });

          // Alerta para Operaciones
          await addDoc(collection(db,'alertas'),{
            audience:'',
            mensaje: `NOTA EN ${actName.toUpperCase()}: ${nota.toUpperCase()}`,
            createdAt: serverTimestamp(),
            createdBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() },
            readBy:{},
            groupInfo:{
              grupoId:grupo.id,
              nombre: (nombreOperativoGrupo(grupo)),
              code: (grupo.numeroNegocio||'')+(grupo.identificador?('-'+grupo.identificador):''),
              destino: (grupo.destino||null),
              programa: (grupo.programa||null),
              fechaActividad: fechaISO,
              actividad: actName
            }
          });

          await loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'));
          div.querySelector('textarea').value='';
          await window.renderGlobalAlertsV2();
        }

        btn.textContent='GUARDADO'; setTimeout(()=>{ btn.textContent='GUARDAR'; btn.disabled=false; },900);
      }catch(e){ console.error(e); btn.disabled=false; alert('NO SE PUDO GUARDAR.'); }
    };

    // ===== CARGAS EN SEGUNDO PLANO =====

    // (1) Bitácora asíncrona (reemplaza el “CARGANDO…” cuando llega)
    loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'))
      .catch(e => {
        console.error(e);
        div.querySelector('.bitItems').innerHTML = '<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>';
      });

    // (2) Servicio / botón de voucher asíncrono (unificado)
      (async () => {
        try {
          const servicio = await findServicio({
            destino:
              act.servicioDestino ||
              grupo.destino,
          
            anoViaje:
              grupo.anoViaje ||
              state.anoViajeActivo,
          
            servicioId:
              act.servicioId ||
              '',
          
            nombre:
              actName
          });
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
  wrap.innerHTML='<div class="muted">CARGANDO…</div>';
  try{
    const coll = collection(db,'grupos',grupoId,'bitacora',actKey,fechaISO);
    const qs=await getDocs(query(coll,orderBy('ts','desc'),limit(50)));
    const frag=document.createDocumentFragment();
    qs.forEach(d=>{ const x=d.data()||{}; const quien=String(x.byEmail||x.byUid||'USUARIO').toUpperCase();
      const cuando=x.ts?.seconds?new Date(x.ts.seconds*1000):null;
      const hora=cuando?cuando.toLocaleString('es-CL').toUpperCase():'';
      const div=document.createElement('div'); div.className='meta';
      div.textContent=`• ${(x.texto||'').toString().toUpperCase()} — ${quien}${hora?(' · '+hora):''}`; frag.appendChild(div);
    });
    wrap.innerHTML=''; wrap.appendChild(frag); if(!qs.size) wrap.innerHTML='<div class="muted">AÚN NO HAY NOTAS.</div>';
  }catch(e){ console.error(e); wrap.innerHTML='<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>'; }
}

/* ====== VIAJE: INICIO / TÉRMINO / REVERSIÓN ====== */
async function openInicioViajeModal(g){
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
      <input id="ivA" type="number" min="0" inputmode="numeric" placeholder="ADULTOS (A)" value="${preA||''}" />
      <input id="ivE" type="number" min="0" inputmode="numeric" placeholder="ESTUDIANTES (E)" value="${preE||''}" />
    </div>
    <div class="meta">TOTAL REAL: <strong id="ivTot">${(preA+preE)||0}</strong></div>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="ivSave" class="btn ok">GUARDAR</button>
    </div>`;

  const $A = body.querySelector('#ivA');
  const $E = body.querySelector('#ivE');
  const $T = body.querySelector('#ivTot');
  const recalc = () => { const t = Number($A.value||0)+Number($E.value||0); $T.textContent = t; };
  $A.oninput = recalc; $E.oninput = recalc;

   // === REEMPLAZA DESDE AQUÍ ===
   body.querySelector('#ivSave').onclick = async () => {
     const A = Math.max(0, Number($A.value||0));
     const E = Math.max(0, Number($E.value||0));
     const total = A + E;
   
     // Si no es el día de inicio y NO es STAFF, pedimos confirmación
     if (!isToday(g.fechaInicio) && !state.is){
       const ok = confirm('No es el día de inicio. ¿Confirmar de todas formas?');
       if (!ok) return;
     }
   
     const path = doc(db,'grupos',g.id);
   
     // 1) Guardado principal (si falla, aborta)
     try {
       await updateDoc(path, {
         paxViajando: { A, E, total, by:(state.user.email||'').toLowerCase(), updatedAt: serverTimestamp() },
         viaje: { ...(g.viaje||{}), estado:'EN_CURSO', inicio:{ at: serverTimestamp(), by:(state.user.email||'').toLowerCase() } }
       });
     } catch (e) {
       console.error('INICIO: updateDoc FAILED', e?.code, e);
       alert('No fue posible guardar el inicio del viaje. ' + (e?.code || ''));
       return; // aborta si falló el guardado principal
     }
   
     // 2) Log inmutable (si falla, no bloquea)
     try {
       await appendViajeLog(g.id, 'INICIO', `INICIO DE VIAJE — A:${A} · E:${E} · TOTAL:${total}`, { A, E, total });
     } catch (e) {
       console.warn('appendViajeLog falló (no bloquea):', e?.code, e);
     }
   
     // 3) Refresco local + re-render (si falla, no bloquea)
     try {
       g.paxViajando = { A, E, total };
       g.viaje = { ...(g.viaje||{}), estado:'EN_CURSO', inicio:{ at:new Date(), by:(state.user.email||'').toLowerCase() } };
       document.getElementById('modalBack').style.display='none';
       await renderOneGroup(g);
     } catch (e) {
       console.warn('renderOneGroup después de inicio falló (no bloquea):', e?.code, e);
     }
   };

  document.getElementById('modalClose').onclick = () => { document.getElementById('modalBack').style.display='none'; };
  back.style.display = 'flex';
}

async function ensureFinanzasSummary(groupId){
  try{
    const d = await getDoc(doc(db,'grupos',groupId,'finanzas','summary'));
    return d.exists()? (d.data()||{}) : null;
  }catch(_){ return null; }
}

async function openTerminoViajeModal(g){
  // BLOQUEO: exige cierre financiero antes de permitir finalizar viaje
  const finSum = await ensureFinanzasSummary(g.id);
  if (!finSum || finSum.closed !== true){
    alert('Debes cerrar FINANZAS (transferencia y boleta) antes de terminar el viaje.');
    // abrir pestaña FINANZAS si existe
    const paneFin = document.getElementById('paneFin');
    if (paneFin){ 
      document.getElementById('paneResumen')?.style && (document.getElementById('paneResumen').style.display='none');
      document.getElementById('paneItin')?.style && (document.getElementById('paneItin').style.display='none');
      paneFin.style.display='';
    }
    return;
  }
   
  if (!g?.viaje?.inicio?.at && !state.is){
    alert('Aún no se ha registrado el inicio del viaje.');
    return;
  }
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = `TERMINAR VIAJE — ${dmy(g.fechaFin)}`;

  body.innerHTML = `
    <div class="meta">¿Deseas cerrar el viaje? Esto pedirá confirmación de administración.</div>
    <label class="meta" style="display:flex;gap:.5rem;align-items:center"><input id="rvRend" type="checkbox"> RENDICIÓN HECHA</label>
    <label class="meta" style="display:flex;gap:.5rem;align-items:center"><input id="rvBol"  type="checkbox"> BOLETA ENTREGADA</label>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="tvSave" class="btn warn">FINALIZAR VIAJE</button>
    </div>`;

  body.querySelector('#tvSave').onclick = async () => {
    const rend = !!body.querySelector('#rvRend').checked;
    const bol  = !!body.querySelector('#rvBol').checked;
    try{
      const path = doc(db,'grupos',g.id);
      await updateDoc(path, {
        viaje: { ...(g.viaje||{}), estado:'FINALIZADO', fin:{ at: serverTimestamp(), by:(state.user.email||'').toLowerCase(), rendicionOk: rend, boletaOk: bol } }
      });
      await appendViajeLog(g.id, 'FIN', `FINALIZAR VIAJE${rend?' · RENDICIÓN OK':''}${bol?' · BOLETA OK':''}`, { rend, bol }); 
      g.viaje = { ...(g.viaje||{}), estado:'FINALIZADO', fin:{ at: new Date(), by:(state.user.email||'').toLowerCase(), rendicionOk: rend, boletaOk: bol } };
      document.getElementById('modalBack').style.display='none';
      await renderOneGroup(g);
    }catch(e){
      console.error(e);
      alert('No fue posible finalizar el viaje.');
    }
  };

  document.getElementById('modalClose').onclick = () => { document.getElementById('modalBack').style.display='none'; };
  back.style.display = 'flex';
}

// Reversión (solo STAFF)
async function staffReopenInicio(g){
  if (!state.is){ alert('Solo staff puede reabrir el inicio.'); return; }
  const ok = confirm('¿Reabrir INICIO DE VIAJE? (se habilitará el botón de inicio para el coordinador)');
  if(!ok) return;
  try{
    const path=doc(db,'grupos',g.id);
    await updateDoc(path,{ 'viaje.inicio': deleteField(), 'viaje.estado': 'PENDIENTE' });
    await appendViajeLog(g.id, 'REABRIR_INICIO', 'SE REABRIÓ EL INICIO DEL VIAJE');
    if (g.viaje){ delete g.viaje.inicio; g.viaje.estado='PENDIENTE'; }
    await renderOneGroup(g);
  }catch(e){ console.error(e); alert('No fue posible reabrir el inicio.'); }
}
async function staffReopenCierre(g){
  if (!state.is){ alert('Solo staff puede reabrir el cierre.'); return; }
  const ok = confirm('¿Reabrir CIERRE DE VIAJE? (volverá a estado EN_CURSO)');
  if(!ok) return;
  try{
    const path = doc(db,'grupos',g.id);
    await updateDoc(path,{ 'viaje.fin': deleteField(), 'viaje.estado': 'EN_CURSO' });

    // Log correcto e inmutable + bitácora
    await appendViajeLog(g.id, 'REABRIR_CIERRE', 'SE REABRIÓ EL CIERRE DEL VIAJE');

    if (g.viaje){ delete g.viaje.fin; g.viaje.estado='EN_CURSO'; }
    await renderOneGroup(g);
  }catch(e){
    console.error(e);
    alert('No fue posible reabrir el cierre.');
  }
}

/* ====== SERVICIOS / VOUCHERS ====== */

// Destinos "compuestos" que reutilizan catálogos de otros destinos
const DESTINO_SERVICIOS_ALIASES = {
  // clave: destino normalizado (con norm)
  'sur de chile y bariloche': ['SUR DE CHILE', 'BARILOCHE'],
};

function expandDestinosServicios(destinoRaw){
  const base = (destinoRaw || '').toString().trim();
  if (!base) return [];

  const out = [base];

  // 1) alias explícitos
  const alias = DESTINO_SERVICIOS_ALIASES[norm(base)];
  if (alias && Array.isArray(alias)){
    for (const d of alias){
      if (d && !out.includes(d)) out.push(d);
    }
  } else {
    // 2) fallback genérico: "SUR DE CHILE Y BARILOCHE" → ["SUR DE CHILE","BARILOCHE"]
    const up = base.toUpperCase();
    if (/\sY\s/.test(up)){
      up.split(/\s+Y\s+/).forEach(part => {
        const clean = part.trim();
        if (clean && !out.includes(clean)) out.push(clean);
      });
    }
  }
  return out;
}

/* =========================================================
   DATOS OPERATIVOS Y ACCIONES DE CONTACTO DE SERVICIOS
   ========================================================= */

function escapeHTMLServicio(valor){
  return (valor ?? '')
    .toString()
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function normalizarTelefonoAccion(valor){
  return (valor || '')
    .toString()
    .trim()
    .replace(/[^\d+]/g, '')
    .replace(/(?!^)\+/g, '');
}

function telefonoWhatsApp(valor){
  return normalizarTelefonoAccion(valor)
    .replace(/\D/g, '');
}

function normalizarAnoViajeServicio(valor){
  const numero = Number(valor);

  if (
    Number.isFinite(numero) &&
    numero >= 2000 &&
    numero <= 2100
  ) {
    return String(Math.trunc(numero));
  }

  return '';
}

function valorServicioPrimero(...valores){
  for (const valor of valores) {
    if (
      valor !== undefined &&
      valor !== null &&
      valor.toString().trim() !== ''
    ) {
      return valor.toString().trim();
    }
  }

  return '';
}

function construirMensajeWhatsAppServicio(
  grupo,
  fechaISO,
  actividad
){
  const grupoNombre = (
    typeof nombreOperativoGrupo === 'function'
      ? nombreOperativoGrupo(grupo)
      : (
          grupo?.aliasGrupo ||
          grupo?.nombreGrupo ||
          grupo?.colegio ||
          'GRUPO RAI TRAI'
        )
  ).toString().trim();

  const codigo = [
    grupo?.numeroNegocio,
    grupo?.identificador
  ].filter(Boolean).join('-');

  const actividadNombre = (
    actividad?.actividad ||
    'SERVICIO'
  ).toString().trim();

  const horario = [
    actividad?.horaInicio,
    actividad?.horaFin
  ].filter(Boolean).join(' - ');

  const partes = [
    'Hola, soy coordinador(a) de Rai Trai.',
    `Me comunico por el servicio ${actividadNombre.toUpperCase()}.`,
    `Grupo: ${grupoNombre.toUpperCase()}${codigo ? ` (${codigo})` : ''}.`,
    fechaISO ? `Fecha: ${dmy(fechaISO)}.` : '',
    horario ? `Horario: ${horario}.` : '',
    'Quedo atento(a).'
  ].filter(Boolean);

  return partes.join('\n');
}

function construirAccionesContactoServicio({
  telefono = '',
  correo = '',
  direccion = '',
  ciudad = '',
  destino = '',
  mensajeWhatsApp = '',
  asuntoCorreo = ''
} = {}){
  const telefonoTel = normalizarTelefonoAccion(
    telefono
  );

  const telefonoWa = telefonoWhatsApp(
    telefono
  );

  const correoLimpio = (correo || '')
    .toString()
    .trim()
    .toLowerCase();

  const ubicacion = [
    direccion,
    ciudad,
    destino
  ]
    .map(valor => (valor || '').toString().trim())
    .filter(Boolean)
    .join(', ');

  const botones = [];

  if (telefonoTel) {
    botones.push(`
      <a
        class="btn sec"
        href="tel:${escapeHTMLServicio(telefonoTel)}"
        style="text-decoration:none"
      >
        📞 LLAMAR
      </a>
    `);
  }

  if (telefonoWa) {
    const urlWhatsApp =
      `https://wa.me/${encodeURIComponent(telefonoWa)}` +
      `?text=${encodeURIComponent(mensajeWhatsApp || '')}`;

    botones.push(`
      <a
        class="btn ok"
        href="${escapeHTMLServicio(urlWhatsApp)}"
        target="_blank"
        rel="noopener noreferrer"
        style="text-decoration:none"
      >
        💬 WHATSAPP
      </a>
    `);
  }

  if (correoLimpio) {
    const urlCorreo =
      `mailto:${encodeURIComponent(correoLimpio)}` +
      `?subject=${encodeURIComponent(asuntoCorreo || '')}`;

    botones.push(`
      <a
        class="btn sec"
        href="${escapeHTMLServicio(urlCorreo)}"
        style="text-decoration:none"
      >
        ✉️ CORREO
      </a>
    `);
  }

  if (ubicacion) {
    const urlMaps =
      'https://www.google.com/maps/search/?api=1&query=' +
      encodeURIComponent(ubicacion);

    botones.push(`
      <a
        class="btn sec"
        href="${escapeHTMLServicio(urlMaps)}"
        target="_blank"
        rel="noopener noreferrer"
        style="text-decoration:none"
      >
        📍 VER DIRECCIÓN
      </a>
    `);
  }

  if (!botones.length) {
    return '';
  }

  return `
    <div
      class="rowflex"
      style="
        margin-top:.65rem;
        gap:.45rem;
        flex-wrap:wrap;
      "
    >
      ${botones.join('')}
    </div>
  `;
}

// Cachea catálogos de servicios por ruta Firestore
// key: 'Servicios/BRASIL/Listado'
async function loadServiciosCatalog(pathArr){
  const key = pathArr.join('/');
  if (!state.cache.servicios) state.cache.servicios = new Map();
  if (state.cache.servicios.has(key)) return state.cache.servicios.get(key);

  let out = [];
  try{
    const colRef = collection(db, ...pathArr);
    const qs = await getDocs(colRef);
    qs.forEach(d=>{
      const x = d.data() || {};
      out.push({ id:d.id, ...x });
    });
  }catch(e){
    console.error('[loadServiciosCatalog]', e);
  }
  state.cache.servicios.set(key, out);
  return out;
}

async function findServicio(
  destinoOConfig,
  nombreLegacy = '',
  anoLegacy = '',
  servicioIdLegacy = ''
){
  /*
    Admite ambos formatos:

    NUEVO:
    findServicio({
      destino,
      nombre,
      anoViaje,
      servicioId
    })

    ANTIGUO:
    findServicio(destino, nombre)
  */

  const config =
    destinoOConfig &&
    typeof destinoOConfig === 'object' &&
    !Array.isArray(destinoOConfig)
      ? destinoOConfig
      : {
          destino: destinoOConfig,
          nombre: nombreLegacy,
          anoViaje:
            anoLegacy ||
            state?.anoViajeActivo ||
            '',
          servicioId: servicioIdLegacy
        };

  const destino = (
    config.destino ||
    config.servicioDestino ||
    ''
  ).toString().trim();

  const nombre = (
    config.nombre ||
    config.actividad ||
    ''
  ).toString().trim();

  const servicioId = (
    config.servicioId ||
    config.idServicio ||
    ''
  ).toString().trim();

  const anoViaje = normalizarAnoViajeServicio(
    config.anoViaje ||
    config.ano ||
    state?.anoViajeActivo ||
    ''
  );

  if (
    !destino ||
    (!nombre && !servicioId)
  ) {
    return null;
  }

  const destinos = expandDestinosServicios(
    destino
  );

  if (!destinos.length) {
    return null;
  }

  const nombreBuscado = norm(nombre);
  const idBuscado = norm(servicioId);

  function coincideServicio(servicio){
    if (!servicio) return false;

    const idActual = norm(
      servicio.id ||
      servicio.servicioId ||
      ''
    );

    const nombreActual = norm(
      servicio.actividad ||
      servicio.nombre ||
      servicio.servicio ||
      servicio.id ||
      ''
    );

    const aliases = Array.isArray(servicio.aliases)
      ? servicio.aliases.map(norm)
      : [];

    const prevIds = Array.isArray(servicio.prevIds)
      ? servicio.prevIds.map(norm)
      : [];

    /*
      Primero se intenta por ID, porque es más preciso.
    */
    if (idBuscado) {
      if (idActual === idBuscado) return true;
      if (prevIds.includes(idBuscado)) return true;
      if (aliases.includes(idBuscado)) return true;
    }

    /*
      Luego se intenta por nombre o nombres anteriores.
    */
    if (nombreBuscado) {
      if (nombreActual === nombreBuscado) return true;
      if (aliases.includes(nombreBuscado)) return true;
      if (prevIds.includes(nombreBuscado)) return true;
    }

    return false;
  }

  for (const destinoCandidato of destinos) {
    const destinoNormalizado = (
      destinoCandidato ||
      ''
    ).toString().trim().toUpperCase();

    /*
      Prioridad:
      1. Catálogo anual.
      2. Catálogo histórico antiguo.
      3. Ruta antigua alternativa.
    */
    const rutas = [];

    if (anoViaje) {
      rutas.push([
        'ServiciosPorAno',
        anoViaje,
        'Destinos',
        destinoNormalizado,
        'Listado'
      ]);
    }

    rutas.push(
      [
        'Servicios',
        destinoNormalizado,
        'Listado'
      ],
      [
        destinoNormalizado,
        'Listado'
      ]
    );

    for (const ruta of rutas) {
      const servicios = await loadServiciosCatalog(
        ruta
      );

      if (
        !Array.isArray(servicios) ||
        !servicios.length
      ) {
        continue;
      }

      const encontrado = servicios.find(
        coincideServicio
      );

      if (encontrado) {
        return {
          ...encontrado,

          /*
            Metadata útil para diagnóstico y para saber
            desde qué catálogo fue resuelto.
          */
          _catalogoRuta: ruta.join('/'),
          _catalogoDestino: destinoNormalizado,
          _catalogoAno:
            ruta[0] === 'ServiciosPorAno'
              ? anoViaje
              : ''
        };
      }
    }
  }

  return null;
}

/* ====== HILOS GLOBALES (A/B/C) + CHEQ OUT ====== */

// A: actividad con proveedor  → DEST:{dest} | PROV:{proveedorId} | SRV:{servicioId}
// B: actividad sin proveedor  → DEST:{dest} | PROV:GENERAL      | ACT:{actKey}
// C: comidas de hotel         → HOTEL:{hotelId} | MEAL:{DESAYUNO|ALMUERZO|CENA}

function isMealAct(name=''){
  const s = String(name||'').toUpperCase();
  if (/\bDESA(Y|LL)UNO\b|^DESA/i.test(s))  return 'DESAYUNO';
  if (/\bALMUERZO\b|^ALM/i.test(s))       return 'ALMUERZO';
  if (/\bCENA\b|^CEN/i.test(s))           return 'CENA';
  return null;
}

function isCheckoutAct(name=''){
  const s = String(name||'').toUpperCase();
  // acepta variaciones: CHEQ OUT, CHECK OUT, CHECK-OUT, CHECKOUT
  return /\bCHE?CK[-\s]?OUT\b|\bCHEQ\s?OUT\b/.test(s);
}

function threadKeyForGeneral(destino, actName){
  const dest = (destino||'').toString().toUpperCase().trim();
  const actKey = slug(actName||'');
  return `DEST:${dest}|PROV:GENERAL|ACT:${actKey}`;
}

function threadKeyForProv(destino, proveedorId, servicioId){
  const dest = (destino||'').toString().toUpperCase().trim();
  return `DEST:${dest}|PROV:${String(proveedorId).toUpperCase()}|SRV:${String(servicioId).toUpperCase()}`;
}

function threadKeyForHotelMeal(hotelId, meal){
  return `HOTEL:${String(hotelId).toUpperCase()}|MEAL:${String(meal).toUpperCase()}`;
}

// Colección única para hilos globales
function threadColl(threadKey){
  return collection(db, 'threads', threadKey, 'msgs');
}

// Proveedor por DESTINO (coincide con tu fetchProveedorByDestino, pero local a este bloque)
async function findProveedorDocByDestino(destino, proveedorName){
  if(!destino || !proveedorName) return null;
  try{
    const qs = await getDocs(collection(db,'Proveedores', String(destino).toUpperCase(), 'Listado'));
    let hit=null;
    qs.forEach(d=>{
      const x=d.data()||{};
      const nom=(x.proveedor || x.nombre || d.id || '').toString();
      if (norm(nom)===norm(proveedorName)) hit={ id:d.id, ...x };
    });
    return hit;
  }catch(_){ return null; }
}

// Alias global idempotente (no redeclara si ya existe en otra parte del bundle)
if (typeof window !== 'undefined' && !window.fetchProveedorByDestino) {
  window.fetchProveedorByDestino = (...args) => findProveedorDocByDestino(...args);
}

/* ====== Lógica de hotel por día con CHEQ OUT ====== */

// Devuelve las actividades del día como array (tolera objeto indexado)
function getDayActsArray(grupo, fechaISO){
  let acts = (grupo?.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
  if (!Array.isArray(acts)) acts = Object.values(acts || {}).filter(x => x && typeof x === 'object');
  // Orden por hora
  return acts.slice().sort((a,b)=> timeVal(a?.horaInicio) - timeVal(b?.horaInicio));
}

// Encuentra el hotel "vigente" para ese día (checkIn <= día < checkOut)
// y, si hay cambio de hotel el MISMO día, detecta el "siguiente del mismo día".
function hotelContextForDay(hoteles, fechaISO){
  const f = String(fechaISO||'');
  let current = null, nextSameDay = null;

  for (const h of hoteles || []){
    const ci = toISO(h.checkIn), co = toISO(h.checkOut);
    if (ci && co && (f >= ci) && (f < co)) current = h;
  }
  // Si hay una asignación con checkIn EXACTO ese día, podría ser el nuevo hotel del mismo día
  for (const h of hoteles || []){
    const ci = toISO(h.checkIn);
    if (ci && ci === f){
      // sólo considera nextSameDay si NO es el mismo objeto que current
      if (!current || current.id !== h.id) nextSameDay = h;
    }
  }
  return { current, nextSameDay };
}

// ¿El ACT ocurre después del CHEQ OUT del día?
function isAfterCheckout(act, dayActs){
  // Busca el primer CHEQ OUT con hora válida
  const co = dayActs.find(a => isCheckoutAct(a?.actividad) && timeVal(a?.horaInicio) < 1e9);
  if (!co) return false;
  const actT = timeVal(act?.horaInicio);
  const coT  = timeVal(co?.horaInicio);
  return (actT < 1e9) && (coT < 1e9) && (actT >= coT);
}

// Para comidas, decide HOTEL correcto considerando CHEQ OUT y posible cambio en el mismo día
function pickHotelForMeal(grupo, fechaISO, act, hoteles){
  const { current, nextSameDay } = hotelContextForDay(hoteles, fechaISO);
  const meal = isMealAct(act?.actividad||'');
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
async function resolveThreadKey(
  grupo,
  fechaISO,
  act,
  servicioHint = null
){
  const actName = (
    act?.actividad ||
    ''
  ).toString();

  const destino = (
    act?.servicioDestino ||
    grupo?.destino ||
    ''
  ).toString().toUpperCase().trim();

  const meal = isMealAct(actName);

  /*
    C: comida de hotel.
  */
  if (meal) {
    const hoteles =
      await loadHotelesInfo(grupo) ||
      [];

    const hotel = pickHotelForMeal(
      grupo,
      fechaISO,
      act,
      hoteles
    );

    const hotelId =
      hotel?.hotel?.id ||
      hotel?.hotelId ||
      hotel?.id ||
      '';

    if (hotelId) {
      return {
        key: threadKeyForHotelMeal(
          hotelId,
          meal
        ),
        scope: 'C'
      };
    }
  }

  /*
    A: servicio asociado a proveedor.
  */
  let servicio = servicioHint;

  if (!servicio) {
    try {
      servicio = await findServicio({
        destino,
        anoViaje:
          grupo?.anoViaje ||
          state?.anoViajeActivo ||
          '',
        servicioId:
          act?.servicioId ||
          '',
        nombre: actName
      });
    } catch (_) {
      servicio = null;
    }
  }

  const servicioId =
    servicio?.id ||
    act?.servicioId ||
    null;

  const proveedorName = valorServicioPrimero(
    servicio?.proveedor,
    act?.proveedor
  );

  let proveedorId = null;

  if (proveedorName) {
    try {
      const proveedorDoc =
        await findProveedorDocByDestino(
          destino,
          proveedorName
        );

      if (proveedorDoc?.id) {
        proveedorId = proveedorDoc.id;
      }
    } catch (_) {}
  }

  if (
    servicioId &&
    proveedorId
  ) {
    return {
      key: threadKeyForProv(
        destino,
        proveedorId,
        servicioId
      ),
      scope: 'A'
    };
  }

  /*
    B: actividad general.
  */
  return {
    key: threadKeyForGeneral(
      destino,
      actName
    ),
    scope: 'B'
  };
}

function renderVoucherHTMLSync(g, fechaISO, act, proveedorDoc=null, compact=false){
  const paxPlan=calcPlan(act,g); const asis=getSavedAsistencia(g,fechaISO,act.actividad); const paxAsist=asis?.paxFinal??'';
  const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
  const provTexto = proveedorDoc
    ? `${(proveedorDoc.nombre||'').toString().toUpperCase()}${proveedorDoc.rut?(' · '+String(proveedorDoc.rut).toUpperCase()):''}${proveedorDoc.direccion?(' · '+String(proveedorDoc.direccion).toUpperCase()):''}`
    : (String(act.proveedor||'').toUpperCase());
  return `
    <div class="card">
      <h3>${(act.actividad||'SERVICIO').toString().toUpperCase()}</h3>
      <div class="meta">PROVEEDOR: ${provTexto||'—'}</div>
      <div class="meta">GRUPO: ${(nombreOperativoGrupo(g)).toString().toUpperCase()} (${code})</div>
      <div class="meta">FECHA: ${dmy(fechaISO)}</div>
      <div class="meta">PAX PLAN: ${paxPlan} · PAX ASISTENTES: ${paxAsist}</div>
      ${compact?'':'<hr><div class="meta">FIRMA COORDINADOR: ________________________________</div>'}
    </div>`;
}
async function openVoucherModal(g, fechaISO, act, servicio, tipo){
  const back=document.getElementById('modalBack');
  const title=document.getElementById('modalTitle');
  const body=document.getElementById('modalBody');
  title.textContent=`VOUCHER — ${(act.actividad||'').toString().toUpperCase()} — ${dmy(fechaISO)}`;

   let proveedorDoc = null;
   try {
     if (servicio?.proveedor) {
       proveedorDoc = await findProveedorDocByDestino(
         (g.destino || '').toString().toUpperCase(),
         servicio.proveedor
       );
     }
   } catch(_) {}

  const voucherHTML=renderVoucherHTMLSync(g,fechaISO,act,proveedorDoc,false);

  if (tipo==='FISICO'){
    body.innerHTML= `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem">
        <button id="vchPrint" class="btn sec">IMPRIMIR</button>
        <button id="vchOk" class="btn ok">FINALIZAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>`;
    document.getElementById('vchPrint').onclick=()=>{ const w=window.open('','_blank'); w.document.write(`<!doctype html><html><body>${voucherHTML}</body></html>`); w.document.close(); w.print(); };
    document.getElementById('vchOk').onclick   =()=> setEstadoServicio(g,fechaISO,act,'FINALIZADA', true);
    document.getElementById('vchPend').onclick =()=> setEstadoServicio(g,fechaISO,act,'PENDIENTE',  true);

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
    } catch(_) {}
  }

  // ——— 3) Datos base ———
  const code        = (g.numeroNegocio||'') + (g.identificador?('-'+g.identificador):'');
  const actividadTX = (act.actividad||'').toString().toUpperCase();
  const grupoTX     = (nombreOperativoGrupo(g)).toString().toUpperCase();
  const destinoTX   = (g.destino||'—').toString().toUpperCase();
  const programaTX  = (g.programa||'—').toString().toUpperCase();
  const fechaTX     = dmy(fechaISO);
  const paxTX       = (asis?.paxFinal ?? '—');
  const coordTX     = (g.coordinadorNombre || '—').toString().toUpperCase();
  const subject     = `CONFIRMACIÓN DE ASISTENCIA — ${actividadTX} — ${fechaTX} — ${grupoTX} (${code})`;

  // cuerpo por defecto (texto plano, editable)
  const defaultBody =
`ESTIMADOS ${(act.proveedor||'PROVEEDOR').toString().toUpperCase()}:

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
    <div class="meta" style="margin-top:.5rem"><strong>PASOS:</strong> 1) ABRIR CORREO · 2) ENVIAR DESDE TU APP · 3) VOLVER Y <u>MARCAR COMO ENVIADA</u> · 4) FINALIZAR ACTIVIDAD.</div>
    <div class="rowflex" style="gap:.4rem;align-items:center;margin:.25rem 0 .25rem 0">
      <input id="rtMailTo" type="email" placeholder="PARA" value="${(provEmail||'')}" style="flex:1"/>
      <input id="rtMailCc" type="email" placeholder="CC" value="operaciones@raitrai.cl" style="flex:1"/>
    </div>
    <input id="rtMailSubj" type="text" placeholder="ASUNTO" value="${subject.replace(/"/g,'&quot;')}" />
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

   // tras crear const $fin = document.getElementById('rtFinalizar');
   (() => {
     const actKey = slug(act.actividad || 'actividad');
     const ya = (g?.serviciosEstado?.[fechaISO]?.[actKey]?.correo?.estado === 'ENVIADA');
     if (ya && $fin) { $fin.disabled = false; $fin.removeAttribute('title'); }
   })();


  // 5.a) Abrir mailto (no cambia estado)
   document.getElementById('rtOpenMail').onclick = () => {
     const to = ($to.value||'').trim();
     if (!to) { alert('INGRESA UN DESTINATARIO (PARA).'); return; }
     const mailto = buildMailto({
       to,
       cc: ($cc.value||'').trim(),
       subject: ($subj.value||'').trim(),
       htmlBody: ($txt.value||'').trim()
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
    const to = ($to.value||'').trim();
    if (!to) { alert('COMPLETA EL CORREO DEL PROVEEDOR.'); return; }
    try {
      const refGrupo = doc(db,'grupos', g.id);
      const payload  = {};
      payload[`serviciosEstado.${fechaISO}.${actKey}.correo`] = { estado:'ENVIADA', enviadaAt: serverTimestamp() };
      await updateDoc(refGrupo, payload);

      // espejo local
      (g.serviciosEstado ||= {});
      (g.serviciosEstado[fechaISO] ||= {});
      (g.serviciosEstado[fechaISO][actKey] ||= {});
      g.serviciosEstado[fechaISO][actKey].correo = { estado:'ENVIADA', enviadaAt: new Date() };

      // log + alerta
      try {
        await appendViajeLog(
          g.id,
          'DECLARACION_CORREO',
          `DECLARACIÓN ENVIADA — ${actividadTX} — ${fechaTX} — PAX:${paxTX}`,
          { actividad: actividadTX, fecha: fechaTX, pax: paxTX }
        );
        await addDoc(collection(db,'alertas'),{
          audience:'',
          mensaje: `DECLARACIÓN ENVIADA — ${actividadTX} — ${fechaTX} — PAX:${paxTX}`,
          createdAt: serverTimestamp(),
          createdBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() },
          readBy:{},
          groupInfo:{
            grupoId:g.id,
            nombre:(nombreOperativoGrupo(g)),
            code: (g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):''),
            destino:(g.destino||null),
            programa:(g.programa||null),
            fechaActividad:fechaISO,
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
    document.getElementById('modalBack').style.display='none';
  };

  // 5.d) Dejar PENDIENTE explícitamente
  document.getElementById('vchPend').onclick = () =>
    setEstadoServicio(g, fechaISO, act, 'PENDIENTE', true);

  } else {
    // ELECTRÓNICO (clave / NFC)
    const clave=(servicio?.clave||'').toString();
    body.innerHTML= `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem">
        <div style="display:flex;gap:.4rem;align-items:center;width:100%">
          <input id="vchClave" type="password" placeholder="CLAVE (O ACERQUE TARJETA NFC)" style="flex:1"/>
          <button id="vchEye" class="btn sec" title="MOSTRAR/OCULTAR">👁</button>
        </div>
        <button id="vchFirmar" class="btn ok">FIRMAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>
      <div class="meta">TIP: SI TU MÓVIL SOPORTA NFC, PUEDES ACERCAR LA TARJETA PARA LEER LA CLAVE AUTOMÁTICAMENTE.</div>`;
    document.getElementById('vchEye').onclick=()=>{ const inp=document.getElementById('vchClave'); inp.type = (inp.type==='password'?'text':'password'); };
    document.getElementById('vchFirmar').onclick=async ()=>{
      const val=(document.getElementById('vchClave').value||'').trim();
      if(!val){ alert('INGRESA LA CLAVE.'); return; }
      if(norm(val)!==norm(clave||'')){ alert('CLAVE INCORRECTA.'); return; }
      await setEstadoServicio(g,fechaISO,act,'FINALIZADA', true);
    };
    document.getElementById('vchPend').onclick =()=> setEstadoServicio(g,fechaISO,act,'PENDIENTE',  true);

    if('NDEFReader' in window){
      try{ const reader=new window.NDEFReader(); await reader.scan();
        reader.onreading=(ev)=>{ const rec=ev.message.records[0]; let text=''; try{ text=(new TextDecoder().decode(rec.data)||'').trim(); }catch(_){}
          if(text){ const inp=document.getElementById('vchClave'); inp.value=text; }
        };
      }catch(_){}
    }
  }

  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}

async function openCorreoConfirmModal(grupo, fechaISO, act, proveedorEmail) {
  // exige asistencia guardada
  const asis = getSavedAsistencia(grupo, fechaISO, act.actividad);
  if (asis?.paxFinal == null) { alert('Primero guarda la ASISTENCIA (PAX).'); return; }

  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  const code = (grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'');
  const asunto =
    `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad||'').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(nombreOperativoGrupo(grupo)).toString().toUpperCase()} (${code})`;

  title.textContent = 'ENVIAR CONFIRMACIÓN POR CORREO';

  body.innerHTML = `
    <div class="meta"><strong>PARA:</strong> ${(proveedorEmail||'—').toUpperCase()}</div>
    <div class="meta"><strong>CC:</strong> OPERACIONES@RAITRAI.CL</div>
    <div class="meta"><strong>ASUNTO:</strong> ${asunto}</div>
    <div class="meta">NOTA ADICIONAL (opcional):</div>
    <textarea id="rt-nota-extra" placeholder="Escribe una nota corta…"></textarea>
    <div class="rowflex" style="margin-top:.6rem">
      <button id="rtSendMail" class="btn ok">ENVIAR</button>
    </div>
  `;

   // === MAIL PATCH START (reemplazo del onclick) ===
   document.getElementById('rtSendMail').onclick = async () => {
     console.group('[MAIL] Enviar');
     const btn = document.getElementById('rtSendMail');
     if (btn.dataset.busy === '1') return;      // anti doble-click
     btn.dataset.busy = '1';
     btn.disabled = true;
   
     try {
       const to = (proveedorEmail || '').trim().toLowerCase();
       console.log('[MAIL] to:', to);
       if (!to) { alert('No hay correo del proveedor. Completa su ficha.'); return; }
   
       const coordNom = (grupo.coordinadorNombre || '').toString().toUpperCase();
       const nota = (document.getElementById('rt-nota-extra').value || '').trim();
   
       const asunto =
         `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad||'').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(nombreOperativoGrupo(grupo)).toString().toUpperCase()} (${(grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'')})`;
   
       const htmlBody =
         `<p>Estimados ${(act.proveedor||'PROVEEDOR').toString().toUpperCase()}:</p>
          <p>Confirmamos la asistencia para el servicio indicado:</p>
          <ul>
            <li><b>Actividad:</b> ${(act.actividad||'').toString().toUpperCase()}</li>
            <li><b>Fecha:</b> ${dmy(fechaISO)}</li>
            <li><b>Grupo:</b> ${(nombreOperativoGrupo(grupo)).toString().toUpperCase()} (${(grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'')})</li>
            <li><b>Destino / Programa:</b> ${(grupo.destino||'—').toString().toUpperCase()} / ${(grupo.programa||'—').toString().toUpperCase()}</li>
            <li><b>Pax asistentes:</b> ${getSavedAsistencia(grupo, fechaISO, act.actividad)?.paxFinal ?? '—'}</li>
            <li><b>Coordinador(a):</b> ${coordNom || '—'}</li>
          </ul>
          <p><b>Observaciones:</b><br>${nota ? nota.replace(/\n/g,'<br>') : '—'}</p>
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
         `CONFIRMACIÓN DE ASISTENCIA — ${(act.actividad||'').toString().toUpperCase()} — ${dmy(fechaISO)} — ${(nombreOperativoGrupo(grupo)).toString().toUpperCase()} (${(grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'')})`;
   
       const fallbackBody =
   `ESTIMADOS ${(act.proveedor||'PROVEEDOR').toString().toUpperCase()}:
   
   CONFIRMAMOS LA ASISTENCIA PARA EL SERVICIO INDICADO:
   
   • ACTIVIDAD: ${(act.actividad||'').toString().toUpperCase()}
   • FECHA: ${dmy(fechaISO)}
   • GRUPO: ${(nombreOperativoGrupo(grupo)).toString().toUpperCase()} (${(grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'')})
   • DESTINO / PROGRAMA: ${(grupo.destino||'—').toString().toUpperCase()} / ${(grupo.programa||'—').toString().toUpperCase()}
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
   // === MAIL PATCH END ===

}

/* === IMPRESIÓN — helpers de formato === */
function formatDateReadable(isoStr){
  if(!isoStr) return '—';
  const [yyyy, mm, dd] = isoStr.split('-').map(Number);
  const d = new Date(yyyy, (mm||1)-1, dd||1);
  const wd = d.toLocaleDateString('es-CL', { weekday: 'long' });
  const name = wd.charAt(0).toUpperCase() + wd.slice(1);
  const ddp = String(dd||'').padStart(2,'0');
  const mmp = String(mm||'').padStart(2,'0');
  return `${name} ${ddp}/${mmp}`;
}

/* Construye el texto “simple y elegante” del despacho */
/* === DESPACHO (texto estilo Word) — con HOTELES, VUELOS, CONTACTOS, FINANZAS === */
function buildPrintTextDespacho(grupo, opts){
  // opts trae: { itinLines, hoteles, vuelos, contactos, finanzas }
  const { itinLines=[], hoteles=[], vuelos=[], contactos=[], finanzas=null } = (opts||{});

  const up = s => (s||'').toString().toUpperCase();
  const code = (grupo.numeroNegocio||'') + (grupo.identificador?('-'+grupo.identificador):'');
  const paxPlan = paxOf(grupo);
  const paxReal = paxRealOf(grupo);
  const { A: A_real, E: E_real } = paxBreakdown(grupo);

  // ===== Encabezado =====
   // ===== Encabezado (HTML) =====
   let out = '';
   out += '<div class="h1">DESPACHO DE VIAJE</div>\n';
   out += `<div><span class="b">GRUPO:</span> ${up(nombreOperativoGrupo(grupo))}  ·  <span class="b">CÓDIGO:</span> ${code}</div>\n`;
   out += `<div><span class="b">DESTINO:</span> ${up(grupo.destino||'—')}  ·  <span class="b">PROGRAMA:</span> ${up(grupo.programa||'—')}</div>\n`;
   out += `<div><span class="b">FECHAS:</span> ${dmy(grupo.fechaInicio||'')} — ${dmy(grupo.fechaFin||'')}  ·  <span class="b">PAX:</span> ${paxPlan}${paxReal?`  <span class="muted">(REAL ${paxReal} · A:${A_real} · E:${E_real})</span>`:''}</div>\n`;
   out += '<div>────────────────────────────────────────────────────────</div>\n\n';


  // ===== Hoteles =====
  out += `HOTELES (${hoteles.length||0}):\n\n`;
  if (!hoteles.length){
    out += '— SIN ASIGNACIÓN DE HOTELES —\n\n\n';
  } else {
    hoteles.forEach((h,i)=>{
      const nombre    = up(h.hotelNombre || h.hotel?.nombre || '');
      const ci        = dmy(toISO(h.checkIn));
      const co        = dmy(toISO(h.checkOut));
      const noches    = (h.noches!=='' && h.noches!=null)? String(h.noches): '';
      const estado    = up(h.status||'');
      const est       = h.estudiantes || {F:0,M:0,O:0};
      const estTot    = Number(h.estudiantesTotal ?? (est.F+est.M+est.O));
      const adu       = h.adultos || {F:0,M:0,O:0};
      const aduTot    = Number(h.adultosTotal ?? (adu.F+adu.M+adu.O));
      const hhab      = h.habitaciones || {};
      const habLine   = (hhab.singles!=null||hhab.dobles!=null||hhab.triples!=null||hhab.cuadruples!=null)
        ? `HABITACIONES: ${[
            (hhab.singles!=null?`SINGLES: ${hhab.singles}`:''),
            (hhab.dobles!=null?`DOBLES: ${hhab.dobles}`:''),
            (hhab.triples!=null?`TRIPLES: ${hhab.triples}`:''),
            (hhab.cuadruples!=null?`CUÁDRUPLES: ${hhab.cuadruples}`:'')
          ].filter(Boolean).join(' · ')}`
        : '';
      const dir       = up(h.hotel?.direccion || h.direccion || '');
      const tel       = up(h.hotel?.contactoTelefono || h.contactoTelefono || '');

      out += `NOMBRE: ${nombre}  CHECK-IN/OUT: ${ci} — ${co}${noches?`  NOCHES: ${noches}`:''}\n`;
      if (estado) out += `ESTADO: ${estado}  `;
      out += `ESTUDIANTES: F: ${est.F||0} · M: ${est.M||0} · O: ${est.O||0} (TOTAL ${estTot||0}) · ` +
             `ADULTOS: F: ${adu.F||0} · M: ${adu.M||0} · O: ${adu.O||0} (TOTAL ${aduTot||0})\n`;
      if (habLine) out += `${habLine}\n`;
      if (dir)     out += `DIRECCIÓN: ${dir}\n`;
      if (tel)     out += `TELÉFONO: ${tel}\n`;
      out += '\n';
    });
    out += '\n';
  }

  // ===== Transportes / Vuelos =====
  out += 'TRANSPORTE / VUELOS:\n\n';
  if (!vuelos.length){
    out += '— SIN VUELOS/TRANSPORTE —\n\n\n';
  } else {
    vuelos.forEach(v=>{
      const numero  = up(v.numero || v.tramos?.[0]?.numero || '');
      const empresa = up(v.proveedor || v.tramos?.[0]?.aerolinea || '');
      const ruta    = [up(v.origen || v.tramos?.[0]?.origen || ''), up(v.destino || v.tramos?.slice(-1)?.[0]?.destino || '')]
                       .filter(Boolean).join(' — ');
      const ida     = dmy(toISO(v.fechaIda)    || toISO(v.tramos?.[0]?.fechaIda) || '');
      const vta     = dmy(toISO(v.fechaVuelta) || toISO(v.tramos?.slice(-1)?.[0]?.fechaVuelta) || '');

      out += `N° / SERVICIO: ${numero || '—'}  EMPRESA: ${empresa || '—'}  RUTA: ${ruta || '—'}\n`;
      out += `IDA: ${ida||'—'}  VUELTA: ${vta||'—'}\n`;

      // tipo
      const isAereo   = (v.tipoTransporte || 'aereo') === 'aereo';
      const isMulti   = isAereo && v.tipoVuelo === 'regular' && Array.isArray(v.tramos) && v.tramos.length>0;
      if (isMulti){
        out += 'TIPO: REGULAR · MULTITRAMO\n';
        (v.tramos||[]).forEach((t,i)=>{
          const idaL = [ dmy(toISO(t.fechaIda)||''), t.presentacionIdaHora?`PRESENTACIÓN ${t.presentacionIdaHora}`:'', t.vueloIdaHora?`VUELO ${t.vueloIdaHora}`:'' ].filter(Boolean).join(' · ');
          const vtaL = toISO(t.fechaVuelta)
                        ? [ dmy(toISO(t.fechaVuelta)||''), t.presentacionVueltaHora?`PRESENTACIÓN ${t.presentacionVueltaHora}`:'', t.vueloVueltaHora?`VUELO ${t.vueloVueltaHora}`:'' ].filter(Boolean).join(' · ')
                        : '';
          out += `TRAMO ${i+1}: ${up(t.aerolinea||'')} ${up(t.numero||'')} — ${up(t.origen||'')} → ${up(t.destino||'')}\n`;
          out += `IDA: ${idaL}\n`;
          if (vtaL) out += `REGRESO: ${vtaL}\n`;
        });
      } else if (isAereo){
        const l1 = [ v.presentacionIdaHora?`PRESENTACIÓN ${v.presentacionIdaHora}`:'', v.vueloIdaHora?`VUELO ${v.vueloIdaHora}`:'' ].filter(Boolean).join(' · ');
        const l2 = [ v.presentacionVueltaHora?`PRESENTACIÓN ${v.presentacionVueltaHora}`:'', v.vueloVueltaHora?`VUELO ${v.vueloVueltaHora}`:'' ].filter(Boolean).join(' · ');
        out += `TIPO: AÉREO${v.tipoVuelo?` · ${up(v.tipoVuelo)}`:''}\n`;
        if (l1) out += `IDA: ${l1}\n`;
        if (l2) out += `REGRESO: ${l2}\n`;
      } else {
        out += 'TIPO: TERRESTRE (BUS)\n';
        if (v.idaHora || v.vueltaHora){
          if (v.idaHora)    out += `SALIDA BUS (IDA): ${v.idaHora}\n`;
          if (v.vueltaHora) out += `REGRESO BUS: ${v.vueltaHora}\n`;
        }
      }
      out += '\n';
    });
    out += '\n';
  }

  // ===== Itinerario (por días) =====
  out += 'ITINERARIO:  (ORDEN Y HORARIOS DE ACTIVIDADES PUEDEN SER MODIFICADOS)\n\n';
  // agrupar por fecha
  const byDate = new Map();
  (itinLines||[]).forEach(x => { if (!byDate.has(x.fechaISO)) byDate.set(x.fechaISO, []); byDate.get(x.fechaISO).push(x); });
  const fechas = Array.from(byDate.keys()).sort();
  fechas.forEach((f, idx)=>{
    out += `<div class="h2">DÍA ${idx+1} – ${formatDateReadable(f)}</div>\n`;
    const items = (byDate.get(f)||[]).slice().sort((a,b)=>(a.hora||'').localeCompare(b.hora||''));
    items.forEach(a=>{
      out += `${a.actividad}\n`;   // sin hora
    });
    out += '\n';
  });
  out += '\n';

  // ===== Contactos importantes =====
  out += 'CONTACTOS IMPORTANTES:\n\n';
  if (!contactos.length){
    out += '— SIN CONTACTOS —\n\n';
  } else {
    contactos.forEach(c=>{
      // formato: NOMBRE SERVICIO/PROVEEDOR · CONTACTO · TELEFONO · CORREO
      const line = [ up(c.etiqueta||c.nombre||''), up(c.persona||''), up(c.telefono||''), up(c.email||'') ]
                    .filter(Boolean).join(' · ');
      out += `${line}\n`;
    });
    out += '\n';
  }

  // ===== Finanzas (abonos) =====
  out += 'FINANZAS:\n\n';
  if (!finanzas || !Array.isArray(finanzas.rows)){
    out += '— SIN ABONOS REGISTRADOS —\n';
  } else {
    out += `ABONOS CLP ${ (finanzas.totales?.CLP||0).toLocaleString('es-CL') } · USD ${ (finanzas.totales?.USD||0) } · BRL ${ (finanzas.totales?.BRL||0) } · ARS ${ (finanzas.totales?.ARS||0) } · TOTAL CLP: ${ (finanzas.totalCLP||0).toLocaleString('es-CL') }\n\n`;
    finanzas.rows.forEach(r=>{
      out += `${up(r.asunto||'')} \n`;
      out += `FECHA: ${r.fecha||'—'}\n`;
      out += `MONEDA/VALOR: ${r.moneda||''} ${r.valor!=null?Number(r.valor).toLocaleString('es-CL'):''}\n`;
      if (r.medio) out += `MEDIO: ${up(r.medio)}\n`;
      if (r.detalle) out += `${up(r.detalle)}\n`;
      out += '\n';
    });
  }

  return out.trimEnd();
}

/* ====== IMPRESIÓN DE DESPACHO (PDF por print) ====== */

// Reúne una línea por actividad del itinerario con contacto de proveedor
async function collectItinLines(grupo){
  const out = [];
  const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);
  for (const f of fechas){
    // tolerar objeto indexado
    let acts = (grupo.itinerario && grupo.itinerario[f]) ? grupo.itinerario[f] : [];
    if (!Array.isArray(acts)) acts = Object.values(acts||{}).filter(x => x && typeof x === 'object');

    // Ocultar "Desayuno Hotel" en la recolección IMPRESIÓN (completa)
    acts = acts.filter(a => String(a?.actividad || '').toUpperCase() !== 'DESAYUNO HOTEL');

    // ordenar por hora
    acts = acts.slice().sort((a,b)=> timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

    for (const a of acts){
      try{
        const actName = (a?.actividad || '').toString();
        const servicio = await findServicio(grupo.destino, actName).catch(()=>null);
        const provNom  = (servicio?.proveedor || a?.proveedor || '').toString();
        let provDoc    = null;
        if (provNom) provDoc = await fetchProveedorByDestino((grupo.destino||'').toString().toUpperCase(), provNom).catch(()=>null);

        const telefono = (provDoc?.telefono || '').toString().toUpperCase();
        const correo   = (provDoc?.correo   || '').toString().toUpperCase();
        const contacto = (provDoc?.contacto || '').toString().toUpperCase();
        const provTxt  = (provDoc?.proveedor || provNom || '—').toString().toUpperCase();
        const estado   = (grupo?.serviciosEstado?.[f]?.[slug(actName)]?.estado || '').toString().toUpperCase();

        out.push({
          fechaISO: f,
          hora: '',
          actividad: actName.toUpperCase(),
          proveedor: provTxt,
          contacto: [contacto, telefono, correo].filter(Boolean).join(' · '),
          estado
        });
      }catch(_){}
    }
  }
  return out;
}

/* Recolector RÁPIDO: no consulta proveedores/servicios, solo usa lo que ya está en g.itinerario */
async function collectItinLinesFast(grupo){
  const out = [];
  const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);

  for (const f of fechas){
    // tolera objeto indexado
    let acts = (grupo.itinerario && grupo.itinerario[f]) ? grupo.itinerario[f] : [];
    if (!Array.isArray(acts)) acts = Object.values(acts||{}).filter(x => x && typeof x === 'object');

    // Ocultar "Desayuno Hotel" en la recolección IMPRESIÓN (rápida)
    acts = acts.filter(a => String(a?.actividad || '').toUpperCase() !== 'DESAYUNO HOTEL');

    // orden por hora
    acts = acts.slice().sort((a,b)=> timeVal(a?.horaInicio) - timeVal(b?.horaInicio));

    for (const a of acts){
      const actName = (a?.actividad || '').toString().toUpperCase();
      const estado  = (grupo?.serviciosEstado?.[f]?.[slug(actName)]?.estado || '').toString().toUpperCase();

      out.push({
        fechaISO: f,
        hora: a?.horaInicio || '--:--',
        actividad: actName,
        proveedor: (a?.proveedor || '').toString().toUpperCase(),  // lo que venga en el act
        contacto: '',                                              // sin consultas (rápido)
        estado
      });
    }
  }
  return out;
}

async function openPrintDespacho(g, w){
  if (!g){
    alert('No hay viaje activo.');
    return;
  }
  console.log('[PRINT] Inicia generación', { grupoId: g.id });

  // ==== DATOS BASE ====
  const code = (g.numeroNegocio||'') + (g.identificador?('-'+g.identificador):'');
  const paxPlan = paxOf(g);
  const paxReal = paxRealOf(g);
  const { A: A_real, E: E_real } = paxBreakdown(g);
  const fechasTxt = `${dmy(g.fechaInicio||'')} — ${dmy(g.fechaFin||'')}`;

  // 1) Itinerario (líneas con proveedor/contacto)
  console.time('[PRINT] collectItinLines');
  const itin = await collectItinLines(g).catch((e)=>{ console.error(e); return []; });
  console.timeEnd('[PRINT] collectItinLines');

  // 2) Finanzas: ABONOS (defensivo si faltan helpers externos)
  const haveLoadAbonos = (typeof loadAbonos === 'function');

  let abonos = [];
  if (haveLoadAbonos){
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

  const abonosRows = (abonos || []).map(a => ({
    fecha : dmy(toISO(a.fecha || '')),
    medio : (a.medio || '').toString().toUpperCase(),
    asunto: (a.asunto || '').toString().toUpperCase(),
    moneda: (a.moneda || '').toString().toUpperCase(),
    valor : Number(a.valor || 0),
  }));

  // ==== HTML IMPRESIÓN ====
  const css = `
  <style>
    @page { size: A4; margin: 14mm; }
    body { font-family: system-ui, Arial, sans-serif; font-size: 12px; color:#0a0a0a; }
    .head { display:flex; align-items:center; justify-content:space-between; margin-bottom:10px; }
    .logo { height: 40px; object-fit:contain; }
    h1 { font-size: 18px; margin: 0 0 6px; }
    h2 { font-size: 14px; margin: 12px 0 6px; border-bottom:1px solid #ddd; padding-bottom:4px; }
    .grid2 { display:grid; grid-template-columns: 1fr 1fr; gap:6px 16px; }
    .muted { color:#555; }
    table { width:100%; border-collapse:collapse; }
    th, td { padding:4px 6px; border-bottom:1px solid #eee; vertical-align:top; }
    th { text-align:left; font-size:11px; color:#444; }
    .t-tight td { padding:3px 4px; }
    .right { text-align:right; }
    .badge { font-weight:700; }
    .small { font-size:11px; }
    .cut { page-break-inside: avoid; }
    .footer { margin-top:8px; font-size:10px; color:#666; }
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
      <div><strong>GRUPO:</strong> ${(nombreOperativoGrupo(g)).toString().toUpperCase()}</div>
      <div><strong>CÓDIGO:</strong> ${code.toUpperCase()}</div>
      <div><strong>DESTINO:</strong> ${(g.destino||'—').toString().toUpperCase()}</div>
      <div><strong>PROGRAMA:</strong> ${(g.programa||'—').toString().toUpperCase()}</div>
      <div><strong>FECHAS:</strong> ${fechasTxt}</div>
      <div><strong>PAX:</strong> PLAN ${paxPlan} ${paxReal?` · REAL ${paxReal} (A:${A_real} · E:${E_real})`:''}</div>
    </div>
  `;

  const resumen = `
    <h2>RESUMEN</h2>
    <div class="small">VIAJE: ${fechasTxt} · DESTINO: ${(g.destino||'—').toString().toUpperCase()} · PROGRAMA: ${(g.programa||'—').toString().toUpperCase()}</div>
  `;

  const itinRows = (itin||[]).map(x => `
    <tr>
      <td>${dmy(x.fechaISO)}</td>
      <td>${(x.actividad||'').toString().toUpperCase()}</td>
      <td>${(x.proveedor||'').toString().toUpperCase()}</td>
      <td>${x.contacto||'—'}</td>
      <td>${(x.estado||'').toString().toUpperCase()}</td>
    </tr>`).join('');

  const itinerario = `
    <h2>ITINERARIO</h2>
    <table class="t-tight">
      <thead>
        <tr><th>FECHA</th><th>HORA</th><th>ACTIVIDAD</th><th>PROVEEDOR</th><th>CONTACTO</th><th>ESTADO</th></tr>
      </thead>
      <tbody>${itinRows || '<tr><td colspan="5" class="muted">SIN ACTIVIDADES.</td></tr>'}</tbody>
    </table>
  `;

  // Si no hay loadAbonos, ocultamos la tabla de abonos para no confundir
  const finRows = (abonosRows || []).map(r => `
    <tr>
      <td>${r.fecha || '—'}</td>
      <td>${r.medio || '—'}</td>
      <td>${r.asunto || '—'}</td>
      <td>${r.moneda || ''}</td>
      <td class="right">${(r.valor || 0).toLocaleString('es-CL')}</td>
    </tr>`).join('');

  const finanzas = haveLoadAbonos ? `
    <h2>FINANZAS — ABONOS</h2>
    <table class="t-tight">
      <thead>
        <tr><th>FECHA</th><th>MEDIO</th><th>ASUNTO</th><th>MONEDA</th><th class="right">MONTO</th></tr>
      </thead>
      <tbody>${finRows || '<tr><td colspan="5" class="muted">SIN ABONOS REGISTRADOS.</td></tr>'}</tbody>
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
        window.addEventListener('load', ()=>{
          try { window.print(); } catch(_) {}
          setTimeout(()=>{ try{ window.close(); }catch(_){ } }, 600);
        });
      </script>
    </body></html>
  `;

  // 3) Escribir en la ventana ya abierta
  try{
    w.document.open('text/html');
    w.document.write(html);
    w.document.close();
    console.log('[PRINT] HTML escrito en ventana.');
  }catch(e){
    console.error('[PRINT] No se pudo escribir el HTML final', e);
    alert('No se pudo escribir el documento de impresión.');
    try{ w.close(); }catch(_){}
  }
}

// Busca proveedor en la ruta por DESTINO (según la actividad/servicio)
async function fetchProveedorByDestino(destino, proveedorName){
  if (!destino || !proveedorName) return null;
  const destKey = String(destino).toUpperCase();
  const provKey = norm(proveedorName);

  if (!state.cache.proveedores) state.cache.proveedores = new Map();
  const cacheKey = `${destKey}::${provKey}`;
  if (state.cache.proveedores.has(cacheKey)){
    return state.cache.proveedores.get(cacheKey);
  }

  let hit = null;
  try{
    const qs = await getDocs(collection(db,'Proveedores', destKey, 'Listado'));
    qs.forEach(d=>{
      const x = d.data() || {};
      const nom = (x.proveedor || d.id || '').toString();
      if (norm(nom) === provKey) hit = { id:d.id, ...x };
    });
  }catch(e){
    console.error('[fetchProveedorByDestino]', e);
  }

  state.cache.proveedores.set(cacheKey, hit);
  return hit;
}

async function openActividadModal(
  g,
  fechaISO,
  act,
  servicio = null,
  tipoVoucher = 'NOAPLICA'
){
  const back = document.getElementById(
    'modalBack'
  );

  const title = document.getElementById(
    'modalTitle'
  );

  const body = document.getElementById(
    'modalBody'
  );

  const actName = (
    act?.actividad ||
    'ACTIVIDAD'
  ).toString();

  const destino = (
    act?.servicioDestino ||
    g?.destino ||
    ''
  ).toString().toUpperCase().trim();

  /*
    Si el servicio no llegó resuelto, buscarlo usando
    año, destino, ID y nombre.
  */
  if (!servicio) {
    try {
      servicio = await findServicio({
        destino,
        anoViaje:
          g?.anoViaje ||
          state?.anoViajeActivo ||
          '',
        servicioId:
          act?.servicioId ||
          '',
        nombre:
          actName
      });
    } catch (_) {
      servicio = null;
    }
  }

  /*
    Resolver el hilo global usado por Tips/Comentarios.
  */
  let thread = {
    key: '',
    scope: 'B'
  };

  try {
    thread = await resolveThreadKey(
      g,
      fechaISO,
      act,
      servicio
    );
  } catch (_) {}

  /*
    Consultar proveedor general como fallback.
  */
  const proveedorNombreBuscado =
    valorServicioPrimero(
      servicio?.proveedor,
      act?.proveedor
    );

  let proveedorDoc = null;

  try {
    if (proveedorNombreBuscado) {
      proveedorDoc =
        await fetchProveedorByDestino(
          destino,
          proveedorNombreBuscado
        );
    }
  } catch (_) {
    proveedorDoc = null;
  }

  /*
    Prioridad:
    1. Servicio específico.
    2. Actividad del itinerario.
    3. Proveedor general.
  */
  const nombreProveedor =
    valorServicioPrimero(
      servicio?.proveedor,
      act?.proveedor,
      proveedorDoc?.proveedor,
      proveedorDoc?.nombre
    );

  const nombreContacto =
    valorServicioPrimero(
      servicio?.contacto,
      servicio?.nombreContacto,
      act?.contacto,
      act?.nombreContacto,
      proveedorDoc?.contacto,
      proveedorDoc?.nombreContacto
    );

  const telefono =
    valorServicioPrimero(
      servicio?.telefono,
      servicio?.fono,
      act?.telefono,
      act?.fono,
      proveedorDoc?.telefono,
      proveedorDoc?.fono
    );

  const correo =
    valorServicioPrimero(
      servicio?.correo,
      servicio?.email,
      act?.correo,
      act?.email,
      proveedorDoc?.correo,
      proveedorDoc?.email
    );

  const direccion =
    valorServicioPrimero(
      servicio?.direccion,
      act?.direccion,
      proveedorDoc?.direccion
    );

  const ciudad =
    valorServicioPrimero(
      servicio?.ciudad,
      act?.ciudad
    );

  const indicaciones =
    valorServicioPrimero(
      servicio?.indicaciones,
      servicio?.instrucciones,
      act?.indicaciones,
      act?.instrucciones
    );

  const restricciones =
    valorServicioPrimero(
      servicio?.restricciones,
      act?.restricciones
    );

  const voucherRaw = valorServicioPrimero(
    servicio?.voucher,
    tipoVoucher,
    'NO APLICA'
  );

  const voucherLabel =
    /electron/i.test(voucherRaw)
      ? 'ELECTRÓNICO'
      : /fisic/i.test(voucherRaw)
        ? 'FÍSICO'
        : /correo/i.test(voucherRaw)
          ? 'CORREO'
          : /ticket/i.test(voucherRaw)
            ? 'TICKET'
            : 'NO APLICA';

  const mensajeWhatsApp =
    construirMensajeWhatsAppServicio(
      g,
      fechaISO,
      act
    );

  const asuntoCorreo = [
    'CONSULTA SERVICIO RAI TRAI',
    actName.toUpperCase(),
    fechaISO ? dmy(fechaISO) : ''
  ]
    .filter(Boolean)
    .join(' — ');

  const accionesContacto =
    construirAccionesContactoServicio({
      telefono,
      correo,
      direccion,
      ciudad,
      destino,
      mensajeWhatsApp,
      asuntoCorreo
    });

  const scopeBadge =
    thread.scope === 'A'
      ? 'PROVEEDOR'
      : thread.scope === 'C'
        ? 'HOTEL/COMIDA'
        : 'GENERAL';

  title.textContent =
    `DETALLE — ${actName.toUpperCase()} — ${dmy(fechaISO)}`;

  body.innerHTML = `
    <div class="meta">
      <strong>HILO:</strong>
      ${escapeHTMLServicio(scopeBadge)}
      ${thread.key
        ? ` · ${escapeHTMLServicio(thread.key)}`
        : ''
      }
    </div>

    <div class="card">
      <h4 style="margin-top:0">
        INFORMACIÓN DEL SERVICIO
      </h4>

      <div class="meta">
        <strong>ACTIVIDAD:</strong>
        ${escapeHTMLServicio(actName.toUpperCase())}
      </div>

      <div class="meta">
        <strong>PROVEEDOR:</strong>
        ${escapeHTMLServicio(
          nombreProveedor
            ? nombreProveedor.toUpperCase()
            : '—'
        )}
      </div>

      ${nombreContacto
        ? `
          <div class="meta">
            <strong>CONTACTO:</strong>
            ${escapeHTMLServicio(
              nombreContacto.toUpperCase()
            )}
          </div>
        `
        : ''
      }

      ${telefono
        ? `
          <div class="meta">
            <strong>TELÉFONO:</strong>
            ${escapeHTMLServicio(telefono)}
          </div>
        `
        : ''
      }

      ${correo
        ? `
          <div class="meta">
            <strong>CORREO:</strong>
            ${escapeHTMLServicio(
              correo.toLowerCase()
            )}
          </div>
        `
        : ''
      }

      ${ciudad
        ? `
          <div class="meta">
            <strong>CIUDAD:</strong>
            ${escapeHTMLServicio(
              ciudad.toUpperCase()
            )}
          </div>
        `
        : ''
      }

      ${direccion
        ? `
          <div class="meta">
            <strong>DIRECCIÓN:</strong>
            ${escapeHTMLServicio(
              direccion.toUpperCase()
            )}
          </div>
        `
        : ''
      }

      <div class="meta">
        <strong>HORARIO:</strong>
        ${escapeHTMLServicio(
          act?.horaInicio ||
          '--:--'
        )}
        –
        ${escapeHTMLServicio(
          act?.horaFin ||
          '--:--'
        )}
      </div>

      <div class="meta">
        <strong>VOUCHER:</strong>
        ${escapeHTMLServicio(voucherLabel)}
      </div>

      ${accionesContacto}
    </div>

    <div class="act">
      <h4>INDICACIONES</h4>

      ${indicaciones
        ? `
          <div
            class="meta"
            style="white-space:pre-wrap"
          >${autoLinkPhones(
            escapeHTMLServicio(
              indicaciones.toUpperCase()
            )
          )}</div>
        `
        : `
          <div class="muted">
            SIN INDICACIONES.
          </div>
        `
      }

      ${restricciones
        ? `
          <div
            class="meta"
            style="
              white-space:pre-wrap;
              margin-top:.65rem;
            "
          >
            <strong>RESTRICCIONES:</strong><br>
            ${escapeHTMLServicio(
              restricciones.toUpperCase()
            )}
          </div>
        `
        : ''
      }
    </div>

    <div class="act" id="foroBox">
      <h4>TIPS O COMENTARIOS</h4>

      <div
        class="rowflex"
        style="margin:.35rem 0"
      >
        <textarea
          id="foroText"
          placeholder="ESCRIBE UN COMENTARIO (SE PUBLICA CON TU CORREO)"
        ></textarea>

        <button
          id="foroSend"
          class="btn ok"
        >
          PUBLICAR
        </button>
      </div>

      <div class="muted">-------</div>

      <div
        id="foroList"
        style="
          display:grid;
          gap:.4rem;
          margin-top:.5rem;
        "
      ></div>

      <div
        class="rowflex"
        style="
          justify-content:center;
          margin-top:.4rem;
        "
      >
        <button
          id="foroMore"
          class="btn sec"
          style="display:none"
        >
          CARGAR MÁS
        </button>
      </div>
    </div>
  `;

  /*
    Paginación de Tips/Comentarios.
  */
  const paging = {
    cursor: null,
    exhausted: false,
    loading: false,
    pageSize: 10,
    items: []
  };

  const renderForo = () => {
    const wrap =
      body.querySelector('#foroList');

    wrap.innerHTML = '';

    const staff = paging.items
      .filter(item => item.isStaff)
      .sort((a, b) => b.tsMs - a.tsMs);

    const resto = paging.items
      .filter(item => !item.isStaff)
      .sort((a, b) => b.tsMs - a.tsMs);

    const ordered = [
      ...staff,
      ...resto
    ];

    if (!ordered.length) {
      wrap.innerHTML =
        '<div class="muted">AÚN NO HAY COMENTARIOS.</div>';

      const moreBtn =
        body.querySelector('#foroMore');

      if (moreBtn) {
        moreBtn.style.display = 'none';
      }

      return;
    }

    ordered.forEach(item => {
      const div =
        document.createElement('div');

      div.className = 'card';

      div.innerHTML = `
        <div
          class="meta"
          style="
            display:flex;
            gap:.5rem;
            align-items:center;
          "
        >
          ${item.isStaff
            ? `
              <span
                class="badge"
                style="
                  background:#1d4ed8;
                  color:#fff;
                "
              >
                STAFF
              </span>
            `
            : ''
          }

          <strong>
            ${escapeHTMLServicio(
              (item.byEmail || '').toUpperCase()
            )}
          </strong>

          · ${escapeHTMLServicio(
            fmtFechaHoraMs(
              item.tsMs ||
              Date.now()
            )
          )}
        </div>

        <div
          style="
            margin-top:.25rem;
            white-space:pre-wrap;
          "
        >${escapeHTMLServicio(
          (item.texto || '').toString().toUpperCase()
        )}</div>
      `;

      wrap.appendChild(div);
    });

    const moreBtn =
      body.querySelector('#foroMore');

    if (moreBtn) {
      moreBtn.style.display =
        paging.exhausted
          ? 'none'
          : '';
    }
  };

  const loadPage = async () => {
    if (
      paging.loading ||
      paging.exhausted
    ) {
      return;
    }

    if (!thread.key) {
      paging.exhausted = true;
      renderForo();
      return;
    }

    paging.loading = true;

    try {
      let consulta = query(
        threadColl(thread.key),
        orderBy('ts', 'desc'),
        limit(paging.pageSize + 1)
      );

      if (paging.cursor) {
        consulta = query(
          threadColl(thread.key),
          orderBy('ts', 'desc'),
          startAfter(paging.cursor),
          limit(paging.pageSize + 1)
        );
      }

      const snap = await getDocs(
        consulta
      );

      const documentos = snap.docs;

      if (
        documentos.length >
        paging.pageSize
      ) {
        paging.cursor =
          documentos[paging.pageSize - 1];
      } else {
        paging.cursor =
          documentos[documentos.length - 1] ||
          paging.cursor;

        paging.exhausted = true;
      }

      const nuevos = documentos
        .slice(0, paging.pageSize)
        .map(documento => {
          const data =
            documento.data() ||
            {};

          const tsMs =
            data.ts?.seconds
              ? data.ts.seconds * 1000
              : Date.now();

          return {
            id: documento.id,
            texto: String(
              data.texto ||
              ''
            ),
            byEmail: String(
              data.byEmail ||
              data.by ||
              ''
            ).toLowerCase(),
            isStaff: !!data.isStaff,
            tsMs
          };
        });

      const vistos = new Set(
        paging.items.map(
          item => item.id
        )
      );

      nuevos.forEach(item => {
        if (!vistos.has(item.id)) {
          paging.items.push(item);
        }
      });

      renderForo();
    } catch (error) {
      console.error(
        'FORO loadPage',
        error
      );

      alert(
        'NO SE PUDO CARGAR COMENTARIOS.'
      );
    } finally {
      paging.loading = false;
    }
  };

  const foroMore =
    body.querySelector('#foroMore');

  if (foroMore) {
    foroMore.onclick = loadPage;
  }

  const foroSend =
    body.querySelector('#foroSend');

  if (foroSend) {
    foroSend.onclick = async () => {
      const textarea =
        body.querySelector('#foroText');

      const texto = (
        textarea?.value ||
        ''
      ).trim();

      if (!texto) {
        alert(
          'ESCRIBE UN COMENTARIO.'
        );
        return;
      }

      if (!thread.key) {
        alert(
          'NO SE PUDO IDENTIFICAR EL HILO DE ESTA ACTIVIDAD.'
        );
        return;
      }

      try {
        await addDoc(
          threadColl(thread.key),
          {
            texto,
            byUid:
              state.user.uid,

            byEmail: (
              state.user.email ||
              ''
            ).toLowerCase(),

            isStaff:
              !!state.is,

            ts:
              serverTimestamp()
          }
        );

        textarea.value = '';

        paging.cursor = null;
        paging.exhausted = false;
        paging.items = [];

        await loadPage();
      } catch (error) {
        console.error(
          'FORO send',
          error
        );

        alert(
          'NO SE PUDO PUBLICAR.'
        );
      }
    };
  }

  document.getElementById(
    'modalClose'
  ).onclick = () => {
    back.style.display = 'none';
  };

  back.style.display = 'flex';

  await loadPage();
}

async function setEstadoServicio(g, fechaISO, act, estado, logBitacora=false){
  try{
    const key=slug(act.actividad||'');
    const path=doc(db,'grupos',g.id);
    const payload={}; payload[`serviciosEstado.${fechaISO}.${key}`]={ estado, updatedAt: serverTimestamp(), by:(state.user.email||'').toLowerCase() };
    await updateDoc(path,payload);
    (g.serviciosEstado ||= {}); (g.serviciosEstado[fechaISO] ||= {}); g.serviciosEstado[fechaISO][key]={estado};
    document.getElementById('modalBack').style.display='none';
    renderItinerario(g, document.getElementById('paneItin'), fechaISO);

    if(logBitacora){
      const timeId = timeIdNowMs();
      const ref = doc(db,'grupos',g.id,'bitacora',key,fechaISO,timeId);
      await setDoc(ref, {
        texto: `ACTIVIDAD ${estado.toLowerCase()}`,
        byUid: state.user.uid,
        byEmail: (state.user.email||'').toLowerCase(),
        ts: serverTimestamp()
      });
    }
  }catch(e){ console.error(e); alert('NO FUE POSIBLE ACTUALIZAR EL ESTADO.'); }
}

/* ====== VIAJE: RESTABLECER (STAFF) ====== */
async function resetInicioFinViaje(grupo){
  if (!state.is){
    alert('Solo el STAFF puede restablecer el inicio/fin de viaje.');
    return;
  }
  if (!confirm('¿Restablecer INICIO/FIN DE VIAJE y borrar PAX VIAJANDO?')){
    return;
  }
  try{
    const ref = doc(db, 'grupos', grupo.id);

    // Borra override y marcas de inicio/fin (cubrimos nombres posibles)
    await updateDoc(ref, {
      paxViajando: deleteField(),
      trip: deleteField(),
      viaje: deleteField(),
      viajeInicioAt: deleteField(),
      viajeFinAt: deleteField(),
      viajeInicioBy: deleteField(),
      viajeFinBy: deleteField(),
    });

    // Limpia en memoria/local
    delete grupo.paxViajando;
    delete grupo.trip;
    delete grupo.viaje;
    delete grupo.viajeInicioAt;
    delete grupo.viajeFinAt;
    delete grupo.viajeInicioBy;
    delete grupo.viajeFinBy;
    try{
      localStorage.removeItem('rt__paxStart_'+grupo.id);
    }catch(_){}

    // Re-render para que desaparezca el “tachado”
    await renderOneGroup(grupo);
  }catch(e){
    console.error(e);
    alert('No se pudo restablecer el viaje.');
  }
}

/* ====== ALERTAS ====== */

/** AYUDA: OBTENER NOMBRE POR EMAIL (MAYÚSCULAS) */
function upperNameByEmail(email){
  const e=(email||'').toLowerCase();
  const c=state.coordinadores.find(x=>(x.email||'').toLowerCase()===e);
  const n=(c?.nombre||'').toString().toUpperCase();
  return n || e.toUpperCase();
}

/** DESTINATARIOS POR FILTROS (DESTINOS, RANGO/FECHA) ESCANEANDO TODOS LOS GRUPOS */
async function recipientsFromFilters(destinos, rango){
  // Normaliza inputs
  const wants = (Array.isArray(destinos) ? destinos : [])
    .map(x => String(x || '').trim().toUpperCase())
    .filter(Boolean);
  const wantAll = new Set(wants); // AND: deben cumplirse todos los destinos listados

  // parseo de fecha/rango: "DD-MM-AAAA" o "DD-MM-AAAA.DD-MM-AAAA"
  const parseDMY = s => {
    const m = String(s||'').match(/^(\d{2})-(\d{2})-(\d{4})$/);
    if (!m) return null;
    const [_, dd, mm, yyyy] = m;
    return `${yyyy}-${mm}-${dd}`; // ISO simple
  };
  let sinceISO = null, untilISO = null;
  if (rango){
    const parts = String(rango).split('.').map(x => x.trim()).filter(Boolean);
    if (parts.length === 2){
      sinceISO = parseDMY(parts[0]);
      untilISO = parseDMY(parts[1]);
    } else if (parts.length === 1){
      sinceISO = parseDMY(parts[0]);
      untilISO = parseDMY(parts[0]);
    }
  }

  // helper de fecha: incluye si cae dentro (inclusive); si no hay rango, no filtra
  const dateOK = (g) => {
    const d = String(g?.fechaActividad || g?.fecha || g?.fechaInicio || '').slice(0,10);
    if (!sinceISO || !untilISO) return true;
    return (d >= sinceISO && d <= untilISO);
  };

  // mapa email→coordId
  const mapEmailToId = new Map((state.coordinadores || [])
    .map(c => [String(c.email || '').toLowerCase(), c.id]));

  // Acumulador por coordId de destinos cumplidos
  const matchedByCoord = new Map(); // coordId -> Set(destinosCumplidos)

  // Consulta grupos (puedes optimizar con filtros si tienes índices)
  const snap = await getDocs(collection(db, 'grupos'));
  snap.forEach(d => {
    const g = { id: d.id, ...(d.data() || {}) };

    // fecha
    if (!dateOK(g)) return;

    // destino del grupo normalizado
    const gd = String(g.destino || g.Destino || '').toUpperCase().trim();
    if (wantAll.size > 0 && !wantAll.has(gd)) return; // si quiero AND, solo cuenta si el grupo está en alguno de los "wants"

    // coord ids del grupo
    const ids = coordDocIdsOf ? coordDocIdsOf(g) : []; // usa tu helper existente si está definido
    if (!ids || !ids.length){
      // fallback por email si no hay ids
      (emailsOf ? emailsOf(g) : []).forEach(e => {
        const cid = mapEmailToId.get(String(e || '').toLowerCase());
        if (cid) ids.push(cid);
      });
    }
    if (!ids.length) return;

    // marca destino cumplido para cada coord
    ids.forEach(cid => {
      if (!matchedByCoord.has(cid)) matchedByCoord.set(cid, new Set());
      if (gd) matchedByCoord.get(cid).add(gd);
    });
  });

  // Devuelve solo los coordIds que cumplen TODOS los destinos solicitados (AND)
  const out = [];
  if (wantAll.size === 0){
    // sin destinos: devuelve TODOS los coordinadores que tienen grupos en el rango (si rango existe)
    matchedByCoord.forEach((_set, cid) => out.push(cid));
  } else {
    matchedByCoord.forEach((setD, cid) => {
      // deben estar todos los "wants" dentro de setD
      let ok = true;
      for (const w of wantAll){ if (!setD.has(w)) { ok = false; break; } }
      if (ok) out.push(cid);
    });
  }
  return out;
}

/** MODAL: CREAR ALERTA () */
async function openCreateAlertModal(){
  const back=document.getElementById('modalBack'), body=document.getElementById('modalBody'), title=document.getElementById('modalTitle');
  title.textContent='CREAR ALERTA';
  const coordOpts=state.coordinadores.map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()} — ${(c.email||'').toUpperCase()}</option>`).join('');
  body.innerHTML=`
    <div class="rowflex">
      <input id="alertDestinos" type="text" placeholder="DESTINOS (SEPARADOS POR COMA = AND)"/>
      <input id="alertRango" type="text" placeholder="FECHA (DD-MM-AAAA) o RANGO DD-MM-AAAA.DD-MM-AAAA"/>
    </div>
  
    <div class="rowflex" style="align-items:center;justify-content:space-between;gap:.5rem">
      <label style="margin:0">DESTINATARIOS (COORDINADORES)</label>
      <label style="margin:0;font-size:.9rem">
        <input type="checkbox" id="alertSelectAll"/> SELECCIONAR TODOS (VISIBLES)
      </label>
    </div>
  
    <div class="rowflex">
      <select id="alertCoords" multiple size="10" style="width:100%">${coordOpts}</select>
    </div>
  
    <div class="rowflex" style="justify-content:flex-end">
      <div id="alertCount" class="muted" style="font-size:.9rem">0 encontrados</div>
    </div>
  
    <div class="rowflex"><textarea id="alertMsg" placeholder="MENSAJE" style="width:100%"></textarea></div>
    <div class="rowflex"><button id="alertSave" class="btn ok">ENVIAR</button></div>`;

  // cache: todos los coordinadores (no renombramos nada)
  const _allCoords = (state.coordinadores || []).map(c => ({
    id: c.id,
    email: String(c.email || '').toLowerCase(),
    nombre: (c.nombre || '').toString().toUpperCase()
  }));
  
  const $dest   = document.getElementById('alertDestinos');
  const $rango  = document.getElementById('alertRango');
  const $sel    = document.getElementById('alertCoords');
  const $count  = document.getElementById('alertCount');
  const $all    = document.getElementById('alertSelectAll');
  
  let _lastFilterIds = null; // ids filtrados por (destinos AND + rango), para mostrar en el select
  
  function renderCoordOptions(ids){
    const show = Array.isArray(ids) && ids.length ? new Set(ids) : null;
    const opts = (show
      ? _allCoords.filter(c => show.has(c.id))
      : _allCoords
    );
  
    $sel.innerHTML = opts.map(c =>
      `<option value="${c.id}">${c.nombre} — ${c.email.toUpperCase()}</option>`
    ).join('');
    $count.textContent = `${opts.length} encontrados`;
  }
  
  let _fTimer = null;
  async function applyCoordFilter(){
    // lee filtros de UI
    const destinosRaw = ($dest.value || '');
    const rangoRaw    = ($rango.value || '');
  
    try{
      // usa tu función existente para consultar por filtros
      const ids = await recipientsFromFilters(
        destinosRaw.split(',').map(x => x.trim()).filter(Boolean), // coma = AND (lo reforzamos en la función abajo)
        rangoRaw.trim()
      );
      _lastFilterIds = Array.isArray(ids) ? ids : [];
    }catch(e){
      console.error('[ALERTAS] filtro coordinadores', e);
      _lastFilterIds = [];
    }
    renderCoordOptions(_lastFilterIds);
    // si estaba activado "seleccionar todos", re-aplicamos selección
    if ($all && $all.checked){
      for (const opt of $sel.options) opt.selected = true;
    }
  }
  
  // eventos de filtro (con debounce simple)
  function debouncedFilter(){
    clearTimeout(_fTimer);
    _fTimer = setTimeout(applyCoordFilter, 250);
  }
  $dest.addEventListener('input', debouncedFilter);
  $rango.addEventListener('input', debouncedFilter);
  
  // seleccionar todos (visibles)
  if ($all){
    $all.onchange = () => {
      const on = !!$all.checked;
      for (const opt of $sel.options) opt.selected = on;
    };
  }
  
  // primera pasada: ya filtra si hay valores pre-cargados; si no, lista completa
  applyCoordFilter();

  document.getElementById('alertSave').onclick = async () => {
    const msg   = (document.getElementById('alertMsg').value || '').trim();
    const sel   = Array.from(document.getElementById('alertCoords').selectedOptions).map(o => o.value);
    const dests = (document.getElementById('alertDestinos').value || '').split(',').map(x => x.trim()).filter(Boolean);
    const rango = (document.getElementById('alertRango').value || '').trim();
  
    if (!msg && !dests.length){ alert('ESCRIBE UN MENSAJE O USA FILTROS.'); return; }
  
    let forCoordIds = sel.slice(); // prioridad a selección manual
    if (forCoordIds.length === 0){
      try {
        const fromFilters = await recipientsFromFilters(dests, rango);
        forCoordIds = Array.isArray(fromFilters) ? fromFilters.slice() : [];
      } catch(e) {
        console.error(e);
      }
    }
  
    if (!forCoordIds.length){
      alert('NO HAY DESTINATARIOS. REVISA FILTROS/SELECCIÓN.');
      return;
    }
  
    await addDoc(collection(db,'alertas'),{
      audience:'coord',
      mensaje: msg.toUpperCase(),
      forCoordIds,
      meta:{ filtros:{ destinos: dests, rango } },
      createdAt: serverTimestamp(),
      createdBy:{ uid: state.user.uid, email: (state.user.email || '').toLowerCase() },
      readBy:{}
    });
  
    document.getElementById('modalBack').style.display = 'none';
    await window.renderGlobalAlertsV2();
  };

  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}

// ====== PANEL GLOBAL DE ALERTAS (COMPLETO) ======
async function renderGlobalAlerts(){
  const box = document.getElementById('alertsPanel');
  if (!box) return;

  // Estado inicial (evita parpadeo)
  box.innerHTML = `
    <div class="alert-head" style="display:flex;align-items:center;gap:.5rem;justify-content:space-between">
      <h4 style="margin:0">ALERTAS</h4>
      ${state.is ? '<button id="btnCreateAlert" class="btn ok">CREAR ALERTA</button>' : ''}
    </div>
    <div class="muted">CARGANDO…</div>
  `;

  // ADICIÓN: engancha el botón del HTML inicial (si existe y si es STAFF)
  if (state.is) {
    const btn = document.getElementById('btnCreateAlert');
    if (btn) btn.onclick = openCreateAlertModal;
  }

  // Cargar todas las alertas
  let all = [];
  try{
    const qs = await getDocs(collection(db,'alertas'));
    qs.forEach(d => all.push({ id:d.id, ...d.data() }));
  }catch(e){
    console.error(e);
    box.innerHTML = '<div class="muted">NO SE PUDIERON CARGAR LAS ALERTAS.</div>';
    return;
  }

  // Coord actual (para filtrar "para mí")
  const myCoordId = state.is
    ? (state.viewingCoordId || (state.coordinadores.find(c => (c.email||'').toLowerCase() === (state.user.email||'').toLowerCase())?.id || 'self'))
    : (state.coordinadores.find(c => (c.email||'').toLowerCase() === (state.user.email||'').toLowerCase())?.id || 'self');

  // Dos listas: a) “para mí” (audience !== '' y me incluye), b) “ops” (audience === '' → Operaciones)
  const paraMi = all.filter(a => (a.audience !== '') && Array.isArray(a.forCoordIds) && a.forCoordIds.includes(myCoordId));
  const ops    = state.is ? all.filter(a => a.audience === '') : [];

  // Renderizador de listas con pestañas "No leídas / Leídas"
  const renderList = (arr, scope) => {
    const readerKey = (scope === 'ops') ? `:${(state.user.email||'').toLowerCase()}` : `coord:${myCoordId}`;

    const isRead = (a) => {
      const rb = a.readBy || {};
      if (scope === 'ops') {
        // para ops marcamos lectura por email con prefijo ':'
        return Object.keys(rb||{}).some(k => k.startsWith(':'));
      }
      return !!rb[readerKey];
    };

    const unread = arr.filter(a => !isRead(a));
    const read   = arr.filter(a =>  isRead(a));

    const mkReadersLine = (a) => {
      const rb = a.readBy || {};
      const entries = Object.entries(rb).map(([k,v]) => {
        const who  = (k || '').toString().toUpperCase();
        const when = (v?.seconds) ? new Date(v.seconds*1000).toLocaleString('es-CL').toUpperCase() : '';
        return `${who}${when ? (' · ' + when) : ''}`;
      });
      return entries.length ? `<div class="meta"><strong>LEÍDO POR:</strong> ${entries.join(' · ')}</div>` : '';
    };

    const mkCard = (a) => {
      const li = document.createElement('div');
      li.className = 'alert-card';

      const fecha = a.createdAt?.seconds ? new Date(a.createdAt.seconds*1000).toLocaleDateString('es-CL').toUpperCase() : '';
      const autorEmail  = (a.createdBy?.email || '').toUpperCase();
      const autorNombre = upperNameByEmail(a.createdBy?.email || '');
      const gi = a.groupInfo || null;

      const cab = (scope === 'ops') ? 'NUEVO COMENTARIO' : 'NOTIFICACIÓN';
      const tipoCoord = (scope !== 'ops')
        ? (Array.isArray(a.forCoordIds) && a.forCoordIds.length > 1 ? 'GLOBAL' : 'PERSONAL')
        : null;

      li.innerHTML = `
        <div class="alert-title">${cab}${tipoCoord ? ` · ${tipoCoord}` : ''}</div>
        <div class="meta">FECHA: ${fecha} · AUTOR: ${autorNombre} (${autorEmail})</div>
        ${gi ? `
          <div class="meta">GRUPO: ${(gi.nombre||'').toString().toUpperCase()} (${(gi.code||'').toString().toUpperCase()}) · DESTINO: ${(gi.destino||'').toString().toUpperCase()} · PROGRAMA: ${(gi.programa||'').toString().toUpperCase()}</div>
          <div class="meta">FECHA ACTIVIDAD: ${dmy(gi.fechaActividad||'')} · ACTIVIDAD: ${(gi.actividad||'').toString().toUpperCase()}</div>
        ` : ''}
        <div style="margin:.45rem 0">${(a.mensaje||'').toString().toUpperCase()}</div>
        ${mkReadersLine(a)}
        <div class="rowflex"><button class="btn ok btnRead">CONFIRMAR LECTURA</button></div>
      `;

      li.querySelector('.btnRead').onclick = async () => {
        try{
          const path = doc(db,'alertas', a.id);
          const payload = {};
          if (scope === 'ops') {
            payload[`readBy.:${(state.user.email||'').toLowerCase()}`] = serverTimestamp();
          } else {
            payload[`readBy.coord:${myCoordId}`] = serverTimestamp();
          }
          await updateDoc(path, payload);
          await window.renderGlobalAlertsV2();
        }catch(e){
          console.error(e);
          alert('NO SE PUDO CONFIRMAR.');
        }
      };

      return li;
    };

    const wrap = document.createElement('div');
    const tabs = document.createElement('div'); tabs.className = 'tabs';
    const t1 = document.createElement('div'); t1.className = 'tab active'; t1.textContent = `NO LEÍDAS (${unread.length})`;
    const t2 = document.createElement('div'); t2.className = 'tab';         t2.textContent = `LEÍDAS (${read.length})`;
    tabs.appendChild(t1); tabs.appendChild(t2); wrap.appendChild(tabs);

    const cont = document.createElement('div'); wrap.appendChild(cont);

    const renderTab = (which) => {
      cont.innerHTML = '';
      t1.classList.toggle('active', which === 'unread');
      t2.classList.toggle('active', which === 'read');
      const arr2 = (which === 'unread') ? unread : read;
      if (!arr2.length) {
        cont.innerHTML = '<div class="muted">SIN MENSAJES.</div>';
        return;
      }
      arr2.forEach(a => cont.appendChild(mkCard(a)));
    };

    t1.onclick = () => renderTab('unread');
    t2.onclick = () => renderTab('read');
    renderTab('unread');

    return { ui: wrap, unreadCount: unread.length, readCount: read.length };
  };

  // Construcción final del panel
  const head = document.createElement('div');
  head.className = 'alert-head';
  head.style.cssText = 'display:flex;align-items:center;gap:.5rem;justify-content:space-between';

  const left = document.createElement('div');
  left.innerHTML = '<h4 style="margin:0">ALERTAS</h4>';

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

  const area = document.createElement('div');

  const mi = renderList(paraMi, 'mi');
  const op = state.is ? renderList(ops, 'ops') : null;

  // Limpia y compone
  box.innerHTML = '';
  box.appendChild(head);

  // Sección "Para mí"
  const secMi = document.createElement('div');
  secMi.className = 'act';
  secMi.innerHTML = `<h4>PARA MÍ</h4>`;
  secMi.appendChild(mi ? mi.ui : document.createTextNode(''));
  if (!mi || (mi.unreadCount + mi.readCount) === 0){
    const empty = document.createElement('div');
    empty.className = 'muted';
    empty.textContent = 'SIN ALERTAS.';
    secMi.appendChild(empty);
  }
  area.appendChild(secMi);

  // Sección "Operaciones" (solo STAFF)
  if (state.is){
    const secOp = document.createElement('div');
    secOp.className = 'act';
    secOp.innerHTML = `<h4>OPERACIONES</h4>`;
    secOp.appendChild(op ? op.ui : document.createTextNode(''));
    if (!op || (op.unreadCount + op.readCount) === 0){
      const empty2 = document.createElement('div');
      empty2.className = 'muted';
      empty2.textContent = 'SIN MENSAJES PARA OPERACIONES.';
      secOp.appendChild(empty2);
    }
    area.appendChild(secOp);
  }

  box.appendChild(area);

  // Hook del botón (en caso de que el DOM se haya recreado)
  const btnCreate = box.querySelector('#btnCreateAlert');
  if (btnCreate) btnCreate.onclick = openCreateAlertModal;
}
/* ====== GASTOS ====== */
async function renderGastos(g, pane, paneRef){
  pane.innerHTML='';
  const form=document.createElement('div'); form.className='act';
  form.innerHTML=`
    <h4>REGISTRAR GASTO</h4>
    <div class="rowflex" style="margin:.4rem 0">
      <input id="spAsunto" type="text" placeholder="ASUNTO"/>
    </div>
    <div class="rowflex" style="margin:.4rem 0">
      <select id="spMoneda">
        <option value="CLP">CLP</option><option value="USD">USD</option><option value="BRL">BRL</option><option value="ARS">ARS</option>
      </select>
      <input id="spValor" type="number" min="0" inputmode="numeric" placeholder="VALOR"/>
            <!-- QUITAMOS capture PARA PERMITIR GALERÍA / ARCHIVOS EN CELULAR -->
            <input id="spImg" type="file" accept="image/*"/>
            <button id="spSave" class="btn ok">GUARDAR GASTO</button>
    </div>`;
  pane.appendChild(form);

  const listBox=document.createElement('div'); listBox.className='act';
  listBox.innerHTML='<h4>GASTOS DEL GRUPO</h4><div class="muted">CARGANDO…</div>';
  pane.appendChild(listBox);

  // ⛔️ Si el STAFF está en "TODOS", no consultamos subcolecciones inválidas
  if (state.is && state.viewingCoordId === '__ALL__'){
    form.style.display = 'none';
    listBox.innerHTML = '<h4>GASTOS DEL GRUPO</h4><div class="muted">SELECCIONA UN COORDINADOR EN EL SELECTOR PARA VER/REGISTRAR GASTOS.</div>';
    return 0;
  }

  const coordId = getActiveCoordIdForGastos();

  form.querySelector('#spSave').onclick=async ()=>{
    const btn=form.querySelector('#spSave');
    try{
      const asunto=(form.querySelector('#spAsunto').value||'').trim();
      const moneda=form.querySelector('#spMoneda').value;
      const valor =Number(form.querySelector('#spValor').value||0);
      const file  =form.querySelector('#spImg').files[0]||null;
      if(!asunto || !valor){ alert('ASUNTO Y VALOR OBLIGATORIOS.'); return; }

      btn.disabled=true;
      let imgUrl=null, imgPath=null;
      if(file){
        if (file.size > 10*1024*1024){ alert('LA IMAGEN SUPERA 10MB.'); btn.disabled=false; return; }
        const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
        const uid  = (auth.currentUser && auth.currentUser.uid) || state.user.uid;
        const path = `gastos/${uid}/${Date.now()}_${safe}`;
        const r    = sRef(storage, path);
        await uploadBytes(r, file, { contentType: file.type || 'image/jpeg' });
        imgUrl  = await getDownloadURL(r); imgPath = path;
      }

      await addDoc(collection(db,'coordinadores',coordId,'gastos'),{
        asunto, moneda, valor, imgUrl, imgPath,
        grupoId:g.id, numeroNegocio:g.numeroNegocio, identificador:g.identificador||null,
        grupoNombre:nombreOperativoGrupo(g), destino:g.destino||null, programa:g.programa||null,
        fechaInicio:g.fechaInicio||null, fechaFin:g.fechaFin||null,
        byUid: state.user.uid, byEmail:(state.user.email||'').toLowerCase(),
        createdAt: serverTimestamp()
      });
      form.querySelector('#spAsunto').value=''; form.querySelector('#spValor').value=''; form.querySelector('#spImg').value='';
      await loadGastosList(g, listBox, coordId, paneRef);
      if (paneRef) await renderFinanzas(g, paneRef);
    }catch(e){ console.error(e); alert('NO FUE POSIBLE GUARDAR EL GASTO.'); }finally{ btn.disabled=false; }
  };

  const hits = await loadGastosList(g, listBox, coordId, pane);
  return hits;
}

/* ===================== FINANZAS ===================== */

function toNumber(n){ return Number(n||0); }
function fmtCL(n){ return toNumber(n).toLocaleString('es-CL'); }

/* ===== TASAS DESDE FIRESTORE: Config/Finanzas (USD como pivote) ===== */
async function loadTasasFinanzas(){
  if (state.cache.tasas && state.cache.tasas.__from==='Config/Finanzas') return state.cache.tasas;
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
      state.cache.tasas = { __from:'Config/Finanzas', perUSD };
      return state.cache.tasas;
    }
  }catch(_){}
  const perUSD = { USD:1, CLP:945, BRL:5.5, ARS:1370 };
  state.cache.tasas = { __from:'fallback', perUSD };
  return state.cache.tasas;
}

/* Conversión genérica usando USD como pivote */
async function convertirMoneda(monto, from='USD', to='CLP'){
  const { perUSD } = await loadTasasFinanzas();
  const f = String(from||'').toUpperCase();
  const t = String(to||'').toUpperCase();
  const val = Number(monto||0);
  if (!val || !perUSD[f] || !perUSD[t]) return 0;
  if (f === t) return val;
  return val * (perUSD[t] / perUSD[f]);
}

/* Atajos: de USD a otras */
const usdAPesosChilenos = (montoUSD) => convertirMoneda(montoUSD, 'USD', 'CLP');
const usdAReal          = (montoUSD) => convertirMoneda(montoUSD, 'USD', 'BRL');
const usdAPesosArg      = (montoUSD) => convertirMoneda(montoUSD, 'USD', 'ARS');

// Suma todo a CLP usando USD como pivote y tasas de Config/Finanzas
async function sumCLPByMoneda(montos, tasasOpt){
  // Si te pasaron tasas (legacy), respétalas; si no, carga desde Config/Finanzas.
  let perUSD;
  if (tasasOpt && (tasasOpt.USD || tasasOpt.CLP || tasasOpt.BRL || tasasOpt.ARS)){
    // Permite ambos formatos: perUSD o campos tc*
    if (tasasOpt.perUSD){
      perUSD = { ...tasasOpt.perUSD, USD:1 };
    } else {
      perUSD = {
        USD: 1,
        CLP: Number(tasasOpt.tcUSD || tasasOpt.CLP || 945),
        BRL: Number(tasasOpt.tcBRL || tasasOpt.BRL || 5.5),
        ARS: Number(tasasOpt.tcARS || tasasOpt.ARS || 1370),
      };
    }
  } else {
    ({ perUSD } = await loadTasasFinanzas());
  }

  const toCLP = (amount, code) => Number(amount||0) * (perUSD.CLP / perUSD[String(code||'').toUpperCase()]);
  const CLP = Number(montos.CLP||0);
  const USD = toCLP(montos.USD, 'USD'); // USD→CLP
  const BRL = toCLP(montos.BRL, 'BRL'); // BRL→CLP vía USD
  const ARS = toCLP(montos.ARS, 'ARS'); // ARS→CLP vía USD
  return { CLP, USD, BRL, ARS, CLPconv: CLP + USD + BRL + ARS };
}

// -------- ABONOS CRUD  (grupos/{gid}/finanzas_abonos) ----------

// PIN para desbloquear abonos confirmados
const ABONO_UNLOCK_PIN = '2025';

async function loadAbonos(gid){
  // ojo: aquí debe usarse gid (parámetro), no g.id
  const qs = await getDocs(collection(db,'grupos', gid, 'finanzas_abonos'));
  const list = [];
  qs.forEach(d => list.push({ id:d.id, ...(d.data()||{}) }));
  list.sort((a,b)=> String(b.fecha||'').localeCompare(String(a.fecha||'')));
  return list;
}

async function saveAbono(gid, abono){
  // separa la id y limpia undefined del resto de campos
  const { id, ...raw } = abono || {};
  const data = Object.fromEntries(
    Object.entries(raw).filter(([k, v]) => v !== undefined)
  );

  if (id){
    // update existente
    const ref = doc(db,'grupos', gid, 'finanzas_abonos', id);
    await setDoc(
      ref,
      {
        ...data,
        updatedAt: serverTimestamp(),
        updatedBy: { uid: state.user.uid, email: (state.user.email||'').toLowerCase() }
      },
      { merge:true }
    );
    return id;
  } else {
    // crear nuevo (sin 'id' dentro del documento)
    const ref = await addDoc(
      collection(db,'grupos', gid, 'finanzas_abonos'),
      {
        ...data,
        createdAt: serverTimestamp(),
        createdBy: { uid: state.user.uid, email: (state.user.email||'').toLowerCase() }
      }
    );
    return ref.id;
  }
}

async function deleteAbono(gid, abonoId){
  await deleteDoc(doc(db,'grupos', gid, 'finanzas_abonos', abonoId));
}

// -------- Sugerencias automáticas de abonos en EFECTIVO ----------
// -------- Sugerencias automáticas de abonos en EFECTIVO ----------

// Precio unitario por PAX desde el servicio
function precioUnitarioFromServicio(svc){
  const cands = [
    svc?.precioPax,
    svc?.precio,
    svc?.tarifa,
    svc?.tarifaPax,
    svc?.valorPax,
    svc?.valorServicio,   // ⬅️ nuevo: coincide con Firestore
    svc?.valor,           // opcional, por si en algún momento cambias el nombre
    svc?.valorUnitario, 
    svc?.precios && svc.precios.pax,
    svc?.valores && svc.valores.pax
  ];
  for (const c of cands){
    const n = Number(c);
    if (!isNaN(n) && n > 0) return n;
  }
  return 0;
}

// Moneda desde el servicio → CLP / USD / BRL / ARS
function monedaFromServicio(svc){
  const candidatos = [
    svc?.moneda,
    svc?.currency,
    svc?.divisa,
    svc?.monedaBase,
    svc?.monedaPax,
    svc?.precios && svc.precios.moneda,
    svc?.valores && svc.valores.moneda
  ];

  for (const raw of candidatos){
    if (!raw) continue;
    const txt = String(raw).trim().toUpperCase();
    if (!txt) continue;

    if (txt.includes('CLP') || txt.includes('CHILE')) return 'CLP';
    if (txt.includes('USD') || txt.includes('DOLAR') || txt.includes('DÓLAR') || txt.includes('US$') || txt.includes('U$S')) return 'USD';
    if (txt.includes('BRL') || txt.includes('REAL') || txt.includes('R$')) return 'BRL';
    if (txt.includes('ARS') || txt.includes('PESO AR') || txt.includes('ARG')) return 'ARS';
  }

  // Fallback si no encontramos nada claro
  return 'CLP';
}

// Método de pago = EFECTIVO desde el servicio
function metodoPagoEsEfectivoFromServicio(svc){
  if (!svc) return false;
  const candidatos = [
    svc.metodoPago,
    svc.medioPago,
    svc.formaPago,
    svc.metodo,
    svc.metodo_de_pago,
    svc.forma_de_pago,
    svc.tipoPago,
    svc.tipo_pago,
    svc.pago,
    svc.medio
  ];
  for (const raw of candidatos){
    if (!raw) continue;
    const txt = norm(String(raw)); // normalizado (minúsculas, sin tildes)
    if (txt.includes('efectivo')) return true;
  }
  return false;
}

// Sugerencias automáticas de abonos en EFECTIVO
async function suggestAbonosFromItin(grupo){
  try{
    D_FIN('suggestAbonosFromItin: inicio', {
      grupoId: grupo?.id,
      destino: grupo?.destino
    });

    const itin = grupo?.itinerario || {};
    if (!itin || typeof itin !== 'object' || !Object.keys(itin).length){
      return [];
    }

    const fechas = Object.keys(itin).sort();
    const registros = [];
    let actsTotales = 0, actsConServicio = 0, actsConPax = 0;

    for (const fechaISO of fechas){
      const acts = Array.isArray(itin[fechaISO]) ? itin[fechaISO] : [];
      for (const a of acts){
        actsTotales++;

        const nombreAct = (a.actividad || a.nombre || '').toString().trim();
        if (!nombreAct) continue;

        // 1) Buscar servicio en catálogos (USANDO CACHE)
        const svc = await findServicio(grupo.destino, nombreAct);
        if (!svc){
          D_FIN('suggestAbonosFromItin: sin servicio', { fecha:fechaISO, actividad:nombreAct });
          continue;
        }
        actsConServicio++;

        // 2) Sólo servicios pagados en EFECTIVO
        if (!metodoPagoEsEfectivoFromServicio(svc)){
          D_FIN('suggestAbonosFromItin: servicio sin EFECTIVO', { fecha:fechaISO, actividad:nombreAct, svcId:svc.id });
          continue;
        }

        // 3) Precio unitario
        const { unit, moneda, fuente } = precioUnitarioFromServicio(svc);
        if (!unit || unit <= 0){
          D_FIN('suggestAbonosFromItin: servicio sin precio usable', {
            fecha:fechaISO, actividad:nombreAct, svcId:svc.id, fuente
          });
          continue;
        }

        // 4) PAX base del grupo
        const paxPlanBase = (grupo && (grupo.cantidadgrupo != null ? grupo.cantidadgrupo : paxOf(grupo))) || 0;
        let paxUsado = paxPlanBase;

        // Filtrado por adultos / estudiantes según nombre de la actividad
        const nombreNorm = norm(nombreAct);
        if (nombreNorm.includes('adult')){
          paxUsado = Number(grupo.adultos || 0) || paxPlanBase;
        } else if (nombreNorm.includes('estudiant')){
          paxUsado = Number(grupo.estudiantes || 0) || paxPlanBase;
        }

        if (!paxUsado || paxUsado <= 0){
          D_FIN('suggestAbonosFromItin: sin PAX usable', {
            fecha:fechaISO, actividad:nombreAct, svcId:svc.id, paxPlanBase
          });
          continue;
        }
        actsConPax++;

        // 5) tipoCobro: POR PERSONA vs POR GRUPO
        const tipoCobroRaw  = (svc.tipoCobro || svc.tipo_cobro || '').toString();
        const tipoCobroNorm = norm(tipoCobroRaw);
        const esPorGrupo    = tipoCobroNorm.includes('grupo');   // ej: "POR GRUPO"
        const factorCobro   = esPorGrupo ? 1 : paxUsado;
        const etiquetaCant  = esPorGrupo ? '1 GRUPO' : `${paxUsado} PAX`;

        const totalSug = unit * factorCobro;

        // 6) Proveedor
        const prov = (svc.proveedor || svc.prov || '').toString().trim();
        if (!prov){
          D_FIN('suggestAbonosFromItin: skip sin proveedor', {
            fecha:fechaISO, actividad:nombreAct, svcId:svc.id
          });
          continue;
        }

        // 7) Texto de abono: actividad + fecha
        const asunto = `${nombreAct.toUpperCase()} – ${dmy(fechaISO)}`;

        // Comentario: "X PAX × VALOR" o "1 GRUPO × VALOR"
        const comentarios = `${etiquetaCant} × ${unit.toLocaleString('es-CL')} ${moneda}`;

        const registro = {
          grupoId: grupo.id,
          grupoNumero: grupo.numeroNegocio || '',
          fecha: fechaISO,
          fechaStr: dmy(fechaISO),
          moneda,
          valor: totalSug,
          medio: 'EFECTIVO',

          // Ya NO usamos estado PRECARGA/CONFIRMADA, sólo marcamos que es auto
          autoCalc: true,

          proveedor: prov,
          asunto,
          comentarios,

          provWhitelistHit: prov,
          refActs: [{
            fecha: fechaISO,
            actividad: nombreAct,
            paxBase: paxPlanBase,
            paxUsado,
            tipoCobro: tipoCobroRaw || null,
            factorCobro,
            precioUnitario: unit,
            moneda,
            fuentePrecio: fuente
          }]
        };

        registros.push(registro);
      }
    }

    D_FIN('suggestAbonosFromItin: fin', {
      totalAct: actsTotales,
      conServicio: actsConServicio,
      conPax: actsConPax,
      sugeridos: registros.length
    });
    return registros;
  }catch(e){
    console.error('[suggestAbonosFromItin]', e);
    return [];
  }
}

// ==== REEMPLAZO: SOLO APROBADOS EN EL SALDO ====
async function sumGastosPorMonedaDelGrupo(g, qNorm){
  // coordId activo (igual que en el resto de tu app)
  const meEmail = (state.user?.email||'').toLowerCase();
  const coordId = state.viewingCoordId && state.viewingCoordId !== '__ALL__'
    ? state.viewingCoordId
    : (state.coordinadores?.find(c => (c.email||'').toLowerCase() === meEmail)?.id) || state.user?.uid;

  const base = collection(db,'coordinadores', coordId,'gastos');
  const qs   = await getDocs(query(base, where('grupoId','==', g.id)));

  const out = { CLP:0, USD:0, BRL:0, ARS:0 };
  qs.forEach(d=>{
    const x = d.data() || {};
    // Filtro de búsqueda (si aplica)
    if (qNorm){
      const hay = norm([x.asunto,x.byEmail,x.moneda,String(x.valor||0)].join(' ')).includes(qNorm);
      if (!hay) return;
    }
    // *** SOLO APROBADOS ***
    const estado = String(x.estado||'PENDIENTE').toUpperCase();
    if (estado !== 'APROBADO') return;

    const m = String(x.moneda||'CLP').toUpperCase();
    const v = Number(x.valor||0);
    if (out[m] != null) out[m] += v;
  });
  return out;
}

// -------- Summary / Cierre finanzas ----------
async function updateFinanzasSummary(gid, patch){
  await setDoc(
    doc(db,'grupos',gid,'finanzas','summary'),
    { ...(patch || {}), lastUpdate:{ ts:serverTimestamp(), uid:state.user.uid, email:(state.user.email||'').toLowerCase() } },
    { merge:true }
  );
}

// ===== SNAPSHOTS DE FINANZAS (HISTORIAL) — NUEVO =====
// Crea una "foto" del estado del grupo en este momento:
// - Datos clave del grupo
// - Itinerario/asistencias/serviciosEstado
// - Abonos, gastos aprobados, totales y saldos
// Se guarda como documento independiente en la subcolección:
//   grupos/{gid}/finanzas_snapshots
// -------- FOTO DE FINANZAS (snapshot interno) ----------
async function crearSnapshotFinanzas(grupo, ctx){
  const payload = {
    grupoId: grupo.id,
    numeroNegocio: grupo.numeroNegocio || null,
    identificador: grupo.identificador || null,
    nombreGrupo: grupo.nombreGrupo || grupo.aliasGrupo || null,
    destino: grupo.destino || null,
    anoViaje: grupo.anoViaje || null,
    createdAt: serverTimestamp(),
    createdBy: (state.user?.email || '').toLowerCase(),
    motivo: ctx.motivo || 'snapshot_manual',
    resumen:{
      totalesAbonos: ctx.totAb || {},
      totalesGastos: ctx.totGa || {},
      saldos: ctx.saldos || {}
    },
    cierrePrevio: ctx.sumPrev || {},
    abonos: (ctx.abonos || []).map(a => ({
      id: a.id || null,
      asunto: a.asunto || '',
      moneda: String(a.moneda || 'CLP').toUpperCase(),
      valor: Number(a.valor || 0),
      medio: a.medio || '',
      fecha: a.fecha || null,
      autoCalc: !!a.autoCalc,
      locked: !!a.locked
    })),
    gastosAprobados: (ctx.gastosAprob || []).map(x => ({
      id: x.id || null,
      proveedor: x.proveedor || '',
      actividad: x.actividad || '',
      moneda: String(x.moneda || 'CLP').toUpperCase(),
      valor: Number(x.valor || 0),
      fecha: x.fecha || null,
      estado: x.estado || ''
    }))
  };

  const col = collection(db,'grupos', grupo.id, 'finanzas_snapshots');
  const ref = await addDoc(col, payload);

  D_FIN('crearSnapshotFinanzas', {
    grupoId: grupo.id,
    snapId: ref.id,
    totAb: ctx.totAb,
    totGa: ctx.totGa,
    saldos: ctx.saldos
  });

  return ref.id;
}


// Trae las últimas fotos de finanzas de un grupo (ordenadas desc por fecha)
async function listarSnapshotsFinanzas(gid, max = 20){
  const qs = await getDocs(
    query(
      collection(db,'grupos',gid,'finanzas_snapshots'),
      orderBy('createdAt','desc'),
      limit(max)
    )
  );
  const out = [];
  qs.forEach(d => out.push({ id:d.id, ...(d.data() || {}) }));
  return out;
}

// =============== RESTABLECER VIAJE COMPLETO — HELPERS ===============
async function resetViajeCompleto(g){
  if (!state.is){ alert('Solo STAFF puede restablecer.'); return; }
  const ok = confirm(
    'Esto restablecerá el viaje COMPLETO:\n' +
    '• Borra todos los GASTOS del grupo\n' +
    '• Limpia Bitácora del itinerario\n' +
    '• Elimina archivos de cierre (boleta/comprobante/constancia)\n' +
    '• Reinicia el estado del viaje (inicio/fin/pax)\n' +
    '• Desmarca los cierres y deja todo editable\n\n' +
    '¿Continuar?'
  );
  if (!ok) return;

  const gid = g.id;
  showFlash('RESTABLECIENDO VIAJE…', 'warn');

  // 1) Eliminar archivos de cierre (Storage)
  try{
    await wipeFinanzasFiles(gid);
  }catch(e){
    console.warn('[reset] wipeFinanzasFiles', e);
  }

  // 2) Eliminar TODOS los gastos del grupo (en cualquier coordinador)
  let borradosGa = 0;
  try{
    borradosGa = await wipeGastosForGroup(gid);
  }catch(e){
    console.warn('[reset] wipeGastosForGroup', e);
  }

  // 3) Vaciar bitácora basada en itinerario del grupo
  let borradosBit = 0;
  try{
    borradosBit = await wipeBitacoraFromItinerario(g);
  }catch(e){
    console.warn('[reset] wipeBitacora', e);
  }

  // 4) Quitar flags/summary de cierre y reiniciar estado de viaje
  try{
    await resetGroupFlags(gid);
  }catch(e){
    console.warn('[reset] resetGroupFlags', e);
  }

  // 5) Registrar auditoría en historial viejo + HISTORIAL DEL VIAJE
  const detalle = `Se restableció el viaje. gastos_borrados=${borradosGa}, bitacora_borrada=${borradosBit}`;
  try{
    await logHistorial(gid, 'RESTABLECER_VIAJE_COMPLETO', detalle);
  }catch(e){
    console.warn('[reset] logHistorial', e);
  }
  try{
    await appendViajeLog(gid, 'RESTABLECER_VIAJE_COMPLETO', detalle);
  }catch(e){
    console.warn('[reset] appendViajeLog', e);
  }

  showFlash('VIAJE RESTABLECIDO', 'ok');

  // 6) Refrescar UI → que vuelva a aparecer INICIO DE VIAJE
  try{
    if (typeof reloadGroupAndRender === 'function'){
      await reloadGroupAndRender(gid);
      window.scrollTo({ top: 0, behavior: 'smooth' });
      setTimeout(() => document.getElementById('btnInicioViaje')?.focus?.(), 80);
    }else if (typeof renderOneGroup === 'function'){
      // Fallback: ajustar objeto en memoria a mano
      delete g.paxViajando;
      if (g.viaje){
        delete g.viaje.inicio;
        delete g.viaje.fin;
        g.viaje.estado = 'PENDIENTE';
      }else{
        g.viaje = { estado:'PENDIENTE' };
      }
      delete g.viajeInicioAt; delete g.viajeFinAt;
      delete g.viajeInicioBy; delete g.viajeFinBy;
      delete g.trip;
      await renderOneGroup(g);
    }else{
      location.reload();
    }
  }catch(e){
    console.warn('[reset] refresh UI', e);
    location.reload();
  }
}

// Borra recursivamente /finanzas/{grupoId}/... (boletas, comprobantes, efectivo_usd)
async function wipeFinanzasFiles(grupoId){
  async function delFolder(refFolder){
    const l = await listAll(refFolder);
    // borrar archivos directos
    await Promise.all((l.items || []).map(it => deleteObject(it).catch(()=>{})));
    // bajar a subcarpetas
    for (const p of (l.prefixes || [])){ await delFolder(p); }
  }
  return delFolder(sRef(storage, `finanzas/${grupoId}`));
}

async function wipeGastosForGroup(grupoId){
  let n = 0;

  // 1) Intento con collectionGroup (rápido)
  try{
    const q1 = query(collectionGroup(db,'gastos'), where('grupoId','==', grupoId));
    const qs = await getDocs(q1);
    for (const d of qs.docs){
      const x = d.data() || {};
      if (x.imgPath){ try{ await deleteObject(sRef(storage, x.imgPath)); }catch{} }
      try{ await deleteDoc(d.ref); n++; }catch{}
    }
    return n;
  }catch(e){
    console.warn('[reset] collectionGroup requiere índice, usando fallback por coordinador:', e?.message||e);
  }

  // 2) Fallback: escanear por coordinador
  try{
    for (const c of (state.coordinadores || [])){
      const base = collection(db,'coordinadores', c.id, 'gastos');
      const qs = await getDocs(base);
      for (const d of qs.docs){
        const x = d.data() || {};
        if (x.grupoId !== grupoId) continue;
        if (x.imgPath){ try{ await deleteObject(sRef(storage, x.imgPath)); }catch{} }
        try{ await deleteDoc(d.ref); n++; }catch{}
      }
    }
  }catch(e){
    console.warn('[reset] fallback coordinador error', e);
  }
  return n;
}




// Borra entradas de bitácora recorriendo el itinerario del grupo
async function wipeBitacoraFromItinerario(g){
  const it = g.itinerario || {};
  let n = 0;
  const fechas = Object.keys(it);
  for (const f of fechas){
    const arr = Array.isArray(it[f]) ? it[f] : Object.values(it[f]||{});
    for (const a of arr){
      const actKey = slugActKey(a);
      if (!actKey) continue;
      const coll = collection(db,'grupos', g.id, 'bitacora', actKey, f);
      const qs = await getDocs(coll);
      for (const d of qs.docs){ try{ await deleteDoc(d.ref); n++; }catch{} }
    }
  }
  return n;
}

// Normaliza clave de actividad usando EXACTAMENTE la misma lógica
// que cuando guardas bitácora (slug(actividad) → minúsculas sin espacios)
function slugActKey(a){
  // nombre de actividad o claves alternativas
  const nombreAct =
    a?.actKey ||
    a?.actividad ||
    a?.nombre ||
    a?.titulo ||
    a?.servicio ||
    '';

  if (!nombreAct) return '';
  // Usa la función slug que ya tienes definida arriba:
  // const slug = s => norm(s).slice(0,60);
  return slug(nombreAct);
}

// Quita flags/summary y vuelve “editable” (coordinador sale de solo-lectura)
// Quita flags de cierre, reabre summary y
// deja el viaje como PENDIENTE (sin inicio/fin/pax)
async function resetGroupFlags(grupoId){
  try{
    const ref = doc(db,'grupos',grupoId);

    await updateDoc(ref, {
      // Volver al estado inicial de viaje
      paxViajando: deleteField(),
      'viaje.inicio': deleteField(),          // sin hora de inicio
      'viaje.fin.at': deleteField(),          // quitamos solo el timestamp de fin
      'viaje.estado': 'PENDIENTE',           // vuelve al estado inicial

      // Flags de rendición / boleta en falso (siguen existiendo, pero “no hechos”)
      'viaje.fin.rendicionOk': false,
      'viaje.fin.boletaOk': false,
      'viaje.fin.cierreFinanzas': deleteField(),

      // Campos legacy que usabas antes
      viajeInicioAt: deleteField(),
      viajeFinAt: deleteField(),
      viajeInicioBy: deleteField(),
      viajeFinBy: deleteField(),

      // “trip” y resumen de finanzas también vuelven a cero
      trip: deleteField(),
      finanzasSummary: deleteField()
    });

  } catch (e) {
    console.warn('[reset] resetGroupFlags.updateDoc', e);
  }
}

// Auditoría (mantener historial)
async function logHistorial(grupoId, accion, detalle){
  try{
    await addDoc(collection(db,'grupos',grupoId,'historial'), {
      accion,
      detalle,
      by: (state.user?.email||'').toLowerCase(),
      ts: serverTimestamp()
    });
  }catch{}
}
// =============== /HELPERS RESTABLECER ===============

// (Se mantiene igual: marca cierre hecho)
async function closeFinanzas(g){
  await updateFinanzasSummary(g.id, {
    closed:true,
    closedAt: serverTimestamp(),
    closedBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() }
  });
  await updateDoc(doc(db,'grupos',g.id), {
    'viaje.fin.rendicionOk': true,
    'viaje.fin.boletaOk': true
  });
  showFlash('FINANZAS CERRADAS', 'ok');
}


// ====== HELPERS FINANZAS (GASTOS APROBADOS) ======
// Devuelve SOLO gastos con estado APROBADO del grupo.
// STAFF “Todos” -> busca con collectionGroup; si hay coordinador seleccionado -> subcolección del coord.
// Coordinador -> su propia subcolección.
// ===== HELPERS FINANZAS (GASTOS APROBADOS) =====
async function loadGastosAprobados(grupoId){
  try{
    const isStaff = !!state.is;
    const viewAll = isStaff && state.viewingCoordId === '__ALL__';
    const out = [];
    if (viewAll){
      const qs = await getDocs(query(
        collectionGroup(db,'gastos'),
        where('grupoId','==', grupoId),
        where('estado','==','APROBADO')
      ));
      qs.forEach(d => out.push({ id:d.id, ...d.data() }));
    } else {
      const coordId = (typeof getActiveCoordIdForGastos==='function')
        ? getActiveCoordIdForGastos()
        : (state.viewingCoordId || state.user.uid);
      const qs = await getDocs(query(
        collection(db,'coordinadores', coordId, 'gastos'),
        where('grupoId','==', grupoId),
        where('estado','==','APROBADO')
      ));
      qs.forEach(d => out.push({ id:d.id, ...d.data() }));
    }
    return out;
  }catch(e){
    console.error('[loadGastosAprobados]', e);
    return [];
  }
}

function totalesPorMoneda(items){
  const t = { CLP:0, USD:0, BRL:0, ARS:0 };
  (items||[]).forEach(x=>{
    const m = String(x.moneda||'').toUpperCase();
    const v = Number(x.valor||0);
    if (m==='CLP') t.CLP += v;
    else if (m==='USD') t.USD += v;
    else if (m==='BRL') t.BRL += v;
    else if (m==='ARS') t.ARS += v;
  });
  return t;
}


// ¿Hay algún gasto en PENDIENTE (o sin estado) para el grupo?
// Para docs antiguos sin `estado`, los tratamos como PENDIENTE (control local).
async function existsGastoPendiente(grupoId){
  try{
    const isStaff = !!state.is;
    const viewAll = isStaff && state.viewingCoordId === '__ALL__';

    if (viewAll){
      const qs = await getDocs(query(
        collectionGroup(db,'gastos'),
        where('grupoId','==', grupoId),
        limit(200)
      ));
      let pend = false;
      qs.forEach(d=>{
        const est = String((d.data()?.estado || 'PENDIENTE')).toUpperCase();
        if (est === 'PENDIENTE') pend = true;
      });
      return pend;
    } else {
      const coordId = (typeof getActiveCoordIdForGastos==='function'
                        ? getActiveCoordIdForGastos()
                        : (state.viewingCoordId || state?.user?.uid || ''));
      if (!coordId) return false;
      const qs = await getDocs(query(
        collection(db,'coordinadores', coordId,'gastos'),
        where('grupoId','==', grupoId),
        limit(200)
      ));
      let pend = false;
      qs.forEach(d=>{
        const est = String((d.data()?.estado || 'PENDIENTE')).toUpperCase();
        if (est === 'PENDIENTE') pend = true;
      });
      return pend;
    }
  }catch(e){
    console.error('existsGastoPendiente()', e);
    return false;
  }
}

async function renderFinanzas(g, pane){
  pane.innerHTML='<div class="muted">CARGANDO…</div>';
  const qNorm = norm(state.groupQ||'');

  // 1) Carga abonos y gastos APROBADOS (base)
  let abonos = await loadAbonos(g.id);
  const gastosAprob = await loadGastosAprobados(g.id);

  D_FIN('renderFinanzas: abonos/gastos cargados', {
    grupoId: g?.id,
    abonos: abonos.length,
    gastosAprob: gastosAprob.length
  });

  // 1.b) Genera y GUARDA abonos sugeridos en EFECTIVO según itinerario
  try{
    const sugeridos = await suggestAbonosFromItin(g) || [];
    D_FIN('renderFinanzas: sugeridos calculados', {
      grupoId: g?.id,
      sugeridos: sugeridos.length
    });

    if (sugeridos.length){
      const yaExiste = (sug)=>{
        const refSug   = (Array.isArray(sug.refActs) && sug.refActs[0]) || {};
        const fSugRaw  = refSug.fechaISO || sug.fecha;
        const actSug   = (refSug.actividad || '').toString();
        const provSug  = (sug.provWhitelistHit || '').toString().toUpperCase();
        const monSug   = (sug.moneda || '').toString().toUpperCase();
        const valSug   = Number(sug.valor || 0);
        const fSugISO  = toISO(fSugRaw || '');

        return abonos.some(a=>{
          const refA   = (Array.isArray(a.refActs) && a.refActs[0]) || {};
          const fARaw  = refA.fechaISO || a.fecha;
          const actA   = (refA.actividad || '').toString();
          const provA  = (a.provWhitelistHit || '').toString().toUpperCase();
          const monA   = (a.moneda || '').toString().toUpperCase();
          const valA   = Number(a.valor || 0);
          const fAISO  = toISO(fARaw || '');

          // (1) Coincidencia fuerte por proveedor + actividad + fecha
          if (provA && provA === provSug && fAISO === fSugISO && actA === actSug) return true;

          // (2) Fallback: mismo asunto + fecha + moneda + valor
          if (
            fAISO === fSugISO &&
            monA === monSug &&
            valA === valSug &&
            String(a.asunto || '') === String(sug.asunto || '')
          ) return true;

          return false;
        });
      };

      for (const sug of sugeridos){
        if (yaExiste(sug)){
          D_FIN('renderFinanzas: sugerido ya existe, omitido', {
            grupoId: g?.id,
            asunto: sug.asunto,
            fecha: sug.fecha,
            moneda: sug.moneda,
            valor: sug.valor
          });
          continue;
        }

        const id = await saveAbono(g.id, sug);
        const saved = { id, ...sug };
        abonos.push(saved);

        D_FIN('renderFinanzas: abono sugerido GUARDADO', {
          grupoId: g?.id,
          abonoId: id,
          asunto: sug.asunto,
          fecha: sug.fecha,
          moneda: sug.moneda,
          valor: sug.valor
        });
      }
    }else{
      D_FIN('renderFinanzas: no llegaron sugerencias', { grupoId: g?.id });
    }
  }catch(e){
    console.warn('[FIN] Error generando abonos sugeridos en efectivo:', e);
    D_FIN('renderFinanzas: ERROR en sugeridos', { grupoId: g?.id, error: String(e) });
  }

  // 2) Totales por moneda (sin conversión)
  const totAb = totalesPorMoneda(abonos);       // {CLP,USD,BRL,ARS}
  const totGa = totalesPorMoneda(gastosAprob);  // {CLP,USD,BRL,ARS}

  // === Saldos por moneda (bolsillos internos, pero UI unificada) ===
  const saldos = {
    CLP: (totAb.CLP||0) - (totGa.CLP||0),
    USD: (totAb.USD||0) - (totGa.USD||0),
    BRL: (totAb.BRL||0) - (totGa.BRL||0),
    ARS: (totAb.ARS||0) - (totGa.ARS||0),
  };
  
  // 4) ¿Quedan gastos PENDIENTES? (bloquea cierre)
  const hayPendientes = await existsGastoPendiente(g.id);
  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem'; pane.innerHTML=''; pane.appendChild(wrap);

  // RESUMEN
  const resum=document.createElement('div'); resum.className='act';
  resum.innerHTML = `
    <h4>RESUMEN FINANZAS</h4>
    <div class="grid-mini">
      <div class="lab">ABONOS</div>
      <div>
        CLP ${fmtCL(totAb.CLP||0)} · USD ${fmtCL(totAb.USD||0)} · BRL ${fmtCL(totAb.BRL||0)} · ARS ${fmtCL(totAb.ARS||0)}
      </div>
      <div class="lab">GASTOS (APROBADOS)</div>
      <div>
        CLP ${fmtCL(totGa.CLP||0)} · USD ${fmtCL(totGa.USD||0)} · BRL ${fmtCL(totGa.BRL||0)} · ARS ${fmtCL(totGa.ARS||0)}
      </div>
      <div class="lab">SALDOS</div>
      <div>
        CLP ${fmtCL(saldos.CLP||0)} · USD ${fmtCL(saldos.USD||0)} · BRL ${fmtCL(saldos.BRL||0)} · ARS ${fmtCL(saldos.ARS||0)}
      </div>
    </div>
  `;
  wrap.appendChild(resum);

  // ABONOS (STAFF edita)
  const boxAb=document.createElement('div'); boxAb.className='act';
    boxAb.innerHTML = `
      <h4>ABONOS ${state.is?'<span class="muted">(STAFF PUEDE EDITAR)</span>':''}</h4>
      ${state.is ? `
        <div class="rowflex" style="margin:.4rem 0;gap:.4rem;flex-wrap:wrap">
          <button id="btnNewAbono"         class="btn ok">NUEVO ABONO</button>
          <button id="btnReloadAbonosAuto" class="btn sec">CARGAR SUGERIDOS</button>
          <button id="btnResetAbonosAuto"  class="btn warn">RESET AUTO</button>
        </div>` : ''}
      <div id="abonosList" style="display:grid;gap:.4rem"></div>
    `;
    wrap.appendChild(boxAb);

  const renderAbonosList = (items)=>{
    const cont = boxAb.querySelector('#abonosList');
    cont.innerHTML = '';
    if (!items.length){
      cont.innerHTML = '<div class="muted">SIN ABONOS.</div>';
      return;
    }

    items.forEach(a=>{
      const card = document.createElement('div');
      card.className = 'card';

      card.innerHTML = `
        <div class="meta" style="display:flex;justify-content:space-between;align-items:center;gap:.5rem;flex-wrap:wrap">
          <div>
            <strong>${(a.asunto||'ABONO').toString().toUpperCase()}</strong>
          </div>
          ${state.is ? `
            <button
              class="btn sec btnLock"
              title="${a.locked ? 'Desbloquear abono (requiere clave)' : 'Marcar como confirmado / bloquear'}"
            >
              ${a.locked ? '🔒' : '🔓'}
            </button>
          ` : ''}
        </div>
        <div class="meta">
          FECHA: ${dmy(a.fecha||'')}
        </div>
        <div class="meta">
          MONEDA/VALOR: ${(a.moneda||'CLP').toUpperCase()} ${fmtCL(a.valor||0)}
        </div>
        <div class="meta">
          MEDIO: ${(a.medio||'').toString().toUpperCase()}
        </div>
        ${a.comentarios
          ? `<div class="meta" style="white-space:pre-wrap">${(a.comentarios||'').toString().toUpperCase()}</div>`
          : ''
        }
        ${(state.is && !a.locked) ? `
          <div class="rowflex" style="margin-top:.4rem;gap:.4rem;flex-wrap:wrap">
            <button class="btn sec btnEdit">EDITAR</button>
            <button class="btn warn btnDel">ELIMINAR</button>
          </div>` : ''
        }
      `;

      if (state.is){
        const bLock = card.querySelector('.btnLock');
        if (bLock) bLock.onclick = async ()=>{
          try{
            if (!a.locked){
              // Bloquear (CONFIRMAR) abono
              if (!confirm('Este abono quedará CONFIRMADO y no se podrá editar ni eliminar sin clave. ¿Continuar?')) return;
              await saveAbono(g.id, { ...a, locked:true });
              a.locked = true;
            } else {
              // Desbloquear abono: pide PIN
              const pin = prompt('Para desbloquear este abono ingresa la clave:');
              if (pin === null) return; // cancelado
              if (pin !== ABONO_UNLOCK_PIN){
                alert('Clave incorrecta.');
                return;
              }
              await saveAbono(g.id, { ...a, locked:false });
              a.locked = false;
            }
            renderAbonosList(items);
          }catch(e){
            console.error(e);
            alert('No se pudo actualizar el estado de bloqueo de este abono.');
          }
        };

        // Solo se puede EDITAR / ELIMINAR si el abono NO está bloqueado
        if (!a.locked){
          const bE = card.querySelector('.btnEdit');
          if (bE) bE.onclick = async ()=>{
            await openAbonoEditor(g, a, (updated)=>{
              Object.assign(a, updated);
              renderAbonosList(items);
            });
          };

          const bD = card.querySelector('.btnDel');
          if (bD) bD.onclick = async ()=>{
            if (!confirm('¿Eliminar abono?')) return;
            await deleteAbono(g.id, a.id);
            const i = items.findIndex(x=>x.id===a.id);
            if (i>=0) items.splice(i,1);
            renderAbonosList(items);
            await renderFinanzas(g, pane);
          };
        }
      }


      cont.appendChild(card);
    });
  };

  if (state.is){
    const btnNew    = boxAb.querySelector('#btnNewAbono');
    const btnReload = boxAb.querySelector('#btnReloadAbonosAuto');
    const btnReset  = boxAb.querySelector('#btnResetAbonosAuto');

    // NUEVO ABONO (igual que antes)
    if (btnNew) btnNew.onclick = async ()=>{
      await openAbonoEditor(g, null, async (saved)=>{
        abonos.unshift(saved);
        renderAbonosList(abonos);
        await renderFinanzas(g, pane);
      });
    };

    // CARGAR SUGERIDOS: vuelve a correr renderFinanzas,
    // que ya incluye la lógica de suggestAbonosFromItin + yaExiste
    if (btnReload) btnReload.onclick = async ()=>{
      await renderFinanzas(g, pane);
    };

    // RESET AUTO: borra todos los abonos autoCalc NO locked y recarga sugeridos
    if (btnReset) btnReset.onclick = async ()=>{
      if (!confirm('Esto eliminará todos los abonos automáticos NO confirmados y recargará las sugerencias del sistema. ¿Continuar?')) return;

      try{
        const actuales = await loadAbonos(g.id);
        const aBorrar = actuales.filter(a=>a.autoCalc && !a.locked);

        for (const a of aBorrar){
          await deleteAbono(g.id, a.id);
        }

        D_FIN('renderFinanzas: reset auto abonos', {
          grupoId: g?.id,
          borrados: aBorrar.length
        });
      }catch(e){
        console.error(e);
        alert('No se pudo resetear los abonos automáticos.');
        return;
      }

      // Volvemos a cargar FINANZAS: esto recrea la UI y recalcula sugeridos
      await renderFinanzas(g, pane);
    };
  }
  renderAbonosList(abonos);


  // GASTOS
  const paneGastos = document.createElement('div');
  // pasamos pane como 3er parámetro para poder refrescar totales al cambiar estado
  const ghits = await renderGastos(g, paneGastos, pane);
  wrap.appendChild(paneGastos);


  // CIERRE FINANCIERO
  const cierre=document.createElement('div'); cierre.className='act';
  cierre.innerHTML=`
    <h4>CIERRE FINANCIERO</h4>
  
    <!-- Hints de requisitos dinámicos -->
    <div id="finHints" class="meta muted" style="margin-bottom:.35rem"></div>
  
    <div class="card">
      <div class="meta"><strong>DATOS DE TRANSFERENCIA</strong></div>
      <div class="meta">CUENTA CORRIENTE N° 03398-07 · BANCO DE CHILE</div>
      <div class="meta">TURISMO RAITRAI LIMITADA · RUT 78.384.230-0</div>
      <div class="meta">aleoperaciones@raitrai.cl</div>
    </div>
  
    <!-- Devolución CLP por transferencia (se oculta si no sobra CLP) -->
    <div id="wrapTransf" class="rowflex" style="margin:.5rem 0; flex-wrap:wrap; gap:.5rem; align-items:center">
      <label class="meta" style="display:flex;gap:.4rem;align-items:center">
        <input id="chTransf" type="checkbox"/> TRANSFERENCIA REALIZADA (CLP)
      </label>
      <input id="upComp" type="file" accept="image/*,application/pdf"/>
      <button id="btnUpComp" class="btn sec">SUBIR COMPROBANTE</button>
    </div>
  
    <!-- Devolución USD en efectivo (se oculta si no sobra USD) -->
    <div id="wrapCashUsd" class="rowflex" style="margin:.5rem 0; flex-wrap:wrap; gap:.5rem; align-items:center">
      <label class="meta" style="display:flex;gap:.4rem;align-items:center">
        <input id="chCashUsd" type="checkbox"/> EFECTIVO DEVUELTO (USD)
      </label>
      <input id="upCash" type="file" accept="image/*,application/pdf"/>
      <button id="btnUpCash" class="btn sec">SUBIR CONSTANCIA</button>
    </div>

  
    <!-- Boleta SIEMPRE obligatoria -->
    <div class="rowflex" style="margin:.5rem 0; flex-wrap:wrap; gap:.5rem; align-items:center">
      <a href="https://www.sii.cl" target="_blank" class="btn sec">IR A SII.CL</a>
      <input id="upBoleta" type="file" accept="image/*,application/pdf"/>
      <button id="btnUpBoleta" class="btn sec">SUBIR BOLETA</button>
    </div>

    ${state.is ? `
    <!-- Snapshot de control de finanzas (solo STAFF) -->
    <div class="card" id="snapFinBox" style="margin-top:.75rem; border:1px dashed #999; padding:.5rem .75rem;">
      <div class="meta"><strong>FOTO DE CONTROL (SNAPSHOT) — STAFF</strong></div>
      <div id="snapFinInfo" class="meta muted" style="margin-top:.25rem;font-size:.85rem"></div>
      <label class="meta" style="display:flex;gap:.4rem;align-items:center;margin-top:.35rem">
        <input id="chSnapFin" type="checkbox"/>
        <span>Marcar para guardar una FOTO del grupo, itinerario y finanzas (requiere PIN).</span>
      </label>
      <div class="rowflex" style="margin-top:.35rem; gap:.5rem; align-items:center;">
        <button id="btnActaFin" type="button" class="btn mini">VER ACTA DE CIERRE</button>
        <span id="snapFinHint" class="meta muted" style="font-size:.8rem;"></span>
      </div>
    </div>
    ` : ''}

    <div class="rowflex" style="margin-top:.6rem">
      <button id="btnCloseFin" class="btn ok" disabled>CERRAR FINANZAS</button>
    </div>
  `;
  wrap.appendChild(cierre);

  // === Estado previo guardado (summary) ===
  const sumPrev = await ensureFinanzasSummary(g.id) || {};
  const chTransf = cierre.querySelector('#chTransf');
  const chCash   = cierre.querySelector('#chCashUsd');
  const chSnap   = cierre.querySelector('#chSnapFin');
  const snapInfo = cierre.querySelector('#snapFinInfo');
  const snapHint = cierre.querySelector('#snapFinHint');

  // Texto inicial del snapshot (si existe info previa)
  if (snapInfo){
    const cnt  = Number(sumPrev?.snapshots?.count || 0);
    const last = sumPrev?.snapshots?.lastAt || null;
    let fechaTxt = '—';
    if (last){
      try{
        const d = last.toDate ? last.toDate() : new Date(last);
        fechaTxt = d.toLocaleDateString('es-CL');
      }catch(_){}
    }
    snapInfo.textContent = cnt
      ? `FOTOS GUARDADAS: ${cnt} · Última: ${fechaTxt}`
      : 'Aún no hay fotos guardadas. Marca el casillero para tomar la primera.';
  }

  if (snapHint){
    snapHint.textContent = 'El ACTA se genera siempre desde la última FOTO guardada.';
  }

  // Inicializa checks si ya había registros subidos
  if (sumPrev?.transfer?.done && chTransf) chTransf.checked = true;
  if (sumPrev?.cashUsd?.done && chCash)   chCash.checked   = true;
  if (sumPrev?.snapshots?.active && chSnap) chSnap.checked = true;


    // === FOTO DE FINANZAS (STAFF, protegida con PIN) ===
  if (state.is){
    const snapInput  = cierre.querySelector('#finSnapshotKey');
    const snapBtn    = cierre.querySelector('#btnFinSnapshot');
    const snapStatus = cierre.querySelector('#finSnapshotStatus');

    if (snapInput && snapBtn){
      snapBtn.onclick = async ()=>{
        const pin = (snapInput.value || '').trim();

        // Usamos el mismo PIN que para desbloquear abonos
        if (pin !== ABONO_UNLOCK_PIN){
          alert('Clave incorrecta.');
          return;
        }

        try{
          snapBtn.disabled = true;
          if (snapStatus) snapStatus.textContent = 'Guardando foto…';

          const snapId = await crearSnapshotFinanzas(g, {
            abonos,
            gastosAprob,
            totAb,
            totGa,      // 👈 aquí va totGa, NO "totGas"
            saldos,
            sumPrev,
            motivo: 'snapshot_manual'
          });

          if (snapStatus){
            snapStatus.textContent = `Foto guardada (${snapId.slice(0,6)}…)`;
          }
          showFlash('FOTO DE FINANZAS GUARDADA', 'ok');
        }catch(e){
          console.error('Error creando snapshot de finanzas', e);
          if (snapStatus) snapStatus.textContent = 'No se pudo guardar la foto.';
          alert('No se pudo guardar la FOTO de finanzas. Revisa consola.');
        }finally{
          snapBtn.disabled = false;
        }
      };
    }
  }

  // helper numérico tolerante (≈0 a 2 decimales)
  const isZero = v => Math.abs(Number(v||0)) < 0.005;

  
  // === Requisitos dinámicos ===
  function checkReady(){
    // Políticas por moneda
    const needTransfCLP = (saldos.CLP || 0) > 0;  // sobra CLP → transferencia + comprobante
    const needCashUSD   = (saldos.USD || 0) > 0;  // sobra USD → efectivo USD + constancia
    const brlZero       = Math.abs(saldos.BRL||0) < 0.005;
    const arsZero       = Math.abs(saldos.ARS||0) < 0.005;
  
    // Mostrar/ocultar bloques según necesidad real
    const wrapTransf = cierre.querySelector('#wrapTransf');
    const wrapCash   = cierre.querySelector('#wrapCashUsd');
    if (wrapTransf) wrapTransf.style.display = needTransfCLP ? '' : 'none';
    if (wrapCash)   wrapCash.style.display   = needCashUSD   ? '' : 'none';
  
    // Datos de transferencia: solo si hace falta CLP
    const datosTransf = cierre.querySelector('.card');
    if (datosTransf) datosTransf.style.display = needTransfCLP ? '' : 'none';
  
    // Flags previos (persistidos)
    const sumPrevOkBoleta  = !!sumPrev?.boleta?.uploaded;
    const sumPrevOkTransf  = !!sumPrev?.transfer?.done;
    const sumPrevOkCashUsd = !!sumPrev?.cashUsd?.done;
  
    // Checks UI actuales
    const okTransf = !needTransfCLP || sumPrevOkTransf || (chTransf && chTransf.checked);
    const okCash   = !needCashUSD   || sumPrevOkCashUsd || (chCash   && chCash.checked);
  
    // Boleta sugerida = 70.000 x días de viaje
    const dias = daysBetweenInclusive(g.fechaInicio, g.fechaFin);
    const boletaSugerida = 70000 * Math.max(0, dias||0);
  
    // Condiciones para habilitar
    const sinPendientes = !hayPendientes;                  // NO deben existir gastos en PENDIENTE
    const brlOk = brlZero, arsOk = arsZero;                // BRL y ARS deben ser 0 exacto
    const boletaOk = sumPrevOkBoleta;                      // boleta subida
    const listo = sinPendientes && brlOk && arsOk && boletaOk && okTransf && okCash;
  
    const btn = cierre.querySelector('#btnCloseFin');
    if (btn) btn.disabled = !listo;
  
    // Mensajes guía
    const hints = [];
    // Línea informativa de boleta
    hints.push(`• MONTO BRUTO DE BOLETA: CLP ${fmtCL(boletaSugerida)} (${dias||0} DÍAS).`);
    if (!boletaOk) hints.push('• FALTA SUBIR BOLETA DEL SII.');
    if (!sinPendientes) hints.push('• HAY GASTOS PENDIENTES: DEBEN SER APROBADOS O RECHAZADOS.');
    if (!brlOk) hints.push('• BRL DEBE QUEDAR EN 0.');
    if (!arsOk) hints.push('• ARS DEBE QUEDAR EN 0.');
    // Mostrar SALDO por moneda (monto a devolver a la empresa) con instrucciones si falta marcar/cargar
    const saldoEmpresaCLP = needTransfCLP ? Math.max(0, Number(saldos.CLP||0)) : 0;
    const saldoEmpresaUSD = needCashUSD   ? Math.max(0, Number(saldos.USD||0)) : 0;
    
    if (needTransfCLP){
      // Mostramos siempre el saldo CLP a devolver; si aún no está ok, agregamos la instrucción
      hints.push(`• SALDO CLP A DEVOLVER: CLP ${fmtCL(saldoEmpresaCLP)}${okTransf ? '' : ' — marca “TRANSFERENCIA REALIZADA (CLP)” y sube comprobante.'}`);
    }
    
    if (needCashUSD){
      // Mostramos siempre el saldo USD a devolver; si aún no está ok, agregamos la instrucción
      hints.push(`• SALDO USD A DEVOLVER: USD ${fmtCL(saldoEmpresaUSD)}${okCash ? '' : ' — marca “EFECTIVO DEVUELTO (USD)” y sube constancia.'}`);
    }

  
    const h = cierre.querySelector('#finHints');
    h.innerHTML = hints.length ? `<div class="muted">${hints.join('<br>')}</div>` : '<div class="muted">TODO LISTO PARA CERRAR.</div>';
  }

  checkReady();

  // === BLOQUEO DE CONTROLES DE CIERRE SI YA ESTÁ CERRADO (SOLO COORDINADOR) ===
  const isReadOnlyFin = !!sumPrev?.closed && !state.is;
  if (isReadOnlyFin){
    // Deshabilita todos los controles del bloque de cierre
    ['#chTransf','#upComp','#btnUpComp',
     '#chCashUsd','#upCash','#btnUpCash',
     '#upBoleta','#btnUpBoleta','#btnCloseFin',
     '#chSnapFin','#btnActaFin'
    ].forEach(sel => {
      const el = cierre.querySelector(sel);
      if (el){
        el.disabled = true;
        el.classList.add('disabled');
      }
    });

    const h = cierre.querySelector('#finHints');
    if (h){
      h.innerHTML = '<div class="muted">VIAJE FINALIZADO · RENDICIÓN HECHA · BOLETA ENTREGADA</div>';
    }
  }

  // === Snapshot de finanzas (solo STAFF, con PIN) ===
  if (state.is && chSnap){
    chSnap.addEventListener('change', async (ev)=>{
      const checked = !!ev.target.checked;

      const pin = prompt('PIN STAFF para marcar/desmarcar la FOTO de finanzas:');
      // Usamos la misma clave que para abonos bloqueados
      if (pin !== ABONO_UNLOCK_PIN){
        alert('PIN incorrecto.');
        ev.target.checked = !checked;
        return;
      }

      if (checked){
        // Al marcar: se crea una nueva FOTO (snapshot)
        try{
          const snapId = await crearSnapshotFinanzas(g, {
            abonos,
            gastosAprob,
            totAb,
            totGa,   // 👈 OJO: aquí va totGa, NO "totGas"
            saldos,
            sumPrev,
            motivo: 'snapshot_manual'
          });

          const prevCount = Number(sumPrev?.snapshots?.count || 0);
          // Actualiza resumen en memoria
          sumPrev.snapshots = {
            ...(sumPrev.snapshots || {}),
            active: true,
            lastId: snapId,
            lastAt: todayISO(),
            count: prevCount + 1
          };

          if (snapInfo){
            snapInfo.textContent = `FOTOS GUARDADAS: ${prevCount + 1} · Última: ${dmy(todayISO())}`;
          }

          await updateFinanzasSummary(g.id, {
            snapshots: {
              active: true,
              lastId: snapId,
              lastAt: serverTimestamp(),
              count: prevCount + 1
            }
          });

          showFlash('FOTO DE FINANZAS GUARDADA (SNAPSHOT)', 'ok');
        }catch(e){
          console.error('Error creando snapshot de finanzas', e);
          alert('No se pudo guardar la FOTO de finanzas. Revisa consola.');
          ev.target.checked = false;
        }
      }else{
        // Al desmarcar: solo se baja el flag "activa", no se borran las fotos anteriores
        try{
          await updateFinanzasSummary(g.id, {
            snapshots: {
              ...(sumPrev.snapshots || {}),
              active: false
            }
          });

          if (sumPrev.snapshots) sumPrev.snapshots.active = false;

          if (snapInfo){
            const cnt = Number(sumPrev.snapshots?.count || 0);
            snapInfo.textContent = cnt
              ? `FOTOS GUARDADAS: ${cnt} · (sin FOTO activa marcada)`
              : 'Aún no hay fotos guardadas.';
          }

          showFlash('FOTO DE FINANZAS DESMARCADA (historial se mantiene)', 'warn');
        }catch(e){
          console.error('Error desmarcando snapshot de finanzas', e);
          alert('No se pudo desmarcar la FOTO de finanzas. Revisa consola.');
          ev.target.checked = true;
        }
      }
    });
  }

  // === Botón ACTA DE CIERRE: imprime usando la última FOTO ===
  if (state.is){
    const btnActa  = cierre.querySelector('#btnActaFin');
    const snapHint = cierre.querySelector('#snapFinHint');
    if (btnActa){
      btnActa.onclick = async ()=>{
        try{
          btnActa.disabled = true;
          if (snapHint) snapHint.textContent = 'Cargando última FOTO de finanzas…';

          const snaps = await listarSnapshotsFinanzas(g.id, 1);
          if (!snaps.length){
            alert('Aún no hay FOTOS de finanzas para este grupo.');
            if (snapHint) snapHint.textContent = 'SIN FOTOS DE FINANZAS.';
            return;
          }

          const snap = snaps[0];
          await preparePrintActaFinanzas(g, snap);
          window.print();

          if (snapHint){
            let fechaTxt = '';
            try{
              const d = snap.createdAt?.toDate ? snap.createdAt.toDate() : snap.createdAt;
              if (d) fechaTxt = new Date(d).toLocaleDateString('es-CL');
            }catch{}
            snapHint.textContent = fechaTxt
              ? `ACTA generada desde FOTO del ${fechaTxt}.`
              : 'ACTA generada desde última FOTO.';
          }
        }catch(e){
          console.error('Error generando ACTA de finanzas', e);
          alert('No se pudo generar el ACTA de finanzas. Revisa consola.');
          if (snapHint) snapHint.textContent = 'Error al generar ACTA.';
        }finally{
          btnActa.disabled = false;
        }
      };
    }
  }
  
  // === Handlers de subida (comprobante transferencia CLP) ===
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
    if (chTransf) chTransf.checked = true;
    checkReady();
    showFlash('COMPROBANTE SUBIDO', 'ok');
  };
  
  // === Handlers de subida (constancia efectivo USD) ===
  const upCashBtn = cierre.querySelector('#btnUpCash');
  if (upCashBtn) upCashBtn.onclick = async ()=>{
    const file = cierre.querySelector('#upCash').files[0]||null;
    if (!file){ alert('Selecciona la constancia (foto/PDF).'); return; }
    if (file.size > 15*1024*1024){ alert('Archivo supera 15MB.'); return; }
    const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
    const path = `finanzas/${g.id}/efectivo_usd/${Date.now()}_${safe}`;
    const r = sRef(storage, path);
    await uploadBytes(r, file, { contentType: file.type || 'application/octet-stream' });
    const url = await getDownloadURL(r);
    await updateFinanzasSummary(g.id, { cashUsd:{ done:true, fecha: todayISO(), medio:'EFECTIVO_USD', comprobanteUrl:url } });
    sumPrev.cashUsd = { done:true, fecha: todayISO(), medio:'EFECTIVO_USD', comprobanteUrl:url };
    if (chCash) chCash.checked = true;
    checkReady();
    showFlash('CONSTANCIA DE EFECTIVO SUBIDA', 'ok');
  };
  
  // === Subida de boleta (siempre obligatoria) ===
  const upBolBtn = cierre.querySelector('#btnUpBoleta');
  if (upBolBtn) upBolBtn.onclick = async ()=>{
    const file = cierre.querySelector('#upBoleta').files[0]||null;
    if (!file){ alert('Selecciona la boleta.'); return; }
    if (file.size > 15*1024*1024){ alert('Archivo supera 15MB.'); return; }
    const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
    const path = `finanzas/${g.id}/boletas/${Date.now()}_${safe}`;
    const r = sRef(storage, path);
    await uploadBytes(r, file, { contentType: file.type || 'application/octet-stream' });
    const url = await getDownloadURL(r);
    await updateFinanzasSummary(g.id, { boleta:{ uploaded:true, fecha: todayISO(), url } });
    sumPrev.boleta = { uploaded:true, fecha: todayISO(), url };
    checkReady();
    showFlash('BOLETA SUBIDA', 'ok');
  };
  
  // === Cerrar finanzas (bloquea edición para coordinador) ===
  const closeBtn = cierre.querySelector('#btnCloseFin');
  if (closeBtn) closeBtn.onclick = async ()=>{
    // Revalidación con mensajes concretos
    const needTransf = (saldos.CLP || 0) > 0;
    const needCash   = (saldos.USD || 0) > 0;
    const brlOk      = isZero(saldos.BRL);
    const arsOk      = isZero(saldos.ARS);
    const boletaOk   = !!sumPrev?.boleta?.uploaded;
  
    if (!boletaOk){ alert('Debes subir boleta para cerrar.'); return; }
    if (!brlOk || !arsOk){ alert('BRL y ARS deben quedar en 0 para cerrar.'); return; }
    if (needTransf && !sumPrev?.transfer?.done && !(chTransf && chTransf.checked)){
      alert('Sobra CLP: marca TRANSFERENCIA REALIZADA (CLP) y sube el comprobante.'); return;
    }
    if (needCash && !sumPrev?.cashUsd?.done && !(chCash && chCash.checked)){
      alert('Sobra USD: marca EFECTIVO DEVUELTO (USD) y sube la constancia.'); return;
    }
  
    await closeFinanzas(g);        // ← ya existente: marca summary.closed y flags de grupo
    await renderFinanzas(g, pane); // refresca modal
  };

  // === BLOQUEO TOTAL DE EDICIÓN EN EL MODAL (ABONOS + GASTOS) SI YA ESTÁ CERRADO (SOLO COORDINADOR) ===
  if (isReadOnlyFin){
    // Deshabilita todos los inputs/select/textarea/button del modal de finanzas
    // (permite que los enlaces "VER" sigan funcionando)
    pane.querySelectorAll('input, select, textarea, button').forEach(el => {
      el.disabled = true;
      el.classList.add('disabled');
    });
  
    // Rehabilita enlaces (por si algún estilo global usa pointer-events)
    pane.querySelectorAll('a').forEach(a => a.style.pointerEvents = 'auto');
  }

  await updateFinanzasSummary(g.id, {
    totals:{
      abonos: totAb,
      gastos: totGa,
      saldos: saldos
    }
  });

  const hitsAb = qNorm ? abonos.filter(a => norm([a.asunto,a.comentarios,a.medio,String(a.valor||0)].join(' ')).includes(qNorm)).length : 0;
  return hitsAb + (ghits||0);
}

// -------- Modal editor de ABONO (STAFF) ----------
async function openAbonoEditor(g, abono, onSaved){
  const isEdit = !!abono;
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  title.textContent = (isEdit ? 'EDITAR ABONO' : 'NUEVO ABONO');

  const seed = abono || {
    asunto:'', comentarios:'', moneda:'CLP', valor:'',
    fecha: todayISO(), medio:'CTA CTE',
    autoCalc:false,
    locked:false,
    provWhitelistHit:null,
    refActs:[]
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
      <input id="abFec" type="date" value="${toISO(seed.fecha||todayISO())}"/>
      <input id="abMed" type="text" placeholder="MEDIO (CTA CTE / EFECTIVO / ...)" value="${(seed.medio||'')}"/>
    </div>
    <div class="rowflex" style="margin-top:.5rem">
      <textarea id="abCom" placeholder="COMENTARIOS" style="width:100%">${seed.comentarios||''}</textarea>
    </div>
    <div class="rowflex" style="margin-top:.5rem;gap:.5rem;flex-wrap:wrap;align-items:center">
      <label class="meta" style="display:flex;align-items:center;gap:.4rem">
        <input id="abLock" type="checkbox"${seed.locked ? ' checked' : ''}/>
        MARCAR COMO CONFIRMADO / INAMOVIBLE
      </label>
      ${seed.autoCalc ? `
        <span class="badge" style="background:#1d4ed8;color:#fff">
          AUTO (SUGERIDO)
        </span>` : ''}
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
      locked: !!body.querySelector('#abLock').checked,
      provWhitelistHit: seed.provWhitelistHit || null,
      refActs: Array.isArray(seed.refActs)? seed.refActs : []
    };
    if (!data.asunto || !data.valor){
      alert('ASUNTO y VALOR son obligatorios.');
      return;
    }
    const id = await saveAbono(g.id, data);
    const saved = { id: id || data.id, ...data };
    document.getElementById('modalBack').style.display='none';
    onSaved && onSaved(saved);
  };

  document.getElementById('modalClose').onclick = ()=>{
    document.getElementById('modalBack').style.display='none';
  };
  back.style.display='flex';
}


async function getTasas(){
  if(state.cache.tasas) return state.cache.tasas;
  try{ const d=await getDoc(doc(db,'config','tasas')); if(d.exists()){ state.cache.tasas=d.data()||{}; return state.cache.tasas; } }catch(_){}
  state.cache.tasas={ USD:950, BRL:170, ARS:1.2 }; return state.cache.tasas;
}

// === CSS compacto para "GASTOS DEL GRUPO" (−20%) ===
function ensureGastosCompactCSS(){
  if (document.getElementById('css-gastos-compact')) return;
  const s = document.createElement('style');
  s.id = 'css-gastos-compact';
  s.textContent = `
    .table.gastos th, .table.gastos td{ padding: 6px 10px; }
    .table.gastos select{ padding: 4px 8px; font-size: .95rem; }
    .totline.gastos{ padding: 6px 10px; font-size: .95rem; }
  `;
  document.head.appendChild(s);
}


async function loadGastosList(g, box, coordId, paneRef){
  // leer gastos de ese coordinador (orden creación desc)
  const qs = await getDocs(
    query(collection(db,'coordinadores',coordId,'gastos'), orderBy('createdAt','desc'))
  );

  // normalizamos lista + estado por defecto
  let list = [];
  qs.forEach(d=>{
    const x = d.data() || {};
    if (x.grupoId === g.id){
      list.push({
        id: d.id,
        ...x,
        estado: String(x.estado || 'GUARDADO').toUpperCase()
      });
    }
  });

  // filtro por buscador global
  const q = norm(state.groupQ || '');
  let hits = 0;
  if (q){
    list = list.filter(x =>
      norm([x.asunto, x.byEmail, x.moneda, String(x.valor||0)].join(' '))
        .includes(q)
    );
    hits = list.length;
  }

  // totales por moneda (solo para la línea TOTAL de la tabla)
  const tot = { CLP:0, USD:0, BRL:0, ARS:0 };
  for (const x of list){
    const m = String(x.moneda||'').toUpperCase();
    if (m && m in tot) tot[m] += Number(x.valor||0);
  }

  // header condicional: ocultar AUTOR a no-staff, pero SIEMPRE mostrar ESTADO
  box.innerHTML = '<h4>GASTOS DEL GRUPO</h4>';
  const showAutor = !!state.is;

  const table = document.createElement('table');
  table.className = 'table gastos';
  // compactado (si definiste ensureGastosCompactCSS)
  if (typeof ensureGastosCompactCSS === 'function') ensureGastosCompactCSS();

  table.innerHTML = `
    <thead>
      <tr>
        <th>ASUNTO</th>
        ${showAutor ? '<th>AUTOR</th>' : ''}
        <th>MONEDA</th>
        <th>VALOR</th>
        <th>ESTADO</th>
        <th>COMPROBANTE</th>
      </tr>
    </thead>
    <tbody></tbody>
  `;
  const tb = table.querySelector('tbody');

  // ---- FILAS ----
  list.forEach(x=>{
    const tr = document.createElement('tr');

    const tdAsu = document.createElement('td');
    tdAsu.setAttribute('data-label','ASUNTO');
    tdAsu.textContent = String(x.asunto||'').toUpperCase();

    let tdAut = null;
    if (showAutor){
      tdAut = document.createElement('td');
      tdAut.setAttribute('data-label','AUTOR');
      tdAut.textContent = String(x.byEmail||'').toUpperCase();
    }

    const tdMon = document.createElement('td');
    tdMon.setAttribute('data-label','MONEDA');
    tdMon.textContent = String(x.moneda||'').toUpperCase();

    const tdVal = document.createElement('td');
    tdVal.setAttribute('data-label','VALOR');
    tdVal.textContent = Number(x.valor||0).toLocaleString('es-CL');

    const tdEst = document.createElement('td');
    tdEst.setAttribute('data-label','ESTADO');

    if (state.is){
      const sel = document.createElement('select');
      sel.innerHTML = `
        <option value="PENDIENTE">PENDIENTE</option>
        <option value="APROBADO">APROBADO</option>
        <option value="RECHAZADO">RECHAZADO</option>
      `;
      sel.value = x.estado || 'PENDIENTE';
      sel.onchange = async ()=>{
        const nuevo = sel.value;
        try{
          await updateDoc(
            doc(db,'coordinadores', coordId, 'gastos', x.id),
            { estado: nuevo, estadoAt: serverTimestamp(), estadoBy: (state.user?.email||'').toLowerCase() }
          );
          x.estado = nuevo;
          showFlash && showFlash('ESTADO ACTUALIZADO','ok');
          // refrescar RESUMEN/SALDOS
          if (paneRef) await renderFinanzas(g, paneRef);
        }catch(e){
          console.error(e);
          showFlash && showFlash('NO SE PUDO ACTUALIZAR EL ESTADO','err');
          sel.value = x.estado || 'PENDIENTE';
        }
      };
      tdEst.appendChild(sel);
    }else{
      tdEst.textContent = String(x.estado || 'PENDIENTE').toUpperCase();
    }

    const tdComp = document.createElement('td');
    tdComp.setAttribute('data-label','COMPROBANTE');
    tdComp.innerHTML = x.imgUrl ? `<a href="${x.imgUrl}" target="_blank">VER</a>` : '—';

    tr.appendChild(tdAsu);
    if (showAutor) tr.appendChild(tdAut);
    tr.appendChild(tdMon);
    tr.appendChild(tdVal);
    tr.appendChild(tdEst);
    tr.appendChild(tdComp);
    tb.appendChild(tr);
  });

  box.appendChild(table);

  const totDiv = document.createElement('div');
  totDiv.className = 'totline gastos';
  totDiv.textContent =
    `TOTAL — CLP: ${fmtCL(tot.CLP||0)} · USD: ${fmtCL(tot.USD||0)} · BRL: ${fmtCL(tot.BRL||0)} · ARS: ${fmtCL(tot.ARS||0)}`;
  box.appendChild(totDiv);

  return hits;
}

/* ====== IMPRIMIR VOUCHERS (STAFF) ====== */
function openPrintVouchersModal(){
  const back=document.getElementById('modalBack'); const body=document.getElementById('modalBody'); const title=document.getElementById('modalTitle');
  title.textContent='IMPRIMIR VOUCHERS (STAFF)';
  const coordOpts=[`<option value="__ALL__">TODOS</option>`].concat(state.coordinadores.map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()}</option>`)).join('');
  body.innerHTML=`
    <div class="rowflex"><label>COORDINADOR</label><select id="pvCoord">${coordOpts}</select></div>
    <div class="rowflex"><input type="text" id="pvDestino" placeholder="DESTINO (OPCIONAL)"/><input type="text" id="pvRango" placeholder="RANGO DD-MM-AAAA..DD-MM-AAAA (OPCIONAL)"/></div>
    <div class="rowflex"><button id="pvGo" class="btn ok">GENERAR</button></div>`;
  document.getElementById('pvGo').onclick=async ()=>{
    const coordSel=document.getElementById('pvCoord').value;
    const dest=(document.getElementById('pvDestino').value||'').trim();
    const rango=(document.getElementById('pvRango').value||'').trim();
    let list=state.grupos.slice();
    if(coordSel!=='__ALL__'){
      const emailElegido=(state.coordinadores.find(c=>c.id===coordSel)?.email || '').toLowerCase();
      list=list.filter(g=> emailsOf(g).includes(emailElegido));
    }
    if(dest) list=list.filter(g=> norm(g.destino||'').includes(norm(dest)));
    if(/^\d{2}-\d{2}-\d{4}\.\.\d{2}-\d{2}-\d{4}$/.test(rango)){ const [a,b]=rango.split('..'); const A=ymdFromDMY(a), B=ymdFromDMY(b);
      list=list.filter(g=> !( (g.fechaFin && g.fechaFin < A) || (g.fechaInicio && g.fechaInicio > B) )); }
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
      for(const a of (g.itinerario[f]||[])){
        const servicio=await findServicio(g.destino, a.actividad);
        const tRaw=(servicio?.voucher||'No Aplica').toString();
        const t = /electron/i.test(tRaw)?'ELECTRONICO':(/fisic/i.test(tRaw)?'FISICO':'NOAPLICA');
        if(t==='NOAPLICA') continue;
        rows += renderVoucherHTMLSync(g,f,a,null,true);
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

    // Log inmutable del restablecimiento (quedará en HISTORIAL DEL VIAJE)
   // Log inmutable (si falla, NO bloquea el flujo)
   try {
     await appendViajeLog(
       grupo.id,
       'RESTABLECER_INICIO',
       'SE RESTABLECIERON INICIO/FIN Y PAX VIAJANDO (LIMPIEZA)'
     );
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
    await renderOneGroup(grupo);
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
