/* COORDINADORES.JS — PORTAL COORDINADORES RT
   — VERSIÓN: ALERTAS MEJORADAS + MOBILE + TODO EN MAYÚSCULAS EN LA UI
   — Cambios: orden de actividades por hora, botón “Crear Alerta” en Alertas,
              contadores de búsqueda por pestaña, menos parpadeo en auto-refresco.
   — + VIAJE: Inicio/Termino, paxViajando, reversibles por STAFF, PAX tachado.
*/

import { app, db, auth, storage } from './firebase-init-portal.js';
import { onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, getDocs, getDoc, doc, updateDoc, addDoc, setDoc,
  serverTimestamp, query, where, orderBy, limit, deleteField, deleteDoc, startAfter
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import { ref as sRef, uploadBytes, getDownloadURL }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

/* ====== UTILS TEXTO/FECHAS ====== */
const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');
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

/* ====== EXTRACCIÓN TOLERANTE DESDE GRUPOS ====== */
const arrify=v=>Array.isArray(v)?v:(v&&typeof v==='object'?Object.values(v):(v?[v]:[]));
function emailsOf(g){ const out=new Set(), push=e=>{if(e) out.add(String(e).toLowerCase());};
  push(g?.coordinadorEmail); push(g?.coordinador?.email); arrify(g?.coordinadoresEmails).forEach(push);
  if(g?.coordinadoresEmailsObj) Object.keys(g.coordinadoresEmailsObj).forEach(push);
  arrify(g?.coordinadores).forEach(x=>{ if(x?.email) push(x.email); else if(typeof x==='string'&&x.includes('@')) push(x); });
  return [...out];
}
function coordDocIdsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); };
  push(g?.coordinadorId); arrify(g?.coordinadoresIds).forEach(push);
  const mapEmailToId = new Map(state.coordinadores.map(c => [String(c.email || '').toLowerCase(), c.id]));
  emailsOf(g).forEach(e=>{ if(mapEmailToId.has(e)) out.add(mapEmailToId.get(e)); });
  return [...out];
}

/* ====== ESTADO APP ====== */
const STAFF_EMAILS = new Set(['aleoperaciones@raitrai.cl','operaciones@raitrai.cl','anamaria@raitrai.cl','tomas@raitrai.cl','sistemas@raitrai.cl'].map(x=>x.toLowerCase()));
const state = {
  user:null,
  is:false,
  coordinadores:[],
  viewingCoordId:null,              // STAFF: ID SELECCIONADO · COORD: SU PROPIO ID
  grupos:[], ados:[], idx:0,
  filter:{ type:'all', value:null },
  groupQ:'',
  alertsTimer:null,                 // AUTO-REFRESCO DE ALERTAS (60S)
  cache:{
    hotel:new Map(),
    vuelos:new Map(),
    tasas:null,
    hoteles:{ loaded:false, byId:new Map(), bySlug:new Map(), all:[] }
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
  ['alertsPanel','staffBar','statsPanel','navPanel','gruposPanel'].forEach(id=>{
    const n=document.getElementById(id);
    if(n) wrap.appendChild(n);
  });
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
  }, 2200);
}

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
   if (btnPrint){
     btnPrint.style.display = state.is ? '' : 'none';
     if (state.is) btnPrint.textContent = 'IMPRIMIR DESPACHO';
   }
   const legacyNewAlert = document.getElementById('btnNewAlert');
   if (legacyNewAlert) legacyNewAlert.style.display = 'none';

  // PANEL ALERTAS
  await renderGlobalAlerts();

  // AUTO-REFRESCO CADA 60S (solo alertas, sin reordenar paneles)
  if (!state.alertsTimer){
    state.alertsTimer = setInterval(renderGlobalAlerts, 60000);
  }
});

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
async function showSelector(coordinadores){
  const bar=ensurePanel('staffBar',
    '<label style="display:block;margin-bottom:6px;color:#cbd5e1">COORDINADOR(A):</label>'+
    '<select id="coordSelect"></select>'
  );
  const sel=bar.querySelector('#coordSelect');
  sel.innerHTML =
    '<option value="__ALL__">TODOS</option>' +
    coordinadores.map(c => `<option value="${c.id}">${(c.nombre||'').toUpperCase()} — ${(c.email||'').toUpperCase()}</option>`).join('');
  sel.onchange = async ()=> {
    const id = sel.value || '';
    const elegido = (id==='__ALL__') ? { id:'__ALL__' } : (coordinadores.find(c=>c.id===id) || null);
    state.viewingCoordId = id || null;
    localStorage.setItem('rt__coord', id);
    await loadGruposForCoordinador(elegido, state.user);
    await renderGlobalAlerts();
  };
  const last=localStorage.getItem('rt__coord');
  if (last){
    sel.value=last;
    const elegido = (last==='__ALL__') ? { id:'__ALL__' } : (coordinadores.find(c=>c.id===last) || null);
    state.viewingCoordId=last;
    await loadGruposForCoordinador(elegido, state.user);
  }
}

/* ====== GRUPOS PARA EL COORDINADOR EN CONTEXTO (O "TODOS") ====== */
async function loadGruposForCoordinador(coord, user){
  const cont=document.getElementById('grupos'); if (cont) cont.textContent='CARGANDO GRUPOS…';

  const allSnap=await getDocs(collection(db,'grupos'));
  const wanted=[];
  const isAll = coord && coord.id==='__ALL__';

  const emailElegido=(coord?.email||'').toLowerCase();
  const docIdElegido=(coord?.id||'').toString();
  const isSelf = !coord || coord.id==='self' || emailElegido===(user.email||'').toLowerCase();

  allSnap.forEach(d=>{
    const raw={id:d.id, ...d.data()};
    const g={
      ...raw,
      fechaInicio: toISO(raw.fechaInicio||raw.inicio||raw.fecha_ini),
      fechaFin: toISO(raw.fechaFin||raw.fin||raw.fecha_fin),
      itinerario: normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
      serviciosEstado: raw.serviciosEstado || {},
      numeroNegocio: String(raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || d.id),
      identificador: String(raw.identificador || raw.codigo || '')
    };
    if (isAll){ wanted.push(g); return; }
    const gEmails=emailsOf(raw), gDocIds=coordDocIdsOf(raw);
    const match=(emailElegido && gEmails.includes(emailElegido)) ||
                (docIdElegido && gDocIds.includes(docIdElegido)) ||
                (isSelf && gEmails.includes((user.email||'').toLowerCase()));
    if (match) wanted.push(g);
  });

  // ORDENAR (FUTUROS → PASADOS)
  const hoy=toISO(new Date());
  const futuros=wanted.filter(g=>(g.fechaInicio||'')>=hoy).sort((a,b)=>(a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  const pasados=wanted.filter(g=>(g.fechaInicio||'')<hoy).sort((a,b)=>(a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  state.grupos=wanted; state.ordenados=[...futuros,...pasados];

  state.filter={type:'all',value:null};
  state.groupQ='';

  renderStatsFiltered();
  renderNavBar();

  const { g:qsG, f:qsF } = parseQS();
  let idx=0;
  if (qsG){
    const byNum=state.ordenados.findIndex(x=> String(x.numeroNegocio)===qsG);
    const byId=state.ordenados.findIndex(x=> String(x.id)===qsG);
    idx=byNum>=0?byNum:(byId>=0?byId:0);
  }else{
    const last=localStorage.getItem('rt_last_group');
    if(last){ const i=state.ordenados.findIndex(x=> x.id===last || x.numeroNegocio===last); if(i>=0) idx=i; }
  }
  state.idx = Math.max(0, Math.min(idx, state.ordenados.length - 1));

  // AÑADIDO: garantizar itinerario antes de renderizar
  const target = state.ordenados[state.idx];
  await ensureItinerarioLoaded(target);

  await renderOneGroup(target, qsF);
}

/* ====== NORMALIZADOR DE ITINERARIO (multiesquema) ====== */
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
  const p=document.getElementById('navPanel');
  const sel=p.querySelector('#allTrips'); sel.textContent='';

  // FILTRO TODOS (SOLO UI DEL SELECT DE VIAJES)
  const ogFiltro=document.createElement('optgroup'); ogFiltro.label='FILTRO';
  ogFiltro.appendChild(new Option('TODOS','all')); sel.appendChild(ogFiltro);

  // VIAJES
  const ogTrips=document.createElement('optgroup'); ogTrips.label='VIAJES';
  state.ordenados.forEach((g,i)=>{
    const name=(g.nombreGrupo||g.aliasGrupo||g.id);
    const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
    const opt=new Option(`${(g.destino||'').toUpperCase()} · ${(name||'').toUpperCase()} (${code}) | IDA: ${dmy(g.fechaInicio||'')}  VUELTA: ${dmy(g.fechaFin||'')}`, `trip:${i}`);
    ogTrips.appendChild(opt);
  });
  sel.appendChild(ogTrips);
  sel.value=`trip:${state.idx}`;

  p.querySelector('#btnPrev').onclick=async ()=>{ const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.max(0,j-1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId); await renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`; };
  p.querySelector('#btnNext').onclick=async ()=>{ const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.min(list.length-1,j+1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId); await renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`; };
  sel.onchange=async ()=>{ const v=sel.value||''; if(v==='all'){ state.filter={type:'all',value:null}; renderStatsFiltered(); sel.value=`trip:${state.idx}`; }
    else if(v.startsWith('trip:')){ state.idx=Number(v.slice(5))||0; await renderOneGroup(state.ordenados[state.idx]); } };

   if (state.is){
     const btn = p.querySelector('#btnPrintVch');
     if (btn){
       btn.textContent = 'IMPRIMIR DESPACHO';
       // btn.onclick = openPrintVouchersModal; // ← LEGACY: comentado a pedido
       btn.onclick = async () => {
         const g = state.ordenados[state.idx];
         await openPrintDespacho(g);
       };
     }
     // (el botón “crear alerta” ya se maneja en el panel de alertas)
   }
}

/* ====== VISTA GRUPO ====== */
async function renderOneGroup(g, preferDate){
  const cont=document.getElementById('grupos'); if(!cont) return; cont.innerHTML='';
  if(!g){ cont.innerHTML='<p class="muted">NO HAY VIAJES.</p>'; return; }
  localStorage.setItem('rt_last_group', g.id);

  const name=(g.nombreGrupo||g.aliasGrupo||g.id);
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
   
   // Botón RESTABLECER (STAFF + viaje iniciado) – gris, full width, debajo del inicio
   const btnResetInicioHtml = (state.is && started && !finished)
     ? `<button id="btnResetInicio" class="btn" style="width:100%;background:#64748b;color:#fff;">RESTABLECER</button>`
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
   if (btnR0) btnR0.onclick = async () => { await staffResetInicio(g); };

   const histBox = header.querySelector('#viajeHistoryBox');
   renderViajeHistory(g, histBox);

  const tabs=document.createElement('div');
  tabs.innerHTML=`
    <div style="display:flex;gap:.5rem;margin:.6rem 0">
      <button id="tabResumen" class="btn sec">RESUMEN</button>
      <button id="tabItin"    class="btn sec">ITINERARIO</button>
      <button id="tabFin"     class="btn sec">FINANZAS</button>
    </div>
    <div id="paneResumen"></div>
    <div id="paneItin" style="display:none"></div>
    <div id="paneFin"  style="display:none"></div>`;
  cont.appendChild(tabs);

  const paneResumen=tabs.querySelector('#paneResumen');
  const paneItin=tabs.querySelector('#paneItin');
  const paneFin=tabs.querySelector('#paneFin');
  const btnResumen=tabs.querySelector('#tabResumen');
  const btnItin=tabs.querySelector('#tabItin');
  const btnFin=tabs.querySelector('#tabFin');

  const setTabLabel=(btn, base, n)=>{
    const q=(state.groupQ||'').trim();
    btn.textContent = (q && n>0) ? `${base} (${n})` : base;
  };
  const show = (w)=>{ 
    paneResumen.style.display=w==='resumen'?'':'none';
    paneItin.style.display   =w==='itin'   ?'':'none';
    paneFin.style.display    =w==='fin'    ?'':'none';
  };
  btnResumen.onclick=()=>show('resumen');
  btnItin.onclick   =()=>show('itin');
  btnFin.onclick    =()=>show('fin');

  // Render y contadores
  const resumenHits = await renderResumen(g, paneResumen);
  const itinHits    = renderItinerario(g, paneItin, preferDate);
  const finHits     = await renderFinanzas(g, paneFin);   // 👈 NUEVO
  setTabLabel(btnResumen, 'RESUMEN', resumenHits);
  setTabLabel(btnItin,    'ITINERARIO', itinHits);
  setTabLabel(btnFin,     'FINANZAS', finHits);

  show('resumen');

  // BÚSQUEDA INTERNA
  const input=header.querySelector('#searchTrips');
  input.value = state.groupQ || '';
  let tmr=null;
  input.oninput=()=>{ clearTimeout(tmr); tmr=setTimeout(async ()=>{
    state.groupQ=input.value||'';
    const active=paneItin.style.display!=='none'?'itin':(paneGastos.style.display!=='none'?'gastos':'resumen');

    const r = await renderResumen(g, paneResumen);
    const i = renderItinerario(g, paneItin, localStorage.getItem('rt_last_date_'+g.id) || preferDate);
    const ga= await renderGastos(g, paneGastos);

    setTabLabel(btnResumen,'RESUMEN',r);
    setTabLabel(btnItin,'ITINERARIO',i);
    setTabLabel(btnGastos,'GASTOS',ga);

    show(active);
  },180); };
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
  const vuelosBox=document.createElement('div'); vuelosBox.className='act';
  vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">BUSCANDO…</div>'; wrap.appendChild(vuelosBox);

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
        (act.horaInicio || '--:--') + '–' + (act.horaFin || '--:--') +
        ' · PLAN: ' + fmtPaxPlan(plan, grupo) + ' PAX' +
      '</div>' +
      '<div class="rowflex" style="margin:.35rem 0">' +
        '<input type="number" min="0" inputmode="numeric" placeholder="N° ASISTENCIA" value="' + paxFinalInit + '"/>' +
        '<textarea placeholder="COMENTARIOS"></textarea>' +
        '<button class="btn ok btnSave">GUARDAR</button>' +
        vchPlaceholder +
        '<button class="btn sec btnActInfo">DETALLE/COMENTARIOS…</button>' +
      '</div>' +
      '<div class="bitacora" style="margin-top:.4rem">' +
        '<div class="muted" style="margin-bottom:.25rem">BITÁCORA</div>' +
        '<div class="bitItems" style="display:grid;gap:.35rem"><div class="muted">CARGANDO…</div></div>' +
      '</div>';

    cont.appendChild(div);

    // — Detalle/Comentarios (servicio se resuelve dentro del modal si es necesario)
    const btnAI = div.querySelector('.btnActInfo');
    if (btnAI) btnAI.onclick = async () => {
      try {
        const servicio = await findServicio(grupo.destino, actName).catch(()=>null);
        const tipoRaw  = (servicio?.voucher || 'No Aplica').toString();
        const tipo = /electron/i.test(tipoRaw) ? 'ELECTRONICO' : (/fisic/i.test(tipoRaw) ? 'FISICO' : 'NOAPLICA');
        await openActividadModal(grupo, fechaISO, act, servicio, tipo);
      } catch(e) {
        console.error('openActividadModal error', e);
      }
    };

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
              nombre: (grupo.nombreGrupo||grupo.aliasGrupo||grupo.id),
              code: (grupo.numeroNegocio||'')+(grupo.identificador?('-'+grupo.identificador):''),
              destino: (grupo.destino||null),
              programa: (grupo.programa||null),
              fechaActividad: fechaISO,
              actividad: actName
            }
          });

          await loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'));
          div.querySelector('textarea').value='';
          await renderGlobalAlerts();
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

    // (2) Servicio / botón de voucher asíncrono
    (async () => {
      try {
        const servicio = await findServicio(grupo.destino, actName);
        const tipoRaw  = (servicio?.voucher || 'No Aplica').toString();
        const tipo = /electron/i.test(tipoRaw) ? 'ELECTRONICO' : (/fisic/i.test(tipoRaw) ? 'FISICO' : 'NOAPLICA');

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
        // Si falla la búsqueda del servicio, simplemente no mostramos botón
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
   // === HASTA AQUÍ ===


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
async function findServicio(destino, nombre){
  if(!destino||!nombre) return null;
  const want=norm(nombre);
  const candidates=[ ['Servicios',destino,'Listado'], [destino,'Listado'] ];
  for(const path of candidates){
    try{
      const snap=await getDocs(collection(db,path[0],path[1],path[2]));
      let best=null; snap.forEach(d=>{ const x=d.data()||{}; const serv=String(x.servicio||x.nombre||d.id||''); if(norm(serv)===want) best={id:d.id,...x}; });
      if(best) return best;
    }catch(_){}
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
async function resolveThreadKey(grupo, fechaISO, act, servicioHint=null){
  const actName = (act?.actividad || '').toString();
  const destino = (grupo?.destino || '').toString().toUpperCase().trim();
  const meal = isMealAct(actName);

  // (C) Comida de hotel → elegir hotel correcto según CHEQ OUT
  if (meal){
    const hoteles = await loadHotelesInfo(grupo) || [];
    const h = pickHotelForMeal(grupo, fechaISO, act, hoteles);
    if (h && (h.hotel?.id || h.hotelId || h.id)){
      const hId = h.hotel?.id || h.hotelId || h.id;
      return { key: threadKeyForHotelMeal(hId, meal), scope:'C' };
    }
    // si no hay hotel identificable, cae a GENERAL (B)
  }

  // Intento (A) proveedor + servicio
  let servicio = servicioHint;
  if (!servicio){
    try{ servicio = await findServicio(destino, actName); }catch(_){/* ignore */}
  }
  const servicioId = servicio?.id || null;

  // Proveedor (prioriza servicio.proveedor o act.proveedor)
  const proveedorName = (servicio?.proveedor || act?.proveedor || '').toString().trim();
  let proveedorId = null;
  if (proveedorName){
    try{
      const provDoc = await findProveedorDocByDestino(destino, proveedorName);
      if (provDoc?.id) proveedorId = provDoc.id;
    }catch(_){}
  }

  if (servicioId && proveedorId){
    return { key: threadKeyForProv(destino, proveedorId, servicioId), scope:'A' };
  }

  // (B) GENERAL
  return { key: threadKeyForGeneral(destino, actName), scope:'B' };
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
      <div class="meta">GRUPO: ${(g.nombreGrupo||g.aliasGrupo||g.id).toString().toUpperCase()} (${code})</div>
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

  let proveedorDoc=null;
  try{
    if(servicio?.proveedor){
      const qs=await getDocs(collection(db,'Proveedores'));
      qs.forEach(d=>{ const x=d.data()||{}; if(norm(x.nombre||d.id||'')===norm(servicio.proveedor||'')) proveedorDoc={id:d.id,...x}; });
    }
  }catch(_){}

  const voucherHTML=renderVoucherHTMLSync(g,fechaISO,act,proveedorDoc,false);

  if(tipo==='FISICO'){
    body.innerHTML= `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem">
        <button id="vchPrint" class="btn sec">IMPRIMIR</button>
        <button id="vchOk" class="btn ok">FINALIZAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>`;
    document.getElementById('vchPrint').onclick=()=>{ const w=window.open('','_blank'); w.document.write(`<!doctype html><html><body>${voucherHTML}</body></html>`); w.document.close(); w.print(); };
    document.getElementById('vchOk').onclick   =()=> setEstadoServicio(g,fechaISO,act,'FINALIZADA', true);
    document.getElementById('vchPend').onclick =()=> setEstadoServicio(g,fechaISO,act,'PENDIENTE',  true);
  } else {
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

/* ====== IMPRESIÓN DE DESPACHO (PDF por print) ====== */

// Reúne una línea por actividad del itinerario con contacto de proveedor
async function collectItinLines(grupo){
  const out = [];
  const fechas = rangoFechas(grupo.fechaInicio, grupo.fechaFin);
  for (const f of fechas){
    // tolerar objeto indexado
    let acts = (grupo.itinerario && grupo.itinerario[f]) ? grupo.itinerario[f] : [];
    if (!Array.isArray(acts)) acts = Object.values(acts||{}).filter(x => x && typeof x === 'object');

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
          hora: a?.horaInicio || '--:--',
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

async function openPrintDespacho(g){
  if (!g){ alert('No hay viaje activo.'); return; }

  // ==== DATOS BASE ====
  const code = (g.numeroNegocio||'') + (g.identificador?('-'+g.identificador):'');
  const paxPlan = paxOf(g);
  const paxReal = paxRealOf(g);
  const { A: A_real, E: E_real } = paxBreakdown(g);
  const fechasTxt = `${dmy(g.fechaInicio||'')} — ${dmy(g.fechaFin||'')}`;

  // Itinerario (líneas)
  const itin = await collectItinLines(g);

  // Finanzas: SOLO ABONOS (usa tu función existente loadAbonos)
  let abonos = [];
  try { abonos = await loadAbonos(g.id); } catch(_){}
  const abonosRows = [];
  for (const a of (abonos||[])){
    const fecha  = dmy(toISO(a.fecha||''));                 // fecha abono
    const moneda = (a.moneda||'').toString().toUpperCase();  // CLP/USD/BRL/ARS
    const valor  = Number(a.valor||0);
    let clpEq    = 0;
    try { clpEq = await convertirMoneda(valor, moneda, 'CLP'); } catch(_){ clpEq = 0; }
    abonosRows.push({ fecha, asunto:(a.asunto||'').toString().toUpperCase(), moneda, valor, clp: clpEq });
  }
  const totalCLP = Math.round(abonosRows.reduce((s,x)=> s + Number(x.clp||0), 0));

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
      <div><strong>GRUPO:</strong> ${(g.nombreGrupo||g.aliasGrupo||g.id).toString().toUpperCase()}</div>
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

  const itinRows = itin.map(x => `
    <tr>
      <td>${dmy(x.fechaISO)}</td>
      <td>${(x.hora||'--:--')}</td>
      <td>${x.actividad}</td>
      <td>${x.proveedor}</td>
      <td>${x.contacto||'—'}</td>
      <td>${x.estado||''}</td>
    </tr>`).join('');

  const itinerario = `
    <h2>ITINERARIO</h2>
    <table class="t-tight">
      <thead>
        <tr><th>FECHA</th><th>HORA</th><th>ACTIVIDAD</th><th>PROVEEDOR</th><th>CONTACTO</th><th>ESTADO</th></tr>
      </thead>
      <tbody>${itinRows || '<tr><td colspan="6" class="muted">SIN ACTIVIDADES.</td></tr>'}</tbody>
    </table>
  `;

  const finRows = abonosRows.map(r => `
    <tr>
      <td>${r.fecha||'—'}</td>
      <td>${r.asunto||'—'}</td>
      <td class="right">${(r.valor||0).toLocaleString('es-CL')}</td>
      <td>${r.moneda||''}</td>
      <td class="right">${Math.round(r.clp||0).toLocaleString('es-CL')}</td>
    </tr>`).join('');

  const finanzas = `
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
  `;

  const html = `
    <!doctype html><html><head><meta charset="utf-8">${css}</head>
    <body>
      ${infoGeneral}
      ${resumen}
      <div class="cut">${itinerario}</div>
      <div class="cut">${finanzas}</div>
      <div class="footer">RAITRAI — Despacho de Viaje. Para PDF usa “Guardar como PDF”.</div>
      <script>window.addEventListener('load',()=>{ window.print(); setTimeout(()=>window.close(), 600); });</script>
    </body></html>
  `;

  const w = window.open('', '_blank', 'noopener');
  if (!w){ alert('No se pudo abrir la ventana de impresión (popup bloqueado).'); return; }
  w.document.open('text/html');
  w.document.write(html);
  w.document.close();
}

function formatCL(dt){
  try{
    return new Intl.DateTimeFormat('es-CL',{
      timeZone:'America/Santiago',
      year:'numeric',month:'2-digit',day:'2-digit',
      hour:'2-digit',minute:'2-digit'
    }).format(dt).toUpperCase();
  }catch(_){ return dt.toLocaleString('es-CL').toUpperCase(); }
}

// Busca proveedor en la ruta por DESTINO (según la actividad/servicio)
async function fetchProveedorByDestino(destino, proveedorName){
  if (!destino || !proveedorName) return null;
  try{
    const qs = await getDocs(collection(db,'Proveedores', String(destino).toUpperCase(), 'Listado'));
    let hit = null;
    qs.forEach(d=>{
      const x = d.data()||{};
      const nom = (x.proveedor || d.id || '').toString();
      if (norm(nom) === norm(proveedorName)) hit = { id:d.id, ...x };
    });
    return hit;
  }catch(_){ return null; }
}

async function openActividadModal(g, fechaISO, act, servicio=null, tipoVoucher='NOAPLICA'){
  const back  = document.getElementById('modalBack');
  const title = document.getElementById('modalTitle');
  const body  = document.getElementById('modalBody');

  const actName = (act?.actividad || 'ACTIVIDAD').toString();
  const actKey  = slug(actName);
  const destino = (g?.destino || '').toString().toUpperCase();
  // === NUEVO: resolver hilo global (A/B/C) considerando CHEQ OUT
  let thread = { key:'', scope:'B' };
  try{
    thread = await resolveThreadKey(g, fechaISO, act, servicio);
  }catch(_){}


  // Servicio (para indicaciones/voucher) ya lo traes con findServicio(destino, actName).
  // Aquí sólo nos aseguramos de tener indicaciones/voucher priorizando Servicios/{destino}/Listado.
  let indicaciones = '';
  let voucherLabel = (tipoVoucher || 'NOAPLICA').toString().toUpperCase();

  try{
    // si ya vino "servicio" desde findServicio úsalo tal cual:
    if (servicio){
      indicaciones = String(
        servicio.indicaciones || servicio.instrucciones || act?.indicaciones || act?.instrucciones || ''
      ).trim();
      const vRaw = (servicio.voucher || voucherLabel || '').toString();
      voucherLabel = /electron/i.test(vRaw) ? 'ELECTRONICO' : (/fisic/i.test(vRaw) ? 'FISICO' : 'NOAPLICA');
    }
  }catch(_){}

  // Proveedor por DESTINO (Proveedores/{destino}/Listado)
  let proveedorDoc = null;
  try{
    const provNom = (servicio?.proveedor || act?.proveedor || '').toString();
    proveedorDoc = await fetchProveedorByDestino(destino, provNom);
  }catch(_){}

  const nombreProv  = (proveedorDoc?.proveedor || act?.proveedor || '—').toString().toUpperCase();
  const contactoNom = (proveedorDoc?.contacto  || '').toString().toUpperCase();
  const contactoTel = (proveedorDoc?.telefono  || '').toString().toUpperCase();
  const contactoMail= (proveedorDoc?.correo    || '').toString().toUpperCase();

  title.textContent = `DETALLE — ${actName.toUpperCase()} — ${dmy(fechaISO)}`;
  const scopeBadge = (thread.scope==='A'?'PROVEEDOR':
                     thread.scope==='C'?'HOTEL/COMIDA':'GENERAL');
  const scopeLine = `<div class="meta"><strong>HILO:</strong> ${scopeBadge} · ${thread.key}</div>`;
   body.innerHTML = `
     ${scopeLine}
     <div class="card">
       <div class="meta"><strong>PROVEEDOR:</strong> ${nombreProv}</div>
       ${contactoNom ? `<div class="meta"><strong>CONTACTO:</strong> ${contactoNom}</div>` : ''}
       ${contactoTel ? `<div class="meta"><strong>TELÉFONO:</strong> ${contactoTel}</div>` : ''}
       ${contactoMail? `<div class="meta"><strong>CORREO:</strong> ${contactoMail}</div>` : ''}
       <div class="meta"><strong>VOUCHER:</strong> ${voucherLabel}</div>
       <div class="meta"><strong>HORARIO:</strong> ${(act.horaInicio||'--:--')}–${(act.horaFin||'--:--')}</div>
     </div>
     <div class="act">
       <h4>INDICACIONES</h4>
       ${indicaciones ? `<div class="meta" style="white-space:pre-wrap">${indicaciones.toUpperCase()}</div>` : '<div class="muted">SIN INDICACIONES.</div>'}
     </div>
     <div class="act" id="foroBox">
       <h4>COMENTARIOS DE LA ACTIVIDAD</h4>
       <div class="rowflex" style="margin:.35rem 0">
         <textarea id="foroText" placeholder="ESCRIBE UN COMENTARIO (SE PUBLICA CON TU CORREO)"></textarea>
         <button id="foroSend" class="btn ok">PUBLICAR</button>
       </div>
       <div class="muted">Los comentarios del STAFF aparecen primero.</div>
       <div id="foroList" style="display:grid;gap:.4rem;margin-top:.5rem"></div>
       <div class="rowflex" style="justify-content:center;margin-top:.4rem">
         <button id="foroMore" class="btn sec" style="display:none">CARGAR MÁS</button>
       </div>
     </div>
   `;

  // ===== Paginación (STAFF arriba, 10 por página, botón "CARGAR MÁS") =====
  const paging = { cursor:null, exhausted:false, loading:false, pageSize:10, items:[] };

  const renderForo = ()=>{
    const wrap = body.querySelector('#foroList');
    wrap.innerHTML = '';

    const staff = paging.items.filter(x=>x.isStaff).sort((a,b)=> b.tsMs - a.tsMs);
    const resto = paging.items.filter(x=>!x.isStaff).sort((a,b)=> b.tsMs - a.tsMs);
    const ordered = [...staff, ...resto];

    if(!ordered.length){
      wrap.innerHTML = '<div class="muted">AÚN NO HAY COMENTARIOS.</div>';
      body.querySelector('#foroMore').style.display = paging.exhausted ? 'none' : 'none';
      return;
    }
    ordered.forEach(x=>{
      const div = document.createElement('div');
      div.className='card';
      div.innerHTML = `
        <div class="meta" style="display:flex;gap:.5rem;align-items:center">
          ${x.isStaff?'<span class="badge" style="background:#1d4ed8;color:#fff">STAFF</span>':''}
          <strong>${(x.byEmail||'').toUpperCase()}</strong> · ${formatCL(new Date(x.tsMs||Date.now()))}
        </div>
        <div style="margin-top:.25rem;white-space:pre-wrap">${(x.texto||'').toString().toUpperCase()}</div>
      `;
      wrap.appendChild(div);
    });

    const moreBtn = body.querySelector('#foroMore');
    moreBtn.style.display = paging.exhausted ? 'none' : '';
  };

   const loadPage = async ()=>{
     if (paging.loading || paging.exhausted) return;
     paging.loading = true;
     try{
       let qy = query(threadColl(thread.key), orderBy('ts','desc'), limit(paging.pageSize + 1));
       if (paging.cursor){
         qy = query(threadColl(thread.key), orderBy('ts','desc'), startAfter(paging.cursor), limit(paging.pageSize + 1));
       }
       const snap = await getDocs(qy);
       const docs = snap.docs;
   
       if (docs.length > paging.pageSize){
         paging.cursor = docs[paging.pageSize - 1];
       }else{
         paging.cursor = docs[docs.length - 1] || paging.cursor;
         paging.exhausted = true;
       }
   
       const add = docs.slice(0, paging.pageSize).map(d=>{
         const x = d.data() || {};
         const tsMs = x.ts?.seconds ? x.ts.seconds*1000 : Date.now();
         return { id:d.id, texto:String(x.texto||''), byEmail:String(x.byEmail||x.by||'').toLowerCase(), isStaff:!!x.isStaff, tsMs };
       });
   
       const seen = new Set(paging.items.map(z=>z.id));
       add.forEach(z=>{ if(!seen.has(z.id)) paging.items.push(z); });
   
       renderForo();
     }catch(e){
       console.error('FORO loadPage', e);
       alert('NO SE PUDO CARGAR COMENTARIOS.');
     }finally{
       paging.loading = false;
     }
   };


  body.querySelector('#foroMore').onclick = loadPage;

  body.querySelector('#foroSend').onclick = async ()=>{
    const ta = body.querySelector('#foroText');
    const texto = (ta.value||'').trim();
    if(!texto){ alert('ESCRIBE UN COMENTARIO.'); return; }
    try{
        await addDoc(threadColl(thread.key),{
          texto,
          byUid: state.user.uid,
          byEmail: (state.user.email||'').toLowerCase(),
          isStaff: !!state.is,
          ts: serverTimestamp()
        });
      ta.value='';
      // refrescar desde el inicio para mantener orden STAFF/fecha
      paging.cursor = null; paging.exhausted = false; paging.items = [];
      await loadPage();
    }catch(e){
      console.error('FORO send', e);
      alert('NO SE PUDO PUBLICAR.');
    }
  };

  document.getElementById('modalClose').onclick = () => { document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
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
async function recipientsFromFilters(destinosList, rangoStr){
  const wantedDest = destinosList.map(d=>norm(d)).filter(Boolean);
  let A=null,B=null;
  if(/^\d{2}-\d{2}-\d{4}\.\.\d{2}-\d{2}-\d{4}$/.test((rangoStr||'').trim())){ const [a,b]=rangoStr.split('..'); A=ymdFromDMY(a); B=ymdFromDMY(b); }
  else if(/^\d{2}-\d{2}-\d{4}$/.test((rangoStr||'').trim())){ const d=ymdFromDMY(rangoStr.trim()); A=d; B=d; }

  if(!wantedDest.length && !A) return new Set();

  const r = new Set();
  const mapEmailToId = new Map(state.coordinadores.map(c=>[(c.email||'').toLowerCase(), c.id]));
  const snap=await getDocs(collection(db,'grupos'));
  snap.forEach(d=>{
    const g={id:d.id, ...(d.data()||{})};
    const destOk = !wantedDest.length || wantedDest.includes(norm(g.destino||'')) || wantedDest.includes(norm(g.Destino||''));
    let dateOk = true;
    if(A){ const ini=toISO(g.fechaInicio||g.inicio||g.fecha_ini), fin=toISO(g.fechaFin||g.fin||g.fecha_fin);
      dateOk = !( (fin && fin < A) || (ini && ini > B) );
    }
    if(destOk && dateOk){
      const ids = coordDocIdsOf(g); ids.forEach(id=>r.add(String(id)));
      emailsOf(g).forEach(e=>{ if(mapEmailToId.has(e)) r.add(mapEmailToId.get(e)); });
    }
  });
  return r;
}

/** MODAL: CREAR ALERTA () */
async function openCreateAlertModal(){
  const back=document.getElementById('modalBack'), body=document.getElementById('modalBody'), title=document.getElementById('modalTitle');
  title.textContent='CREAR ALERTA ()';
  const coordOpts=state.coordinadores.map(c=>`<option value="${c.id}">${(c.nombre||'').toUpperCase()} — ${(c.email||'').toUpperCase()}</option>`).join('');
  body.innerHTML=`
   <div class="rowflex">
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
      createdBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() },
      readBy:{}
    });
    document.getElementById('modalBack').style.display='none';
    await renderGlobalAlerts();
  };
  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}


/** PANEL GLOBAL DE ALERTAS */
async function renderGlobalAlerts(){
  const box = document.getElementById('alertsPanel');
  if (!box) return;

  const all=[];
  try{
    const qs=await getDocs(collection(db,'alertas'));
    qs.forEach(d=>all.push({id:d.id, ...d.data()}));
  }catch(e){
    console.error(e);
    box.innerHTML='<div class="muted">NO SE PUDIERON CARGAR LAS ALERTAS.</div>';
    return;
  }

  const myCoordId = state.is
    ? (state.viewingCoordId || (state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self'))
    : (state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self');

  const paraMi = all.filter(a => (a.audience!=='') && Array.isArray(a.forCoordIds) && a.forCoordIds.includes(myCoordId));
  const ops    = state.is ? all.filter(a => a.audience==='') : [];

  const renderList = (arr, scope)=>{
    const readerKey = (scope==='ops') ? `:${(state.user.email||'').toLowerCase()}` : `coord:${myCoordId}`;
    const isRead = (a)=>{
      const rb=a.readBy||{};
      if(scope==='ops') return Object.keys(rb||{}).some(k=>k.startsWith(':'));
      return !!rb[readerKey];
    };
    const unread = arr.filter(a=>!isRead(a));
    const read   = arr.filter(a=> isRead(a));

    const mkReadersLine = (a)=>{
      const rb=a.readBy||{};
      const entries = Object.entries(rb).map(([k,v])=>{
        const who = k.toUpperCase();
        const when = (v?.seconds)? new Date(v.seconds*1000).toLocaleString('es-CL').toUpperCase() : '';
        return `${who}${when?(' · '+when):''}`;
      });
      return entries.length ? `<div class="meta"><strong>LEÍDO POR:</strong> ${entries.join(' · ')}</div>` : '';
    };

    const mkCard = (a)=>{
      const li=document.createElement('div'); li.className='alert-card';
      const fecha=a.createdAt?.seconds? new Date(a.createdAt.seconds*1000).toLocaleDateString('es-CL').toUpperCase() : '';
      const autorEmail=(a.createdBy?.email||'').toUpperCase();
      const autorNombre = upperNameByEmail(a.createdBy?.email || '');
      const gi=a.groupInfo||null;

      const cab = (scope==='ops') ? 'NUEVO COMENTARIO' : 'NOTIFICACIÓN';
      const tipoCoord = (scope!=='ops')
        ? (Array.isArray(a.forCoordIds) && a.forCoordIds.length>1 ? 'GLOBAL' : 'PERSONAL')
        : null;

      li.innerHTML=`
        <div class="alert-title">${cab}${tipoCoord?` · ${tipoCoord}`:''}</div>
        <div class="meta">FECHA: ${fecha} · AUTOR: ${autorNombre} (${autorEmail})</div>
        ${gi?`<div class="meta">GRUPO: ${(gi.nombre||'').toString().toUpperCase()} (${(gi.code||'').toString().toUpperCase()}) · DESTINO: ${(gi.destino||'').toString().toUpperCase()} · PROGRAMA: ${(gi.programa||'').toString().toUpperCase()}</div>
             <div class="meta">FECHA ACTIVIDAD: ${dmy(gi.fechaActividad||'')} · ACTIVIDAD: ${(gi.actividad||'').toString().toUpperCase()}</div>`:''}
        <div style="margin:.45rem 0">${(a.mensaje||'').toString().toUpperCase()}</div>
        ${mkReadersLine(a)}
        <div class="rowflex"><button class="btn ok btnRead">CONFIRMAR LECTURA</button></div>`;
      li.querySelector('.btnRead').onclick=async ()=>{
        try{
          const path=doc(db,'alertas',a.id); const payload={};
          if(scope==='ops'){ payload[`readBy.:${(state.user.email||'').toLowerCase()}`]=serverTimestamp(); }
          else            { payload[`readBy.coord:${myCoordId}`]=serverTimestamp(); }
          await updateDoc(path,payload); await renderGlobalAlerts();
        }catch(e){ console.error(e); alert('NO SE PUDO CONFIRMAR.'); }
      };
      return li;
    };

    const wrap=document.createElement('div');
    const tabs=document.createElement('div'); tabs.className='tabs';
    const t1=document.createElement('div'); t1.className='tab active'; t1.textContent=`NO LEÍDAS (${unread.length})`;
    const t2=document.createElement('div'); t2.className='tab';         t2.textContent=`LEÍDAS (${read.length})`;
    tabs.appendChild(t1); tabs.appendChild(t2); wrap.appendChild(tabs);

    const cont=document.createElement('div'); wrap.appendChild(cont);
    const renderTab=(which)=>{
      cont.innerHTML=''; t1.classList.toggle('active',which==='unread'); t2.classList.toggle('active',which==='read');
      const arr2=(which==='unread'?unread:read);
      if(!arr2.length){ cont.innerHTML='<div class="muted">SIN MENSAJES.</div>'; return; }
      arr2.forEach(a=> cont.appendChild(mkCard(a)));
    };
    t1.onclick = ()=>renderTab('unread');
    t2.onclick = ()=>renderTab('read');
    renderTab('unread');
    return { ui:wrap, unreadCount:unread.length, readCount:read.length };
  };

  const head=document.createElement('div'); head.className='alert-head';
  const area=document.createElement('div');

  const mi = renderList(paraMi,'mi');
  const op = state.is ? renderList(ops,'ops') : { ui:null, unreadCount:0 };

  const totalUnread = (mi.unreadCount||0) + (op.unreadCount||0);

  head.innerHTML = `
    <div class="alert-title-row">
      <h4 style="margin:.1rem 0 .0rem">ALERTAS ${totalUnread>0?`<span class="badge">${totalUnread}</span>`:''}</h4>
    </div>
    ${state.is ? `
      <div class="scope-chips">
        <div id="chipMi"  class="scope-chip active">PARA COORDINADOR(A) ${mi.unreadCount?`<span class="badge">${mi.unreadCount}</span>`:''}</div>
        <div id="chipOps" class="scope-chip">PARA OPERACIONES ${op.unreadCount?`<span class="badge">${op.unreadCount}</span>`:''}</div>
      </div>
    ` : '' }
  `;

  if (state.is){
    const createBtn = document.createElement('button');
    createBtn.id = 'btnNewAlertPanel';
    createBtn.className = 'btn sec';
    createBtn.textContent = 'CREAR ALERTA…';
    createBtn.onclick = openCreateAlertModal;
    head.appendChild(createBtn);
  }

  box.innerHTML=''; box.appendChild(head); box.appendChild(area);

  const showScope=(s)=>{
    if(!state.is){ area.innerHTML=''; area.appendChild(mi.ui); return; }
    const chipMi=head.querySelector('#chipMi');
    const chipOps=head.querySelector('#chipOps');
    if(s==='mi'){
      chipMi.classList.add('active'); chipOps.classList.remove('active');
      area.innerHTML=''; area.appendChild(mi.ui);
    }else{
      chipOps.classList.add('active'); chipMi.classList.remove('active');
      area.innerHTML=''; area.appendChild(op.ui);
    }
  };
  if(state.is){
    head.querySelector('#chipMi').onclick  = ()=>showScope('mi');
    head.querySelector('#chipOps').onclick = ()=>showScope('ops');
    showScope('mi');
  }else{
    area.appendChild(mi.ui);
  }
}

/* ====== GASTOS ====== */
async function renderGastos(g, pane){
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
      <input id="spImg" type="file" accept="image/*" capture="environment"/>
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
        grupoNombre:g.nombreGrupo||g.aliasGrupo||g.id, destino:g.destino||null, programa:g.programa||null,
        fechaInicio:g.fechaInicio||null, fechaFin:g.fechaFin||null,
        byUid: state.user.uid, byEmail:(state.user.email||'').toLowerCase(),
        createdAt: serverTimestamp()
      });
      form.querySelector('#spAsunto').value=''; form.querySelector('#spValor').value=''; form.querySelector('#spImg').value='';
      await loadGastosList(g, listBox, coordId);
    }catch(e){ console.error(e); alert('NO FUE POSIBLE GUARDAR EL GASTO.'); }finally{ btn.disabled=false; }
  };

  const hits = await loadGastosList(g, listBox, coordId);
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

// -------- Sugerencias (CONAF / CERO GRADOS) ----------
function precioUnitarioFromServicio(svc){
  const cands = [ svc?.precioPax, svc?.precio, svc?.tarifa, svc?.tarifaPax, svc?.valorPax,
                  (svc?.precios && svc.precios.pax), (svc?.valores && svc.valores.pax) ];
  for (const c of cands){
    const n = Number(c);
    if (!isNaN(n) && n>0) return n;
  }
  return 0;
}
function isProveedorWhitelisted(name=''){
  const s = String(name||'').trim().toUpperCase();
  return (s.includes('CONAF') || s.includes('CERO GRADOS') || s.includes('CERO-GRADOS') || s.includes('CERO_GRADOS'));
}
async function suggestAbonosFromItin(grupo){
  const destino = (grupo?.destino||'').toString().toUpperCase();
  const paxPlan = Number(grupo?.cantidadgrupo || 0);
  if (!paxPlan) return [];

  const it = grupo?.itinerario || {};
  const out = [];

  for (const [fechaISO, arr] of Object.entries(it)){
    const acts = Array.isArray(arr) ? arr : Object.values(arr||{});
    for (const a of acts){
      const actName = (a?.actividad||'').toString();
      if (!actName) continue;

      const svc = await findServicio(destino, actName).catch(()=>null);
      const proveedor = (svc?.proveedor || a?.proveedor || '').toString();
      if (!isProveedorWhitelisted(proveedor)) continue;

      const unit = precioUnitarioFromServicio(svc);
      if (!unit) continue;

      const totalSug = unit * paxPlan;
      out.push({
        asunto: `ABONO ${proveedor} — ${actName.toUpperCase()} ${dmy(fechaISO)}`,
        comentarios: `Sugerido por sistema: ${paxPlan} PAX × ${unit.toLocaleString('es-CL')} CLP`,
        moneda: 'CLP',
        valor: totalSug,
        fecha: fechaISO,
        medio: 'CTA CTE',
        autoCalc: true,
        provWhitelistHit: proveedor.toUpperCase(),
        refActs: [{ fechaISO, actividad: actName, paxUsado: paxPlan, precioUnitario: unit, totalSug }]
      });
    }
  }
  return out;
}

// -------- Totales de gastos (reusa colec. coordinadores/{id}/gastos) ----------
async function sumGastosPorMonedaDelGrupo(g, qNorm=''){
  const coordId = getActiveCoordIdForGastos();
  const qs=await getDocs(query(collection(db,'coordinadores',coordId,'gastos'), orderBy('createdAt','desc')));
  const list=[]; qs.forEach(d=>{ const x=d.data()||{}; if(x.grupoId===g.id) list.push({id:d.id,...x}); });

  if(qNorm){
    return list
      .filter(x => norm([x.asunto,x.byEmail,x.moneda,String(x.valor||0)].join(' ')).includes(qNorm))
      .reduce((acc,x)=>{ const m=String(x.moneda||'CLP').toUpperCase(); acc[m]=(acc[m]||0)+Number(x.valor||0); return acc; },{});
  }
  return list.reduce((acc,x)=>{ const m=String(x.moneda||'CLP').toUpperCase(); acc[m]=(acc[m]||0)+Number(x.valor||0); return acc; },{});
}

// -------- Summary / Cierre finanzas ----------
async function updateFinanzasSummary(gid, patch){
  await setDoc(doc(db,'grupos',gid,'finanzas','summary'), { ...patch, updatedAt: serverTimestamp(), updatedBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() } }, { merge:true });
}
async function closeFinanzas(g){
  await updateFinanzasSummary(g.id, { closed:true, closedAt: serverTimestamp(), closedBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() } });
  await updateDoc(doc(db,'grupos',g.id), {
    'viaje.fin.rendicionOk': true,
    'viaje.fin.boletaOk': true
  });
  showFlash('FINANZAS CERRADAS', 'ok');
}

async function renderFinanzas(g, pane){
  pane.innerHTML='<div class="muted">CARGANDO…</div>';
  const qNorm = norm(state.groupQ||'');
  const tasas = await getTasas();

  const abonos = await loadAbonos(g.id);
  const totAb = abonos.reduce((acc,a)=>{ const m=String(a.moneda||'CLP').toUpperCase(); acc[m]=(acc[m]||0)+Number(a.valor||0); return acc; },{CLP:0,USD:0,BRL:0,ARS:0});
  const totGa = await sumGastosPorMonedaDelGrupo(g, qNorm);

  const abCLP = await sumCLPByMoneda(totAb, tasas);
  const gaCLP = await sumCLPByMoneda(totGa, tasas);
  const saldoCLP = abCLP.CLPconv - gaCLP.CLPconv;

  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem'; pane.innerHTML=''; pane.appendChild(wrap);

  // RESUMEN
  const resum=document.createElement('div'); resum.className='act';
  resum.innerHTML = `
    <h4>RESUMEN FINANZAS</h4>
    <div class="grid-mini">
      <div class="lab">ABONOS</div>
      <div>CLP ${fmtCL(totAb.CLP||0)} · USD ${fmtCL(totAb.USD||0)} · BRL ${fmtCL(totAb.BRL||0)} · ARS ${fmtCL(totAb.ARS||0)} · <strong>TOTAL CLP: ${fmtCL(abCLP.CLPconv)}</strong></div>
      <div class="lab">GASTOS</div>
      <div>CLP ${fmtCL(totGa.CLP||0)} · USD ${fmtCL(totGa.USD||0)} · BRL ${fmtCL(totGa.BRL||0)} · ARS ${fmtCL(totGa.ARS||0)} · <strong>TOTAL CLP: ${fmtCL(gaCLP.CLPconv)}</strong></div>
      <div class="lab">SALDO</div>
      <div><strong>${saldoCLP>=0?'A FAVOR COORD.':'A FAVOR EMPRESA'}: CLP ${fmtCL(saldoCLP)}</strong></div>
    </div>
  `;
  wrap.appendChild(resum);

  // ABONOS (STAFF edita)
  const boxAb=document.createElement('div'); boxAb.className='act';
  boxAb.innerHTML = `
    <h4>ABONOS ${state.is?'<span class="muted">(STAFF PUEDE EDITAR)</span>':''}</h4>
    ${state.is ? `
      <div class="rowflex" style="margin:.4rem 0">
        <button id="btnNewAbono"  class="btn ok">NUEVO ABONO</button>
      </div>` : ''}
    <div id="abonosList" style="display:grid;gap:.4rem"></div>
  `;
  wrap.appendChild(boxAb);

  const renderAbonosList = (items)=>{
    const cont = boxAb.querySelector('#abonosList');
    cont.innerHTML = '';
    if (!items.length){ cont.innerHTML = '<div class="muted">SIN ABONOS.</div>'; return; }
    items.forEach(a=>{
      const card=document.createElement('div'); card.className='card';
      card.innerHTML = `
        <div class="meta"><strong>${(a.asunto||'ABONO').toString().toUpperCase()}</strong></div>
        <div class="meta">FECHA: ${dmy(a.fecha||'')}</div>
        <div class="meta">MONEDA/VALOR: ${(a.moneda||'CLP').toUpperCase()} ${fmtCL(a.valor||0)}</div>
        <div class="meta">MEDIO: ${(a.medio||'').toString().toUpperCase()}</div>
        ${a.comentarios?`<div class="meta" style="white-space:pre-wrap">${(a.comentarios||'').toString().toUpperCase()}</div>`:''}
        ${a.autoCalc?`<div class="badge" style="background:#334155;color:#fff">SUGERIDO</div>`:''}
        ${state.is?`
          <div class="rowflex" style="margin-top:.4rem;gap:.4rem;flex-wrap:wrap">
            <button class="btn sec btnEdit">EDITAR</button>
            <button class="btn warn btnDel">ELIMINAR</button>
          </div>`:''}
      `;
      if (state.is){
        const bE = card.querySelector('.btnEdit');
        if (bE) bE.onclick = async ()=>{
          await openAbonoEditor(g, a, (updated)=>{ Object.assign(a,updated); renderAbonosList(items); });
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
      cont.appendChild(card);
    });
  };

  if (state.is){
    const btnNew = boxAb.querySelector('#btnNewAbono');
    if (btnNew) btnNew.onclick = async ()=>{
      await openAbonoEditor(g, null, async (saved)=>{
        abonos.unshift(saved);
        renderAbonosList(abonos);
        await renderFinanzas(g, pane);
      });
    };
  }
  renderAbonosList(abonos);

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

  const sumPrev = await ensureFinanzasSummary(g.id) || {};
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
    showFlash('COMPROBANTE SUBIDO', 'ok');
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
    showFlash('BOLETA SUBIDA', 'ok');
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

  const hitsAb = qNorm ? abonos.filter(a => norm([a.asunto,a.comentarios,a.medio,String(a.valor||0)].join(' ')).includes(qNorm)).length : 0;
  return hitsAb + (ghits||0);
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

async function getTasas(){
  if(state.cache.tasas) return state.cache.tasas;
  try{ const d=await getDoc(doc(db,'config','tasas')); if(d.exists()){ state.cache.tasas=d.data()||{}; return state.cache.tasas; } }catch(_){}
  state.cache.tasas={ USD:950, BRL:170, ARS:1.2 }; return state.cache.tasas;
}
async function loadGastosList(g, box, coordId){
  const qs=await getDocs(query(collection(db,'coordinadores',coordId,'gastos'), orderBy('createdAt','desc')));
  let list=[]; qs.forEach(d=>{ const x=d.data()||{}; if(x.grupoId===g.id) list.push({id:d.id,...x}); });

  const q = norm(state.groupQ||''); let hits=0;
  if(q){ const before=list.length; list = list.filter(x => norm([x.asunto,x.byEmail,x.moneda,String(x.valor||0)].join(' ')).includes(q)); hits = list.length; }

  const tasas=await getTasas();
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

     if(x.moneda==='CLP') tot.CLP+=Number(x.valor||0);
     if(x.moneda==='USD') tot.USD+=Number(x.valor||0);
     if(x.moneda==='BRL') tot.BRL+=Number(x.valor||0);
     if(x.moneda==='ARS') tot.ARS+=Number(x.valor||0);
  });

  tot.CLPconv = tot.CLP + (tot.USD*(tasas.USD||0)) + (tot.BRL*(tasas.BRL||0)) + (tot.ARS*(tasas.ARS||0));
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

