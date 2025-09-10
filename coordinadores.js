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
  serverTimestamp, query, where, orderBy, limit, deleteField
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import { ref as sRef, uploadBytes, getDownloadURL }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

/* ====== UTILS TEXTO/FECHAS ====== */
const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');
const slug = s => norm(s).slice(0,60);
const toISO=(x)=>{ if(!x) return ''; if (typeof x==='string'){ if(/^\d{4}-\d{2}-\d{2}$/.test(x)) return x; const d=new Date(x); return isNaN(d)?'':d.toISOString().slice(0,10); }
  if (x && typeof x==='object' && 'seconds' in x) return new Date(x.seconds*1000).toISOString().slice(0,10);
  if (x instanceof Date) return x.toISOString().slice(0,10); return ''; };
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

  // BOTONES SOLO PARA  (en NAV solo queda imprimir; crear alerta va en Alertas)
  const btnPrint = document.getElementById('btnPrintVch');
  if (btnPrint) btnPrint.style.display = state.is ? '' : 'none';
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
  state.idx=Math.max(0,Math.min(idx,state.ordenados.length-1));
  await renderOneGroup(state.ordenados[state.idx], qsF);
}

/* ====== NORMALIZADOR DE ITINERARIO ====== */
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){ const map={}; for(const item of raw){ const f=toISO(item && item.fecha); if(!f) continue; (map[f] ||= []).push({...item}); } return map; }
  return raw;
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
        <span class="item nowrap">TOTAL PAX: <strong>${paxTot}</strong></span>
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

  if(state.is){
    p.querySelector('#btnPrintVch').onclick = openPrintVouchersModal;
    // (botón crear alerta se mueve al panel de alertas)
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
  const started = !!viaje.inicio?.at;
  const finished = !!viaje.fin?.at;

  const header=document.createElement('div'); header.className='group-card';
  header.innerHTML=`<h3>${(name||'').toUpperCase()} · CÓDIGO: (${code})</h3>
    <div class="grid-mini">
      <div class="lab">DESTINO</div><div>${(g.destino||'—').toUpperCase()}</div>
      <div class="lab">GRUPO</div><div>${(name||'').toUpperCase()}</div>
      <div class="lab">PAX TOTAL</div>
      <div>${fmtPaxPlan(paxPlan, g)}${real?` <span class="muted">(A:${A_real} · E:${E_real})</span>`:''}</div>
      <div class="lab">PROGRAMA</div><div>${(g.programa||'—').toUpperCase()}</div>
      <div class="lab">FECHAS</div><div>${rango}</div>
    </div>

    <div class="rowflex" style="margin-top:.6rem;gap:.5rem;flex-wrap:wrap">
      <input id="searchTrips" type="text" placeholder="BUSCADOR EN RESUMEN, ITINERARIO Y GASTOS..." style="flex:1"/>
    </div>

    <div class="rowflex" style="margin-top:.4rem;gap:.5rem;align-items:center;flex-wrap:wrap">
      ${(!started)
          ? `<button id="btnInicioViaje" class="btn sec"${isStartDay?'':` title="No es el día de inicio. Se pedirá confirmación."`}>INICIO DE VIAJE</button>`
          : `<div class="muted">VIAJE EN CURSO</div>`}
      ${(started && !finished)
          ? `<button id="btnTerminoViaje" class="btn warn">TERMINAR VIAJE</button>`
          : ``}
      ${(finished)
          ? `<div class="muted">VIAJE FINALIZADO${viaje?.fin?.rendicionOk?` · RENDICIÓN HECHA`:''}${viaje?.fin?.boletaOk?` · BOLETA ENTREGADA`:''}</div>`
          : ``}
      ${state.is
          ? `<div class="muted" style="opacity:.9">STAFF:</div>
             ${started ? `<button id="btnReabrirInicio"  class="btn sec">RESTABLECER INICIO</button>` : ``}
             ${finished? `<button id="btnReabrirCierre" class="btn sec">RESTABLECER CIERRE</button>` : ``}`
             <button id="btnTripReset" class="btn warn" title="Borra paxViajando e INICIO/FIN">RESTABLECER</button>`
          : ``}
    </div>`;
  cont.appendChild(header);

  // Handlers viaje
  const btnIV = header.querySelector('#btnInicioViaje');
  if (btnIV) btnIV.onclick = () => openInicioViajeModal(g);
  const btnTV = header.querySelector('#btnTerminoViaje');
  if (btnTV) btnTV.onclick = () => openTerminoViajeModal(g);
  const btnRX = header.querySelector('#btnReabrirInicio');
  if (btnRX) btnRX.onclick = () => staffReopenInicio(g);
  const btnRY = header.querySelector('#btnReabrirCierre');
  if (btnRY) btnRY.onclick = () => staffReopenCierre(g);
  const btnTR = header.querySelector('#btnTripReset');
  if (btnTR) btnTR.onclick = () => resetInicioFinViaje(g);


  const tabs=document.createElement('div');
  tabs.innerHTML=`
    <div style="display:flex;gap:.5rem;margin:.6rem 0">
      <button id="tabResumen" class="btn sec">RESUMEN</button>
      <button id="tabItin"    class="btn sec">ITINERARIO</button>
      <button id="tabGastos"  class="btn sec">GASTOS</button>
    </div>
    <div id="paneResumen"></div>
    <div id="paneItin" style="display:none"></div>
    <div id="paneGastos" style="display:none"></div>`;
  cont.appendChild(tabs);

  const paneResumen=tabs.querySelector('#paneResumen');
  const paneItin=tabs.querySelector('#paneItin');
  const paneGastos=tabs.querySelector('#paneGastos');
  const btnResumen=tabs.querySelector('#tabResumen');
  const btnItin=tabs.querySelector('#tabItin');
  const btnGastos=tabs.querySelector('#tabGastos');

  const setTabLabel=(btn, base, n)=>{
    const q=(state.groupQ||'').trim();
    btn.textContent = (q && n>0) ? `${base} (${n})` : base;
  };
  const show = (w)=>{ paneResumen.style.display=w==='resumen'?'':'none'; paneItin.style.display=w==='itin'?'':'none'; paneGastos.style.display=w==='gastos'?'':'none'; };
  btnResumen.onclick=()=>show('resumen');
  btnItin.onclick   =()=>show('itin');
  btnGastos.onclick =()=>show('gastos');

  // Render y contadores de búsqueda por pestaña
  const resumenHits = await renderResumen(g, paneResumen);
  const itinHits    = renderItinerario(g, paneItin, preferDate);
  const gastosHits  = await renderGastos(g, paneGastos);
  setTabLabel(btnResumen, 'RESUMEN', resumenHits);
  setTabLabel(btnItin,    'ITINERARIO', itinHits);
  setTabLabel(btnGastos,  'GASTOS', gastosHits);

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
  try{
    const vuelosRaw = await loadVuelosInfo(g);
    const vuelos = vuelosRaw.map(normalizeVuelo);
    const flt = (!q) ? vuelos : vuelos.filter(v=>{
      const s=[v.numero,v.proveedor,v.origen,v.destino,toISO(v.fechaIda),toISO(v.fechaVuelta)].join(' ');
      return norm(s).includes(q);
    });
    if (q) hits += flt.length;

    if(!flt.length){
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4><div class="muted">SIN VUELOS.</div>';
    }else{
      vuelosBox.innerHTML = '<h4>TRANSPORTE / VUELOS</h4>';

      flt.forEach((v, i) => {
        const numero   = (v.numero || '').toString().toUpperCase();
        const empresa  = (v.proveedor || '').toString().toUpperCase();
        const ruta     = [v.origen, v.destino].map(x => (x||'').toString().toUpperCase()).filter(Boolean).join(' — ');
        const ida      = dmy(toISO(v.fechaIda))  || '';
        const vuelta   = dmy(toISO(v.fechaVuelta)) || '';

        const block = document.createElement('div');
        block.innerHTML = `
          <div class="meta"><strong>N° VUELO:</strong> ${numero || '—'}</div>
          <div class="meta"><strong>EMPRESA:</strong> ${empresa || '—'}</div>
          <div class="meta"><strong>RUTA:</strong> ${ruta || '—'}</div>
          <div class="meta"><strong>IDA:</strong> ${ida || '—'}</div>
          <div class="meta"><strong>VUELTA:</strong> ${vuelta || '—'}</div>
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
  const origen      = get('origen','desde','from','salida.origen','salida.iata','origenIATA','origenSigla','origenCiudad');
  const destino     = get('destino','hasta','to','llegada.destino','llegada.iata','destinoIATA','destinoSigla','destinoCiudad');
  const fechaIda    = get('fechaIda','ida','salida.fecha','fechaSalida','fecha_ida','fecha');
  const fechaVuelta = get('fechaVuelta','vuelta','regreso.fecha','fechaRegreso','fecha_vuelta');
  return { numero, proveedor, origen, destino, fechaIda, fechaVuelta };
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

  const q = norm(state.groupQ||'');
  let acts=(grupo.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];

  // Orden por hora de inicio (temprano → tarde)
  acts = acts.slice().sort((a,b)=> timeVal(a.horaInicio) - timeVal(b.horaInicio));

  if(q) acts = acts.filter(a => norm([a.actividad,a.proveedor,a.horaInicio,a.horaFin].join(' ')).includes(q));
  if (!acts.length){ cont.innerHTML='<div class="muted">SIN ACTIVIDADES PARA ESTE DÍA.</div>'; return; }

  for(const act of acts){
    const plan=calcPlan(act,grupo);
    const saved=getSavedAsistencia(grupo,fechaISO,act.actividad);
    const estado=(grupo.serviciosEstado?.[fechaISO]?.[slug(act.actividad||'')]?.estado)||'';

    const paxFinalInit=(saved?.paxFinal ?? '');
    const actName=act.actividad||'ACTIVIDAD';
    const actKey=slug(actName);

    const servicio = await findServicio(grupo.destino, actName);
    const tipoRaw = (servicio?.voucher || 'No Aplica').toString();
    const tipo = /electron/i.test(tipoRaw) ? 'ELECTRONICO' : (/fisic/i.test(tipoRaw) ? 'FISICO' : 'NOAPLICA');

    const div=document.createElement('div'); div.className='act';
    div.innerHTML=`
      <h4>${(actName||'').toUpperCase()} ${estado?`· <span class="muted">${String(estado).toUpperCase()}</span>`:''}</h4>
      <div class="meta">
        ${(act.horaInicio||'--:--')}–${(act.horaFin||'--:--')}
        · PLAN: ${fmtPaxPlan(plan, grupo)} PAX
      </div>
      <div class="rowflex" style="margin:.35rem 0">
        <input type="number" min="0" inputmode="numeric" placeholder="N° ASISTENCIA" value="${paxFinalInit}"/>
        <textarea placeholder="COMENTARIOS"></textarea>
        <button class="btn ok btnSave">GUARDAR</button>
        ${tipo!=='NOAPLICA'?`<button class="btn sec btnVch">FINALIZAR…</button>`:''}
      </div>
      <div class="bitacora" style="margin-top:.4rem">
        <div class="muted" style="margin-bottom:.25rem">BITÁCORA</div>
        <div class="bitItems" style="display:grid;gap:.35rem"></div>
      </div>`;
    cont.appendChild(div);

    // BITÁCORA
    const itemsWrap=div.querySelector('.bitItems'); await loadBitacora(grupo.id,fechaISO,actKey,itemsWrap);

    // GUARDAR ASISTENCIA + NOTA
    div.querySelector('.btnSave').onclick=async ()=>{
      const btn=div.querySelector('.btnSave'); btn.disabled=true;
      try{
        const pax=Number(div.querySelector('input').value||0);
        const nota=String(div.querySelector('textarea').value||'').trim();
        const refGrupo=doc(db,'grupos',grupo.id);
        const payload={}; payload[`asistencias.${fechaISO}.${actKey}`]={
          paxFinal:pax, notas:nota, byUid:auth.currentUser.uid,
          byEmail:String(auth.currentUser.email||'').toLowerCase(), updatedAt:serverTimestamp()
        };
        await updateDoc(refGrupo,payload);
        setSavedAsistenciaLocal(grupo,fechaISO,actName,{paxFinal:pax,notas:nota});
        if(nota){
           const timeId = timeIdNowMs();
           const ref = doc(db,'grupos',grupo.id,'bitacora',actKey,fechaISO,timeId);
           await setDoc(ref, {
             texto: nota,
             byUid: auth.currentUser.uid,
             byEmail: (auth.currentUser.email||'').toLowerCase(),
             ts: serverTimestamp()
           });

          // ALERTA PARA  (OPERACIONES)
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

          await loadBitacora(grupo.id,fechaISO,actKey,itemsWrap);
          div.querySelector('textarea').value='';
          await renderGlobalAlerts();
        }
        btn.textContent='GUARDADO'; setTimeout(()=>{ btn.textContent='GUARDAR'; btn.disabled=false; },900);
      }catch(e){ console.error(e); btn.disabled=false; alert('NO SE PUDO GUARDAR.'); }
    };

    if(tipo!=='NOAPLICA'){
      div.querySelector('.btnVch').onclick = async ()=>{ await openVoucherModal(grupo,fechaISO,act,servicio,tipo); };
    }
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

  body.querySelector('#ivSave').onclick = async () => {
    const A = Math.max(0, Number($A.value||0));
    const E = Math.max(0, Number($E.value||0));
    const total = A + E;

    if (!isToday(g.fechaInicio) && !state.is){
      const ok = confirm('No es el día de inicio. ¿Confirmar de todas formas?');
      if (!ok) return;
    }
    try{
      const path = doc(db,'grupos',g.id);
      await updateDoc(path, {
        paxViajando: { A, E, total, by:(state.user.email||'').toLowerCase(), updatedAt: serverTimestamp() },
        viaje: { ...(g.viaje||{}), estado:'EN_CURSO', inicio:{ at: serverTimestamp(), by:(state.user.email||'').toLowerCase() } }
      });
      g.paxViajando = { A, E, total };
      g.viaje = { ...(g.viaje||{}), estado:'EN_CURSO', inicio:{ at: new Date(), by:(state.user.email||'').toLowerCase() } };
      document.getElementById('modalBack').style.display='none';
      await renderOneGroup(g);
    }catch(e){
      console.error(e);
      alert('No fue posible guardar el inicio del viaje.');
    }
  };

  document.getElementById('modalClose').onclick = () => { document.getElementById('modalBack').style.display='none'; };
  back.style.display = 'flex';
}

async function openTerminoViajeModal(g){
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
    if (g.viaje){ delete g.viaje.inicio; g.viaje.estado='PENDIENTE'; }
    await renderOneGroup(g);
  }catch(e){ console.error(e); alert('No fue posible reabrir el inicio.'); }
}
async function staffReopenCierre(g){
  if (!state.is){ alert('Solo staff puede reabrir el cierre.'); return; }
  const ok = confirm('¿Reabrir CIERRE DE VIAJE? (volverá a estado EN_CURSO)');
  if(!ok) return;
  try{
    const path=doc(db,'grupos',g.id);
    await updateDoc(path,{ 'viaje.fin': deleteField(), 'viaje.estado': 'EN_CURSO' });
    if (g.viaje){ delete g.viaje.fin; g.viaje.estado='EN_CURSO'; }
    await renderOneGroup(g);
  }catch(e){ console.error(e); alert('No fue posible reabrir el cierre.'); }
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
  listBox.innerHTML='<h4>GASTOS DEL GRUPO</h4><div class="muted">CARGANDO…</div>'; pane.appendChild(listBox);

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
      const coordId = state.viewingCoordId || (state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self');
      await addDoc(collection(db,'coordinadores',coordId,'gastos'),{
        asunto, moneda, valor, imgUrl, imgPath,
        grupoId:g.id, numeroNegocio:g.numeroNegocio, identificador:g.identificador||null,
        grupoNombre:g.nombreGrupo||g.aliasGrupo||g.id, destino:g.destino||null, programa:g.programa||null,
        fechaInicio:g.fechaInicio||null, fechaFin:g.fechaFin||null,
        byUid: state.user.uid, byEmail:(state.user.email||'').toLowerCase(),
        createdAt: serverTimestamp()
      });
      form.querySelector('#spAsunto').value=''; form.querySelector('#spValor').value=''; form.querySelector('#spImg').value='';
      await loadGastosList(g,listBox);
    }catch(e){ console.error(e); alert('NO FUE POSIBLE GUARDAR EL GASTO.'); }finally{ btn.disabled=false; }
  };
  const hits = await loadGastosList(g,listBox);
  return hits;
}
async function getTasas(){
  if(state.cache.tasas) return state.cache.tasas;
  try{ const d=await getDoc(doc(db,'config','tasas')); if(d.exists()){ state.cache.tasas=d.data()||{}; return state.cache.tasas; } }catch(_){}
  state.cache.tasas={ USD:950, BRL:170, ARS:1.2 }; return state.cache.tasas;
}
async function loadGastosList(g, box){
  const coordId = state.viewingCoordId || (state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self');
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
