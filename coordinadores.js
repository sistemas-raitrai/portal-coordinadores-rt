/* coordinadores.js — Portal Coordinadores RT (Staff: TODOS, buscador, stats, gastos, alertas, vouchers) */

import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, addDoc, setDoc,
  serverTimestamp, query, where, orderBy, limit
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import {
  getStorage, ref as storageRef, uploadBytes, getDownloadURL
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

/* ============== Auth ============== */
const auth = getAuth(app);
const storage = getStorage(app);
const logoutBtn = document.getElementById('logout');
if (logoutBtn) logoutBtn.onclick = () => signOut(auth).then(() => (location = 'index.html'));

/* ============== Staff permitido ============== */
const STAFF_EMAILS = new Set(
  ['aleoperaciones@raitrai.cl','tomas@raitrai.cl','operaciones@raitrai.cl','anamaria@raitrai.cl','sistemas@raitrai.cl']
  .map(e=>e.toLowerCase())
);

/* ============== Utils ============== */
const norm = (s='') =>
  s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');

const slug = s => norm(s).slice(0,60);

const toISO = (x)=>{
  if(!x) return '';
  if (typeof x==='string'){
    if (/^\d{4}-\d{2}-\d{2}$/.test(x)) return x;
    const d=new Date(x);
    return isNaN(d)?'':d.toISOString().slice(0,10);
  }
  if (x && typeof x==='object' && 'seconds' in x) return new Date(x.seconds*1000).toISOString().slice(0,10);
  if (x instanceof Date) return x.toISOString().slice(0,10);
  return '';
};

const dmy = iso => { if(!iso) return ''; const [y,m,d]=iso.split('-'); return `${d}-${m}-${y}`; };

const parseDateFlexible = (s)=>{
  const t = s.trim();
  if (/^\d{2}-\d{2}-\d{4}$/.test(t)){ const [dd,mm,yy] = t.split('-'); return `${yy}-${mm}-${dd}`; }
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return t;
  return '';
};

const daysInclusive = (ini, fin)=>{
  const a = toISO(ini), b = toISO(fin); if(!a||!b) return 0;
  return Math.max(1, Math.round((new Date(b)-new Date(a))/86400000)+1);
};

const rangoFechas = (ini, fin)=>{
  const out=[]; const A=toISO(ini), B=toISO(fin); if(!A||!B) return out;
  for(let d=new Date(A+'T00:00:00'); d<=new Date(B+'T00:00:00'); d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10));
  return out;
};

const parseQS = ()=>{ const p=new URLSearchParams(location.search); return { g:p.get('g')||'', f:p.get('f')||'' }; };
const fmt = iso => { if(!iso) return ''; const d=new Date(iso+'T00:00:00'); return d.toLocaleDateString('es-CL',{weekday:'short',day:'2-digit',month:'short'}); };

const arrify = v => Array.isArray(v) ? v : (v && typeof v==='object' ? Object.values(v) : (v ? [v] : []));
const paxOf = g => Number(g?.cantidadgrupo ?? g?.pax ?? 0);

/* itinerario tolerante (obj o array legado) */
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){
    const map={}; for(const item of raw){ const f=toISO(item && item.fecha); if(!f) continue; (map[f] ||= []).push({...item}); }
    return map;
  }
  return raw;
}

/* extracción tolerante desde grupos */
function emailsOf(g){ const out=new Set(), push=e=>{if(e) out.add(String(e).toLowerCase());};
  push(g?.coordinadorEmail); push(g?.coordinador?.email);
  arrify(g?.coordinadoresEmails).forEach(push);
  if(g?.coordinadoresEmailsObj) Object.keys(g.coordinadoresEmailsObj).forEach(push);
  arrify(g?.coordinadores).forEach(x=>{ if (x?.email) push(x.email); else if (typeof x==='string'&&x.includes('@')) push(x); });
  return [...out];
}
function uidsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); };
  push(g?.coordinadorUid||g?.coordinadorId);
  if (g?.coordinador?.uid) push(g.coordinador.uid);
  arrify(g?.coordinadoresUids||g?.coordinadoresIds||g?.coordinadores).forEach(x=>{ if (x?.uid) push(x.uid); else push(x); });
  return [...out];
}
function coordDocIdsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); }; push(g?.coordinadorId); arrify(g?.coordinadoresIds).forEach(push); return [...out]; }
function nombresOf(g){ const out=new Set(), push=s=>{ if(s) out.add(norm(String(s))); }; push(g?.coordinadorNombre||g?.coordinador); if (g?.coordinador?.nombre) push(g.coordinador.nombre); arrify(g?.coordinadoresNombres).forEach(push); return [...out]; }

/* asistencia helpers */
function getSavedAsistencia(grupo, fechaISO, actividad){
  const byDate = grupo?.asistencias?.[fechaISO]; if(!byDate) return null;
  const key = slug(actividad||'actividad');
  if (Object.prototype.hasOwnProperty.call(byDate,key)) return byDate[key];
  for (const k of Object.keys(byDate)) if (slug(k)===key) return byDate[k];
  return null;
}
function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data){
  const key=slug(actividad||'actividad'); (grupo.asistencias ||= {}); (grupo.asistencias[fechaISO] ||= {}); grupo.asistencias[fechaISO][key]=data;
}
function calcPlan(actividad, grupo){
  const a=actividad||{}; const ad=Number(a.adultos||0), es=Number(a.estudiantes||0); const suma=ad+es;
  if (suma>0) return suma;
  const base=(grupo && (grupo.cantidadgrupo!=null?grupo.cantidadgrupo:grupo.pax));
  return Number(base||0);
}

/* estado global */
const state = {
  user:null,
  isStaff:false,
  coordinadores:[],
  grupos:[],
  ordenados:[],
  idx:0,
  cache:{ hotel:new Map(), vuelos:new Map(), tiposCambio:null },

  scope: 'coord',  // 'coord' | 'all'
  coordId: null,
  q: ''
};

/* ===== panel helpers ===== */
function ensurePanel(id, html = '') {
  let p = document.getElementById(id);
  if (!p) {
    p = document.createElement('div');
    p.id = id; p.className = 'panel';
    (document.querySelector('.wrap') || document.body).appendChild(p);
  }
  if (html) p.innerHTML = html;
  ensureLayoutOrder();
  return p;
}
function ensureLayoutOrder() {
  const wrap = document.querySelector('.wrap'); if (!wrap) return;
  ['staffBar','navPanel','statsPanel','gruposPanel'].forEach(id=>{
    const el=document.getElementById(id); if (el) wrap.appendChild(el);
  });
}

/* ============== Arranque ============== */
onAuthStateChanged(auth, async (user) => {
  if (!user) { location.href='index.html'; return; }
  state.user = user;
  state.isStaff = STAFF_EMAILS.has((user.email||'').toLowerCase());

  const coordinadores = await loadCoordinadores();
  state.coordinadores = coordinadores;

  // panel de grupos es panel (mismo ancho)
  const gp=document.getElementById('gruposPanel'); gp?.classList.add('panel');

  if (state.isStaff) {
    await showStaffSelector(coordinadores, user);
  } else {
    state.scope = 'coord';
    const miReg = findCoordinadorForUser(coordinadores, user);
    state.coordId = miReg?.id || null;
    await loadGruposForScope(user);
  }
});

/* ============== Cargar coordinadores ============== */
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list=[]; snap.forEach(d=>{ const x=d.data()||{}; list.push({
    id:d.id, nombre:String(x.nombre||x.Nombre||x.coordinador||''), email:String(x.email||x.correo||x.mail||'').toLowerCase(), uid:String(x.uid||x.userId||'')
  });});
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre,'es',{sensitivity:'base'}));
  const seen=new Set(), dedup=[]; for(const c of list){ const k=(c.nombre+'|'+c.email).toLowerCase(); if(!seen.has(k)){ seen.add(k); dedup.push(c); } }
  return dedup;
}

/* ============== Selector Staff (incluye "TODOS") ============== */
async function showStaffSelector(coordinadores, user){
  const bar = ensurePanel('staffBar',
    `<label style="display:block;margin-bottom:6px;color:#cbd5e1">VER VIAJES POR COORDINADOR</label>
     <select id="coordSelect"></select>`
  );
  const sel = bar.querySelector('#coordSelect');

  sel.innerHTML =
    `<option value="ALL">TODOS LOS COORDINADORES</option>` +
    coordinadores.map(c=>`<option value="${c.id}">${(c.nombre||'')} — ${c.email||'SIN CORREO'}</option>`).join('');

  sel.onchange = async ()=>{
    const val = sel.value;
    if (val === 'ALL'){ state.scope = 'all'; state.coordId = null; }
    else { state.scope = 'coord'; state.coordId = val; }
    localStorage.setItem('rt_staff_coord_scope', state.scope);
    localStorage.setItem('rt_staff_coord_id', state.coordId||'');
    await loadGruposForScope(user);
  };

  const lastScope = localStorage.getItem('rt_staff_coord_scope');
  const lastId    = localStorage.getItem('rt_staff_coord_id') || '';
  if (lastScope === 'all'){ sel.value = 'ALL'; state.scope='all'; state.coordId=null; }
  else if (lastId && coordinadores.find(c=>c.id===lastId)){ sel.value = lastId; state.scope='coord'; state.coordId=lastId; }

  await loadGruposForScope(user);
}

/* ============== Resolver coordinador (no staff) ============== */
function findCoordinadorForUser(coordinadores, user){
  const email=(user.email||'').toLowerCase(), uid=user.uid;
  let c = coordinadores.find(x=> x.email && x.email.toLowerCase()===email); if(c) return c;
  if (uid){ c=coordinadores.find(x=>x.uid && x.uid===uid); if(c) return c; }
  const disp=norm(user.displayName||''); if (disp){ c=coordinadores.find(x=> norm(x.nombre)===disp); if(c) return c; }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

/* ============== Tipos de cambio ============== */
async function getTiposCambio(){
  if (state.cache.tiposCambio) return state.cache.tiposCambio;
  try{
    const d = await getDoc(doc(db,'config','tiposCambio'));
    if (d.exists()) { state.cache.tiposCambio = d.data(); return state.cache.tiposCambio; }
  }catch{}
  state.cache.tiposCambio = { CLP:1, USD:1, BRL:1, ARS:1 };
  return state.cache.tiposCambio;
}

/* ============== Cargar grupos según scope (ALL / COORD) ============== */
async function loadGruposForScope(user){
  const cont=document.getElementById('grupos'); if (cont) cont.textContent='CARGANDO GRUPOS…';

  const allSnap=await getDocs(collection(db,'grupos'));
  const wanted=[];

  allSnap.forEach(d=>{
    const raw={id:d.id, ...d.data()};
    const g={
      ...raw,
      fechaInicio: toISO(raw.fechaInicio||raw.inicio||raw.fecha_ini),
      fechaFin: toISO(raw.fechaFin||raw.fin||raw.fecha_fin),
      itinerario: normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
      numeroNegocio: String(raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || d.id)
    };

    // index para búsqueda: incluye actividades
    const actividadesIndex=[];
    for(const f of Object.keys(g.itinerario||{})){
      (g.itinerario[f]||[]).forEach(a=>{
        if (a?.actividad) actividadesIndex.push(String(a.actividad));
        if (a?.servicioId||a?.servicioDocId) actividadesIndex.push(String(a.servicioId||a.servicioDocId));
      });
    }
    g._search = {
      name: (g.nombreGrupo || g.aliasGrupo || g.id || '').toString(),
      destino: (g.destino || '').toString(),
      programa: (g.programa || '').toString(),
      coordNombre: (g.coordinadorNombre || g.coordinador?.nombre || '').toString(),
      coordEmail: (g.coordinadorEmail || g.coordinador?.email || '').toString(),
      actividades: actividadesIndex.join(' ')
    };

    if (state.scope === 'coord'){
      const emailElegido=(state.coordId && state.coordinadores.find(c=>c.id===state.coordId)?.email || '').toLowerCase();
      const uidElegido  =(state.coordinadores.find(c=>c.id===state.coordId)?.uid || '').toString();
      const docIdElegido= state.coordId;
      const nombreElegido = norm(state.coordinadores.find(c=>c.id===state.coordId)?.nombre || '');

      const gEmails=emailsOf(raw), gUids=uidsOf(raw), gDocIds=coordDocIdsOf(raw), gNames=nombresOf(raw);
      const match=(emailElegido && gEmails.includes(emailElegido)) ||
                  (uidElegido && gUids.includes(uidElegido)) ||
                  (docIdElegido && gDocIds.includes(docIdElegido)) ||
                  (nombreElegido && gNames.includes(nombreElegido));
      if (!match) return;
    }

    wanted.push(g);
  });

  const hoy = toISO(new Date());
  const futuros = wanted.filter(g => (g.fechaInicio||'') >= hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  const pasados = wanted.filter(g => (g.fechaInicio||'') < hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  state.grupos = wanted;
  state.ordenados = [...futuros, ...pasados];

  const { g:qsG, f:qsF } = parseQS();
  let idx = 0;
  if (qsG){
    const byNum = state.ordenados.findIndex(x=> String(x.numeroNegocio)===qsG);
    const byId  = state.ordenados.findIndex(x=> String(x.id)===qsG);
    idx = byNum>=0?byNum:(byId>=0?byId:0);
  } else {
    const last = localStorage.getItem('rt_last_group');
    if (last){ const i = state.ordenados.findIndex(x=> x.id===last || x.numeroNegocio===last); if (i>=0) idx=i; }
  }
  state.idx = Math.max(0, Math.min(idx, state.ordenados.length-1));

  renderNavBar();
  renderStatsFiltered();
  renderOneGroup(state.ordenados[state.idx], qsF);
}

/* ============== Filtro por texto (q) ============== */
function applyTextFilter(list){
  const q = (state.q || '').trim();
  if (!q) return list.slice();

  const tokens = q.split(/\s+/).map(t=>t.trim()).filter(Boolean);
  let clamp = list.slice();

  for (const tk of tokens){
    if (/^\S+\.\.\S+$/.test(tk)){ // rango "a..b"
      const [a,b] = tk.split('..').map(parseDateFlexible);
      if (a && b){ clamp = clamp.filter(g => !( (g.fechaFin && g.fechaFin < a) || (g.fechaInicio && g.fechaInicio > b) )); continue; }
    }
    const maybeDate = parseDateFlexible(tk);
    if (maybeDate){ clamp = clamp.filter(g => (g.fechaInicio && g.fechaFin && (g.fechaInicio <= maybeDate && maybeDate <= g.fechaFin))); continue; }

    const nk = norm(tk);
    clamp = clamp.filter(g=>{
      const S = g._search || {};
      return (
        norm(S.name).includes(nk) ||
        norm(S.destino).includes(nk) ||
        norm(S.programa).includes(nk) ||
        norm(S.coordNombre).includes(nk) ||
        norm(S.coordEmail).includes(nk) ||
        norm(S.actividades||'').includes(nk) ||   // ACTIVIDADES/SERVICIOS
        norm(g.numeroNegocio).includes(nk)
      );
    });
  }
  return clamp;
}

/* ============== Estadísticas filtradas ============== */
function renderStatsFiltered(){
  const base = state.ordenados;
  const list = applyTextFilter(base);
  const p = ensurePanel('statsPanel');

  if (!list.length){
    p.innerHTML = '<div class="meta">SIN RESULTADOS PARA EL FILTRO ACTUAL.</div>';
    fillTripsSelect([]);
    return;
  }

  const n = list.length;
  const minIniISO = list.map(g=>g.fechaInicio).filter(Boolean).sort()[0] || '';
  const maxFinISO = list.map(g=>g.fechaFin).filter(Boolean).sort().slice(-1)[0] || '';
  const totalDias = list.reduce((sum,g)=> sum + daysInclusive(g.fechaInicio,g.fechaFin), 0);
  const destinos = [...new Set(list.map(g=>(String(g.destino||'').trim())).filter(Boolean))];
  const paxTot = list.reduce((s,g)=> s + paxOf(g), 0);
  const paxPorViaje = list.map(g=> `${(g.aliasGrupo||g.nombreGrupo||g.id)} (${paxOf(g)} PAX)`).join(' · ');

  const labelScope = (state.scope==='all') ? 'ÁMBITO: TODOS LOS COORDINADORES' : 'ÁMBITO: COORDINADOR SELECCIONADO';
  const labelFiltro = state.q ? `FILTRO TEXTO: “${state.q}”` : 'FILTRO TEXTO: (NINGUNO)';

  p.innerHTML = `
    <div style="display:grid;gap:.4rem">
      <div class="meta">${labelScope}</div>
      <div class="meta">${labelFiltro}</div>
      <div class="meta"><strong>TOTAL VIAJES: ${n}</strong> · <strong>TOTAL DÍAS: ${totalDias}</strong> · <strong>TOTAL PAX: ${paxTot}</strong></div>
      <div class="meta">RANGO GLOBAL: ${minIniISO?dmy(minIniISO):'—'} — ${maxFinISO?dmy(maxFinISO):'—'}</div>
      <div class="meta">DESTINOS: ${destinos.length? destinos.join(' · ') : '—'}</div>
      <div class="meta">PAX POR VIAJE: ${paxPorViaje || '—'}</div>
    </div>
  `;

  fillTripsSelect(list);
}

/* ============== NavBar ============== */
function renderNavBar(){
  const p = ensurePanel('navPanel',
    `<div id="navBar">
       <div class="btns">
         <button id="btnPrev">‹ ANTERIOR</button>
         <button id="btnNext">SIGUIENTE ›</button>
         ${state.isStaff ? '<button id="btnPrintAll" class="btn-sec">IMPRIMIR VOUCHERS</button>' : ''}
       </div>
       <input id="searchTrips" type="text" placeholder="BUSCAR: destino, grupo, actividad/servicio, #negocio, coordinador, 29-11-2025 o 15-11-2025..19-12-2025"/>
       <select id="allTrips"></select>
     </div>`
  );

  const search = p.querySelector('#searchTrips');
  search.value = state.q || '';
  let tmr=null;
  search.oninput = ()=>{
    clearTimeout(tmr);
    tmr = setTimeout(()=>{
      state.q = search.value || '';
      renderStatsFiltered();
      const list = applyTextFilter(state.ordenados);
      if (!list.length) return;
      const currentId = state.ordenados[state.idx]?.id;
      const i = list.findIndex(g=>g.id===currentId);
      state.idx = (i>=0 ? state.ordenados.findIndex(x=>x.id===currentId) : state.ordenados.findIndex(x=>x.id==(list[0]?.id)));
      renderOneGroup(state.ordenados[state.idx]);
      const sel = p.querySelector('#allTrips');
      const j = list.findIndex(g=>g.id===state.ordenados[state.idx]?.id);
      if (j>=0) sel.value = String(j);
    }, 180);
  };

  p.querySelector('#btnPrev').onclick = ()=>{
    const list = applyTextFilter(state.ordenados); if (!list.length) return;
    const curId = state.ordenados[state.idx]?.id; const j = list.findIndex(g=>g.id===curId);
    const j2 = Math.max(0, j-1); const targetId = list[j2].id;
    state.idx = state.ordenados.findIndex(g=>g.id===targetId);
    renderOneGroup(state.ordenados[state.idx]); p.querySelector('#allTrips').value = String(j2);
  };
  p.querySelector('#btnNext').onclick = ()=>{
    const list = applyTextFilter(state.ordenados); if (!list.length) return;
    const curId = state.ordenados[state.idx]?.id; const j = list.findIndex(g=>g.id===curId);
    const j2 = Math.min(list.length-1, j+1); const targetId = list[j2].id;
    state.idx = state.ordenados.findIndex(g=>g.id===targetId);
    renderOneGroup(state.ordenados[state.idx]); p.querySelector('#allTrips').value = String(j2);
  };

  if (state.isStaff) p.querySelector('#btnPrintAll').onclick = openBulkVoucherModal;

  fillTripsSelect(applyTextFilter(state.ordenados));
}

function fillTripsSelect(list){
  const sel = document.getElementById('allTrips'); if (!sel) return;
  sel.textContent = '';
  if (!list.length){ sel.appendChild(new Option('(SIN RESULTADOS)', '')); return; }
  list.forEach((g,i)=>{
    const name=(g.nombreGrupo||g.aliasGrupo||g.id);
    const label = `${name} | IDA: ${dmy(g.fechaInicio||'')} VUELTA: ${dmy(g.fechaFin||'')}`;
    sel.appendChild(new Option(label, String(i)));
  });
  const curId = state.ordenados[state.idx]?.id;
  const j = list.findIndex(x=>x.id===curId);
  sel.value = (j>=0 ? String(j) : '0');
  sel.onchange = ()=>{ const j2 = Number(sel.value||0); const targetId = list[j2]?.id; if (!targetId) return; state.idx = state.ordenados.findIndex(g=>g.id===targetId); renderOneGroup(state.ordenados[state.idx]); };
}

/* ============== Vista 1 grupo ============== */
function renderOneGroup(g, preferDate){
  const cont=document.getElementById('grupos'); if(!cont) return;
  cont.innerHTML='';

  if(!g){ cont.innerHTML='<p class="muted">NO HAY VIAJES.</p>'; return; }
  localStorage.setItem('rt_last_group', g.id);

  const titulo = (g.nombreGrupo || g.aliasGrupo || g.id);
  const sub = `${g.destino||''} · ${g.programa||''} · ${(g.cantidadgrupo ?? g.pax ?? 0)} PAX`;
  const rango = `${dmy(g.fechaInicio||'')} — ${dmy(g.fechaFin||'')}`;

  const header=document.createElement('div');
  header.className='group-card';
  header.innerHTML = `
    <h3>${titulo}</h3>
    <div class="group-sub">${sub}</div>
    <div class="muted" style="margin-top:4px">${rango}</div>
  `;
  cont.appendChild(header);

  const tabs=document.createElement('div');
  tabs.innerHTML = `
    <div style="display:flex;gap:.5rem;margin:.6rem 0">
      <button id="tabResumen" class="btn-sec">RESUMEN</button>
      <button id="tabItin"    class="btn-sec">ITINERARIO</button>
      <button id="tabGastos"  class="btn-sec">GASTOS</button>
    </div>
    <div id="paneResumen"></div>
    <div id="paneItin" style="display:none"></div>
    <div id="paneGastos" style="display:none"></div>
  `;
  cont.appendChild(tabs);

  const paneResumen=tabs.querySelector('#paneResumen');
  const paneItin=tabs.querySelector('#paneItin');
  const paneGastos=tabs.querySelector('#paneGastos');

  tabs.querySelector('#tabResumen').onclick=()=>{ paneResumen.style.display=''; paneItin.style.display='none'; paneGastos.style.display='none'; };
  tabs.querySelector('#tabItin').onclick   =()=>{ paneResumen.style.display='none'; paneItin.style.display=''; paneGastos.style.display='none'; };
  tabs.querySelector('#tabGastos').onclick =()=>{ paneResumen.style.display='none'; paneItin.style.display='none'; paneGastos.style.display=''; };

  renderResumen(g, paneResumen);
  renderItinerario(g, paneItin, preferDate);
  renderGastos(g, paneGastos);
}

/* ============== Resumen (Hotel + Vuelos + Alertas) ============== */
async function renderResumen(g, pane){
  pane.innerHTML='<div class="loader">CARGANDO RESUMEN…</div>';
  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem';
  pane.innerHTML='';

  const alertBox=document.createElement('div'); alertBox.className='act';
  alertBox.innerHTML='<h4>ALERTAS</h4><div class="muted">CARGANDO…</div>';
  wrap.appendChild(alertBox);

  const hotelBox=document.createElement('div'); hotelBox.className='act';
  hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">BUSCANDO ASIGNACIÓN…</div>';
  wrap.appendChild(hotelBox);

  const vuelosBox=document.createElement('div'); vuelosBox.className='act';
  vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">BUSCANDO VUELOS…</div>';
  wrap.appendChild(vuelosBox);

  pane.appendChild(wrap);

  // ALERTAS
  loadAndRenderAlerts(g, alertBox);

  // HOTEL
  try{
    const h = await loadHotelInfo(g);
    if (!h){ hotelBox.innerHTML='<h4>HOTEL ASIGNADO</h4><div class="muted">SIN ASIGNACIÓN DE HOTEL.</div>'; }
    else{
      const nombre = h.hotelNombre || h.hotel?.nombre || 'HOTEL ASIGNADO';
      const fechas = `${dmy(h.checkIn||'')} — ${dmy(h.checkOut||'')}`;
      const dir = h.hotel?.direccion || '';
      const contacto = [h.hotel?.contactoNombre, h.hotel?.contactoTelefono, h.hotel?.contactoCorreo].filter(Boolean).join(' · ');
      hotelBox.innerHTML = `
        <h4>${nombre}</h4>
        ${dir? `<div class="prov">${dir}</div>`:''}
        <div class="meta">CHECK-IN/OUT: ${fechas}</div>
        ${contacto? `<div class="meta">${contacto}</div>`:''}
      `;
    }
  }catch(e){ console.error(e); hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">NO FUE POSIBLE CARGAR.</div>'; }

  // VUELOS
  try{
    const vuelos = await loadVuelosInfo(g);
    if (!vuelos.length){ vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">SIN VUELOS REGISTRADOS.</div>'; }
    else{
      const list=document.createElement('div');
      vuelos.forEach(v=>{
        const titulo = (v.numero ? ('#'+v.numero+' — ') : '') + (v.proveedor || '');
        const ida = toISO(v.fechaIda), vuelta = toISO(v.fechaVuelta);
        const linea = `${v.origen||''} — ${v.destino||''} ${ida?(' · '+dmy(ida)) : ''}${vuelta?(' — '+dmy(vuelta)) : ''}`;
        const tip = v.tipoVuelo ? (' ('+v.tipoVuelo+')') : '';
        const item=document.createElement('div'); item.className='meta'; item.textContent = `${titulo} · ${linea}${tip}`;
        list.appendChild(item);
      });
      vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4>';
      vuelosBox.appendChild(list);
    }
  }catch(e){ console.error(e); vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">NO FUE POSIBLE CARGAR.</div>'; }
}

async function loadHotelInfo(g){
  const key = g.numeroNegocio;
  if (state.cache.hotel.has(key)) return state.cache.hotel.get(key);
  const qs = await getDocs(query(collection(db,'hotelAssignments'), where('grupoId','==', String(key))));
  if (qs.empty){ state.cache.hotel.set(key, null); return null; }
  let elegido=null, score=1e15; const rango={ini:toISO(g.fechaInicio), fin:toISO(g.fechaFin)};
  qs.forEach(d=>{
    const x=d.data()||{}; const ci=toISO(x.checkIn), co=toISO(x.checkOut);
    let s=5e14; if (ci && co && rango.ini && rango.fin){ const overlap = !(co < rango.ini || ci > rango.fin); s = overlap ? 0 : Math.abs(new Date(ci) - new Date(rango.ini)); }
    if (s<score){ score=s; elegido={ id:d.id, ...x }; }
  });
  let hotelDoc=null; if (elegido?.hotelId){ const hd=await getDoc(doc(db,'hoteles', String(elegido.hotelId))); if (hd.exists()) hotelDoc={ id:hd.id, ...hd.data() }; }
  const out = { ...elegido, hotel:hotelDoc, hotelNombre: elegido?.nombre || elegido?.hotelNombre || hotelDoc?.nombre || '' };
  state.cache.hotel.set(key, out); return out;
}

async function loadVuelosInfo(g){
  const key=g.numeroNegocio; if (state.cache.vuelos.has(key)) return state.cache.vuelos.get(key);
  let found=[]; try{
    const qs=await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains', String(key))));
    qs.forEach(d=> found.push({ id:d.id, ...d.data() }));
  }catch(e){}
  if (!found.length){
    const ss=await getDocs(collection(db,'vuelos'));
    ss.forEach(d=>{ const v=d.data()||{}; const arr=Array.isArray(v.grupos)?v.grupos:[]; if (arr.some(x=> String(x?.id||'')===String(key))) found.push({ id:d.id, ...v }); });
  }
  found.sort((a,b)=> (toISO(a.fechaIda)||'').localeCompare(toISO(b.fechaIda)||'')); state.cache.vuelos.set(key, found); return found;
}

/* ============== Alertas ============== */
async function loadAndRenderAlerts(g, box){
  try{
    // alertas por grupo
    const ag = await getDocs(query(collection(db,'grupos', g.id, 'alertas'), orderBy('createdAt','desc'), limit(50)));
    const groupAlerts=[]; ag.forEach(d=> groupAlerts.push({id:d.id, scope:'group', ...d.data()}));

    // alertas globales
    const gg = await getDocs(query(collection(db,'alertasGlobales'), orderBy('createdAt','desc'), limit(150)));
    const applicable=[];
    gg.forEach(d=>{
      const a=d.data()||{};
      const okCoord = !a.coordinadorIds?.length || a.coordinadorIds.includes(state.coordId) || a.coordinadorIds.includes(g.coordinadorId);
      const okDest  = !a.destinoIn?.length || a.destinoIn.includes(String(g.destino||'').trim());
      const okRange = !(a.rango?.ini && g.fechaFin && g.fechaFin < a.rango.ini) && !(a.rango?.fin && g.fechaInicio && g.fechaInicio > a.rango.fin);
      if (okCoord && okDest && okRange) applicable.push({id:d.id, scope:'global', ...a});
    });

    const all=[...groupAlerts, ...applicable];
    const isCoord = !state.isStaff;

    // render
    const frag=document.createElement('div');
    if (!all.length) frag.innerHTML = '<div class="meta">SIN ALERTAS.</div>';

    for (const a of all){
      const div=document.createElement('div'); div.className='meta';
      div.innerHTML = `• ${a.mensaje || '(sin mensaje)'} ${a.scope==='global' ? '· GLOBAL' : ''}`;
      // checkbox leído para coordinador
      if (isCoord){
        const chk=document.createElement('input'); chk.type='checkbox'; chk.style.marginLeft='8px';
        const ackPath = a.scope==='global' ? ['alertasGlobales',a.id,'acks',auth.currentUser.uid] : ['grupos',g.id,'alertas',a.id,'acks',auth.currentUser.uid];
        try{
          // leer ack rápido (sincrónico no posible aquí; lo marcamos solo on change)
        }catch{}
        chk.onchange = async ()=>{
          try{ await setDoc(doc(db,...ackPath), { leido:true, leidoAt: serverTimestamp() }, { merge:true }); }catch(e){ console.error(e); }
        };
        div.appendChild(chk);
      }
      frag.appendChild(div);
    }

    // bloque crear (solo staff)
    if (state.isStaff){
      const mk=document.createElement('div'); mk.style.marginTop='.6rem';
      mk.innerHTML = `
        <div class="meta">CREAR ALERTA</div>
        <textarea id="alMsg" placeholder="MENSAJE PARA COORDINADORES / GRUPOS"></textarea>
        <div class="row">
          <input id="alDestinos" type="text" placeholder="DESTINOS (coma: BARILOCHE, BRASIL)"/>
          <input id="alIni" type="date" placeholder="INICIO (opcional)"/>
          <input id="alFin" type="date" placeholder="FIN (opcional)"/>
        </div>
        <div class="row">
          <select id="alCoordinadores" multiple style="height:120px">
            ${state.coordinadores.map(c=>`<option value="${c.id}">${c.nombre} — ${c.email}</option>`).join('')}
          </select>
          <button id="alCrear" class="btn-sec">AGREGAR MENSAJE</button>
          <button id="alCrearGrupo" class="btn-sec">AGREGAR AL GRUPO ACTUAL</button>
        </div>`;
      frag.appendChild(mk);

      mk.querySelector('#alCrear').onclick = async ()=>{
        const msg=mk.querySelector('#alMsg').value.trim(); if(!msg) return alert('Escribe un mensaje.');
        const coordIds=[...mk.querySelector('#alCoordinadores').selectedOptions].map(o=>o.value);
        const destinos=(mk.querySelector('#alDestinos').value||'').split(',').map(s=>s.trim()).filter(Boolean);
        const ini=mk.querySelector('#alIni').value||''; const fin=mk.querySelector('#alFin').value||'';
        try{
          await addDoc(collection(db,'alertasGlobales'), {
            mensaje: msg, coordinadorIds: coordIds, destinoIn: destinos, rango: (ini||fin)?{ini,fin}:{},
            createdByUid: auth.currentUser.uid, createdByEmail: (auth.currentUser.email||'').toLowerCase(), createdAt: serverTimestamp()
          });
          alert('Alerta creada.');
          loadAndRenderAlerts(g, box);
        }catch(e){ console.error(e); alert('No se pudo crear.'); }
      };
      mk.querySelector('#alCrearGrupo').onclick = async ()=>{
        const msg=mk.querySelector('#alMsg').value.trim(); if(!msg) return alert('Escribe un mensaje.');
        try{
          await addDoc(collection(db,'grupos', g.id, 'alertas'), {
            mensaje: msg, createdByUid: auth.currentUser.uid, createdByEmail: (auth.currentUser.email||'').toLowerCase(), createdAt: serverTimestamp()
          });
          alert('Alerta enviada al grupo.');
          loadAndRenderAlerts(g, box);
        }catch(e){ console.error(e); alert('No se pudo crear.'); }
      };
    }

    box.innerHTML='<h4>ALERTAS</h4>';
    box.appendChild(frag);
  }catch(e){ console.error(e); box.innerHTML='<h4>ALERTAS</h4><div class="meta">NO FUE POSIBLE CARGAR.</div>'; }
}

/* ============== Itinerario + asistencia + bitácora + finalizar ============== */
function renderItinerario(g, pane, preferDate){
  pane.innerHTML='';
  const fechas = rangoFechas(g.fechaInicio, g.fechaFin);
  if (!fechas.length){ pane.innerHTML='<div class="muted">FECHAS NO DEFINIDAS.</div>'; return; }

  const pillsWrap=document.createElement('div'); pillsWrap.className='date-pills'; pane.appendChild(pillsWrap);
  const actsWrap=document.createElement('div'); actsWrap.className='acts'; pane.appendChild(actsWrap);

  const hoy=toISO(new Date());
  let startDate = preferDate || ( (hoy>=fechas[0] && hoy<=fechas.at(-1)) ? hoy : fechas[0] );

  fechas.forEach(f=>{
    const pill=document.createElement('div'); pill.className='pill'+(f===startDate?' active':''); pill.textContent=fmt(f); pill.title=dmy(f); pill.dataset.fecha=f;
    pill.onclick=()=>{ pillsWrap.querySelectorAll('.pill').forEach(p=>p.classList.remove('active')); pill.classList.add('active'); renderActs(g, f, actsWrap); localStorage.setItem('rt_last_date_'+g.id, f); };
    pillsWrap.appendChild(pill);
  });

  const lastSaved = localStorage.getItem('rt_last_date_'+g.id);
  if (lastSaved && fechas.includes(lastSaved)) startDate = lastSaved;
  renderActs(g, startDate, actsWrap);
}

async function renderActs(grupo, fechaISO, cont){
  cont.innerHTML='';
  const acts = (grupo.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
  if (!acts.length){ cont.innerHTML='<div class="muted">SIN ACTIVIDADES PARA ESTE DÍA.</div>'; return; }

  for (const act of acts){
    const plan = calcPlan(act, grupo);
    const saved = getSavedAsistencia(grupo, fechaISO, act.actividad);
    const estado = await getActividadEstado(grupo.id, fechaISO, act.actividad);

    const horaIni = act.horaInicio || '--:--';
    const horaFin = act.horaFin    || '--:--';
    const paxFinalInit = (saved?.paxFinal ?? '');
    const actName = act.actividad || 'ACTIVIDAD';
    const actKey  = slug(actName);

    const div=document.createElement('div'); div.className='act';
    div.innerHTML = `
      <h4>${actName}</h4>
      <div class="meta">${horaIni}–${horaFin} · PLAN: <strong>${plan}</strong> PAX</div>
      <div class="row">
        <input class="inpPax" type="number" min="0" inputmode="numeric" placeholder="ASISTENTES"/>
        <textarea class="inpNota" placeholder="NOTAS (SE GUARDA EN BITÁCORA)"></textarea>
        <button class="btnSave">GUARDAR</button>
        <button class="btnFinalizar btn-warn">FINALIZAR</button>
        ${state.isStaff ? '<button class="btnReabrir btn-danger">PONER PENDIENTE</button>' : ''}
      </div>
      <div class="bitacora" style="margin-top:.5rem">
        <div class="muted" style="margin-bottom:.25rem">BITÁCORA</div>
        <div class="bitItems" style="display:grid;gap:.35rem"></div>
      </div>
    `;
    cont.appendChild(div);

    div.querySelector('.inpPax').value = paxFinalInit;

    // Estado inicial botones
    setButtonsByEstado(div, estado?.status || 'pendiente');

    // Guardar asistencia + nota -> bitácora
    div.querySelector('.btnSave').onclick = async ()=>{
      const btn = div.querySelector('.btnSave'); btn.disabled=true;
      try{
        const pax = Number(div.querySelector('.inpPax').value || 0);
        const nota = String(div.querySelector('.inpNota').value || '');
        const refGrupo = doc(db,'grupos', grupo.id);
        const keyPath = `asistencias.${fechaISO}.${actKey}`;
        const data = { paxFinal: pax, notas: nota, byUid: auth.currentUser.uid, byEmail: (auth.currentUser.email||'').toLowerCase(), updatedAt: serverTimestamp() };
        const payload={}; payload[keyPath]=data;
        await updateDoc(refGrupo, payload);
        setSavedAsistenciaLocal(grupo, fechaISO, actName, { ...data });

        if (nota.trim()){
          const coll = collection(db, 'grupos', grupo.id, 'bitacora', `${fechaISO}-${actKey}`, 'items');
          await addDoc(coll, { texto: nota.trim(), byUid: auth.currentUser.uid, byEmail: (auth.currentUser.email||'').toLowerCase(), ts: serverTimestamp() });
          div.querySelector('.inpNota').value = '';
          await loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'));
        }
        btn.textContent='GUARDADO'; setTimeout(()=>{ btn.textContent='GUARDAR'; btn.disabled=false; }, 900);
      }catch(e){ console.error(e); btn.disabled=false; alert('NO SE PUDO GUARDAR.'); }
    };

    // Bitácora
    await loadBitacora(grupo.id, fechaISO, actKey, div.querySelector('.bitItems'));

    // Finalizar / Voucher flow
    div.querySelector('.btnFinalizar').onclick = async ()=> openFinalizarModal(grupo, fechaISO, act, div);
    if (state.isStaff) div.querySelector('.btnReabrir').onclick = async ()=>{
      await setActividadEstado(grupo.id, fechaISO, act.actividad, { status:'pendiente' });
      setButtonsByEstado(div, 'pendiente');
    };

    // Ocultar finalizar si "no aplica"
    // (lo resolvemos on-demand dentro del modal; aquí queda visible y el modal avisa/no muestra)
  }
}

function setButtonsByEstado(div, status){
  const finBtn = div.querySelector('.btnFinalizar');
  if (status==='finalizada'){ finBtn.textContent='FINALIZADA'; finBtn.disabled=true; }
  else { finBtn.textContent='FINALIZAR'; finBtn.disabled=false; }
}

async function getActividadEstado(grupoId, fechaISO, actName){
  try{
    const d=await getDoc(doc(db,'grupos',grupoId,'estadoActividades',`${fechaISO}-${slug(actName)}`));
    if (d.exists()) return d.data();
  }catch{}
  return { status:'pendiente' };
}
async function setActividadEstado(grupoId, fechaISO, actName, data){
  try{
    await setDoc(doc(db,'grupos',grupoId,'estadoActividades',`${fechaISO}-${slug(actName)}`), {
      ...data, updatedAt: serverTimestamp(), byUid: auth.currentUser.uid, byEmail:(auth.currentUser.email||'').toLowerCase()
    }, { merge:true });
  }catch(e){ console.error(e); alert('No se pudo actualizar estado.'); }
}

async function loadBitacora(grupoId, fechaISO, actKey, wrap){
  wrap.innerHTML='<div class="muted">CARGANDO…</div>';
  try{
    const coll = collection(db, 'grupos', grupoId, 'bitacora', `${fechaISO}-${actKey}`, 'items');
    const qs = await getDocs(query(coll, orderBy('ts','desc'), limit(50)));
    const frag=document.createDocumentFragment();
    qs.forEach(d=>{
      const x=d.data()||{}; const quien = String(x.byEmail || x.byUid || 'USUARIO');
      const cuando = x.ts?.seconds ? new Date(x.ts.seconds*1000) : null;
      const hora = cuando ? cuando.toLocaleString('es-CL') : '';
      const div=document.createElement('div'); div.className='meta';
      div.textContent = `• ${x.texto || ''} — ${quien}${hora?(' · '+hora):''}`; frag.appendChild(div);
    });
    wrap.innerHTML=''; wrap.appendChild(frag);
    if (!qs.size) wrap.innerHTML='<div class="muted">AÚN NO HAY NOTAS.</div>';
  }catch(e){ console.error(e); wrap.innerHTML='<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>'; }
}

/* ============== Finalizar / Vouchers ============== */
async function openFinalizarModal(grupo, fechaISO, act, cardDiv){
  const sid = String(act.servicioId || act.servicioDocId || '').trim();
  if (!sid){ return openModal('<div class="meta">Esta actividad no tiene servicio asociado.</div>'); }
  let sdoc=null; try{ const sd=await getDoc(doc(db,'Servicios', sid)); if(sd.exists()) sdoc={id:sd.id, ...sd.data()}; }catch(e){}
  const tipo = String(sdoc?.voucherTipo||'').toLowerCase();
  if (!tipo || tipo.includes('no aplica')){ return openModal('<div class="meta">Esta actividad no requiere voucher.</div>'); }

  const paxAsist = Number(cardDiv.querySelector('.inpPax').value || 0);
  const paxPlan  = calcPlan(act, grupo);

  const baseInfo = `
    <div id="printArea">
      <h2>VOUCHER ${tipo.includes('electron') ? 'ELECTRÓNICO' : 'FÍSICO'}</h2>
      <div class="line"><strong>ACTIVIDAD/SERVICIO:</strong> ${act.actividad || sdoc?.nombre || '(sin nombre)'}</div>
      <div class="line"><strong>PROVEEDOR:</strong> ${sdoc?.proveedor || sdoc?.contactoNombre || '(s/i)'}</div>
      <div class="line"><strong>GRUPO:</strong> ${grupo.nombreGrupo || grupo.aliasGrupo || grupo.id}</div>
      <div class="line"><strong>FECHA:</strong> ${dmy(fechaISO)}</div>
      <div class="line"><strong>PAX PLAN:</strong> ${paxPlan}</div>
      <div class="line"><strong>PAX ASISTEN:</strong> ${tipo.includes('electron') ? paxAsist : '_____ (llenado a mano)'}</div>
      <div class="line"><strong>COORDINADOR:</strong> ${tipo.includes('electron') ? (state.user.email||'') : '________________________'}</div>
    </div>
  `;

  if (tipo.includes('electron')){
    openModal(`
      ${baseInfo}
      <div class="row" style="margin-top:.6rem">
        <input id="pin" type="text" placeholder="CLAVE DEL SERVICIO (PIN)"/>
        <button id="btnNFC" class="btn-sec">ESCANEAR NFC</button>
        <button id="btnFirmar" class="btn-warn">FIRMAR</button>
        <button id="btnPend" class="btn-sec">PENDIENTE</button>
      </div>
    `);

    const m=document.getElementById('rtModal');
    m.querySelector('#btnPend').onclick=async()=>{ await setActividadEstado(grupo.id, fechaISO, act.actividad, { status:'pendiente', tipoVoucher:'electronico' }); setButtonsByEstado(cardDiv,'pendiente'); m.style.display='none'; };
    m.querySelector('#btnFirmar').onclick=async()=>{
      const pin=m.querySelector('#pin').value.trim();
      const pinOk = String(sdoc?.voucherClave||sdoc?.pin||'').trim();
      if (!pin || pin!==pinOk) return alert('Clave incorrecta.');
      await setActividadEstado(grupo.id, fechaISO, act.actividad, { status:'finalizada', tipoVoucher:'electronico' });
      setButtonsByEstado(cardDiv,'finalizada'); m.style.display='none';
    };
    m.querySelector('#btnNFC').onclick=async()=>{
      if (!('NDEFReader' in window)) return alert('NFC no disponible en este dispositivo/navegador.');
      try{
        const reader = new NDEFReader(); await reader.scan();
        reader.onreading = (ev)=>{ const rec = ev.message.records?.[0]; if(!rec) return; const txt = new TextDecoder().decode(rec.data||new Uint8Array()); m.querySelector('#pin').value = (txt||'').trim(); };
      }catch(e){ alert('No se pudo iniciar NFC.'); }
    };
  }else{ // físico
    openModal(`
      ${baseInfo}
      <div class="row" style="margin-top:.6rem">
        <button id="btnPrint" class="btn-sec">IMPRIMIR</button>
        <button id="btnFin" class="btn-warn">FINALIZAR</button>
        <button id="btnPend" class="btn-sec">PENDIENTE</button>
      </div>
    `);
    const m=document.getElementById('rtModal');
    m.querySelector('#btnPrint').onclick=()=>window.print();
    m.querySelector('#btnFin').onclick=async()=>{ await setActividadEstado(grupo.id, fechaISO, act.actividad, { status:'finalizada', tipoVoucher:'fisico' }); setButtonsByEstado(cardDiv,'finalizada'); m.style.display='none'; };
    m.querySelector('#btnPend').onclick=async()=>{ await setActividadEstado(grupo.id, fechaISO, act.actividad, { status:'pendiente', tipoVoucher:'fisico' }); setButtonsByEstado(cardDiv,'pendiente'); m.style.display='none'; };
  }
}

/* ===== modal simple ===== */
function openModal(html){
  const m=document.getElementById('rtModal'); const box=document.getElementById('rtBox');
  box.innerHTML=html; m.style.display='flex'; m.onclick=(e)=>{ if(e.target===m) m.style.display='none'; };
}

/* ============== Gastos (Storage + TC) ============== */
async function renderGastos(g, pane){
  pane.innerHTML='<div class="loader">CARGANDO…</div>';
  const tipos=await getTiposCambio();

  const form=document.createElement('div'); form.className='act';
  form.innerHTML=`
    <h4>REGISTRAR GASTO</h4>
    <div class="row">
      <input id="gsAsunto" type="text" placeholder="ASUNTO"/>
      <select id="gsMoneda">
        <option value="CLP">CLP (PRED)</option>
        <option value="USD">USD</option>
        <option value="BRL">BRL</option>
        <option value="ARS">ARS</option>
      </select>
      <input id="gsValor" type="number" step="0.01" min="0" placeholder="VALOR"/>
      <input id="gsImg" type="file" accept="image/*" capture="environment"/>
      <button id="gsSave">GUARDAR GASTO</button>
    </div>
  `;

  const lista=document.createElement('div'); lista.className='act';
  lista.innerHTML='<h4>GASTOS DEL GRUPO</h4><div id="gsList" class="gastos-grid"></div><div class="totales" id="gsTotals"></div>';

  pane.innerHTML=''; pane.appendChild(form); pane.appendChild(lista);

  async function refreshList(){
    const qs = await getDocs(query(collection(db,'grupos',g.id,'gastos'), orderBy('ts','desc'), limit(200)));
    const grid=lista.querySelector('#gsList'); grid.innerHTML='';
    const totals = { CLP:0, USD:0, BRL:0, ARS:0, CLPTotal:0 };
    qs.forEach(d=>{
      const x=d.data()||{};
      const row=document.createElement('div'); row.className='gasto-item';
      row.innerHTML=`
        <div>${x.asunto||'(sin asunto)'} <span class="muted">· ${x.byEmail||''}</span></div>
        <div>${x.moneda||''}</div>
        <div>${Number(x.valor||0).toLocaleString('es-CL')}</div>
        <div>${x.imgUrl?`<a href="${x.imgUrl}" target="_blank" rel="noopener">VER COMPROBANTE</a>`:''}</div>
      `;
      grid.appendChild(row);
      totals[x.moneda||'CLP'] += Number(x.valor||0);
      totals.CLPTotal += Number(x.valorCLP || 0);
    });
    lista.querySelector('#gsTotals').innerHTML = `
      <div class="meta">TOTAL CLP: ${Math.round(totals.CLPTotal).toLocaleString('es-CL')}</div>
      <div class="meta">DESGLOSE · CLP: ${totals.CLP.toLocaleString('es-CL')} · USD: ${totals.USD.toLocaleString('es-CL')} · BRL: ${totals.BRL.toLocaleString('es-CL')} · ARS: ${totals.ARS.toLocaleString('es-CL')}</div>
      <div class="muted">TC USADOS: USD ${tipos.USD||1} · BRL ${tipos.BRL||1} · ARS ${tipos.ARS||1}</div>
    `;
  }
  await refreshList();

  form.querySelector('#gsSave').onclick = async ()=>{
    const asunto=form.querySelector('#gsAsunto').value.trim();
    const moneda=form.querySelector('#gsMoneda').value||'CLP';
    const valor=Number(form.querySelector('#gsValor').value||0);
    const file=form.querySelector('#gsImg').files[0]||null;
    if (!asunto || !valor) return alert('Completa asunto y valor.');

    let url=''; let gastoId='';
    try{
      const tc = await getTiposCambio(); const fx = Number(tc[moneda]||1);
      const data = {
        asunto, moneda, valor, fxRateUsed: fx, valorCLP: Math.round(valor*fx),
        byUid: auth.currentUser.uid, byEmail: (auth.currentUser.email||'').toLowerCase(), ts: serverTimestamp()
      };
      const ref = await addDoc(collection(db,'grupos',g.id,'gastos'), data); gastoId=ref.id;
      if (file){
        const path=`groups/${g.id}/gastos/${gastoId}/${Date.now()}_${file.name}`;
        const sref=storageRef(storage, path);
        await uploadBytes(sref, file);
        url = await getDownloadURL(sref);
        await updateDoc(ref, { imgUrl:url, imgPath:path });
      }
      form.querySelector('#gsAsunto').value=''; form.querySelector('#gsValor').value=''; form.querySelector('#gsImg').value='';
      await refreshList();
      alert('Gasto registrado.');
    }catch(e){ console.error(e); alert('No se pudo guardar el gasto.'); }
  };
}

/* ============== Impresión masiva (staff) ============== */
function openBulkVoucherModal(){
  const coords = state.coordinadores;
  openModal(`
    <h3>IMPRIMIR VOUCHERS</h3>
    <div class="row">
      <select id="pvCoord">
        <option value="ALL">TODOS LOS COORDINADORES</option>
        ${coords.map(c=>`<option value="${c.id}">${c.nombre} — ${c.email}</option>`).join('')}
      </select>
      <input id="pvDestino" type="text" placeholder="DESTINO (opcional)"/>
      <input id="pvIni" type="date" placeholder="INI"/>
      <input id="pvFin" type="date" placeholder="FIN"/>
      <button id="pvGo" class="btn-sec">GENERAR</button>
    </div>
    <div id="pvOut" style="margin-top:.6rem"></div>
  `);
  const m=document.getElementById('rtModal');
  m.querySelector('#pvGo').onclick = ()=>{
    const coordId=m.querySelector('#pvCoord').value;
    const dst=(m.querySelector('#pvDestino').value||'').trim();
    const ini=m.querySelector('#pvIni').value||''; const fin=m.querySelector('#pvFin').value||'';
    const list = applyTextFilter(state.ordenados).filter(g=>{
      const okCoord = (coordId==='ALL') || (g.coordinadorId===coordId || g.coordinador?.id===coordId);
      const okDest  = !dst || String(g.destino||'').trim().toLowerCase()===dst.toLowerCase();
      const okRange = !(ini && g.fechaFin < ini) && !(fin && g.fechaInicio > fin);
      return okCoord && okDest && okRange;
    });
    const out=m.querySelector('#pvOut'); out.innerHTML='';
    const printArea=document.createElement('div'); printArea.id='printArea';

    for(const g of list){
      const fechas=rangoFechas(g.fechaInicio,g.fechaFin);
      for(const f of fechas){
        const acts=(g.itinerario?.[f]||[]);
        for(const a of acts){
          const sid=String(a.servicioId||a.servicioDocId||'').trim(); if(!sid) continue;
          // no buscamos el doc aquí por performance; imprimimos “voucher de actividad” genérico
          const block=document.createElement('div');
          block.style.cssText='page-break-inside:avoid;border:1px solid #000;padding:10px;margin:8px 0';
          block.innerHTML=`
            <h3>${g.nombreGrupo||g.aliasGrupo||g.id}</h3>
            <div><strong>ACTIVIDAD:</strong> ${a.actividad||'(sin nombre)'} — <strong>FECHA:</strong> ${dmy(f)}</div>
            <div><strong>DESTINO:</strong> ${g.destino||''} · <strong>PAX PLAN:</strong> ${calcPlan(a,g)}</div>
          `;
          printArea.appendChild(block);
        }
      }
    }
    out.appendChild(printArea);
    window.print();
  };
}
