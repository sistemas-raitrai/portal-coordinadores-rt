/* coordinadores.js — Portal Coordinadores RT (v3)
   - Orden de paneles: staffBar → navPanel → statsPanel → gruposPanel
   - #grupos dentro de un panel para igual ancho/márgenes
   - #allTrips con: FILTRO (TODOS), DESTINOS, VIAJES
   - #statsPanel refleja filtro (todos o por destino)
   - Fechas en DD-MM-AAAA
*/
import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, addDoc,
  serverTimestamp, query, where, orderBy, limit
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* ============== Auth ============== */
const auth = getAuth(app);
const logoutBtn = document.getElementById('logout');
if (logoutBtn) logoutBtn.onclick = () => signOut(auth).then(() => (location = 'index.html'));

/* ============== Staff permitido ============== */
const STAFF_EMAILS = new Set(
  ['aleoperaciones@raitrai.cl','tomas@raitrai.cl','operaciones@raitrai.cl','anamaria@raitrai.cl','sistemas@raitrai.cl']
  .map(e=>e.toLowerCase())
);

/* ============== Utils de texto/fechas ============== */
const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');
const slug = s => norm(s).slice(0,60);

const toISO = (x)=>{
  if(!x) return '';
  if (typeof x==='string'){
    if (/^\d{4}-\d{2}-\d{2}$/.test(x)) return x;
    const d=new Date(x); return isNaN(d)?'':d.toISOString().slice(0,10);
  }
  if (x && typeof x==='object' && 'seconds' in x) return new Date(x.seconds*1000).toISOString().slice(0,10);
  if (x instanceof Date) return x.toISOString().slice(0,10);
  return '';
};

const dmy = (iso)=>{ // YYYY-MM-DD -> DD-MM-YYYY
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(iso||'');
  return m ? `${m[3]}-${m[2]}-${m[1]}` : '';
};

const daysInclusive = (ini, fin)=>{
  const a = toISO(ini), b = toISO(fin); if(!a||!b) return 0;
  return Math.max(1, Math.round((new Date(b)-new Date(a))/86400000)+1);
};

const rangoFechas = (ini, fin)=>{
  const out=[]; const A=toISO(ini), B=toISO(fin); if(!A||!B) return out;
  const da=new Date(A+'T00:00:00'), db=new Date(B+'T00:00:00');
  for(let d=new Date(da); d<=db; d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10));
  return out;
};

const parseQS = ()=>{ const p=new URLSearchParams(location.search); return { g:p.get('g')||'', f:p.get('f')||'' }; };

/* tolerancia de itinerario (obj o array legado) */
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){
    const map={}; for(const item of raw){ const f=toISO(item && item.fecha); if(!f) continue; (map[f] ||= []).push({...item}); }
    return map;
  }
  return raw;
}

/* extracción tolerante desde grupos */
const arrify = v => Array.isArray(v) ? v : (v && typeof v==='object' ? Object.values(v) : (v ? [v] : []));
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
const paxOf = g => Number(g?.cantidadgrupo ?? g?.pax ?? 0);

/* ====== Estado ====== */
const state = {
  user:null,
  coordinadores:[],
  grupos:[],
  ordenados:[],
  idx:0,
  filter: { type:'all', value:null }, // 'all' | 'dest'
  cache:{ hotel:new Map(), vuelos:new Map() }
};

/* ====== Helpers UI: crear paneles y forzar orden ====== */
function ensurePanel(id, html=''){
  let p = document.getElementById(id);
  if (!p){
    p = document.createElement('div');
    p.id = id; p.className = 'panel';
    document.querySelector('.wrap').prepend(p);
  }
  if (html) p.innerHTML = html;
  enforceOrder();
  return p;
}
function enforceOrder(){
  const wrap = document.querySelector('.wrap');
  const order = ['staffBar','navPanel','statsPanel','gruposPanel'];
  order.forEach(id=>{
    const node = document.getElementById(id);
    if (node) wrap.appendChild(node); // mueve al final en el orden indicado
  });
}

/* ============== Arranque ============== */
onAuthStateChanged(auth, async (user) => {
  if (!user) { location.href='index.html'; return; }
  state.user = user;

  // Asegura que #grupos está dentro de #gruposPanel (panel)
  ensurePanel('gruposPanel'); // ya existe en el HTML, pero garantizamos orden

  const email = (user.email||'').toLowerCase();
  const isStaff = STAFF_EMAILS.has(email);
  const coordinadores = await loadCoordinadores();
  state.coordinadores = coordinadores;

  if (isStaff) {
    await showStaffSelector(coordinadores, user);
  } else {
    const miReg = findCoordinadorForUser(coordinadores, user);
    await loadGruposForCoordinador(miReg, user);
  }
});

/* ============== Cargar coordinadores ============== */
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list=[]; snap.forEach(d=>{ const x=d.data()||{}; list.push({
    id:d.id, nombre:String(x.nombre||x.Nombre||x.coordinador||''), email:String(x.email||x.correo||x.mail||'').toLowerCase(), uid:String(x.uid||x.userId||'')
  });});
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre,'es',{sensitivity:'base'}));
  // eliminar duplicados por (nombre|email)
  const seen=new Set(), dedup=[]; for(const c of list){ const k=(c.nombre+'|'+c.email).toLowerCase(); if(!seen.has(k)){ seen.add(k); dedup.push(c); } }
  return dedup;
}

/* ============== Selector Staff ============== */
async function showStaffSelector(coordinadores){
  const bar = ensurePanel('staffBar',
    '<label style="display:block;margin-bottom:6px;color:#cbd5e1">VER VIAJES POR COORDINADOR</label>'+
    '<select id="coordSelect"></select>'
  );
  const sel = bar.querySelector('#coordSelect');
  sel.innerHTML = '<option value="">— SELECCIONA COORDINADOR —</option>' +
    coordinadores.map(c=>`<option value="${c.id}">${(c.nombre||'')} — ${c.email||'SIN CORREO'}</option>`).join('');
  sel.onchange = async ()=>{
    const id = sel.value;
    const elegido = state.coordinadores.find(c=>c.id===id) || null;
    localStorage.setItem('rt_staff_coord', id||'');
    await loadGruposForCoordinador(elegido, state.user);
  };

  // selección previa
  const last = localStorage.getItem('rt_staff_coord');
  if (last && state.coordinadores.find(c=>c.id===last)){
    sel.value = last;
    const elegido = state.coordinadores.find(c=>c.id===last);
    await loadGruposForCoordinador(elegido, state.user);
  }
}

/* ============== Resolver coordinador (no staff) ============== */
function findCoordinadorForUser(coordinadores, user){
  const email=(user.email||'').toLowerCase(), uid=user.uid;
  let c = coordinadores.find(x=> x.email && x.email.toLowerCase()===email); if(c) return c;
  if (uid){ c=coordinadores.find(x=>x.uid && x.uid===uid); if(c) return c; }
  const disp=norm(user.displayName||''); if (disp){ c=coordinadores.find(x=> norm(x.nombre)===disp); if(c) return c; }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

/* ============== Cargar grupos del coordinador ============== */
async function loadGruposForCoordinador(coord, user){
  const cont=document.getElementById('grupos'); if (cont) cont.textContent='CARGANDO GRUPOS…';

  const allSnap=await getDocs(collection(db,'grupos'));
  const wanted=[];
  const emailElegido=(coord?.email||'').toLowerCase();
  const uidElegido=(coord?.uid||'').toString();
  const docIdElegido=(coord?.id||'').toString();
  const nombreElegido=norm(coord?.nombre||'');
  const isSelf = !coord || coord.id==='self' || emailElegido===(user.email||'').toLowerCase();

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
    const gEmails=emailsOf(raw), gUids=uidsOf(raw), gDocIds=coordDocIdsOf(raw), gNames=nombresOf(raw);
    const match=(emailElegido && gEmails.includes(emailElegido)) || (uidElegido && gUids.includes(uidElegido)) ||
                (docIdElegido && gDocIds.includes(docIdElegido)) || (nombreElegido && gNames.includes(nombreElegido)) ||
                (isSelf && gUids.includes(user.uid));
    if (match) wanted.push(g);
  });

  // fallback por conjuntos (si fuese necesario)
  if (wanted.length===0 && coord && coord.id!=='self'){
    try{
      const qs=[]; if(uidElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorId','==',uidElegido)));
      if(emailElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorEmail','==',emailElegido)));
      if(docIdElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorDocId','==',docIdElegido)));
      const ids=new Set();
      for(const qy of qs){ const ss=await getDocs(qy); ss.forEach(docu=>{ (docu.data()?.viajes||[]).forEach(id=>ids.add(String(id))); }); }
      for(const id of ids){ const ref=doc(db,'grupos',id); const dd=await getDoc(ref); if(dd.exists()){ const raw={id:dd.id, ...dd.data()}; wanted.push({
        ...raw, fechaInicio:toISO(raw.fechaInicio||raw.inicio||raw.fecha_ini), fechaFin:toISO(raw.fechaFin||raw.fin||raw.fecha_fin),
        itinerario:normalizeItinerario(raw.itinerario), asistencias:raw.asistencias||{}, numeroNegocio:String(raw.numeroNegocio||raw.id||dd.id)
      });}}
    }catch(e){ console.warn('Fallback conjuntos no disponible:', e); }
  }

  // ordenar por “próximos primero”
  const hoy = toISO(new Date());
  const futuros = wanted.filter(g => (g.fechaInicio||'') >= hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  const pasados = wanted.filter(g => (g.fechaInicio||'') < hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||'')); // mantén asc
  state.grupos = wanted;
  state.ordenados = [...futuros, ...pasados];

  // filtro por defecto
  state.filter = { type:'all', value:null };

  // PANEL: navegación + estadísticas + viaje
  renderNavBar();
  renderStatsFiltered();

  // índice inicial por ?g= o por último visto
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
  renderOneGroup(state.ordenados[state.idx], qsF);
}

/* ============== Estadísticas (según filtro) ============== */
function getFilteredList(){
  if (state.filter.type==='dest' && state.filter.value){
    return state.ordenados.filter(g => String(g.destino||'') === state.filter.value);
  }
  return state.ordenados.slice();
}
function renderStatsFiltered(){
  renderStats(getFilteredList());
}
function renderStats(list){
  const p = ensurePanel('statsPanel');
  if (!list.length){
    p.innerHTML = '<div class="muted">SIN VIAJES PARA EL COORDINADOR SELECCIONADO.</div>';
    return;
  }
  const labelFiltro = (state.filter.type==='dest')
    ? `FILTRO: DESTINO — ${state.filter.value}`
    : 'FILTRO: TODOS';

  const n = list.length;
  const minIniISO = list.map(g=>g.fechaInicio).filter(Boolean).sort()[0] || '';
  const maxFinISO = list.map(g=>g.fechaFin).filter(Boolean).sort().slice(-1)[0] || '';
  const totalDias = list.reduce((sum,g)=> sum + daysInclusive(g.fechaInicio,g.fechaFin), 0);
  const destinos = [...new Set(list.map(g=>(g.destino||'').toString().trim()).filter(Boolean))];
  const paxTot = list.reduce((s,g)=> s + paxOf(g), 0);
  const paxPorViaje = list.map(g=> `${(g.aliasGrupo||g.nombreGrupo||g.id)} (${paxOf(g)} PAX)`).join(' · ');

  p.innerHTML = `
    <div style="display:grid;gap:.4rem">
      <div class="meta">${labelFiltro}</div>
      <div class="meta">TOTAL VIAJES: <strong>${n}</strong> · TOTAL DÍAS: <strong>${totalDias}</strong></div>
      <div class="meta">RANGO GLOBAL: ${minIniISO?dmy(minIniISO):'—'} — ${maxFinISO?dmy(maxFinISO):'—'}</div>
      <div class="meta">DESTINOS: ${destinos.length? destinos.join(' · ') : '—'}</div>
      <div class="meta">PAX TOTALES: ${paxTot}</div>
      <div class="meta">PAX POR VIAJE: ${paxPorViaje || '—'}</div>
    </div>
  `;
}

/* ============== Barra navegación ============== */
function renderNavBar(){
  const p = ensurePanel('navPanel',
    `<div id="navBar">
       <div class="btns">
         <button id="btnPrev">‹ ANTERIOR</button>
         <button id="btnNext">SIGUIENTE ›</button>
       </div>
       <select id="allTrips"></select>
     </div>`
  );

  const sel = p.querySelector('#allTrips');
  sel.textContent = '';

  // FILTRO: TODOS
  const ogFiltro = document.createElement('optgroup'); ogFiltro.label = 'FILTRO';
  ogFiltro.appendChild(new Option('TODOS', 'all'));
  sel.appendChild(ogFiltro);

  // DESTINOS
  const destinos = [...new Set(state.ordenados.map(g=>String(g.destino||'')).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'es'));
  if (destinos.length){
    const ogDest = document.createElement('optgroup'); ogDest.label = 'DESTINOS';
    destinos.forEach(d => ogDest.appendChild(new Option(d || '(sin destino)', 'dest:'+d)));
    sel.appendChild(ogDest);
  }

  // VIAJES
  const ogTrips = document.createElement('optgroup'); ogTrips.label = 'VIAJES';
  state.ordenados.forEach((g,i)=>{
    const name=(g.nombreGrupo||g.aliasGrupo||g.id);
    const opt = new Option(
      `${name} | IDA: ${dmy(g.fechaInicio||'')} VUELTA: ${dmy(g.fechaFin||'')}`,
      `trip:${i}`
    );
    ogTrips.appendChild(opt);
  });
  sel.appendChild(ogTrips);

  // selecciona el viaje actual
  sel.value = `trip:${state.idx}`;

  // handlers
  p.querySelector('#btnPrev').onclick = ()=>{ if(state.idx>0){ state.idx--; renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`; } };
  p.querySelector('#btnNext').onclick = ()=>{ if(state.idx<state.ordenados.length-1){ state.idx++; renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`; } };

  sel.onchange = ()=>{
    const v = sel.value || '';
    if (v === 'all'){
      state.filter = { type:'all', value:null };
      renderStatsFiltered();
      // mantiene el viaje actual
      sel.value = `trip:${state.idx}`;
    } else if (v.startsWith('dest:')){
      const dest = v.slice(5);
      state.filter = { type:'dest', value: dest };
      renderStatsFiltered();
      // mantiene el viaje actual
      sel.value = `trip:${state.idx}`;
    } else if (v.startsWith('trip:')){
      state.idx = Number(v.slice(5)) || 0;
      renderOneGroup(state.ordenados[state.idx]);
    }
  };
}

/* ============== Vista 1 viaje ============== */
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
      <button id="tabResumen" style="background:#0b1329;color:#e5e7eb;border:1px solid #334155;border-radius:10px;padding:.45rem .8rem;cursor:pointer">RESUMEN</button>
      <button id="tabItin"    style="background:#0b1329;color:#e5e7eb;border:1px solid #334155;border-radius:10px;padding:.45rem .8rem;cursor:pointer">ITINERARIO</button>
    </div>
    <div id="paneResumen"></div>
    <div id="paneItin" style="display:none"></div>
  `;
  cont.appendChild(tabs);

  const paneResumen=tabs.querySelector('#paneResumen');
  const paneItin=tabs.querySelector('#paneItin');

  tabs.querySelector('#tabResumen').onclick=()=>{ paneResumen.style.display=''; paneItin.style.display='none'; };
  tabs.querySelector('#tabItin').onclick   =()=>{ paneResumen.style.display='none'; paneItin.style.display=''; };

  renderResumen(g, paneResumen);
  renderItinerario(g, paneItin, preferDate);
}

/* ============== Resumen (Hotel + Vuelos) ============== */
async function renderResumen(g, pane){
  pane.innerHTML='<div class="loader">CARGANDO RESUMEN…</div>';

  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem';
  pane.innerHTML='';

  const hotelBox=document.createElement('div'); hotelBox.className='act';
  hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">BUSCANDO ASIGNACIÓN…</div>';
  wrap.appendChild(hotelBox);

  const vuelosBox=document.createElement('div'); vuelosBox.className='act';
  vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">BUSCANDO VUELOS…</div>';
  wrap.appendChild(vuelosBox);

  pane.appendChild(wrap);

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
        const ida = dmy(toISO(v.fechaIda));
        const vuelta = dmy(toISO(v.fechaVuelta));
        const linea = `${v.origen||''} — ${v.destino||''} ${ida?(' · '+ida):''}${vuelta?(' — '+vuelta):''}`;
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
  }catch(e){ /* índice faltante */ }
  if (!found.length){
    const ss=await getDocs(collection(db,'vuelos'));
    ss.forEach(d=>{ const v=d.data()||{}; const arr=Array.isArray(v.grupos)?v.grupos:[]; if (arr.some(x=> String(x?.id||'')===String(key))) found.push({ id:d.id, ...v }); });
  }
  found.sort((a,b)=> (toISO(a.fechaIda)||'').localeCompare(toISO(b.fechaIda)||'')); state.cache.vuelos.set(key, found); return found;
}

/* ============== Itinerario + asistencia + bitácora ============== */
function renderItinerario(g, pane, preferDate){
  pane.innerHTML='';
  const fechas = rangoFechas(g.fechaInicio, g.fechaFin);
  if (!fechas.length){ pane.innerHTML='<div class="muted">FECHAS NO DEFINIDAS.</div>'; return; }

  const pillsWrap=document.createElement('div'); pillsWrap.className='date-pills'; pane.appendChild(pillsWrap);
  const actsWrap=document.createElement('div'); actsWrap.className='acts'; pane.appendChild(actsWrap);

  const hoy=toISO(new Date());
  let startDate = preferDate || ( (hoy>=fechas[0] && hoy<=fechas.at(-1)) ? hoy : fechas[0] );

  fechas.forEach(f=>{
    const pill=document.createElement('div'); pill.className='pill'+(f===startDate?' active':''); pill.textContent=dmy(f); pill.title=f; pill.dataset.fecha=f;
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

    const horaIni = act.horaInicio || '--:--';
    const horaFin = act.horaFin    || '--:--';
    const paxFinalInit = (saved?.paxFinal ?? '');
    const notasInit    = (saved?.notas ?? '');
    const actName = act.actividad || 'ACTIVIDAD';
    const actKey  = slug(actName);

    const div=document.createElement('div'); div.className='act';
    div.innerHTML = `
      <h4>${actName}</h4>
      <div class="meta">${horaIni}–${horaFin} · PLAN: <strong>${plan}</strong> PAX</div>
      <div class="row">
        <input type="number" min="0" inputmode="numeric" placeholder="ASISTENTES"/>
        <textarea placeholder="NOTAS (OPCIONAL)"></textarea>
        <button class="btnSave">GUARDAR</button>
      </div>
      <div class="bitacora" style="margin-top:.5rem">
        <div class="muted" style="margin-bottom:.25rem">BITÁCORA</div>
        <div class="bitItems" style="display:grid;gap:.35rem"></div>
        <div class="row" style="margin-top:.35rem">
          <input class="bitInput" type="text" placeholder="AÑADIR NOTA..."/>
          <button class="bitAdd">AGREGAR</button>
        </div>
      </div>
    `;
    cont.appendChild(div);

    div.querySelector('input').value = paxFinalInit;
    div.querySelector('textarea').value = notasInit;

    // Guardar asistencia
    div.querySelector('.btnSave').onclick = async ()=>{
      const btn = div.querySelector('.btnSave'); btn.disabled=true;
      try{
        const refGrupo = doc(db,'grupos', grupo.id);
        const keyPath = `asistencias.${fechaISO}.${actKey}`;
        const data = {
          paxFinal: Number(div.querySelector('input').value || 0),
          notas: String(div.querySelector('textarea').value || ''),
          byUid: auth.currentUser.uid,
          byEmail: String(auth.currentUser.email||'').toLowerCase(),
          updatedAt: serverTimestamp()
        };
        const payload={}; payload[keyPath]=data;
        await updateDoc(refGrupo, payload);
        setSavedAsistenciaLocal(grupo, fechaISO, actName, { ...data });
        btn.textContent='GUARDADO'; setTimeout(()=>{ btn.textContent='GUARDAR'; btn.disabled=false; }, 900);
      }catch(e){ console.error(e); btn.disabled=false; alert('NO SE PUDO GUARDAR LA ASISTENCIA.'); }
    };

    // Bitácora
    const itemsWrap = div.querySelector('.bitItems');
    await loadBitacora(grupo.id, fechaISO, actKey, itemsWrap);

    div.querySelector('.bitAdd').onclick = async ()=>{
      const inp = div.querySelector('.bitInput');
      const texto = (inp.value||'').trim(); if (!texto) return;
      try{
        const coll = collection(db, 'grupos', grupo.id, 'bitacora', `${fechaISO}-${actKey}`, 'items');
        await addDoc(coll, { texto, byUid: auth.currentUser.uid, byEmail: String(auth.currentUser.email||'').toLowerCase(), ts: serverTimestamp() });
        inp.value=''; await loadBitacora(grupo.id, fechaISO, actKey, itemsWrap);
      }catch(e){ console.error(e); alert('NO SE PUDO GUARDAR LA NOTA.'); }
    };
  }
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
      div.textContent = `• ${x.texto||''} — ${quien}${hora?(' · '+hora):''}`; frag.appendChild(div);
    });
    wrap.innerHTML=''; wrap.appendChild(frag);
    if (!qs.size) wrap.innerHTML='<div class="muted">AÚN NO HAY NOTAS.</div>';
  }catch(e){ console.error(e); wrap.innerHTML='<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>'; }
}
