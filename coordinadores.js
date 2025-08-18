/* coordinadores.js — Portal Coordinadores RT (modo 1-viaje-a-la-vez)
   - Orden por cercanía a hoy, navegación Prev/Sig y deep link ?g=&f=
   - Resumen: Hotel (hotelAssignments + hoteles) y Vuelos (vuelos.grupoIds)
   - Itinerario: asistencia + bitácora por actividad + ficha Servicio + Voucher
   - Robusto a cambios de esquema; IDs CSS-safe; sin "||" en templates
*/

import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, addDoc,
  serverTimestamp, query, where, orderBy, limit
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* ============== Auth + salir ============== */
const auth = getAuth(app);
const logoutBtn = document.getElementById('logout');
if (logoutBtn) logoutBtn.onclick = () => signOut(auth).then(() => (location = 'index.html'));

/* ============== Staff permitido ============== */
const STAFF_EMAILS = new Set([
  'aleoperaciones@raitrai.cl','tomas@raitrai.cl','operaciones@raitrai.cl',
  'anamaria@raitrai.cl','sistemas@raitrai.cl',
].map(e => e.toLowerCase()));

/* ============== Utils ============== */
const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');
const slug = (s) => norm(s).slice(0,60);
const cssSafeId = (prefix, raw) => prefix + '_' + String(raw==null?'':raw).replace(/[^A-Za-z0-9_-]/g,'_');

function toISO(x){
  if(!x) return '';
  if (typeof x==='string'){ if (/^\d{4}-\d{2}-\d{2}$/.test(x)) return x; const d=new Date(x); return isNaN(d)?'':d.toISOString().slice(0,10); }
  if (x && typeof x==='object' && 'seconds' in x) return new Date(x.seconds*1000).toISOString().slice(0,10);
  if (x instanceof Date) return x.toISOString().slice(0,10);
  return '';
}
function fmt(iso){ if(!iso) return ''; const d=new Date(iso+'T00:00:00'); return d.toLocaleDateString('es-CL',{weekday:'short',day:'2-digit',month:'short'}); }
function rangoFechas(ini, fin){ const out=[]; const A=toISO(ini), B=toISO(fin); if(!A||!B) return out; const a=new Date(A+'T00:00:00'), b=new Date(B+'T00:00:00'); for(let d=new Date(a); d<=b; d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10)); return out; }
function parseQS(){ const p=new URLSearchParams(location.search); return { g: p.get('g')||'', f: p.get('f')||'' }; }

/* ===== itinerario tolerante (objeto o array legado) ===== */
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){
    const map={}; for(const item of raw){ const f=toISO(item && item.fecha); if(!f) continue; if(!map[f]) map[f]=[]; map[f].push({...item}); }
    return map;
  }
  return raw;
}

/* ===== extracción tolerante de campos de grupos ===== */
const arrify = v => Array.isArray(v) ? v : (v && typeof v==='object' ? Object.values(v) : (v ? [v] : []));
function emailsOf(g){ const out=new Set(), push=e=>{if(e) out.add(String(e).toLowerCase());}; push(g?.coordinadorEmail); push(g?.coordinador?.email); arrify(g?.coordinadoresEmails).forEach(push); if(g?.coordinadoresEmailsObj) Object.keys(g.coordinadoresEmailsObj).forEach(push); arrify(g?.coordinadores).forEach(x=>{ if (x?.email) push(x.email); else if (typeof x==='string'&&x.includes('@')) push(x); }); return [...out]; }
function uidsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); }; push(g?.coordinadorUid||g?.coordinadorId); if (g?.coordinador?.uid) push(g.coordinador.uid); arrify(g?.coordinadoresUids||g?.coordinadoresIds||g?.coordinadores).forEach(x=>{ if (x?.uid) push(x.uid); else push(x); }); return [...out]; }
function coordDocIdsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); }; push(g?.coordinadorId); arrify(g?.coordinadoresIds).forEach(push); return [...out]; }
function nombresOf(g){ const out=new Set(), push=s=>{ if(s) out.add(norm(String(s))); }; push(g?.coordinadorNombre||g?.coordinador); if (g?.coordinador?.nombre) push(g.coordinador.nombre); arrify(g?.coordinadoresNombres).forEach(push); return [...out]; }

/* ===== asistencia helpers ===== */
function getSavedAsistencia(grupo, fechaISO, actividad){
  const byDate = grupo?.asistencias?.[fechaISO]; if(!byDate) return null;
  const key = slug(actividad||'actividad'); if (Object.prototype.hasOwnProperty.call(byDate,key)) return byDate[key];
  for (const k of Object.keys(byDate)) if (slug(k)===key) return byDate[k]; return null;
}
function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data){ const key=slug(actividad||'actividad'); if(!grupo.asistencias) grupo.asistencias={}; if(!grupo.asistencias[fechaISO]) grupo.asistencias[fechaISO]={}; grupo.asistencias[fechaISO][key]=data; }
function calcPlan(actividad, grupo){ const a=actividad||{}; const ad=Number(a.adultos||0), es=Number(a.estudiantes||0); const suma=ad+es; if (suma>0) return suma; const base=(grupo && (grupo.cantidadgrupo!=null?grupo.cantidadgrupo:grupo.pax)); return Number(base||0); }

/* ===== estado global ===== */
const state = {
  user: null,
  coordinadores: [],
  grupos: [],
  ordenados: [],
  idx: 0,
  cache: { hotel: new Map(), vuelos: new Map(), servicios: new Map() }
};

/* ============== Arranque ============== */
onAuthStateChanged(auth, async (user) => {
  if (!user) { location.href='index.html'; return; }
  state.user = user;

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

/* ============== Leer coordinadores ============== */
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list=[]; snap.forEach(d=>{ const x=d.data()||{}; list.push({
    id:d.id, nombre:String(x.nombre||x.Nombre||x.coordinador||''), email:String(x.email||x.correo||x.mail||'').toLowerCase(), uid:String(x.uid||x.userId||'')
  });});
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre,'es',{sensitivity:'base'}));
  return list;
}

/* ============== Staff selector ============== */
async function showStaffSelector(coordinadores, user){
  const wrap=document.querySelector('.wrap');
  let bar=document.getElementById('staffBar');
  if(!bar){
    bar=document.createElement('div');
    bar.id='staffBar'; bar.style.cssText='margin:12px 0 8px; padding:8px; border:1px solid #223053; border-radius:12px; background:#0f1530;';
    bar.innerHTML='<label style="display:block; margin-bottom:6px; color:#cbd5e1">Ver viajes por coordinador</label><select id="coordSelect" style="width:100%; padding:.55rem; border-radius:10px; border:1px solid #334155; background:#0b1329; color:#e5e7eb"></select>';
    if (wrap) wrap.prepend(bar);
  }
  const sel=document.getElementById('coordSelect'); sel.textContent='';
  const opt0=document.createElement('option'); opt0.value=''; opt0.textContent='— Selecciona coordinador —'; sel.appendChild(opt0);
  coordinadores.forEach(c=>{ const o=document.createElement('option'); o.value=c.id; o.textContent=(c.nombre||'')+' — '+(c.email||'sin correo'); sel.appendChild(o); });
  sel.onchange=async()=>{ const id=sel.value; const elegido=coordinadores.find(c=>c.id===id)||null; localStorage.setItem('rt_staff_coord', id||''); await loadGruposForCoordinador(elegido, state.user); };
  const last=localStorage.getItem('rt_staff_coord'); if(last && coordinadores.find(c=>c.id===last)){ sel.value=last; const elegido=coordinadores.find(c=>c.id===last); await loadGruposForCoordinador(elegido, state.user); }
}

/* ============== Resolver coordinador usuario no-staff ============== */
function findCoordinadorForUser(coordinadores, user){
  const email=(user.email||'').toLowerCase(), uid=user.uid;
  let c = coordinadores.find(x=> x.email && x.email.toLowerCase()===email); if(c) return c;
  if (uid){ c=coordinadores.find(x=>x.uid && x.uid===uid); if(c) return c; }
  const disp=norm(user.displayName||''); if (disp){ c=coordinadores.find(x=> norm(x.nombre)===disp); if(c) return c; }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

/* ============== Carga de grupos para el coordinador ============== */
async function loadGruposForCoordinador(coord, user){
  const cont=document.getElementById('grupos'); if (cont) cont.textContent='Cargando grupos…';

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
    const match=(emailElegido && gEmails.includes(emailElegido)) || (uidElegido && gUids.includes(uidElegido)) || (docIdElegido && gDocIds.includes(docIdElegido)) || (nombreElegido && gNames.includes(nombreElegido)) || (isSelf && gUids.includes(user.uid));
    if (match) wanted.push(g);
  });

  // Fallback por conjuntos->viajes
  if (wanted.length===0 && coord && coord.id!=='self'){
    try{
      const qs=[]; if(uidElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorId','==',uidElegido)));
      if(emailElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorEmail','==',emailElegido)));
      if(docIdElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorDocId','==',docIdElegido)));
      const ids=new Set();
      for(const qy of qs){ const ss=await getDocs(qy); ss.forEach(docu=>{ const v=docu.data()?.viajes || []; v.forEach(id=>ids.add(String(id))); }); }
      for(const id of ids){ const ref=doc(db,'grupos',id); const dd=await getDoc(ref); if(dd.exists()){ const raw={id:dd.id, ...dd.data()}; wanted.push({
        ...raw,
        fechaInicio: toISO(raw.fechaInicio||raw.inicio||raw.fecha_ini),
        fechaFin: toISO(raw.fechaFin||raw.fin||raw.fecha_fin),
        itinerario: normalizeItinerario(raw.itinerario),
        asistencias: raw.asistencias || {},
        numeroNegocio: String(raw.numeroNegocio || raw.numNegocio || raw.idNegocio || raw.id || dd.id)
      });}}
    }catch(e){ console.warn('Fallback conjuntos no disponible:', e); }
  }

  // Ordenar por cercanía (próximos primero)
  const hoy = toISO(new Date());
  const futuros = wanted.filter(g => (g.fechaInicio||'') >= hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  const pasados = wanted.filter(g => (g.fechaInicio||'') < hoy).sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  state.grupos = wanted;
  state.ordenados = [...futuros, ...pasados];

  // Elegir índice inicial: ?g= o último o 0
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
  renderNavBar();
}

/* ============== Barra navegación (prev/sig + selector) ============== */
function renderNavBar(){
  const wrap=document.querySelector('.wrap'); if(!wrap) return;
  let nb=document.getElementById('navBar');
  if(!nb){
    nb=document.createElement('div'); nb.id='navBar';
    nb.style.cssText='margin:8px 0 12px; display:flex; gap:.5rem; align-items:center; flex-wrap:wrap;';
    nb.innerHTML='<button id="btnPrev">‹ Anterior</button><button id="btnNext">Siguiente ›</button><select id="allTrips" style="min-width:260px"></select>';
    wrap.prepend(nb);
    const styleBtn='background:#1f2937;color:#e5e7eb;border:1px solid #334155;border-radius:10px;padding:.45rem .8rem;cursor:pointer';
    nb.querySelector('#btnPrev').style.cssText=styleBtn;
    nb.querySelector('#btnNext').style.cssText=styleBtn;
    nb.querySelector('#allTrips').style.cssText='padding:.5rem;border-radius:10px;border:1px solid #334155;background:#0b1329;color:#e5e7eb';
  }
  const sel=nb.querySelector('#allTrips'); sel.textContent='';
  state.ordenados.forEach((g,i)=>{ const o=document.createElement('option'); const name=(g.nombreGrupo||g.aliasGrupo||g.id); const date=(g.fechaInicio||'')+(g.fechaFin?(' → '+g.fechaFin):''); o.value=String(i); o.textContent=`${name} — ${date}`; sel.appendChild(o); });
  sel.value=String(state.idx);
  nb.querySelector('#btnPrev').onclick=()=>{ if(state.idx>0){ state.idx--; renderOneGroup(state.ordenados[state.idx]); sel.value=String(state.idx);} };
  nb.querySelector('#btnNext').onclick=()=>{ if(state.idx<state.ordenados.length-1){ state.idx++; renderOneGroup(state.ordenados[state.idx]); sel.value=String(state.idx);} };
  sel.onchange=()=>{ state.idx=Number(sel.value||0); renderOneGroup(state.ordenados[state.idx]); };
}

/* ============== Vista 1 viaje ============== */
function renderOneGroup(g, preferDate){
  const cont=document.getElementById('grupos'); if(!cont) return;
  cont.innerHTML='';

  if(!g){ cont.innerHTML='<p class="muted">No hay viajes.</p>'; return; }
  localStorage.setItem('rt_last_group', g.id);

  // encabezado
  const titulo = (g.nombreGrupo!=null && g.nombreGrupo!=='') ? g.nombreGrupo : (g.aliasGrupo!=null && g.aliasGrupo!=='') ? g.aliasGrupo : g.id;
  const sub = String(g.destino||'') + ' · ' + String(g.programa||'') + ' · ' + (g.cantidadgrupo!=null ? g.cantidadgrupo : (g.pax!=null ? g.pax : 0)) + ' pax';
  const rango = (g.fechaInicio||'') + (g.fechaFin?(' — '+g.fechaFin):'');

  const header=document.createElement('div');
  header.className='group-card';
  header.innerHTML =
    '<h3 style="margin:.1rem 0">'+ titulo +'</h3>'+
    '<div class="group-sub">'+ sub +'</div>'+
    '<div class="muted" style="margin-top:4px">'+ rango +'</div>';
  cont.appendChild(header);

  // tabs
  const tabs=document.createElement('div'); tabs.style.cssText='margin-top:10px';
  tabs.innerHTML =
    '<div id="tabs" style="display:flex; gap:.5rem; margin-bottom:.6rem">'+
      '<button id="tabResumen">Resumen</button>'+
      '<button id="tabItin">Itinerario</button>'+
    '</div>'+
    '<div id="paneResumen"></div>'+
    '<div id="paneItin" style="display:none"></div>';
  cont.appendChild(tabs);

  const styleTab='background:#0b1329;color:#e5e7eb;border:1px solid #334155;border-radius:10px;padding:.45rem .8rem;cursor:pointer';
  tabs.querySelector('#tabResumen').style.cssText=styleTab;
  tabs.querySelector('#tabItin').style.cssText=styleTab;

  const paneResumen=tabs.querySelector('#paneResumen');
  const paneItin=tabs.querySelector('#paneItin');

  tabs.querySelector('#tabResumen').onclick=()=>{ paneResumen.style.display=''; paneItin.style.display='none'; };
  tabs.querySelector('#tabItin').onclick=()=>{ paneResumen.style.display='none'; paneItin.style.display=''; };

  renderResumen(g, paneResumen);
  renderItinerario(g, paneItin, preferDate);
}

/* ============== Resumen (Hotel + Vuelos) ============== */
async function renderResumen(g, pane){
  pane.innerHTML='<div class="loader">Cargando resumen…</div>';

  const wrap=document.createElement('div'); wrap.style.cssText='display:grid; gap:.8rem';
  pane.innerHTML='';

  // Hotel
  const hotelBox=document.createElement('div'); hotelBox.className='act';
  hotelBox.innerHTML='<h4>Hotel</h4><div class="muted">Buscando asignación…</div>';
  wrap.appendChild(hotelBox);

  // Vuelos
  const vuelosBox=document.createElement('div'); vuelosBox.className='act';
  vuelosBox.innerHTML='<h4>Transporte / Vuelos</h4><div class="muted">Buscando vuelos…</div>';
  wrap.appendChild(vuelosBox);

  pane.appendChild(wrap);

  // ---- HOTEL ----
  try{
    const h = await loadHotelInfo(g);
    if (!h){ hotelBox.innerHTML='<h4>Hotel</h4><div class="muted">Sin asignación de hotel.</div>'; }
    else{
      const nombre = h.hotelNombre || h.hotel?.nombre || 'Hotel asignado';
      const dir = h.hotel?.direccion || '';
      const contacto = [h.hotel?.contactoNombre, h.hotel?.contactoTelefono, h.hotel?.contactoCorreo].filter(Boolean).join(' · ');
      const fechas = (h.checkIn||'') + (h.checkOut?(' → '+h.checkOut):'');
      hotelBox.innerHTML =
        '<h4>'+nombre+'</h4>'+
        '<div class="meta">'+ (h.destino||'') +'</div>'+
        '<div class="prov">'+ (dir||'') +'</div>'+
        '<div class="meta">Check-in/out: '+ fechas +'</div>'+
        (contacto ? ('<div class="meta">'+ contacto +'</div>') : '');
    }
  }catch(e){ hotelBox.innerHTML='<h4>Hotel</h4><div class="muted">No fue posible cargar.</div>'; console.error(e); }

  // ---- VUELOS ----
  try{
    const vuelos = await loadVuelosInfo(g);
    if (!vuelos.length){ vuelosBox.innerHTML='<h4>Transporte / Vuelos</h4><div class="muted">Sin vuelos registrados.</div>'; }
    else{
      const list=document.createElement('div');
      vuelos.forEach(v=>{
        const titulo = (v.numero ? ('#'+v.numero+' — ') : '') + (v.proveedor || '');
        const ida = toISO(v.fechaIda), vuelta = toISO(v.fechaVuelta);
        const linea = (v.origen||'')+' → '+(v.destino||'') + (ida?(' · ' + ida):'') + (vuelta?(' → '+vuelta):'');
        const tip = v.tipoVuelo ? (' ('+v.tipoVuelo+')') : '';
        const item=document.createElement('div');
        item.className='meta';
        item.textContent = titulo + ' · ' + linea + tip;
        list.appendChild(item);
      });
      vuelosBox.innerHTML='<h4>Transporte / Vuelos</h4>';
      vuelosBox.appendChild(list);
    }
  }catch(e){ vuelosBox.innerHTML='<h4>Transporte / Vuelos</h4><div class="muted">No fue posible cargar.</div>'; console.error(e); }
}

async function loadHotelInfo(g){
  const key = g.numeroNegocio;
  if (state.cache.hotel.has(key)) return state.cache.hotel.get(key);

  // buscar asignaciones por grupoId == numeroNegocio
  const qs = await getDocs(query(collection(db,'hotelAssignments'), where('grupoId','==', String(key))));
  if (qs.empty){ state.cache.hotel.set(key, null); return null; }

  // elegir mejor asignación (que cruce rango; si hay varias, la de checkIn más cercana)
  const rango = { ini: toISO(g.fechaInicio), fin: toISO(g.fechaFin) };
  let elegido=null, score=1e15;
  qs.forEach(d=>{
    const x=d.data()||{};
    const ci = toISO(x.checkIn), co = toISO(x.checkOut);
    let s=5e14;
    if (ci && co && rango.ini && rango.fin){
      const overlap = !(co < rango.ini || ci > rango.fin);
      s = overlap ? 0 : Math.abs(new Date(ci) - new Date(rango.ini));
    }
    if (s < score){ score=s; elegido={ id:d.id, ...x }; }
  });

  // traer ficha de hotel si hay id
  let hotelDoc=null;
  if (elegido?.hotelId){
    const hd = await getDoc(doc(db,'hoteles', String(elegido.hotelId)));
    if (hd.exists()) hotelDoc = { id: hd.id, ...hd.data() };
  }
  const out = { ...elegido, hotel: hotelDoc, hotelNombre: elegido?.nombre || elegido?.hotelNombre || hotelDoc?.nombre || '', destino: hotelDoc?.destino || '' };
  state.cache.hotel.set(key, out);
  return out;
}

async function loadVuelosInfo(g){
  const key = g.numeroNegocio;
  if (state.cache.vuelos.has(key)) return state.cache.vuelos.get(key);

  // primero: query eficiente por grupoIds
  let found=[];
  try{
    const qs = await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains', String(key))));
    qs.forEach(d=> found.push({ id:d.id, ...d.data() }));
  }catch(e){ console.warn('Query vuelos por grupoIds falló/índice:', e); }

  // fallback: escanear y filtrar por grupos[].id
  if (!found.length){
    const ss = await getDocs(collection(db,'vuelos'));
    ss.forEach(d=>{
      const v=d.data()||{}; const arr=Array.isArray(v.grupos)?v.grupos:[];
      const hit = arr.some(x=> String(x?.id||'')===String(key));
      if (hit) found.push({ id:d.id, ...v });
    });
  }

  // orden simple por fechaIda asc
  found.sort((a,b)=> (toISO(a.fechaIda)||'').localeCompare(toISO(b.fechaIda)||''));
  state.cache.vuelos.set(key, found);
  return found;
}

/* ============== Itinerario + asistencia + bitácora + servicios ============== */
function renderItinerario(g, pane, preferDate){
  pane.innerHTML='';
  const fechas = rangoFechas(g.fechaInicio, g.fechaFin);
  if (!fechas.length){ pane.innerHTML='<div class="muted">Fechas no definidas.</div>'; return; }

  // pills de fechas
  const pillsWrap=document.createElement('div'); pillsWrap.className='date-pills';
  pane.appendChild(pillsWrap);

  const actsWrap=document.createElement('div'); actsWrap.className='acts'; pane.appendChild(actsWrap);

  const hoy=toISO(new Date());
  let startDate = preferDate || ( (hoy>=fechas[0] && hoy<=fechas[fechas.length-1]) ? hoy : fechas[0] );

  fechas.forEach((f)=>{
    const pill=document.createElement('div');
    pill.className='pill'+(f===startDate?' active':''); pill.textContent=fmt(f); pill.title=f; pill.dataset.fecha=f;
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
  if (!acts.length){ cont.innerHTML='<div class="muted">Sin actividades para este día.</div>'; return; }

  for (const act of acts){
    const plan = calcPlan(act, grupo);
    const saved = getSavedAsistencia(grupo, fechaISO, act.actividad);

    const horaIni = (act.horaInicio!=null && act.horaInicio!=='') ? act.horaInicio : '--:--';
    const horaFin = (act.horaFin!=null && act.horaFin!=='') ? act.horaFin : '--:--';
    const paxFinalInit = (saved && saved.paxFinal!=null) ? saved.paxFinal : '';
    const notasInit = (saved && saved.notas) ? saved.notas : '';
    const actName = (act.actividad!=null && act.actividad!=='') ? act.actividad : 'Actividad';
    const actKey = slug(actName);

    const div=document.createElement('div'); div.className='act';
    div.innerHTML =
      '<h4>'+ actName +'</h4>'+
      '<div class="meta">'+ horaIni +'–'+ horaFin +' · Plan: <strong>'+ plan +'</strong> pax</div>'+
      '<div class="row">'+
        '<input type="number" min="0" inputmode="numeric" placeholder="Asistentes" />'+
        '<textarea placeholder="Notas (opcional)"></textarea>'+
        '<button class="btnSave">Guardar</button>'+
        '<button class="btnServicio">Ficha</button>'+
        '<button class="btnVoucher" style="display:none">Voucher</button>'+
      '</div>'+
      '<div class="bitacora" style="margin-top:.5rem">'+
        '<div class="muted" style="margin-bottom:.25rem">Bitácora</div>'+
        '<div class="bitItems" style="display:grid; gap:.35rem"></div>'+
        '<div class="row" style="margin-top:.35rem">'+
          '<input class="bitInput" type="text" placeholder="Añadir nota..."/>'+
          '<button class="bitAdd">Agregar</button>'+
        '</div>'+
      '</div>';
    cont.appendChild(div);

    // set initial values
    div.querySelector('input').value = paxFinalInit;
    div.querySelector('textarea').value = notasInit;

    // --- Guardar asistencia ---
    div.querySelector('.btnSave').onclick = async ()=>{
      const btn = div.querySelector('.btnSave'); btn.disabled=true;
      try{
        const refGrupo = doc(db,'grupos', grupo.id);
        const keyPath = 'asistencias.'+fechaISO+'.'+actKey;
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
        btn.textContent='Guardado'; setTimeout(()=>{ btn.textContent='Guardar'; btn.disabled=false; }, 900);
      }catch(e){ console.error(e); btn.disabled=false; alert('No se pudo guardar la asistencia.'); }
    };

    // --- Bitácora: cargar y agregar ---
    const itemsWrap = div.querySelector('.bitItems');
    await loadBitacora(grupo.id, fechaISO, actKey, itemsWrap);

    div.querySelector('.bitAdd').onclick = async ()=>{
      const inp = div.querySelector('.bitInput');
      const texto = (inp.value||'').trim(); if (!texto) return;
      try{
        const coll = collection(db, 'grupos', grupo.id, 'bitacora', `${fechaISO}-${actKey}`, 'items');
        await addDoc(coll, {
          texto, byUid: auth.currentUser.uid, byEmail: String(auth.currentUser.email||'').toLowerCase(), ts: serverTimestamp()
        });
        inp.value=''; await loadBitacora(grupo.id, fechaISO, actKey, itemsWrap);
      }catch(e){ console.error(e); alert('No se pudo guardar la nota.'); }
    };

    // --- Ficha Servicio + Voucher ---
    const btnServicio = div.querySelector('.btnServicio');
    const btnVoucher  = div.querySelector('.btnVoucher');

    btnServicio.onclick = async ()=>{
      const sid = String(act.servicioId || act.servicioDocId || '').trim();
      if (!sid){ openModal('<div class="muted">Sin ficha de servicio asociada a esta actividad.</div>'); return; }
      try{
        const sdoc = await getDoc(doc(db,'Servicios', sid));
        if (!sdoc.exists()){ openModal('<div class="muted">Servicio no encontrado.</div>'); return; }
        const s = { id:sdoc.id, ...sdoc.data() };
        const nombre = String(s.nombre || actName);
        const contacto = [s.contactoNombre, s.contactoTelefono, s.contactoCorreo].filter(Boolean).join(' · ');
        const indic = String(s.indicaciones || s.instrucciones || '');
        const forma = String(s.formaPago || s.formaDePago || '');
        const ciudad = String(s.ciudad || '');
        const voucherTipo = String(s.voucherTipo || '').toLowerCase();
        const voucherUrl = s.voucherUrl || s.voucherURL || '';

        const html =
          '<h3 style="margin-top:0">'+nombre+'</h3>'+
          (ciudad?('<div class="meta">Ciudad: '+ciudad+'</div>'):'')+
          (contacto?('<div class="meta">Contacto: '+contacto+'</div>'):'')+
          (indic?('<div class="prov">Indicaciones: '+indic+'</div>'):'')+
          (forma?('<div class="meta">Forma de pago: '+forma+'</div>'):'')+
          ((voucherTipo==='fisico' || voucherTipo==='físico' || voucherTipo==='electronico' || voucherTipo==='electrónico') ?
            ('<div class="meta">Voucher: '+(s.voucherTipo||'')+'</div>' + (voucherUrl?('<div class="meta"><a href="'+voucherUrl+'" target="_blank" rel="noopener">Abrir voucher</a></div>'):''))
            : '<div class="meta">Voucher: No aplica</div>');
        openModal(html);
      }catch(e){ console.error(e); openModal('<div class="muted">Error al cargar el servicio.</div>'); }
    };

    // Mostrar botón Voucher si el servicio viene en el item (sin modal)
    (async ()=>{
      try{
        const sid = String(act.servicioId || act.servicioDocId || '').trim();
        if (!sid){ btnVoucher.style.display='none'; return; }
        const sdoc = await getDoc(doc(db,'Servicios', sid));
        if (!sdoc.exists()){ btnVoucher.style.display='none'; return; }
        const s=sdoc.data()||{};
        const t=String(s.voucherTipo||'').toLowerCase();
        if (t==='fisico'||t==='físico'||t==='electronico'||t==='electrónico'){
          btnVoucher.style.display='';
          btnVoucher.onclick=()=>{ const url=s.voucherUrl||s.voucherURL||''; if(url) window.open(url,'_blank'); else openModal('<div class="muted">Voucher sin URL. Revise indicaciones del servicio.</div>'); };
        }
      }catch(e){ btnVoucher.style.display='none'; }
    })();
  }
}

async function loadBitacora(grupoId, fechaISO, actKey, wrap){
  wrap.innerHTML='<div class="muted">Cargando…</div>';
  try{
    const coll = collection(db, 'grupos', grupoId, 'bitacora', `${fechaISO}-${actKey}`, 'items');
    const qs = await getDocs(query(coll, orderBy('ts','desc'), limit(50)));
    const frag=document.createDocumentFragment();
    qs.forEach(d=>{
      const x=d.data()||{};
      const quien = String(x.byEmail || x.byUid || 'usuario');
      const cuando = x.ts?.seconds ? new Date(x.ts.seconds*1000) : null;
      const hora = cuando ? cuando.toLocaleString('es-CL') : '';
      const div=document.createElement('div');
      div.className='meta';
      div.textContent = '• ' + (x.texto || '') + ' — ' + quien + (hora?(' · '+hora):'');
      frag.appendChild(div);
    });
    wrap.innerHTML=''; wrap.appendChild(frag);
    if (!qs.size) wrap.innerHTML='<div class="muted">Aún no hay notas.</div>';
  }catch(e){ console.error(e); wrap.innerHTML='<div class="muted">No se pudo cargar la bitácora.</div>'; }
}

/* ============== Modal simple ============== */
function openModal(html){
  let m=document.getElementById('rtModal');
  if(!m){
    m=document.createElement('div'); m.id='rtModal';
    m.style.cssText='position:fixed; inset:0; background:rgba(0,0,0,.55); display:flex; align-items:center; justify-content:center; z-index:9999';
    m.innerHTML='<div id="rtBox" style="max-width:720px; background:#0f1530; border:1px solid #223053; border-radius:14px; padding:16px"></div>';
    m.onclick=(e)=>{ if(e.target===m) m.remove(); };
    document.body.appendChild(m);
  }
  const box=m.querySelector('#rtBox'); box.innerHTML=html;
  m.style.display='flex';
}
