/* coordinadores.js — Portal Coordinadores RT (v3+ UI)
   - staffBar → navPanel → statsPanel → gruposPanel
   - Botones prev/next/print/new antes de #allTrips
   - #searchTrips ahora está dentro del grupo (búsqueda interna)
   - Se elimina .group-sub (era redundante)
   - Selector STAFF con escritura + “TODOS LOS COORDINADORES”
   - Resaltado de matches y contadores por pestaña
*/

import { app, db, auth, storage } from './firebase-init-portal.js';
import { onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, addDoc,
  serverTimestamp, query, where, orderBy, limit
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';
import {
  ref as sRef, uploadBytes, getDownloadURL
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-storage.js';

/* ============== Logout ============== */
const logoutBtn = document.getElementById('logout');
if (logoutBtn) logoutBtn.onclick = () => signOut(auth).then(() => (location = 'index.html'));

/* ============== Staff permitido ============== */
const STAFF_EMAILS = new Set(
  ['aleoperaciones@raitrai.cl','tomas@raitrai.cl','operaciones@raitrai.cl','anamaria@raitrai.cl','sistemas@raitrai.cl']
  .map(e=>e.toLowerCase())
);

/* ============== Utils ============== */
const norm = (s='') => s.toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().replace(/[^a-z0-9]+/g,'');
const slug = s => norm(s).slice(0,60);
const toISO=(x)=>{ if(!x) return '';
  if (typeof x==='string'){ if(/^\d{4}-\d{2}-\d{2}$/.test(x)) return x; const d=new Date(x); return isNaN(d)?'':d.toISOString().slice(0,10); }
  if (x && typeof x==='object' && 'seconds' in x) return new Date(x.seconds*1000).toISOString().slice(0,10);
  if (x instanceof Date) return x.toISOString().slice(0,10);
  return '';
};
const dmy=(iso)=>{ const m=/^(\d{4})-(\d{2})-(\d{2})$/.exec(iso||''); return m?`${m[3]}-${m[2]}-${m[1]}`:''; };
const ymdFromDMY=(s)=>{ const t=(s||'').trim(); if(/^\d{2}-\d{2}-\d{4}$/.test(t)){ const [dd,mm,yy]=t.split('-'); return `${yy}-${mm}-${dd}`;} return ''; };
const daysInclusive=(ini,fin)=>{ const a=toISO(ini), b=toISO(fin); if(!a||!b) return 0; return Math.max(1,Math.round((new Date(b)-new Date(a))/86400000)+1); };
const rangoFechas=(ini,fin)=>{ const out=[]; const A=toISO(ini), B=toISO(fin); if(!A||!B) return out;
  for(let d=new Date(A+'T00:00:00'); d<=new Date(B+'T00:00:00'); d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10)); return out; };
const parseQS=()=>{ const p=new URLSearchParams(location.search); return { g:p.get('g')||'', f:p.get('f')||'' }; };

/* ===== Highlight + helpers ===== */
function htmlEsc(s){ return String(s??'').replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
function _indexForHighlight(str){
  let normed='', map=[];
  for (let i=0;i<str.length;i++){
    const n = str[i].normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase();
    normed += n; for (let j=0;j<n.length;j++) map.push(i);
  }
  return { normed, map };
}
function highlight(s, q){
  const text = String(s??''); const qn = norm(q||''); if(!qn) return htmlEsc(text);
  const {normed,map} = _indexForHighlight(text); let i=0, ranges=[];
  while (true){
    const pos = normed.indexOf(qn, i);
    if (pos === -1) break;
    const a = map[pos], b = map[Math.min(pos+qn.length-1, map.length-1)] + 1;
    if (ranges.length && a <= ranges[ranges.length-1][1]) ranges[ranges.length-1][1] = Math.max(b, ranges[ranges.length-1][1]);
    else ranges.push([a,b]);
    i = pos + Math.max(qn.length,1);
  }
  if (!ranges.length) return htmlEsc(text);
  let out='', last=0;
  for (const [a,b] of ranges){
    out += htmlEsc(text.slice(last,a)) + '<span class="hl">' + htmlEsc(text.slice(a,b)) + '</span>';
    last=b;
  }
  return out + htmlEsc(text.slice(last));
}
function setTabCount(which, n){
  const el = document.querySelector({resumen:'#tabResumen', itin:'#tabItin', gastos:'#tabGastos'}[which]);
  if(!el) return;
  const base = {resumen:'RESUMEN', itin:'ITINERARIO', gastos:'GASTOS'}[which];
  const q = (state.groupQ||'').trim();
  el.textContent = q ? `${base} (${Number(n)||0})` : base;
}

/* Itinerario tolerante */
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){ const map={}; for(const item of raw){ const f=toISO(item && item.fecha); if(!f) continue; (map[f] ||= []).push({...item}); } return map; }
  return raw;
}

/* extracción tolerante desde grupos */
const arrify=v=>Array.isArray(v)?v:(v&&typeof v==='object'?Object.values(v):(v?[v]:[]));
function emailsOf(g){ const out=new Set(), push=e=>{if(e) out.add(String(e).toLowerCase());};
  push(g?.coordinadorEmail); push(g?.coordinador?.email); arrify(g?.coordinadoresEmails).forEach(push);
  if(g?.coordinadoresEmailsObj) Object.keys(g.coordinadoresEmailsObj).forEach(push);
  arrify(g?.coordinadores).forEach(x=>{ if(x?.email) push(x.email); else if(typeof x==='string'&&x.includes('@')) push(x); });
  return [...out];
}
function uidsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); };
  push(g?.coordinadorUid||g?.coordinadorId); if(g?.coordinador?.uid) push(g.coordinador.uid);
  arrify(g?.coordinadoresUids||g?.coordinadoresIds||g?.coordinadores).forEach(x=>{ if (x?.uid) push(x.uid); else push(x); });
  return [...out];
}
function coordDocIdsOf(g){ const out=new Set(), push=x=>{ if(x) out.add(String(x)); }; push(g?.coordinadorId); arrify(g?.coordinadoresIds).forEach(push); return [...out]; }
function nombresOf(g){ const out=new Set(), push=s=>{ if(s) out.add(norm(String(s))); }; push(g?.coordinadorNombre||g?.coordinador); if(g?.coordinador?.nombre) push(g.coordinador.nombre); arrify(g?.coordinadoresNombres).forEach(push); return [...out]; }

const paxOf = g => Number(g?.cantidadgrupo ?? g?.pax ?? 0);

/* ====== Estado ====== */
const state = {
  user:null,
  isStaff:false,
  coordId:null,                 // selección staff actual; null = todos
  coordinadores:[],
  grupos:[],
  ordenados:[],
  idx:0,
  filter:{ type:'all', value:null }, // solo 'all' ahora
  groupQ:'',                        // búsqueda interna del grupo
  cache:{ hotel:new Map(), vuelos:new Map(), tasas:null }
};

/* ====== Helpers UI ====== */
function ensurePanel(id, html=''){
  let p=document.getElementById(id);
  if(!p){ p=document.createElement('div'); p.id=id; p.className='panel'; document.querySelector('.wrap').prepend(p); }
  if(html) p.innerHTML=html;
  enforceOrder();
  return p;
}
function enforceOrder(){
  const wrap=document.querySelector('.wrap');
  ['staffBar','navPanel','statsPanel','gruposPanel'].forEach(id=>{
    const node=document.getElementById(id); if(node) wrap.appendChild(node);
  });
}

/* ============== Arranque ============== */
onAuthStateChanged(auth, async (user) => {
  if (!user) { location.href='index.html'; return; }
  state.user = user; state.isStaff = STAFF_EMAILS.has((user.email||'').toLowerCase());

  ensurePanel('gruposPanel');

  const coordinadores = await loadCoordinadores(); state.coordinadores = coordinadores;

  if (state.isStaff) { await showStaffSelector(coordinadores); }
  else {
    const mine = findCoordinadorForUser(coordinadores, user);
    await loadGruposForCoordinador(mine, user);
  }

  // mostrar u ocultar botones staff
  const pv = document.getElementById('btnPrintVch');
  const na = document.getElementById('btnNewAlert');
  if(pv) pv.style.display = state.isStaff ? '' : 'none';
  if(na) na.style.display = state.isStaff ? '' : 'none';
});

/* ============== Firestore loads ============== */
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list=[]; snap.forEach(d=>{ const x=d.data()||{}; list.push({
    id:d.id, nombre:String(x.nombre||x.Nombre||x.coordinador||''), email:String(x.email||x.correo||x.mail||'').toLowerCase(), uid:String(x.uid||x.userId||'')
  });});
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre,'es',{sensitivity:'base'}));
  const seen=new Set(), dedup=[]; for(const c of list){ const k=(c.nombre+'|'+c.email).toLowerCase(); if(!seen.has(k)){ seen.add(k); dedup.push(c); } }
  return dedup;
}
function findCoordinadorForUser(coordinadores, user){
  const email=(user.email||'').toLowerCase(), uid=user.uid;
  let c = coordinadores.find(x=> x.email && x.email.toLowerCase()===email); if(c) return c;
  if (uid){ c=coordinadores.find(x=>x.uid && x.uid===uid); if(c) return c; }
  const disp=norm(user.displayName||''); if (disp){ c=coordinadores.find(x=> norm(x.nombre)===disp); if(c) return c; }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

/* ============== Selector Staff con escritura + TODOS ============== */
async function showStaffSelector(coordinadores){
  const bar = ensurePanel(
    'staffBar',
    '<label style="display:block;margin-bottom:6px;color:#cbd5e1">VER VIAJES POR COORDINADOR</label>' +
    '<input id="coordInput" list="coordList" placeholder="ESCRIBE UN COORDINADOR" style="width:100%"/>' +
    '<datalist id="coordList"></datalist>'
  );

  const input = bar.querySelector('#coordInput');
  const list  = bar.querySelector('#coordList');

  list.innerHTML =
    `<option value="TODOS LOS COORDINADORES" data-id="__ALL__"></option>` +
    coordinadores.map(c => `<option data-id="${c.id}" value="${(c.nombre||'')} — ${(c.email||'SIN CORREO')}"></option>`).join('');

  function chooseById(id){
    state.coordId = id && id !== '__ALL__' ? id : null;
    const elegido = id==='__ALL__' ? { id:'__ALL__' } : coordinadores.find(c => c.id===id) || null;
    localStorage.setItem('rt_staff_coord', id || '');
    loadGruposForCoordinador(elegido, state.user);
  }
  function tryPickFromInput(){
    const val = (input.value||'').trim();
    const opt = Array.from(list.options).find(o => o.value.toLowerCase() === val.toLowerCase());
    if (opt){ chooseById(opt.dataset.id); return true; }
    return false;
  }

  input.addEventListener('change', tryPickFromInput);
  input.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ e.preventDefault(); tryPickFromInput(); } });

  const last = localStorage.getItem('rt_staff_coord') || '__ALL__';
  const optLast = Array.from(list.options).find(o=>o.dataset.id===last);
  input.value = optLast ? optLast.value : 'TODOS LOS COORDINADORES';
  chooseById(last);
}

/* ============== Cargar grupos ============== */
async function loadGruposForCoordinador(coord, user){
  const cont=document.getElementById('grupos'); if (cont) cont.textContent='CARGANDO GRUPOS…';

  const allSnap=await getDocs(collection(db,'grupos'));
  const wanted=[];
  const emailElegido=(coord?.email||'').toLowerCase();
  const uidElegido=(coord?.uid||'').toString();
  const docIdElegido=(coord?.id||'').toString();
  const nombreElegido=norm(coord?.nombre||'');
  const isSelf = !coord || coord.id==='self' || emailElegido===(user.email||'').toLowerCase();
  const isAll  = coord && coord.id==='__ALL__';

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
    const gEmails=emailsOf(raw), gUids=uidsOf(raw), gDocIds=coordDocIdsOf(raw), gNames=nombresOf(raw);
    const match=(emailElegido && gEmails.includes(emailElegido)) || (uidElegido && gUids.includes(uidElegido)) ||
                (docIdElegido && gDocIds.includes(docIdElegido)) || (nombreElegido && gNames.includes(nombreElegido)) ||
                (isSelf && gUids.includes(user.uid));
    if (match) wanted.push(g);
  });

  // ordenar: futuros ↑ luego pasados ↑
  const hoy=toISO(new Date());
  const futuros=wanted.filter(g=>(g.fechaInicio||'')>=hoy).sort((a,b)=>(a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  const pasados=wanted.filter(g=>(g.fechaInicio||'')<hoy).sort((a,b)=>(a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  state.grupos=wanted; state.ordenados=[...futuros,...pasados];

  state.filter={type:'all',value:null};
  state.groupQ='';

  renderNavBar();       // nav (sin buscador)
  renderStatsFiltered();

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
  renderOneGroup(state.ordenados[state.idx], qsF);
}

/* ============== Stats ============== */
function getFilteredList(){ return state.ordenados.slice(); }
function renderStatsFiltered(){ renderStats(getFilteredList()); }
function renderStats(list){
  const p=ensurePanel('statsPanel');
  if(!list.length){ p.innerHTML='<div class="muted">SIN VIAJES PARA EL FILTRO ACTUAL.</div>'; return; }
  const n=list.length;
  const minIniISO=list.map(g=>g.fechaInicio).filter(Boolean).sort()[0]||'';
  const maxFinISO=list.map(g=>g.fechaFin).filter(Boolean).sort().slice(-1)[0]||'';
  const totalDias=list.reduce((s,g)=> s+daysInclusive(g.fechaInicio,g.fechaFin),0);
  const paxTot=list.reduce((s,g)=> s+paxOf(g),0);
  const destinos=[...new Set(list.map(g=>String(g.destino||'')).filter(Boolean))];

  p.innerHTML = `
    <div style="display:grid;gap:.4rem">
      <div class="meta">TOTAL VIAJES: <strong>${n}</strong> · TOTAL DÍAS: <strong>${totalDias}</strong> · TOTAL PAX: <strong>${paxTot}</strong></div>
      <div class="meta">RANGO GLOBAL: ${minIniISO?dmy(minIniISO):'—'} — ${maxFinISO?dmy(maxFinISO):'—'}</div>
      <div class="meta">DESTINOS: ${destinos.length? destinos.join(' · ') : '—'}</div>
    </div>`;
}

/* ============== Nav (botones antes del select) ============== */
function renderNavBar(){
  const p=ensurePanel('navPanel'); // el HTML ya está en index
  const sel=p.querySelector('#allTrips');
  if(!sel) return;
  sel.textContent='';

  // FILTRO: TODOS
  const ogFiltro=document.createElement('optgroup'); ogFiltro.label='FILTRO';
  ogFiltro.appendChild(new Option('TODOS','all')); sel.appendChild(ogFiltro);

  // VIAJES (sin sección de destinos)
  const ogTrips=document.createElement('optgroup'); ogTrips.label='VIAJES';
  state.ordenados.forEach((g,i)=>{
    const name=(g.nombreGrupo||g.aliasGrupo||g.id);
    const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
    const opt=new Option(`${g.destino||''} · ${name} (${code}) | IDA: ${dmy(g.fechaInicio||'')}  VUELTA: ${dmy(g.fechaFin||'')}`, `trip:${i}`);
    ogTrips.appendChild(opt);
  });
  sel.appendChild(ogTrips);
  sel.value=`trip:${state.idx}`;

  // handlers
  const btnPrev = p.querySelector('#btnPrev');
  const btnNext = p.querySelector('#btnNext');
  const btnPrint= p.querySelector('#btnPrintVch');
  const btnAlert= p.querySelector('#btnNewAlert');

  if(btnPrev) btnPrev.onclick=()=>{ const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.max(0,j-1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId); renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`;
  };
  if(btnNext) btnNext.onclick=()=>{ const list=getFilteredList(); if(!list.length) return;
    const cur=state.ordenados[state.idx]?.id; const j=list.findIndex(g=>g.id===cur);
    const j2=Math.min(list.length-1,j+1), targetId=list[j2].id;
    state.idx=state.ordenados.findIndex(g=>g.id===targetId); renderOneGroup(state.ordenados[state.idx]); sel.value=`trip:${state.idx}`;
  };
  sel.onchange=()=>{ const v=sel.value||'';
    if(v==='all'){ sel.value=`trip:${state.idx}`; } // solo resetea
    else if(v.startsWith('trip:')){ state.idx=Number(v.slice(5))||0; renderOneGroup(state.ordenados[state.idx]); }
  };

  if(state.isStaff){
    if(btnPrint) btnPrint.onclick = openPrintVouchersModal;
    if(btnAlert) btnAlert.onclick = openCreateAlertModal;
  }
}

/* ============== Vista 1 viaje (con buscador interno) ============== */
function renderOneGroup(g, preferDate){
  const cont=document.getElementById('grupos'); if(!cont) return; cont.innerHTML='';
  if(!g){ cont.innerHTML='<p class="muted">NO HAY VIAJES.</p>'; return; }
  localStorage.setItem('rt_last_group', g.id);

  const name=(g.nombreGrupo||g.aliasGrupo||g.id);
  const code=(g.numeroNegocio||'')+(g.identificador?('-'+g.identificador):'');
  const rango = `${dmy(g.fechaInicio||'')} — ${dmy(g.fechaFin||'')}`;

  const header=document.createElement('div'); header.className='group-card';
  header.innerHTML=`<h3>${htmlEsc(name)} (${htmlEsc(code)})</h3>
    <div class="grid-mini">
      <div class="lab">DESTINO</div><div>${htmlEsc(g.destino||'—')}</div>
      <div class="lab">GRUPO</div><div>${htmlEsc(name)}</div>
      <div class="lab">PAX TOTAL</div><div>${(g.cantidadgrupo ?? g.pax ?? 0)}</div>
      <div class="lab">PROGRAMA</div><div>${htmlEsc(g.programa||'—')}</div>
      <div class="lab">FECHAS</div><div>${htmlEsc(rango)}</div>
    </div>
    <div class="rowflex" style="margin-top:.6rem">
      <input id="searchTrips" type="text" placeholder="BUSCAR EN ESTE GRUPO (FECHAS, ACTIVIDADES, GASTOS, ALERTAS…)"/>
    </div>`;
  cont.appendChild(header);

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

  const show = (which)=> {
    paneResumen.style.display = which==='resumen'?'':'none';
    paneItin.style.display    = which==='itin'   ?'':'none';
    paneGastos.style.display  = which==='gastos' ?'':'none';
  };

  tabs.querySelector('#tabResumen').onclick=()=> show('resumen');
  tabs.querySelector('#tabItin').onclick   =()=> show('itin');
  tabs.querySelector('#tabGastos').onclick =()=> show('gastos');

  // primeros renders
  renderResumen(g, paneResumen);
  renderItinerario(g, paneItin, preferDate);
  renderGastos(g, paneGastos);
  show('resumen');

  // búsqueda interna
  const input=header.querySelector('#searchTrips');
  input.value = state.groupQ || '';
  let tmr=null;
  input.oninput=()=>{ clearTimeout(tmr); tmr=setTimeout(()=>{
    state.groupQ = input.value || '';
    const active = paneItin.style.display!== 'none' ? 'itin' : (paneGastos.style.display!=='none' ? 'gastos' : 'resumen');
    renderResumen(g, paneResumen);
    const last = localStorage.getItem('rt_last_date_'+g.id);
    renderItinerario(g, paneItin, last || preferDate);
    renderGastos(g, paneGastos);
    show(active);
  },180); };
}

/* ============== Resumen (Hotel + Vuelos + Alertas) ============== */
async function renderResumen(g, pane){
  pane.innerHTML='<div class="loader">CARGANDO…</div>';
  const wrap=document.createElement('div'); wrap.style.cssText='display:grid;gap:.8rem'; pane.innerHTML='';
  const q = norm(state.groupQ||''); let resumenCountBase = 0;

  // HOTEL
  const hotelBox=document.createElement('div'); hotelBox.className='act';
  hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">BUSCANDO…</div>'; wrap.appendChild(hotelBox);
  // VUELOS
  const vuelosBox=document.createElement('div'); vuelosBox.className='act';
  vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">BUSCANDO…</div>'; wrap.appendChild(vuelosBox);
  // ALERTAS
  const alertBox=document.createElement('div'); alertBox.className='act';
  alertBox.innerHTML='<h4>ALERTAS</h4><div class="muted">CARGANDO…</div>'; wrap.appendChild(alertBox);
  pane.appendChild(wrap);

  try{
    const h=await loadHotelInfo(g);
    if(!h){ hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">SIN ASIGNACIÓN.</div>'; }
    else{
      const nombre=h.hotelNombre||h.hotel?.nombre||'HOTEL';
      const fechas=`${dmy(h.checkIn||'')} — ${dmy(h.checkOut||'')}`;
      const dir=h.hotel?.direccion||'';
      const contacto=[h.hotel?.contactoNombre,h.hotel?.contactoTelefono,h.hotel?.contactoCorreo].filter(Boolean).join(' · ');
      const txtJoin = [nombre,dir,contacto,fechas].join(' ');
      const hit = !q || norm(txtJoin).includes(q);
      if (hit) resumenCountBase += 1;
      hotelBox.innerHTML = `
        <h4>${highlight(nombre, state.groupQ)}</h4>
        ${dir?`<div class="prov">${highlight(dir, state.groupQ)}</div>`:''}
        <div class="meta">CHECK-IN/OUT: ${highlight(fechas, state.groupQ)}</div>
        ${contacto?`<div class="meta">${highlight(contacto, state.groupQ)}</div>`:''}`;
      if (!hit && q) hotelBox.innerHTML = '<h4>HOTEL</h4><div class="muted">SIN COINCIDENCIAS CON LA BÚSQUEDA.</div>';
    }
  }catch(e){ console.error(e); hotelBox.innerHTML='<h4>HOTEL</h4><div class="muted">ERROR AL CARGAR.</div>'; }

  // VUELOS
  let vuelosCount=0;
  try{
    const vuelos = await loadVuelosInfo(g);
    const flt = (!q)?vuelos : vuelos.filter(v=>{
      const s=[v.numero,v.proveedor,v.origen,v.destino,toISO(v.fechaIda),toISO(v.fechaVuelta)].join(' ');
      return norm(s).includes(q);
    });
    vuelosCount = flt.length;
    if(!flt.length){ vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">SIN VUELOS.</div>'; }
    else{
      const table=document.createElement('table'); table.className='table';
      table.innerHTML='<thead><tr><th>#</th><th>PROVEEDOR</th><th>RUTA</th><th>IDA</th><th>VUELTA</th></tr></thead><tbody></tbody>';
      const tb=table.querySelector('tbody');
      flt.forEach(v=>{
        const tr=document.createElement('tr');
        tr.innerHTML=`<td>${highlight(v.numero||'', state.groupQ)}</td>
          <td>${highlight(v.proveedor||'', state.groupQ)}</td>
          <td>${highlight(`${v.origen||''} — ${v.destino||''}`, state.groupQ)}</td>
          <td>${highlight(dmy(toISO(v.fechaIda))||'', state.groupQ)}</td>
          <td>${highlight(dmy(toISO(v.fechaVuelta))||'', state.groupQ)}</td>`;
        tb.appendChild(tr);
      });
      vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4>'; vuelosBox.appendChild(table);
    }
  }catch(e){ console.error(e); vuelosBox.innerHTML='<h4>TRANSPORTE / VUELOS</h4><div class="muted">ERROR AL CARGAR.</div>'; }

  // ALERTAS (reporta cuenta al tab)
  try{
    await renderAlertas(g, alertBox, q, (alertsCount)=> setTabCount('resumen', resumenCountBase + vuelosCount + alertsCount));
  }catch(e){
    console.error(e); alertBox.innerHTML='<h4>ALERTAS</h4><div class="muted">ERROR AL CARGAR.</div>';
    setTabCount('resumen', resumenCountBase + vuelosCount);
  }
}

async function loadHotelInfo(g){
  const key=g.numeroNegocio;
  if(state.cache.hotel.has(key)) return state.cache.hotel.get(key);
  let cand=[];
  try{
    const qs=await getDocs(query(collection(db,'hotelAssignments'), where('grupoId','==',String(key))));
    qs.forEach(d=>cand.push({id:d.id,...(d.data()||{})}));
  }catch(_){}
  try{
    const qs2=await getDocs(query(collection(db,'hotelAssignments'), where('grupoDocId','==',String(g.id))));
    qs2.forEach(d=>cand.push({id:d.id,...(d.data()||{})}));
  }catch(_){}
  if(!cand.length){ state.cache.hotel.set(key,null); return null; }
  let elegido=null,score=1e15; const rango={ini:toISO(g.fechaInicio), fin:toISO(g.fechaFin)};
  cand.forEach(x=>{ const ci=toISO(x.checkIn), co=toISO(x.checkOut);
    let s=5e14; if(ci&&co&&rango.ini&&rango.fin){ const overlap=!(co<rango.ini || ci>rango.fin); s=overlap?0:Math.abs(new Date(ci)-new Date(rango.ini)); }
    if(s<score){ score=s; elegido=x; }
  });
  let hotelDoc=null; if(elegido?.hotelId){ const hd=await getDoc(doc(db,'hoteles',String(elegido.hotelId))); if(hd.exists()) hotelDoc={id:hd.id,...hd.data()}; }
  const out={...elegido,hotel:hotelDoc,hotelNombre:elegido?.nombre||elegido?.hotelNombre||hotelDoc?.nombre||''};
  state.cache.hotel.set(key,out); return out;
}
async function loadVuelosInfo(g){
  const key=g.numeroNegocio; if(state.cache.vuelos.has(key)) return state.cache.vuelos.get(key);
  let found=[]; try{
    const qs=await getDocs(query(collection(db,'vuelos'), where('grupoIds','array-contains',String(key))));
    qs.forEach(d=>found.push({id:d.id,...d.data()}));
  }catch(_){}
  if(!found.length){
    const ss=await getDocs(collection(db,'vuelos'));
    ss.forEach(d=>{ const v=d.data()||{}; const arr=Array.isArray(v.grupos)?v.grupos:[]; if(arr.some(x=>String(x?.id||'')===String(key))) found.push({id:d.id,...v}); });
  }
  found.sort((a,b)=> (toISO(a.fechaIda)||'').localeCompare(toISO(b.fechaIda)||'')); state.cache.vuelos.set(key,found); return found;
}

/* ============== Itinerario + asistencia + vouchers ============== */
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

function countItinMatches(g, q){
  if(!q) return 0; const qn=norm(q); let n=0;
  const fechas=rangoFechas(g.fechaInicio,g.fechaFin);
  for (const f of fechas){
    const arr=(g.itinerario && g.itinerario[f])||[];
    for (const a of arr){
      const s=[a.actividad,a.proveedor,a.horaInicio,a.horaFin,dmy(f)].join(' ');
      if (norm(s).includes(qn)) n++;
    }
  }
  return n;
}

function renderItinerario(g, pane, preferDate){
  pane.innerHTML='';
  const q = norm(state.groupQ||'');
  const fechas=rangoFechas(g.fechaInicio,g.fechaFin);
  if(!fechas.length){ pane.innerHTML='<div class="muted">FECHAS NO DEFINIDAS.</div>'; setTabCount('itin', 0); return; }

  const pillsWrap=document.createElement('div'); pillsWrap.className='date-pills'; pane.appendChild(pillsWrap);
  const actsWrap=document.createElement('div'); actsWrap.className='acts'; pane.appendChild(actsWrap);

  const hoy=toISO(new Date());
  let startDate=preferDate || ((hoy>=fechas[0] && hoy<=fechas.at(-1))?hoy:fechas[0]);

  const fechasMostrar = (!q) ? fechas : fechas.filter(f=>{
    const arr=(g.itinerario && g.itinerario[f])? g.itinerario[f] : [];
    const hitAct = arr.some(a => norm([a.actividad,a.proveedor,a.horaInicio,a.horaFin].join(' ')).includes(q));
    const hitFecha = norm(dmy(f)).includes(q);
    return hitAct || hitFecha;
  });
  if(!fechasMostrar.length){ actsWrap.innerHTML='<div class="muted">SIN COINCIDENCIAS PARA EL ITINERARIO.</div>'; setTabCount('itin', 0); return; }

  if(!fechasMostrar.includes(startDate)) startDate=fechasMostrar[0];

  fechasMostrar.forEach(f=>{
    const pill=document.createElement('div'); pill.className='pill'+(f===startDate?' active':'');
    pill.innerHTML = highlight(dmy(f), state.groupQ);
    pill.title=f; pill.dataset.fecha=f;
    pill.onclick=()=>{ pillsWrap.querySelectorAll('.pill').forEach(p=>p.classList.remove('active')); pill.classList.add('active'); renderActs(g,f,actsWrap); localStorage.setItem('rt_last_date_'+g.id,f); };
    pillsWrap.appendChild(pill);
  });

  setTabCount('itin', countItinMatches(g, state.groupQ));
  const last=localStorage.getItem('rt_last_date_'+g.id); if(last && fechasMostrar.includes(last)) startDate=last;
  renderActs(g,startDate,actsWrap);
}

async function renderActs(grupo, fechaISO, cont){
  cont.innerHTML='';
  const q = norm(state.groupQ||'');
  let acts=(grupo.itinerario && grupo.itinerario[fechaISO]) ? grupo.itinerario[fechaISO] : [];
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
      <h4>${highlight(actName, state.groupQ)} ${estado?`· <span class="muted">${htmlEsc(estado)}</span>`:''}</h4>
      <div class="meta">${highlight((act.horaInicio||'--:--')+'–'+(act.horaFin||'--:--'), state.groupQ)} · PLAN: <strong>${plan}</strong> PAX</div>
      <div class="rowflex" style="margin:.35rem 0">
        <input type="number" min="0" inputmode="numeric" placeholder="ASISTENTES" value="${paxFinalInit}"/>
        <textarea placeholder="NOTA (SE GUARDA EN BITÁCORA AL GUARDAR)"></textarea>
        <button class="btn ok btnSave">GUARDAR</button>
        ${tipo!=='NOAPLICA'?`<button class="btn sec btnVch">FINALIZAR…</button>`:''}
      </div>
      <div class="bitacora" style="margin-top:.4rem">
        <div class="muted" style="margin-bottom:.25rem">BITÁCORA</div>
        <div class="bitItems" style="display:grid;gap:.35rem"></div>
      </div>`;
    cont.appendChild(div);

    // Bitácora
    const itemsWrap=div.querySelector('.bitItems'); await loadBitacora(grupo.id,fechaISO,actKey,itemsWrap);

    // Guardar asistencia (+nota → bitácora)
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
          const coll=collection(db,'grupos',grupo.id,'bitacora',`${fechaISO}-${actKey}`,'items');
          await addDoc(coll,{ texto:nota, byUid:auth.currentUser.uid, byEmail:(auth.currentUser.email||'').toLowerCase(), ts:serverTimestamp() });
          await loadBitacora(grupo.id,fechaISO,actKey,itemsWrap);
          div.querySelector('textarea').value='';
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
    const coll=collection(db,'grupos',grupoId,'bitacora',`${fechaISO}-${actKey}`,'items');
    const qs=await getDocs(query(coll,orderBy('ts','desc'),limit(50)));
    const frag=document.createDocumentFragment();
    qs.forEach(d=>{ const x=d.data()||{}; const quien=String(x.byEmail||x.byUid||'USUARIO');
      const cuando=x.ts?.seconds?new Date(x.ts.seconds*1000):null;
      const hora=cuando?cuando.toLocaleString('es-CL'):''; const div=document.createElement('div'); div.className='meta';
      div.innerHTML=`• ${highlight(x.texto||'', state.groupQ)} — ${htmlEsc(quien)}${hora?(' · '+htmlEsc(hora)):''}`; frag.appendChild(div);
    });
    wrap.innerHTML=''; wrap.appendChild(frag); if(!qs.size) wrap.innerHTML='<div class="muted">AÚN NO HAY NOTAS.</div>';
  }catch(e){ console.error(e); wrap.innerHTML='<div class="muted">NO SE PUDO CARGAR LA BITÁCORA.</div>'; }
}

/* ====== Servicios / Vouchers ====== */
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
    ? `${proveedorDoc.nombre||''}${proveedorDoc.rut?(' · '+proveedorDoc.rut):''}${proveedorDoc.direccion?(' · '+proveedorDoc.direccion):''}`
    : (act.proveedor||'');
  return `
    <div class="card">
      <h3>${htmlEsc(act.actividad||'SERVICIO')}</h3>
      <div class="meta">PROVEEDOR: ${htmlEsc(provTexto||'—')}</div>
      <div class="meta">GRUPO: ${htmlEsc(g.nombreGrupo||g.aliasGrupo||g.id)} (${htmlEsc(code)})</div>
      <div class="meta">FECHA: ${htmlEsc(dmy(fechaISO))}</div>
      <div class="meta">PAX PLAN: ${paxPlan} · PAX ASISTENTES: ${paxAsist}</div>
      ${compact?'':'<hr><div class="meta">FIRMA COORDINADOR: ________________________________</div>'}
    </div>`;
}

async function openVoucherModal(g, fechaISO, act, servicio, tipo){
  const back=document.getElementById('modalBack');
  const title=document.getElementById('modalTitle');
  const body=document.getElementById('modalBody');
  title.textContent=`VOUCHER — ${act.actividad||''} — ${dmy(fechaISO)}`;

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
  } else { // ELECTRONICO
    const clave=(servicio?.clave||'').toString();
    body.innerHTML= `${voucherHTML}
      <div class="rowflex" style="margin-top:.6rem;gap:.4rem">
        <input id="vchClave" type="password" placeholder="CLAVE (O ACERQUE TARJETA NFC)"/>
        <button id="vchToggle" class="btn sec" title="MOSTRAR/OCULTAR">👁</button>
        <button id="vchFirmar" class="btn ok">FIRMAR</button>
        <button id="vchPend" class="btn warn">PENDIENTE</button>
      </div>
      <div class="meta">TIP: SI TU MÓVIL SOPORTA NFC, PUEDES ACERCAR LA TARJETA PARA LEER LA CLAVE AUTOMÁTICAMENTE.</div>`;

    const inp = document.getElementById('vchClave');
    const eye = document.getElementById('vchToggle');
    eye.onclick = ()=>{ inp.type = (inp.type==='password'?'text':'password'); eye.textContent = inp.type==='password' ? '👁' : '🙈'; };

    document.getElementById('vchFirmar').onclick=async ()=>{
      const val=(inp.value||'').trim();
      if(!val){ alert('Ingresa la clave.'); return; }
      if(norm(val)!==norm(clave||'')){ alert('Clave incorrecta.'); return; }
      await setEstadoServicio(g,fechaISO,act,'FINALIZADA', true);
    };
    document.getElementById('vchPend').onclick =()=> setEstadoServicio(g,fechaISO,act,'PENDIENTE',  true);

    // NFC opcional
    if('NDEFReader' in window){
      try{ const reader=new window.NDEFReader(); await reader.scan();
        reader.onreading=(ev)=>{ const rec=ev.message.records[0]; let text=''; try{ text=(new TextDecoder().decode(rec.data)||'').trim(); }catch(_){}
          if(text){ inp.value=text; }
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
      const coll=collection(db,'grupos',g.id,'bitacora',`${fechaISO}-${key}`,'items');
      await addDoc(coll,{ texto:`Actividad ${estado.toLowerCase()}`, byUid:state.user.uid, byEmail:(state.user.email||'').toLowerCase(), ts:serverTimestamp() });
    }
  }catch(e){ console.error(e); alert('No fue posible actualizar el estado.'); }
}

/* ============== ALERTAS ============== */
async function renderAlertas(g, box, qNorm='', afterCount=(n)=>{}){
  const coordIdGuess = state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self';
  const qs=await getDocs(collection(db,'alertas'));
  const all=[]; qs.forEach(d=>all.push({id:d.id,...d.data()}));

  const mine = state.isStaff ? all : all.filter(a=> Array.isArray(a.forCoordIds) && a.forCoordIds.includes(coordIdGuess));
  const filtered = (!qNorm) ? mine : mine.filter(a=> norm([a.mensaje,a.createdBy?.email].join(' ')).includes(qNorm));

  const unread = filtered.filter(a=> !(a.readBy && a.readBy[coordIdGuess]));
  const read   = filtered.filter(a=>  (a.readBy && a.readBy[coordIdGuess]));
  const totalCount = unread.length + read.length;
  afterCount(totalCount);

  const wrap=document.createElement('div');
  if(state.isStaff){
    const row=document.createElement('div'); row.className='rowflex';
    const btn=document.createElement('button'); btn.className='btn sec'; btn.textContent='CREAR ALERTA…';
    btn.onclick=openCreateAlertModal; row.appendChild(btn); wrap.appendChild(row);
  }

  const mk=(arr,tit)=>{
    const card=document.createElement('div'); card.style.marginTop='.5rem'; card.innerHTML=`<div class="meta" style="margin-bottom:.25rem">${htmlEsc(tit)}</div>`;
    if(!arr.length){ card.innerHTML+=`<div class="muted">SIN MENSAJES.</div>`; return card; }
    const table=document.createElement('table'); table.className='table';
    table.innerHTML='<thead><tr><th>FECHA</th><th>MENSAJE</th><th>DE</th><th>ACCIONES</th></tr></thead><tbody></tbody>';
    const tb=table.querySelector('tbody');
    arr.forEach(a=>{
      const fecha=a.createdAt?.seconds? new Date(a.createdAt.seconds*1000).toLocaleDateString('es-CL') : '';
      const tr=document.createElement('tr'); tr.innerHTML=
        `<td>${highlight(fecha, state.groupQ)}</td>
         <td>${highlight(a.mensaje||'', state.groupQ)}</td>
         <td>${highlight(a.createdBy?.email||'', state.groupQ)}</td>
         <td></td>`;
      const td=tr.lastElementChild;
      if(!state.isStaff){
        const ch=document.createElement('input'); ch.type='checkbox'; ch.checked=!!(a.readBy && a.readBy[coordIdGuess]);
        ch.onchange=async ()=>{ const path=doc(db,'alertas',a.id); const payload={}; payload[`readBy.${coordIdGuess}`]= ch.checked ? serverTimestamp() : null; await updateDoc(path,payload); renderResumen(g, document.getElementById('paneResumen')); };
        td.appendChild(ch); td.appendChild(document.createTextNode(' LEÍDO'));
      }else{ td.textContent='—'; }
      tb.appendChild(tr);
    }); card.appendChild(table); return card;
  };

  wrap.appendChild(mk(unread,'NO LEÍDAS'));
  wrap.appendChild(mk(read,'LEÍDAS'));
  box.innerHTML='<h4>ALERTAS</h4>'; box.appendChild(wrap);
}

function openCreateAlertModal(){
  const back=document.getElementById('modalBack'); const body=document.getElementById('modalBody'); const title=document.getElementById('modalTitle');
  title.textContent='CREAR ALERTA (STAFF)';
  const coordOpts=state.coordinadores.map(c=>`<option value="${c.id}">${htmlEsc(c.nombre)} — ${htmlEsc(c.email)}</option>`).join('');
  body.innerHTML=`
    <div class="rowflex"><textarea id="alertMsg" placeholder="MENSAJE" style="width:100%"></textarea></div>
    <div class="rowflex">
      <label>DESTINATARIOS</label>
      <select id="alertCoords" multiple size="8" style="width:100%">${coordOpts}</select>
    </div>
    <div class="rowflex">
      <input id="alertGroupId" type="text" placeholder="OPCIONAL: ID DE GRUPO"/>
      <button id="alertSave" class="btn ok">AGREGAR</button>
    </div>`;
  document.getElementById('alertSave').onclick=async ()=>{
    const msg=(document.getElementById('alertMsg').value||'').trim();
    const sel=Array.from(document.getElementById('alertCoords').selectedOptions).map(o=>o.value);
    const gid=(document.getElementById('alertGroupId').value||'').trim();
    if(!msg || !sel.length){ alert('Mensaje y al menos un coordinador.'); return; }
    await addDoc(collection(db,'alertas'),{
      mensaje:msg, forCoordIds:sel, grupoId:gid||null,
      createdAt:serverTimestamp(), createdBy:{ uid:state.user.uid, email:(state.user.email||'').toLowerCase() }, readBy:{}
    });
    document.getElementById('modalBack').style.display='none';
    renderResumen(state.ordenados[state.idx], document.getElementById('paneResumen'));
  };
  document.getElementById('modalClose').onclick=()=>{ document.getElementById('modalBack').style.display='none'; };
  back.style.display='flex';
}

/* ============== Imprimir Vouchers (staff) ============== */
function openPrintVouchersModal(){
  const back=document.getElementById('modalBack'); const body=document.getElementById('modalBody'); const title=document.getElementById('modalTitle');
  title.textContent='IMPRIMIR VOUCHERS (STAFF)';
  const coordOpts=[`<option value="__ALL__">TODOS</option>`].concat(state.coordinadores.map(c=>`<option value="${c.id}">${htmlEsc(c.nombre)}</option>`)).join('');
  body.innerHTML=`
    <div class="rowflex"><label>COORDINADOR</label><select id="pvCoord">${coordOpts}</select></div>
    <div class="rowflex"><input type="text" id="pvDestino" placeholder="DESTINO (OPCIONAL)"/><input type="text" id="pvRango" placeholder="RANGO dd-mm-aaaa..dd-mm-aaaa (OPCIONAL)"/></div>
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
  return `<!doctype html><html><head><meta charset="utf-8"><title>Vouchers</title>
<style>body{font-family:system-ui,Segoe UI,Roboto,Arial;color:#111;padding:20px}
.card{border:1px solid #999;border-radius:8px;padding:12px;margin:10px 0}
h3{margin:.2rem 0 .4rem}.meta{color:#333;font-size:14px}hr{border:0;border-top:1px dashed #999;margin:.4rem 0}
</style></head><body><h2>Vouchers</h2>${rows || '<div>Sin actividades.</div>'}</body></html>`;
}

/* ============== Gastos (tab) ============== */
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
      if(!asunto || !valor){ alert('Asunto y valor obligatorios.'); return; }

      btn.disabled=true;

      let imgUrl=null, imgPath=null;
      if(file){
        if (file.size > 10*1024*1024){ alert('La imagen supera 10MB.'); btn.disabled=false; return; }
        const safe = file.name.replace(/[^a-z0-9.\-_]/gi,'_');
        const uid  = (auth.currentUser && auth.currentUser.uid) || state.user.uid;
        const path = `gastos/${uid}/${Date.now()}_${safe}`;
        const r    = sRef(storage, path);
        await uploadBytes(r, file, { contentType: file.type || 'image/jpeg' });
        imgUrl  = await getDownloadURL(r);
        imgPath = path;
      }

      const coordId = state.coordId || (
        state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self'
      );

      await addDoc(collection(db,'coordinadores',coordId,'gastos'),{
        asunto, moneda, valor, imgUrl, imgPath,
        grupoId:g.id, numeroNegocio:g.numeroNegocio, identificador:g.identificador||null,
        grupoNombre:g.nombreGrupo||g.aliasGrupo||g.id, destino:g.destino||null, programa:g.programa||null,
        fechaInicio:g.fechaInicio||null, fechaFin:g.fechaFin||null,
        byUid: state.user.uid, byEmail:(state.user.email||'').toLowerCase(),
        createdAt: serverTimestamp()
      });

      form.querySelector('#spAsunto').value='';
      form.querySelector('#spValor').value='';
      form.querySelector('#spImg').value='';
      await loadGastosList(g,listBox);
    }catch(e){
      console.error(e);
      alert('No fue posible guardar el gasto.');
    }finally{
      btn.disabled=false;
    }
  };

  await loadGastosList(g,listBox);
}

async function getTasas(){
  if(state.cache.tasas) return state.cache.tasas;
  try{ const d=await getDoc(doc(db,'config','tasas')); if(d.exists()){ state.cache.tasas=d.data()||{}; return state.cache.tasas; } }catch(_){}
  state.cache.tasas={ USD:950, BRL:170, ARS:1.2 }; return state.cache.tasas;
}

async function loadGastosList(g, box){
  const coordId = state.coordinadores.find(c=> (c.email||'').toLowerCase()===(state.user.email||'').toLowerCase())?.id || 'self';
  const qs=await getDocs(query(collection(db,'coordinadores',coordId,'gastos'), orderBy('createdAt','desc')));
  let list=[]; qs.forEach(d=>{ const x=d.data()||{}; if(x.grupoId===g.id) list.push({id:d.id,...x}); });

  const q = norm(state.groupQ||'');
  if(q){
    list = list.filter(x => norm([x.asunto,x.byEmail,x.moneda,String(x.valor||0)].join(' ')).includes(q));
  }

  setTabCount('gastos', list.length);

  const tasas=await getTasas();
  const tot={ CLP:0, USD:0, BRL:0, ARS:0, CLPconv:0 };

  const table=document.createElement('table'); table.className='table';
  table.innerHTML='<thead><tr><th>ASUNTO</th><th>AUTOR</th><th>MONEDA</th><th>VALOR</th><th>COMPROBANTE</th></tr></thead><tbody></tbody>';
  const tb=table.querySelector('tbody');
  list.forEach(x=>{
    const tr=document.createElement('tr');
    tr.innerHTML=`<td>${highlight(x.asunto||'', state.groupQ)}</td>
      <td>${highlight(x.byEmail||'', state.groupQ)}</td>
      <td>${highlight(x.moneda||'', state.groupQ)}</td>
      <td>${highlight(Number(x.valor||0).toLocaleString('es-CL'), state.groupQ)}</td>
      <td>${x.imgUrl?`<a href="${x.imgUrl}" target="_blank">VER</a>`:'—'}</td>`;
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
}
