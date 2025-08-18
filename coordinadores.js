// coordinadores.js — Portal de Coordinadores RT (robusto a cambios de esquema)
import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, serverTimestamp,
  query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

const auth = getAuth(app);
document.getElementById('logout').onclick = () =>
  signOut(auth).then(()=>location='index.html');

// ---- STAFF permitido (sistema principal) ----
const STAFF_EMAILS = new Set([
  "aleoperaciones@raitrai.cl",
  "tomas@raitrai.cl",
  "operaciones@raitrai.cl",
  "anamaria@raitrai.cl",
  "sistemas@raitrai.cl",
].map(e => e.toLowerCase()));

// ---- Normalizadores ----
const norm = (s='') => s.toString()
  .normalize('NFD').replace(/[\u0300-\u036f]/g,'')
  .toLowerCase().replace(/[^a-z0-9]+/g,'');

const slug = s => norm(s).slice(0,60);

// Fecha → 'YYYY-MM-DD' desde ISO, Timestamp o Date
function toISO(x){
  if(!x) return '';
  if (typeof x === 'string'){
    if (/^\d{4}-\d{2}-\d{2}$/.test(x)) return x;
    const d = new Date(x);
    return isNaN(d) ? '' : d.toISOString().slice(0,10);
  }
  if (x && typeof x === 'object' && 'seconds' in x){ // Firestore Timestamp
    const d = new Date(x.seconds * 1000);
    return d.toISOString().slice(0,10);
  }
  if (x instanceof Date){
    return x.toISOString().slice(0,10);
  }
  return '';
}

function fmt(iso){
  if(!iso) return '';
  const d=new Date(iso+'T00:00:00');
  return d.toLocaleDateString('es-CL',{weekday:'short',day:'2-digit',month:'short'});
}
function rangoFechas(ini, fin){
  const out=[]; const A=toISO(ini), B=toISO(fin);
  if(!A||!B) return out;
  const a=new Date(A+'T00:00:00'), b=new Date(B+'T00:00:00');
  for(let d=new Date(a); d<=b; d.setDate(d.getDate()+1)){
    out.push(d.toISOString().slice(0,10));
  }
  return out;
}

// ---- Itinerario: soporta objeto por fecha o array legado [{fecha, ...}] ----
function normalizeItinerario(raw){
  if (!raw) return {};
  if (Array.isArray(raw)){
    const map = {};
    for (const item of raw){
      const f = toISO(item?.fecha);
      if (!f) continue;
      if (!map[f]) map[f] = [];
      map[f].push({...item});
    }
    return map;
  }
  // se asume objeto { "YYYY-MM-DD": [ ... ] }
  return raw;
}

// ---- Extracción tolerante de coordinadores desde un grupo ----
const arrify = v => Array.isArray(v) ? v : (v && typeof v==='object' ? Object.values(v) : (v ? [v] : []));

function emailsOf(g){
  const out = new Set();
  const push = e => e && out.add(String(e).toLowerCase());
  push(g.coordinadorEmail);
  push(g?.coordinador?.email);
  (arrify(g.coordinadoresEmails)).forEach(e => push(e));
  // a veces guardan objeto {email:true}
  if (g.coordinadoresEmailsObj && typeof g.coordinadoresEmailsObj==='object'){
    Object.keys(g.coordinadoresEmailsObj).forEach(push);
  }
  // strings sueltos
  (arrify(g.coordinadores)).forEach(x=>{
    if (x && typeof x==='object' && x.email) push(x.email);
    else if (typeof x==='string' && x.includes('@')) push(x);
  });
  return [...out];
}
function uidsOf(g){
  const out = new Set();
  const push = x => x && out.add(String(x));
  push(g.coordinadorUid || g.coordinadorId);
  if (g?.coordinador?.uid) push(g.coordinador.uid);
  (arrify(g.coordinadoresUids || g.coordinadoresIds || g.coordinadores)).forEach(x=>{
    if (x && typeof x==='object' && x.uid) push(x.uid);
    else push(x);
  });
  return [...out];
}
function nombresOf(g){
  const out = new Set();
  const push = s => s && out.add(norm(String(s)));
  push(g.coordinadorNombre || g.coordinador);
  if (g?.coordinador?.nombre) push(g.coordinador.nombre);
  (arrify(g.coordinadoresNombres)).forEach(push);
  return [...out];
}

// === Helpers de asistencia (buscar por slug o clave equivalente) ===
function getSavedAsistencia(grupo, fechaISO, actividad) {
  const d = grupo?.asistencias?.[fechaISO];
  if (!d) return null;
  const key = slug(actividad || 'actividad');
  if (d[key]) return d[key];
  // tolera claves antiguas
  for (const k of Object.keys(d)){
    if (slug(k) === key) return d[k];
  }
  return null;
}
function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data) {
  const key = slug(actividad || 'actividad');
  if (!grupo.asistencias) grupo.asistencias = {};
  if (!grupo.asistencias[fechaISO]) grupo.asistencias[fechaISO] = {};
  grupo.asistencias[fechaISO][key] = data;
}

// === Plan estimado de la actividad
function calcPlan(actividad, grupo){
  const a=actividad||{};
  const ad=Number(a.adultos||0), es=Number(a.estudiantes||0);
  const porAct=(ad+es)>0?(ad+es):null;
  return porAct ?? Number(grupo.cantidadgrupo||grupo.pax||0) || 0;
}

// ====== Arranque ======
onAuthStateChanged(auth, async (user) => {
  if (!user) return location.href = 'index.html';

  const email = (user.email||'').toLowerCase();
  const isStaff = STAFF_EMAILS.has(email);

  const coordinadores = await loadCoordinadores();

  if (isStaff) {
    await showStaffSelector(coordinadores, user);
  } else {
    const miReg = findCoordinadorForUser(coordinadores, user);
    await loadGruposForCoordinador(miReg, user);
  }
});

// ====== Lee coleccion "coordinadores" (tolerante a claves) ======
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list = [];
  snap.forEach(d=>{
    const x = d.data()||{};
    list.push({
      id: d.id,
      nombre: (x.nombre || x.Nombre || x.coordinador || '').toString(),
      email: (x.email || x.correo || x.mail || '').toString().toLowerCase(),
      uid: (x.uid || x.userId || '').toString()
    });
  });
  list.sort((a,b)=> a.nombre.localeCompare(b.nombre, 'es', {sensitivity:'base'}));
  return list;
}

// ====== Staff selector ======
async function showStaffSelector(coordinadores, user){
  const wrap = document.querySelector('.wrap');
  let bar = document.getElementById('staffBar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'staffBar';
    bar.style.cssText = 'margin:12px 0 8px; padding:8px; border:1px solid #223053; border-radius:12px; background:#0f1530;';
    bar.innerHTML = (
      '<label style="display:block; margin-bottom:6px; color:#cbd5e1">Ver viajes por coordinador</label>' +
      '<select id="coordSelect" style="width:100%; padding:.55rem; border-radius:10px; border:1px solid #334155; background:#0b1329; color:#e5e7eb"></select>'
    );
    wrap.prepend(bar);
  }

  const sel = document.getElementById('coordSelect');
  // Limpiar y poblar sin templates
  sel.textContent = '';
  const opt0 = document.createElement('option');
  opt0.value = '';
  opt0.textContent = '— Selecciona coordinador —';
  sel.appendChild(opt0);

  coordinadores.forEach(c => {
    const opt = document.createElement('option');
    opt.value = c.id;
    if (c.email) opt.setAttribute('data-email', String(c.email));
    if (c.uid)   opt.setAttribute('data-uid', String(c.uid));
    opt.textContent = (c.nombre || '') + ' — ' + (c.email ? String(c.email) : 'sin correo');
    sel.appendChild(opt);
  });

  sel.onchange = async () => {
    const id = sel.value;
    const elegido = coordinadores.find(c => c.id === id) || null;
    localStorage.setItem('rt_staff_coord', id || '');
    await loadGruposForCoordinador(elegido, user);
  };

  const last = localStorage.getItem('rt_staff_coord');
  if (last && coordinadores.find(c=>c.id===last)) {
    sel.value = last;
    const elegido = coordinadores.find(c => c.id === last);
    await loadGruposForCoordinador(elegido, user);
  } else {
    renderGrupos([], user);
  }
}

// ====== Resolver coordinador de un usuario (no staff) ======
function findCoordinadorForUser(coordinadores, user){
  const email = (user.email||'').toLowerCase();
  const uid = user.uid;
  let c = coordinadores.find(x => x.email && x.email.toLowerCase() === email);
  if (c) return c;
  if (uid){
    c = coordinadores.find(x => x.uid && x.uid === uid);
    if (c) return c;
  }
  const disp = norm(user.displayName || '');
  if (disp) {
    c = coordinadores.find(x => norm(x.nombre) === disp);
    if (c) return c;
  }
  return { id:'self', nombre: user.displayName || email, email, uid };
}

// ====== Cargar grupos para un coordinador ======
async function loadGruposForCoordinador(coord, user){
  const cont = document.getElementById('grupos');
  cont.textContent = 'Cargando grupos…';

  const allSnap = await getDocs(collection(db,'grupos'));
  const wanted = [];
  const emailElegido = (coord?.email || '').toLowerCase();
  const uidElegido   = (coord?.uid || '').toString();
  const nombreElegido = norm(coord?.nombre || '');
  const isSelf = !coord || coord.id === 'self' || emailElegido === (user.email||'').toLowerCase();

  allSnap.forEach(d => {
    const raw = { id:d.id, ...d.data() };
    const g = {
      ...raw,
      // normalizaciones mínimas que usaremos después:
      fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
      fechaFin:    toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
      itinerario:  normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {}
    };

    // Candidatos
    const gEmails = emailsOf(raw);         // todas las formas de email
    const gUids   = uidsOf(raw);           // todas las formas de uid/id
    const gNames  = nombresOf(raw);        // nombres normalizados

    // Regla de match (OR):
    const match =
      (emailElegido && gEmails.includes(emailElegido)) ||
      (uidElegido && gUids.includes(uidElegido)) ||
      (nombreElegido && gNames.includes(nombreElegido)) ||
      (isSelf && gUids.includes(user.uid)); // por si almacenaron el propio uid

    if (match) wanted.push(g);
  });

  // Fallback: usa collectionGroup('conjuntos') → viajes[]
  if (wanted.length === 0 && coord && coord.id !== 'self'){
    try{
      const qs = [];
      if (uidElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorId','==', uidElegido)));
      if (emailElegido) qs.push(query(collectionGroup(db,'conjuntos'), where('coordinadorEmail','==', emailElegido)));
      const ids = new Set();
      for (const qy of qs){
        const ss = await getDocs(qy);
        ss.forEach(docu=>{
          const v = docu.data()?.viajes || [];
          v.forEach(id => ids.add(String(id)));
        });
      }
      // traer los grupos por id
      for (const id of ids){
        const ref = doc(db,'grupos', id);
        const dd = await getDoc(ref);
        if (dd.exists()){
          const raw = { id: dd.id, ...dd.data() };
          wanted.push({
            ...raw,
            fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
            fechaFin:    toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
            itinerario:  normalizeItinerario(raw.itinerario),
            asistencias: raw.asistencias || {}
          });
        }
      }
    }catch(e){
      console.warn('Fallback collectionGroup(conjuntos) no disponible/indizado:', e);
    }
  }

  // Orden y render
  wanted.sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  renderGrupos(wanted, user);

  // Diagnóstico en consola (primeros 3)
  console.info('[Portal Coord] grupos cargados:', wanted.length);
  console.table(wanted.slice(0,3).map(g=>({
    id:g.id, inicio:g.fechaInicio, fin:g.fechaFin,
    emails: emailsOf(g), uids: uidsOf(g),
    nombres: nombresOf(g)
  })));
}

// ====== Render ======
async function renderGrupos(grupos, user){
  const cont = document.getElementById('grupos');
  cont.innerHTML = '';

  if (!grupos.length){
    cont.innerHTML = '<p class="muted">No hay grupos para el coordinador seleccionado.</p>';
    return;
  }

  for (const g of grupos){
    const card = document.createElement('div');
    card.className = 'group-card';
    card.innerHTML = `
      <h3>${g.nombreGrupo || g.aliasGrupo || g.id}</h3>
      <div class="group-sub">${g.destino||''} · ${g.programa||''} · ${(g.cantidadgrupo||g.pax||0)} pax</div>
      <div class="date-pills" id="pills-${g.id}"></div>
      <div class="acts" id="acts-${g.id}"></div>
    `;
    cont.appendChild(card);

    const fechas = rangoFechas(g.fechaInicio, g.fechaFin);
    const pills  = card.querySelector(`#pills-${g.id}`);
    fechas.forEach((f,i)=>{
      const pill = document.createElement('div');
      pill.className = 'pill'+(i===0?' active':'');

      pill.textContent = fmt(f);
      pill.title = f; // tooltip ISO
      pill.dataset.fecha = f;
      pill.onclick = ()=> {
        pills.querySelectorAll('.pill').forEach(p=>p.classList.remove('active'));
        pill.classList.add('active');
        renderActs(g, f, card.querySelector(`#acts-${g.id}`), user);
      };
      pills.appendChild(pill);
    });

    // primera fecha
    if (fechas[0]) renderActs(g, fechas[0], card.querySelector(`#acts-${g.id}`), user);
    else card.querySelector(`#acts-${g.id}`).innerHTML = '<div class="muted">Fechas no definidas.</div>';
  }
}

// ====== Actividades + guardar asistencia dentro del doc del grupo ======
async function renderActs(grupo, fechaISO, cont, user){
  cont.innerHTML = '';
  const acts = (grupo.itinerario && grupo.itinerario[fechaISO]) || [];
  if (!acts.length){
    cont.innerHTML = '<div class="muted">Sin actividades para este día.</div>';
    return;
  }

  for (const act of acts){
    const plan  = calcPlan(act, grupo);
    const saved = getSavedAsistencia(grupo, fechaISO, act.actividad);

    const div = document.createElement('div');
    div.className = 'act';
    div.innerHTML = `
      <h4>${act.actividad || 'Actividad'}</h4>
      <div class="meta">${act.horaInicio||'--:--'}–${act.horaFin||'--:--'} · Plan: <strong>${plan}</strong> pax</div>
      <div class="row">
        <input type="number" min="0" inputmode="numeric" placeholder="Asistentes" value="${saved?.paxFinal ?? ''}"/>
        <textarea placeholder="Notas (opcional)">${saved?.notas ?? ''}</textarea>
        <button>Guardar</button>
      </div>
    `;
    cont.appendChild(div);

    const inp = div.querySelector('input');
    const txt = div.querySelector('textarea');
    const btn = div.querySelector('button');

    btn.onclick = async ()=>{
      btn.disabled = true;
      try{
        const refGrupo = doc(db,'grupos', grupo.id);
        const keyPath  = `asistencias.${fechaISO}.${slug(act.actividad||'actividad')}`;
        const data = {
          paxFinal: Number(inp.value||0),
          notas: txt.value || '',
          byUid:  auth.currentUser.uid,
          byEmail:(auth.currentUser.email||'').toLowerCase(),
          updatedAt: serverTimestamp()
        };
        await updateDoc(refGrupo, { [keyPath]: data });

        setSavedAsistenciaLocal(grupo, fechaISO, act.actividad, { ...data });

        btn.textContent = 'Guardado';
        setTimeout(()=>{ btn.textContent='Guardar'; btn.disabled=false; }, 900);
      }catch(e){
        console.error(e);
        btn.disabled = false;
        alert('No se pudo guardar la asistencia.');
      }
    };
  }
}
