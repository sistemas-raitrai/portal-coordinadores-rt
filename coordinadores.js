// coordinadores.js — Portal de Coordinadores RT (staff + cruce coordinadores + asistencia)
import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, getDocs, doc, updateDoc, serverTimestamp,
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

// ---- Normalizador: sin tildes/espacios/punt., lower ----
const norm = (s='') => s
  .toString()
  .normalize('NFD').replace(/[\u0300-\u036f]/g,'')   // quita diacríticos
  .toLowerCase()
  .replace(/[^a-z0-9]+/g,'');                        // quita espacios/puntuación

// ---- util: slug para clave de actividad ----
const slug = s => norm(s).slice(0,60);

// ---- UI helpers ----
function fmt(iso){ if(!iso) return ''; const d=new Date(iso+'T00:00:00'); return d.toLocaleDateString('es-CL',{weekday:'short',day:'2-digit',month:'short'}); }
function rangoFechas(ini, fin){ const out=[]; if(!ini||!fin) return out; const a=new Date(ini+'T00:00:00'), b=new Date(fin+'T00:00:00'); for(let d=new Date(a); d<=b; d.setDate(d.getDate()+1)) out.push(d.toISOString().slice(0,10)); return out; }
function calcPlan(actividad, grupo){ const a=actividad||{}; const ad=Number(a.adultos||0), es=Number(a.estudiantes||0); const porAct=(ad+es)>0?(ad+es):null; return porAct ?? Number(grupo.cantidadgrupo||0); }

// === Helpers de asistencia (leer/actualizar en memoria) ===
function getSavedAsistencia(grupo, fechaISO, actividad) {
  const key = slug(actividad || 'actividad');
  return grupo?.asistencias?.[fechaISO]?.[key] || null;
}
function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data) {
  const key = slug(actividad || 'actividad');
  if (!grupo.asistencias) grupo.asistencias = {};
  if (!grupo.asistencias[fechaISO]) grupo.asistencias[fechaISO] = {};
  grupo.asistencias[fechaISO][key] = data;
}

// ====== Arranque ======
onAuthStateChanged(auth, async (user) => {
  if (!user) return location.href = 'index.html';

  const email = (user.email||'').toLowerCase();
  const isStaff = STAFF_EMAILS.has(email);

  // Carga lista de coordinadores (colección "coordinadores": campos esperados {nombre, email})
  const coordinadores = await loadCoordinadores();

  if (isStaff) {
    await showStaffSelector(coordinadores, user);    // staff elige coordinador
  } else {
    const miReg = findCoordinadorForUser(coordinadores, user); // auto-resolver
    await loadGruposForCoordinador(miReg, user);
  }
});

// ====== Lee coleccion "coordinadores" ======
async function loadCoordinadores(){
  const snap = await getDocs(collection(db,'coordinadores'));
  const list = [];
  snap.forEach(d=>{
    const x = d.data();
    list.push({
      id: d.id,
      nombre: (x.nombre || x.Nombre || x.coordinador || '').toString(),
      email: (x.email || x.correo || '').toString().toLowerCase(),
    });
  });
  // ordena por nombre
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
    bar.innerHTML = `
      <label style="display:block; margin-bottom:6px; color:#cbd5e1">Ver viajes por coordinador</label>
      <select id="coordSelect" style="width:100%; padding:.55rem; border-radius:10px; border:1px solid #334155; background:#0b1329; color:#e5e7eb"></select>
    `;
    wrap.prepend(bar);
  }

  const sel = document.getElementById('coordSelect');
  sel.innerHTML = `<option value="">— Selecciona coordinador —</option>` +
    coordinadores.map(c => `<option value="${c.id}">${c.nombre} — ${c.email||'sin correo'}</option>`).join('');

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
  // 1) match por email
  let c = coordinadores.find(x => x.email && x.email.toLowerCase() === email);
  if (c) return c;
  // 2) match por nombre normalizado
  const disp = norm(user.displayName || '');
  if (disp) {
    c = coordinadores.find(x => norm(x.nombre) === disp);
    if (c) return c;
  }
  // 3) fallback: usa su propio email/nombre
  return { id:'self', nombre: user.displayName || email, email };
}

// ====== Cargar grupos para un coordinador (staff o no staff) ======
async function loadGruposForCoordinador(coord, user){
  const cont = document.getElementById('grupos');
  cont.textContent = 'Cargando grupos…';

  // Trae TODOS los grupos (lectura pública). Luego filtramos en cliente
  const all = await getDocs(collection(db,'grupos'));
  const wanted = [];

  // datos del coordinador elegido
  const emailElegido = (coord?.email || '').toLowerCase();
  const nombreElegido = norm(coord?.nombre || '');

  const isSelf = !coord || coord.id === 'self' || emailElegido === (user.email||'').toLowerCase();

  all.forEach(d => {
    const g = { id: d.id, ...d.data() };

    // Candidatos de coincidencia en el grupo
    const gName = norm(g.coordinador || g.coordinadorNombre || '');
    const gEmail = (g.coordinadorEmail || '').toLowerCase();
    const arrEmails = (g.coordinadoresEmails || []).map(x => (x||'').toLowerCase());
    const arrUids   = Array.isArray(g.coordinadores) ? g.coordinadores : [];

    // Regla de match (OR):
    const match =
      (emailElegido && gEmail === emailElegido) ||
      (emailElegido && arrEmails.includes(emailElegido)) ||
      (nombreElegido && gName && gName === nombreElegido) ||
      (isSelf && arrUids.includes(user.uid));   // por si el grupo usa UIDs

    if (match) wanted.push(g);
  });

  wanted.sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  renderGrupos(wanted, user);
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
      <h3>${g.nombreGrupo || g.id}</h3>
      <div class="group-sub">${g.destino||''} · ${g.programa||''} · ${g.cantidadgrupo||0} pax</div>
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
      pill.dataset.fecha = f;
      pill.onclick = ()=> {
        pills.querySelectorAll('.pill').forEach(p=>p.classList.remove('active'));
        pill.classList.add('active');
        renderActs(g, f, card.querySelector(`#acts-${g.id}`), user);
      };
      pills.appendChild(pill);
    });

    if (fechas[0]) renderActs(g, fechas[0], card.querySelector(`#acts-${g.id}`), user);
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

        // actualizar en memoria para mantener prellenado
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
