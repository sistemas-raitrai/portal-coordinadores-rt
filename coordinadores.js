/* coordinadores.js — Portal de Coordinadores RT
   - Robusto a cambios de esquema en Firestore (nombres/formatos de campos)
   - Staff selector (elige coordinador) + resolución automática no-staff
   - Lectura de grupos + fallback por collectionGroup('conjuntos') → viajes[]
   - IDs CSS-safe para evitar errores con querySelector()
   - Itinerario normalizado (objeto por fecha o array legado)
   - Registro de asistencia en asistencias.FECHA.SLUG
   - Sin uso de `||` dentro de template strings
*/

import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, collectionGroup, getDocs, getDoc, doc, updateDoc, serverTimestamp,
  query, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

/* =========================================================
   0) Auth + botón salir
   ========================================================= */
const auth = getAuth(app);
const logoutBtn = document.getElementById('logout');
if (logoutBtn) {
  logoutBtn.onclick = () => signOut(auth).then(() => (location = 'index.html'));
}

/* =========================================================
   1) Config: staff permitido
   ========================================================= */
const STAFF_EMAILS = new Set([
  'aleoperaciones@raitrai.cl',
  'tomas@raitrai.cl',
  'operaciones@raitrai.cl',
  'anamaria@raitrai.cl',
  'sistemas@raitrai.cl',
].map(e => e.toLowerCase()));

/* =========================================================
   2) Utilitarios
   ========================================================= */
const norm = (s = '') => s.toString()
  .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
  .toLowerCase().replace(/[^a-z0-9]+/g, '');

const slug = (s) => norm(s).slice(0, 60);

// Genera un ID seguro para CSS/DOM (reemplaza espacios y símbolos)
function cssSafeId(prefix, raw) {
  const base = String(raw == null ? '' : raw)
    .replace(/[^A-Za-z0-9_-]/g, '_'); // solo letras, números, _ y -
  return prefix + '_' + base;          // siempre inicia con letra
}

// Fecha → 'YYYY-MM-DD' desde ISO string, Timestamp o Date
function toISO(x) {
  if (!x) return '';
  if (typeof x === 'string') {
    if (/^\d{4}-\d{2}-\d{2}$/.test(x)) return x;
    const d = new Date(x);
    return isNaN(d) ? '' : d.toISOString().slice(0, 10);
  }
  if (x && typeof x === 'object' && 'seconds' in x) { // Firestore Timestamp
    const d = new Date(x.seconds * 1000);
    return d.toISOString().slice(0, 10);
  }
  if (x instanceof Date) return x.toISOString().slice(0, 10);
  return '';
}

function fmt(iso) {
  if (!iso) return '';
  const d = new Date(iso + 'T00:00:00');
  return d.toLocaleDateString('es-CL', { weekday: 'short', day: '2-digit', month: 'short' });
}

function rangoFechas(ini, fin) {
  const out = [];
  const A = toISO(ini), B = toISO(fin);
  if (!A || !B) return out;
  const a = new Date(A + 'T00:00:00'), b = new Date(B + 'T00:00:00');
  for (let d = new Date(a); d <= b; d.setDate(d.getDate() + 1)) {
    out.push(d.toISOString().slice(0, 10));
  }
  return out;
}

// Itinerario: soporta objeto por fecha o array legado [{fecha, ...}]
function normalizeItinerario(raw) {
  if (!raw) return {};
  if (Array.isArray(raw)) {
    const map = {};
    for (const item of raw) {
      const f = toISO(item && item.fecha);
      if (!f) continue;
      if (!map[f]) map[f] = [];
      map[f].push({ ...item });
    }
    return map;
  }
  return raw; // se asume { 'YYYY-MM-DD': [ ... ] }
}

const arrify = (v) => Array.isArray(v) ? v
  : (v && typeof v === 'object' ? Object.values(v) : (v ? [v] : []));

/* =========================================================
   3) Extracción tolerante de coordinadores desde un grupo
   ========================================================= */
function emailsOf(g) {
  const out = new Set();
  const push = (e) => { if (e) out.add(String(e).toLowerCase()); };
  push(g && g.coordinadorEmail);
  push(g && g.coordinador && g.coordinador.email);
  arrify(g && g.coordinadoresEmails).forEach(e => push(e));
  if (g && g.coordinadoresEmailsObj && typeof g.coordinadoresEmailsObj === 'object') {
    Object.keys(g.coordinadoresEmailsObj).forEach(push);
  }
  arrify(g && g.coordinadores).forEach(x => {
    if (x && typeof x === 'object' && x.email) push(x.email);
    else if (typeof x === 'string' && x.indexOf('@') !== -1) push(x);
  });
  return Array.from(out);
}

function uidsOf(g) {
  const out = new Set();
  const push = (x) => { if (x) out.add(String(x)); };
  push(g && (g.coordinadorUid || g.coordinadorId)); // a veces usan esto como "uid"
  if (g && g.coordinador && g.coordinador.uid) push(g.coordinador.uid);
  arrify((g && g.coordinadoresUids) || (g && g.coordinadoresIds) || (g && g.coordinadores)).forEach(x => {
    if (x && typeof x === 'object' && x.uid) push(x.uid);
    else push(x);
  });
  return Array.from(out);
}

// Doc IDs de coordinador (cuando en grupos guardan la referencia al doc de /coordinadores)
function coordDocIdsOf(g) {
  const out = new Set();
  const push = (x) => { if (x) out.add(String(x)); };
  push(g && g.coordinadorId);
  arrify(g && g.coordinadoresIds).forEach(push);
  return Array.from(out);
}

function nombresOf(g) {
  const out = new Set();
  const push = (s) => { if (s) out.add(norm(String(s))); };
  push(g && (g.coordinadorNombre || g.coordinador));
  if (g && g.coordinador && g.coordinador.nombre) push(g.coordinador.nombre);
  arrify(g && g.coordinadoresNombres).forEach(push);
  return Array.from(out);
}

/* =========================================================
   4) Asistencias helpers (claves tolerantes via slug)
   ========================================================= */
function getSavedAsistencia(grupo, fechaISO, actividad) {
  const byDate = grupo && grupo.asistencias && grupo.asistencias[fechaISO];
  if (!byDate) return null;
  const key = slug(actividad || 'actividad');
  if (Object.prototype.hasOwnProperty.call(byDate, key)) return byDate[key];
  // tolerar claves antiguas (mismo slug)
  const keys = Object.keys(byDate);
  for (const k of keys) { if (slug(k) === key) return byDate[k]; }
  return null;
}

function setSavedAsistenciaLocal(grupo, fechaISO, actividad, data) {
  const key = slug(actividad || 'actividad');
  if (!grupo.asistencias) grupo.asistencias = {};
  if (!grupo.asistencias[fechaISO]) grupo.asistencias[fechaISO] = {};
  grupo.asistencias[fechaISO][key] = data;
}

// Plan estimado de la actividad
function calcPlan(actividad, grupo) {
  const a = actividad || {};
  const ad = Number(a.adultos || 0);
  const es = Number(a.estudiantes || 0);
  const suma = ad + es;
  if (suma > 0) return suma;
  const base = (grupo && (grupo.cantidadgrupo != null ? grupo.cantidadgrupo : grupo.pax));
  return Number(base || 0);
}

/* =========================================================
   5) Arranque
   ========================================================= */
onAuthStateChanged(auth, async (user) => {
  if (!user) {
    location.href = 'index.html';
    return;
  }

  const email = (user.email || '').toLowerCase();
  const isStaff = STAFF_EMAILS.has(email);

  const coordinadores = await loadCoordinadores();

  if (isStaff) {
    await showStaffSelector(coordinadores, user);
  } else {
    const miReg = findCoordinadorForUser(coordinadores, user);
    await loadGruposForCoordinador(miReg, user);
  }
});

/* =========================================================
   6) Lectura de coordinadores (tolerante a llaves)
   ========================================================= */
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
  return list;
}

/* =========================================================
   7) Staff selector (sin templates riesgosos)
   ========================================================= */
async function showStaffSelector(coordinadores, user) {
  const wrap = document.querySelector('.wrap');
  let bar = document.getElementById('staffBar');
  if (!bar) {
    bar = document.createElement('div');
    bar.id = 'staffBar';
    bar.style.cssText = 'margin:12px 0 8px; padding:8px; border:1px solid #223053; border-radius:12px; background:#0f1530;';
    bar.innerHTML =
      '<label style="display:block; margin-bottom:6px; color:#cbd5e1">Ver viajes por coordinador</label>' +
      '<select id="coordSelect" style="width:100%; padding:.55rem; border-radius:10px; border:1px solid #334155; background:#0b1329; color:#e5e7eb"></select>';
    if (wrap) wrap.prepend(bar);
  }

  const sel = document.getElementById('coordSelect');
  sel.textContent = '';
  const opt0 = document.createElement('option');
  opt0.value = '';
  opt0.textContent = '— Selecciona coordinador —';
  sel.appendChild(opt0);

  coordinadores.forEach((c) => {
    const opt = document.createElement('option');
    opt.value = c.id;
    if (c.email) opt.setAttribute('data-email', String(c.email));
    if (c.uid) opt.setAttribute('data-uid', String(c.uid));
    const nombre = c.nombre || '';
    const correo = c.email ? String(c.email) : 'sin correo';
    opt.textContent = nombre + ' — ' + correo;
    sel.appendChild(opt);
  });

  sel.onchange = async () => {
    const id = sel.value;
    const elegido = coordinadores.find((c) => c.id === id) || null;
    localStorage.setItem('rt_staff_coord', id || '');
    await loadGruposForCoordinador(elegido, user);
  };

  const last = localStorage.getItem('rt_staff_coord');
  if (last && coordinadores.find((c) => c.id === last)) {
    sel.value = last;
    const elegido = coordinadores.find((c) => c.id === last);
    await loadGruposForCoordinador(elegido, user);
  } else {
    renderGrupos([], user);
  }
}

/* =========================================================
   8) Resolver coordinador de un usuario no-staff
   ========================================================= */
function findCoordinadorForUser(coordinadores, user) {
  const email = (user.email || '').toLowerCase();
  const uid = user.uid;

  // 1) email
  let c = coordinadores.find((x) => x.email && x.email.toLowerCase() === email);
  if (c) return c;

  // 2) uid
  if (uid) {
    c = coordinadores.find((x) => x.uid && x.uid === uid);
    if (c) return c;
  }

  // 3) displayName normalizado
  const disp = norm(user.displayName || '');
  if (disp) {
    c = coordinadores.find((x) => norm(x.nombre) === disp);
    if (c) return c;
  }

  // 4) fallback
  return { id: 'self', nombre: user.displayName || email, email, uid };
}

/* =========================================================
   9) Cargar grupos para un coordinador (+ fallback conjuntos)
   ========================================================= */
async function loadGruposForCoordinador(coord, user) {
  const cont = document.getElementById('grupos');
  if (cont) cont.textContent = 'Cargando grupos…';

  const allSnap = await getDocs(collection(db, 'grupos'));
  const wanted = [];

  const emailElegido = ((coord && coord.email) ? coord.email : '').toLowerCase();
  const uidElegido = (coord && coord.uid) ? String(coord.uid) : '';
  const docIdElegido = (coord && coord.id) ? String(coord.id) : '';
  const nombreElegido = norm((coord && coord.nombre) ? coord.nombre : '');
  const isSelf = !coord || coord.id === 'self' || emailElegido === (user.email || '').toLowerCase();

  allSnap.forEach((d) => {
    const raw = Object.assign({ id: d.id }, d.data());
    const g = Object.assign({}, raw, {
      fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
      fechaFin: toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
      itinerario: normalizeItinerario(raw.itinerario),
      asistencias: raw.asistencias || {},
    });

    const gEmails = emailsOf(raw);
    const gUids = uidsOf(raw);
    const gDocIds = coordDocIdsOf(raw);
    const gNames = nombresOf(raw);

    const match =
      (emailElegido && gEmails.indexOf(emailElegido) !== -1) ||
      (uidElegido && gUids.indexOf(uidElegido) !== -1) ||
      (docIdElegido && gDocIds.indexOf(docIdElegido) !== -1) ||
      (nombreElegido && gNames.indexOf(nombreElegido) !== -1) ||
      (isSelf && gUids.indexOf(user.uid) !== -1);

    if (match) wanted.push(g);
  });

  // Fallback: collectionGroup('conjuntos') → viajes[]
  if (wanted.length === 0 && coord && coord.id !== 'self') {
    try {
      const queries = [];
      if (uidElegido) queries.push(query(collectionGroup(db, 'conjuntos'), where('coordinadorId', '==', uidElegido)));
      if (emailElegido) queries.push(query(collectionGroup(db, 'conjuntos'), where('coordinadorEmail', '==', emailElegido)));
      if (docIdElegido) queries.push(query(collectionGroup(db, 'conjuntos'), where('coordinadorDocId', '==', docIdElegido)));

      const ids = new Set();
      for (const qy of queries) {
        const ss = await getDocs(qy);
        ss.forEach((docu) => {
          const data = docu.data();
          const v = (data && data.viajes) ? data.viajes : [];
          v.forEach((id) => ids.add(String(id)));
        });
      }

      for (const id of ids) {
        const ref = doc(db, 'grupos', id);
        const dd = await getDoc(ref);
        if (dd.exists()) {
          const raw = Object.assign({ id: dd.id }, dd.data());
          wanted.push({
            ...raw,
            fechaInicio: toISO(raw.fechaInicio || raw.inicio || raw.fecha_ini),
            fechaFin: toISO(raw.fechaFin || raw.fin || raw.fecha_fin),
            itinerario: normalizeItinerario(raw.itinerario),
            asistencias: raw.asistencias || {},
          });
        }
      }
    } catch (e) {
      console.warn('Fallback collectionGroup(conjuntos) no disponible/indizado:', e);
    }
  }

  wanted.sort((a, b) => (a.fechaInicio || '').localeCompare(b.fechaInicio || ''));
  renderGrupos(wanted, user);

  // Diagnóstico rápido
  console.info('[Portal Coord] grupos cargados:', wanted.length);
  console.table(wanted.slice(0, 3).map(g => ({
    id: g.id,
    inicio: g.fechaInicio,
    fin: g.fechaFin,
    emails: emailsOf(g),
    uids: uidsOf(g),
    coordDocIds: coordDocIdsOf(g),
    nombres: nombresOf(g),
  })));
}

/* =========================================================
   10) Render de grupos (IDs CSS-safe)
   ========================================================= */
async function renderGrupos(grupos, user) {
  const cont = document.getElementById('grupos');
  if (!cont) return;
  cont.innerHTML = '';

  if (!grupos.length) {
    cont.innerHTML = '<p class="muted">No hay grupos para el coordinador seleccionado.</p>';
    return;
  }

  for (const g of grupos) {
    // Precalcular textos (evita `||` en el template)
    const nombreTxt = (g && g.nombreGrupo != null && g.nombreGrupo !== '') ? g.nombreGrupo
      : (g && g.aliasGrupo != null && g.aliasGrupo !== '') ? g.aliasGrupo : g.id;

    const destinoTxt = (g && g.destino != null) ? String(g.destino) : '';
    const programaTxt = (g && g.programa != null) ? String(g.programa) : '';
    const paxTxt = (g && g.cantidadgrupo != null) ? g.cantidadgrupo
      : (g && g.pax != null) ? g.pax : 0;
    const subTxt = destinoTxt + ' · ' + programaTxt + ' · ' + paxTxt + ' pax';

    // IDs seguros
    const pillsId = cssSafeId('pills', g.id);
    const actsId  = cssSafeId('acts',  g.id);

    const card = document.createElement('div');
    card.className = 'group-card';
    card.setAttribute('data-gid', String(g.id)); // guardamos el id real por si acaso
    card.innerHTML =
      '<h3>' + nombreTxt + '</h3>' +
      '<div class="group-sub">' + subTxt + '</div>' +
      '<div class="date-pills" id="' + pillsId + '"></div>' +
      '<div class="acts" id="' + actsId + '"></div>';
    cont.appendChild(card);

    const fechas = rangoFechas(g && g.fechaInicio, g && g.fechaFin);
    const pills = card.querySelector('#' + pillsId);

    fechas.forEach((f, i) => {
      const pill = document.createElement('div');
      pill.className = 'pill' + (i === 0 ? ' active' : '');
      pill.textContent = fmt(f);
      pill.title = f;
      pill.dataset.fecha = f;
      pill.onclick = () => {
        pills.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
        pill.classList.add('active');
        renderActs(g, f, card.querySelector('#' + actsId), user);
      };
      pills.appendChild(pill);
    });

    if (fechas[0]) {
      renderActs(g, fechas[0], card.querySelector('#' + actsId), user);
    } else {
      card.querySelector('#' + actsId).innerHTML = '<div class="muted">Fechas no definidas.</div>';
    }
  }
}

/* =========================================================
   11) Render actividades + guardar asistencia (payload seguro)
   ========================================================= */
async function renderActs(grupo, fechaISO, cont, user) {
  cont.innerHTML = '';
  const acts = (grupo && grupo.itinerario && grupo.itinerario[fechaISO])
    ? grupo.itinerario[fechaISO] : [];
  if (!acts.length) {
    cont.innerHTML = '<div class="muted">Sin actividades para este día.</div>';
    return;
  }

  for (const act of acts) {
    const plan = calcPlan(act, grupo);
    const saved = getSavedAsistencia(grupo, fechaISO, act && act.actividad);

    const horaIni = (act && act.horaInicio != null && act.horaInicio !== '') ? act.horaInicio : '--:--';
    const horaFin = (act && act.horaFin != null && act.horaFin !== '') ? act.horaFin : '--:--';
    const paxFinalInit = (saved && saved.paxFinal != null) ? saved.paxFinal : '';
    const notasInit = (saved && saved.notas) ? saved.notas : '';
    const actName = (act && act.actividad != null && act.actividad !== '') ? act.actividad : 'Actividad';
    const actKey = slug(actName);

    const div = document.createElement('div');
    div.className = 'act';
    div.innerHTML =
      '<h4>' + actName + '</h4>' +
      '<div class="meta">' + horaIni + '–' + horaFin + ' · Plan: <strong>' + plan + '</strong> pax</div>' +
      '<div class="row">' +
        '<input type="number" min="0" inputmode="numeric" placeholder="Asistentes" value="' + paxFinalInit + '"/>' +
        '<textarea placeholder="Notas (opcional)">' + notasInit + '</textarea>' +
        '<button>Guardar</button>' +
      '</div>';
    cont.appendChild(div);

    const inp = div.querySelector('input');
    const txt = div.querySelector('textarea');
    const btn = div.querySelector('button');

    btn.onclick = async () => {
      btn.disabled = true;
      try {
        const refGrupo = doc(db, 'grupos', grupo.id);
        const keyPath = 'asistencias.' + fechaISO + '.' + actKey;
        const data = {
          paxFinal: Number(inp.value || 0),
          notas: (txt.value || ''),
          byUid: auth.currentUser.uid,
          byEmail: String(auth.currentUser.email || '').toLowerCase(),
          updatedAt: serverTimestamp(),
        };
        const payload = {};
        payload[keyPath] = data; // evitar propiedades computadas inline
        await updateDoc(refGrupo, payload);

        setSavedAsistenciaLocal(grupo, fechaISO, actName, { ...data });

        btn.textContent = 'Guardado';
        setTimeout(() => { btn.textContent = 'Guardar'; btn.disabled = false; }, 900);
      } catch (e) {
        console.error(e);
        btn.disabled = false;
        alert('No se pudo guardar la asistencia.');
      }
    };
  }
}
