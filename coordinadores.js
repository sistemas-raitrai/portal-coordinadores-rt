// coordinadores.js — Portal de Coordinadores RT
// Requiere: firebase-init-portal.js en la misma carpeta.

import { app, db } from './firebase-init-portal.js';
import { getAuth, onAuthStateChanged, signOut }
  from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';
import {
  collection, getDocs, getDoc, doc, setDoc, serverTimestamp,
  query, where                        // <-- añade estos
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

const auth = getAuth(app);
document.getElementById('logout').onclick = () =>
  signOut(auth).then(()=>location='index.html');

onAuthStateChanged(auth, async (user) => {
  if (!user) return location.href = 'index.html';
  load(user);
});

async function load(user){
  const cont = document.getElementById('grupos');
  cont.textContent = 'Cargando tus grupos…';

  // 🔑 Trae SOLO los grupos donde el usuario esté asignado
  const q = query(collection(db,'grupos'),
                  where('coordinadores','array-contains', user.uid));
  const snap = await getDocs(q);
  const mis = [];
  snap.forEach(d => { const g = d.data(); g.id = d.id; mis.push(g); });
  mis.sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));
  renderGrupos(mis, user);
}


// ======================
// Carga y render de grupos
// ======================
async function load(user){
  const cont = document.getElementById('grupos');
  cont.textContent = 'Cargando tus grupos…';

  const all = await getDocs(collection(db,'grupos'));
  const mis = [];
  all.forEach(snap=>{
    const g = snap.data(); g.id = snap.id;
    if ((g.coordinadores||[]).includes(user.uid)) mis.push(g);
  });

  // Orden por fechaInicio asc
  mis.sort((a,b)=> (a.fechaInicio||'').localeCompare(b.fechaInicio||''));

  renderGrupos(mis, user);
}

function fmt(iso){
  if (!iso) return '';
  const d = new Date(iso+'T00:00:00');
  return d.toLocaleDateString('es-CL', {weekday:'short', day:'2-digit', month:'short'});
}

function rangoFechas(ini, fin){
  const out=[]; if(!ini||!fin) return out;
  const a=new Date(ini+'T00:00:00'), b=new Date(fin+'T00:00:00');
  for(let d=new Date(a); d<=b; d.setDate(d.getDate()+1)){
    out.push(d.toISOString().slice(0,10));
  }
  return out;
}

const slug = s => (s||'').toLowerCase().normalize('NFKD')
  .replace(/[^\w\s-]/g,'').trim().replace(/\s+/g,'-').slice(0,60);

function calcPlan(actividad, grupo){
  const a = actividad || {};
  const ad = Number(a.adultos||0), es = Number(a.estudiantes||0);
  const porAct = (ad+es)>0 ? (ad+es) : null;
  return porAct ?? Number(grupo.cantidadgrupo||0);
}

async function renderGrupos(grupos, user){
  const cont = document.getElementById('grupos');
  cont.innerHTML = '';

  if (!grupos.length){
    cont.innerHTML = '<p class="muted">No tienes grupos asignados.</p>';
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
    // Mostrar día 1 por defecto
    if (fechas[0]) renderActs(g, fechas[0], card.querySelector(`#acts-${g.id}`), user);
  }
}

// ======================
// Actividades + Asistencia
// ======================
async function getAsistenciaDoc(grupoId, fecha, actName){
  const id = `${fecha}_${slug(actName)}`;
  const ref = doc(db, 'grupos', grupoId, 'asistencias', id);
  const snap = await getDoc(ref);
  return { ref, data: snap.exists()? snap.data(): null };
}

async function renderActs(grupo, fechaISO, cont, user){
  cont.innerHTML = '';
  const acts = (grupo.itinerario && grupo.itinerario[fechaISO]) || [];
  if (!acts.length){
    cont.innerHTML = '<div class="muted">Sin actividades para este día.</div>';
    return;
  }

  for (const act of acts){
    const plan = calcPlan(act, grupo);
    const { ref, data } = await getAsistenciaDoc(grupo.id, fechaISO, act.actividad||'actividad');

    const div = document.createElement('div');
    div.className = 'act';
    div.innerHTML = `
      <h4>${act.actividad || 'Actividad'}</h4>
      <div class="meta">${act.horaInicio||'--:--'}–${act.horaFin||'--:--'} · Plan: <strong>${plan}</strong> pax</div>
      <div class="prov"></div>
      <div class="row">
        <input type="number" min="0" inputmode="numeric" placeholder="Asistentes" value="${data?.paxAsistentes ?? ''}" />
        <textarea placeholder="Notas (opcional)">${data?.notas ?? ''}</textarea>
        <button>Guardar</button>
      </div>
      <div class="muted" style="margin-top:.3rem">${data?'Última actualización disponible.':''}</div>
    `;
    cont.appendChild(div);

    // Ficha proveedor (si existen catálogos)
    injectProveedorInfo(div.querySelector('.prov'), act);

    const inp = div.querySelector('input');
    const txt = div.querySelector('textarea');
    const btn = div.querySelector('button');

    btn.onclick = async ()=>{
      btn.disabled = true;
      const paxAsist = Number(inp.value||0);
      const payload = {
        grupoId: grupo.id,
        fechaISO,
        actividad: act.actividad||'',
        horaInicio: act.horaInicio||'',
        paxPlanificados: plan,
        paxAsistentes: paxAsist,
        notas: txt.value||'',
        coordinadorUid: user.uid,
        coordinadorEmail: user.email,
        updatedAt: serverTimestamp(),
        createdAt: data?.createdAt ?? serverTimestamp()
      };
      await setDoc(ref, payload, { merge:true });
      btn.textContent = 'Guardado';
      setTimeout(()=>{ btn.textContent='Guardar'; btn.disabled=false; }, 1000);
    };
  }
}

// ======================
// Catálogos (opcional)
// ======================
let _SERV = null, _PROV = null;

async function ensureCatalogos(){
  if (_SERV && _PROV) return;
  try{
    const s = await getDocs(collection(db, 'Servicios/BRASIL/Listado'));
    _SERV = {}; s.forEach(d=>{ const x=d.data(); _SERV[x.servicio]=x; });
  }catch{ _SERV = {}; }
  try{
    const p = await getDocs(collection(db, 'Proveedores/BRASIL/Listado'));
    _PROV = {}; p.forEach(d=>{ const x=d.data(); _PROV[x.proveedor]=x; });
  }catch{ _PROV = {}; }
}

async function injectProveedorInfo(node, act){
  node.textContent = '';
  await ensureCatalogos();
  const s = _SERV?.[act?.actividad||''];
  if (!s){ node.innerHTML = '<span class="muted">Sin ficha de servicio</span>'; return; }
  const prov = _PROV?.[s.proveedor||''];
  node.innerHTML = `
    <div>Proveedor: <strong>${s.proveedor||'—'}</strong>${prov? ` · ${prov?.telefono||''} · ${prov?.correo||''}`:''}</div>
    ${s.restricciones? `<div class="muted">Restricciones: ${s.restricciones}</div>`:''}
  `;
}
