import { mostrarCaja } from './coordinacion-general-caja.js';
import { auth, db } from './firebase-init-portal.js';
import { onAuthStateChanged, signInWithEmailAndPassword, signOut } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';

import {
  collection, doc, getDocs, onSnapshot, query, runTransaction, serverTimestamp, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

import { mostrarHoteles, mostrarComidas, mostrarDocumentos, mostrarRenuncia } from './coordinacion-general-consultas.js';

const API = 'https://southamerica-west1-sist-op-rt.cloudfunctions.net/controlCoordinadoresApi';
const $ = id => document.getElementById(id);
const labels = {
  itinerario: 'Itinerarios',
  hoy: 'Hoy',
  nomina: 'Nóminas',
  hoteles: 'Hoteles',
  comidas: 'Comidas especiales',
  documentos: 'Documentos',
  nfc: 'Pulseras NFC',
  renuncia: 'Renuncia a actividad',
  caja: 'Caja'
};

let account, access, section;
let generation = 0;
let detenerPulseras = () => {};

function nuevaVista() {
  detenerPulseras();
  detenerPulseras = () => {};
  return ++generation;
}

async function api(action, data = {}) {
  const token = await account.getIdToken();
  const response = await fetch(API, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
    body: JSON.stringify({ action, data, requestId: crypto.randomUUID() })
  });
  const result = await response.json().catch(() => ({}));
  if (!response.ok || !result.ok) {
    throw new Error(result.error || 'No se pudo consultar Coordinación General.');
  }
  return result;
}

async function apiPasajeros(accion, datos) {
  const token = await account.getIdToken();
  const respuesta = await fetch(
    'https://southamerica-west1-sist-op-rt.cloudfunctions.net/consultarPulseraPublica',
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ accion, ...datos })
    }
  );
  const resultado = await respuesta.json().catch(() => ({}));
  if (!respuesta.ok || !resultado.ok) {
    throw new Error(resultado.error || resultado.message || 'No se pudo consultar el grupo.');
  }
  return resultado;
}

function esc(valor) {
  const nodo = document.createElement('span');
  nodo.textContent = String(valor ?? '');
  return nodo.innerHTML.replaceAll('"', '&quot;').replaceAll("'", '&#39;');
}

function buildPassengerHtml(
  passenger,
  group
) {
  const identity =
    passenger.identificacion ||
    {};

  const contact =
    passenger.contactoPrincipal ||
    {};

  const secondaryContact =
    passenger.contactoSecundario ||
    {};

  const emergency =
    passenger.emergencia ||
    {};

  const secondaryEmergency =
    passenger.emergenciaSecundaria ||
    {};

  const health =
    passenger.salud ||
    {};

  const alerts =
    Array.isArray(
      passenger.alertasMedicas
    )
      ? passenger.alertasMedicas
      : [];

  const statusClass =
    passenger.anulado
      ? "danger"
      : passenger.fichaCompleta
        ? "ok"
        : "warn";

  return `
    <div class="person-header">
      <h3>
        ${esc(
          identity.nombreCompleto ||
          passenger.nombreCompleto ||
          "Pasajero"
        )}
      </h3>

      <p>
        ${esc(
          [
            passenger.tipo,
            group.nombre ||
            group.colegio,
            group.destino
          ]
            .filter(Boolean)
            .join(" · ")
        )}
      </p>

      <div class="status-row">
        <span class="status-pill ${statusClass}">
          ${
            passenger.anulado
              ? "Anulado / no viaja"
              : passenger.fichaCompleta
                ? "Ficha médica completa"
                : "Ficha médica pendiente"
          }
        </span>

        ${
          alerts.length
            ? `
              <span class="status-pill danger">
                ${alerts.length} alerta(s)
              </span>
            `
            : `
              <span class="status-pill ok">
                Sin alertas detectadas
              </span>
            `
        }
      </div>
    </div>

    ${
      alerts.length
        ? `
          <div class="alerts-box">
            <h4>
              Alertas médicas
            </h4>

            <ul>
              ${alerts
                .map(
                  (
                    alert
                  ) => `
                    <li>
                      ${esc(alert)}
                    </li>
                  `
                )
                .join("")}
            </ul>
          </div>
        `
        : ""
    }

    ${buildSection(
      "Identificación",
      [
        [
          "Documento",
          identity.documento ||
          passenger.documento
        ],
        [
          "Fecha de nacimiento",
          identity.fechaNacimiento
        ],
        [
          "Edad",
          identity.edad
        ],
        [
          "Género",
          identity.genero
        ],
        [
          "Nacionalidad",
          identity.nacionalidad
        ],
        [
          "Tipo de viajante",
          passenger.tipoViajante
        ]
      ]
    )}

    ${buildSection(
      "Contacto principal",
      [
        [
          "Nombre",
          contact.nombre
        ],
        [
          "Relación",
          contact.relacion
        ],
        [
          "Teléfono",
          contact.telefono
        ],
        [
          "Correo",
          contact.correo
        ]
      ]
    )}

    ${buildSection(
      "Contacto secundario",
      [
        [
          "Nombre",
          secondaryContact.nombre
        ],
        [
          "Relación",
          secondaryContact.relacion
        ],
        [
          "Teléfono",
          secondaryContact.telefono
        ],
        [
          "Correo",
          secondaryContact.correo
        ]
      ]
    )}

    ${buildSection(
      "Contacto de emergencia",
      [
        [
          "Nombre",
          emergency.nombre
        ],
        [
          "Relación",
          emergency.relacion
        ],
        [
          "Teléfono",
          emergency.telefono
        ],
        [
          "Correo",
          emergency.correo
        ]
      ]
    )}

    ${buildSection(
      "Emergencia secundaria",
      [
        [
          "Nombre",
          secondaryEmergency.nombre
        ],
        [
          "Relación",
          secondaryEmergency.relacion
        ],
        [
          "Teléfono",
          secondaryEmergency.telefono
        ]
      ]
    )}

    ${buildMedicalSection(
      health
    )}
  `;
}

function buildMedicalSection(
  health
) {
  const rows = [
    [
      "Grupo sanguíneo",
      health.grupoSanguineo
    ],
    [
      "Discapacidad",
      health.discapacidad
    ],
    [
      "Ayudas técnicas",
      health.ayudasTecnicas
    ],
    [
      "Apoyos de autonomía",
      health.apoyosAutonomia
    ],
    [
      "Neurodivergencia",
      health.neurodivergencia
    ],
    [
      "Factores de sobrecarga",
      health.neuroFactores
    ],
    [
      "Estrategias de regulación",
      health.neuroEstrategias
    ],
    [
      "Apoyos neurodivergencia",
      health.neuroApoyos
    ],
    [
      "Salud mental",
      health.saludMental
    ],
    [
      "Enfermedad de base",
      health.enfermedadBase
    ],
    [
      "Salud general",
      health.saludGeneral
    ],
    [
      "Cirugías previas",
      health.cirugiasPrevias
    ],
    [
      "Emergencias médicas",
      health.emergenciaMedica
    ],
    [
      "Medicamentos",
      health.medicamentos
    ],
    [
      "Medicamentos prohibidos",
      health.medicamentosProhibidos
    ],
    [
      "Alergias",
      health.alergias
    ],
    [
      "Alergias alimentarias",
      health.alergiasAlimentarias
    ],
    [
      "Dieta principal",
      health.dietaPrincipal
    ],
    [
      "Restricciones alimentarias",
      health.dietaRestricciones
    ],
    [
      "Dieta especial",
      health.dieta
    ],
    [
      "Otros antecedentes",
      health.otrosAntecedentes
    ]
  ];

  return buildSection(
    "Ficha médica",
    rows,
    {
      medical:
        true,

      note:
        "Información confidencial para uso operativo durante el viaje."
    }
  );
}

function buildSection(
  title,
  rows,
  options = {}
) {
  const visible =
    rows.filter(
      (
        [
          _,
          value
        ]
      ) =>
        value !== undefined &&
        value !== null &&
        String(
          value
        ).trim() !== ""
    );

  if (!visible.length) {
    return "";
  }

  return `
    <section class="info-section ${
      options.medical
        ? "medical"
        : ""
    }">
      <h3>
        ${esc(title)}
      </h3>

      ${
        options.note
          ? `
            <p class="section-note">
              ${esc(options.note)}
            </p>
          `
          : ""
      }

      <div class="data-grid">
        ${visible
          .map(
            (
              [
                label,
                value
              ]
            ) => `
              <div class="data-card">
                <span>
                  ${esc(label)}
                </span>

                <strong>
                  ${esc(value)}
                </strong>
              </div>
            `
          )
          .join("")}
      </div>
    </section>
  `;
}

async function cargarPasajeros() {
  const request = nuevaVista();
  const resultado = $('resultado');
  resultado.replaceChildren();
  const grupoId = $('grupo').value;
  if (!grupoId) return;
  const destino = destination().id;
  const soloFichas = !destination().modulos.includes('nomina');
  const puedeVerFichas = destination().modulos.includes('fichas');
  resultado.textContent = 'Cargando pasajeros…';

  try {
    const { pasajeros } = await apiPasajeros(
      soloFichas ? 'pasajerosFichaCoordinacionGeneral' : 'nominaCoordinacionGeneral',
      { destino, grupoId }
    );
    if (request !== generation) return;
    resultado.textContent = pasajeros.length
      ? `Integrantes · ${pasajeros.length}`
      : 'Sin integrantes disponibles.';

    for (const pasajero of pasajeros) {
      const fila = document.createElement('article');
      const nombre = document.createElement('strong');
      nombre.textContent = pasajero.nombre;
      fila.append(nombre);

      if (!soloFichas && pasajero.talla) {
        const talla = document.createElement('p');
        talla.textContent = 'Polera ' + pasajero.talla;
        fila.append(talla);
      }
      if (puedeVerFichas) {
        const boton = document.createElement('button');
        boton.type = 'button';
        boton.textContent = 'Ver ficha';
        boton.setAttribute('aria-label', 'Ver ficha de ' + pasajero.nombre);
        const ficha = document.createElement('div');
        ficha.hidden = true;
        boton.setAttribute('aria-expanded', 'false');
        boton.addEventListener('click', async () => {
          if (!ficha.hidden) {
            ficha.hidden = true;
            ficha.replaceChildren();
            boton.setAttribute('aria-expanded', 'false');
            return;
          }
          ficha.hidden = false;
          ficha.textContent = 'Cargando ficha…';
          boton.disabled = true;
          boton.setAttribute('aria-expanded', 'true');
          try {
            const datos = await apiPasajeros('fichaCoordinacionGeneral', {
              destino, grupoId, inscripcionId: pasajero.inscripcionId
            });
            if (request === generation) ficha.innerHTML = buildPassengerHtml(datos.pasajero, datos.grupo);
          } catch (error) {
            if (request === generation) ficha.textContent = error.message;
          } finally {
            boton.disabled = false;
          }
        });
        fila.append(boton, ficha);
      }
      resultado.append(fila);
    }
  } catch (error) {
    if (request === generation) resultado.textContent = error.message;
  }
}

function remember(value) {
  try {
    const key = 'rt-general-destino-' + account.uid;
    if (value !== undefined) localStorage.setItem(key, value);
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function destination() {
  return access.destinos.find(item => item.id === $('destino').value);
}

function renderMenu() {
  nuevaVista();
  section = '';
  $('contenido').hidden = true;
  $('resultado').replaceChildren();
  $('menu').replaceChildren();
  const selected = destination();
  if (!selected) return;

  const menu = [...new Set(selected.modulos.map(id => id === 'fichas' ? 'nomina' : id))];
  if (menu.includes('caja')) menu.unshift(...menu.splice(menu.indexOf('caja'), 1));
  for (const id of menu) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = labels[id] || id;
    if (id === 'caja') button.style.cssText = 'grid-column:1/-1;background:#155e52;color:white';
    button.addEventListener('click', () => {
      for (const item of $('menu').children) item.removeAttribute('aria-current');
      button.setAttribute('aria-current', 'true');
      section = id;
      $('titulo').textContent = labels[id] || id;
      $('contenido').hidden = false;
      $('titulo').focus();
      void openModule();
    });
    $('menu').append(button);
  }
}

async function openModule() {
  const request = nuevaVista();
  $('grupo').hidden = $('label-grupo').hidden = true;
  $('grupo').replaceChildren();
  $('resultado').textContent = 'Cargando…';
  if (section === 'hoy') {
    mostrarHoy(request, destination().id);
    return;
  }
  const vistas = { hoteles: mostrarHoteles, documentos: mostrarDocumentos, caja: mostrarCaja };
  if (vistas[section]) {
    void vistas[section]({ api, destino: destination().id, panel: $('resultado'), vigente: () => request === generation });
    return;
  }
  if (!['itinerario', 'nomina', 'nfc', 'comidas', 'renuncia'].includes(section)) {
    $('resultado').textContent = 'Esta función no está disponible en esta versión del portal.';
    return;
  }
  try {
    const { grupos } = await api('generalGrupos', { destino: destination().id });
    if (request !== generation) return;
    $('resultado').textContent = grupos.length ? '' : 'No hay grupos para este destino.';
    if (!grupos.length) return;
    $('grupo').append(new Option('Selecciona un grupo', ''));
    for (const group of grupos) {
      const inicio = group.fechaInicio?.split('-').reverse().join('/');
      const nombre = section === 'itinerario'
        ? `${inicio ? 'Inicio ' + inicio : 'Sin fecha de inicio'} · ${group.nombre}`
        : group.nombre;
      $('grupo').append(new Option(nombre, group.id));
    }
    $('grupo').hidden = $('label-grupo').hidden = false;
  } catch (error) {
    if (request === generation) $('resultado').textContent = error.message;
  }
}

async function loadItinerary() {
  const request = nuevaVista();
  const result = $('resultado');
  result.replaceChildren();
  const group = $('grupo').value;
  if (!group) return;
  result.textContent = 'Cargando itinerario…';
  try {
    const { dias } = await api('generalItinerario', { destino: destination().id, grupoId: group });
    if (request !== generation) return;
    result.textContent = dias.length ? '' : 'Este grupo aún no tiene itinerario cargado.';
    for (const day of dias) {
      const card = document.createElement('article');
      const title = document.createElement('h3');
      title.textContent = day.fecha.split('-').reverse().join('/');
      const list = document.createElement('ul');
      for (const activity of day.actividades) {
        const item = document.createElement('li');
        item.textContent = [activity.hora, activity.nombre].filter(Boolean).join(' · ');
        list.append(item);
      }
      card.append(title, list);
      result.append(card);
    }
  } catch (error) {
    if (request === generation) result.textContent = error.message;
  }
}

$('destino').addEventListener('change', () => {
  remember($('destino').value);
  renderMenu();
});
$('grupo').addEventListener('change', () => {
  if (section === 'renuncia' || section === 'comidas') {
    const request = nuevaVista();
    const mostrar = section === 'renuncia' ? mostrarRenuncia : mostrarComidas;
    void mostrar({ api, destino: destination().id, grupoId: $('grupo').value,
      panel: $('resultado'), vigente: () => request === generation });
  } else if (section === 'nfc') void abrirGrabadorNfc();
  else if (section === 'itinerario') void loadItinerary();
  else if (section === 'nomina' || section === 'fichas') void cargarPasajeros();
});

const botonPerfil = document.createElement('button');
botonPerfil.type = 'button';
botonPerfil.textContent = 'Mi perfil';
botonPerfil.hidden = true;
const panelPerfil = document.createElement('dialog');
panelPerfil.className = 'cg-perfil-dialogo';
const estilosPerfil = document.createElement('style');
estilosPerfil.textContent = `
  .cg-ingreso { display:grid; gap:.85rem; margin:.75rem 0; }
  .cg-ingreso[hidden] { display:none; }
  .cg-ingreso input { box-sizing:border-box; display:block; width:100%; min-height:46px;
    padding:.65rem; border:1px solid #d5c9df; border-radius:.65rem; font:inherit; }
  .cg-perfil-dialogo { box-sizing:border-box; width:min(calc(100% - 1.5rem),42rem); max-height:90dvh;
    overflow:auto; border:1px solid #d9cde3; border-radius:1rem; background:#f9f6fb; color:#271a32; padding:1rem; }
  .cg-perfil-dialogo::backdrop { background:rgba(36,24,46,.48); }
  .cg-perfil-dialogo h3 { margin:1rem 0 .7rem; }
  .cg-perfil-dialogo form { display:grid; gap:.9rem; margin:0 0 1rem; padding:1rem;
    background:#fff; border:1px solid #e3d9ea; border-radius:.8rem; }
  .cg-perfil-dialogo fieldset { min-width:0; display:grid; gap:.65rem; margin:0; padding:0; border:0; }
  .cg-perfil-dialogo legend { color:#542c6f; font-weight:700; margin-bottom:.5rem; }
  .cg-perfil-dialogo .rtPerfilCampo { display:grid; gap:.35rem; min-width:0; margin:0; }
  .cg-perfil-dialogo input, .cg-perfil-dialogo textarea {
    box-sizing:border-box; width:100%; min-width:0; min-height:44px; padding:.6rem;
    border:1px solid #d5c9df; border-radius:.45rem; background:#fff; font:inherit; color:inherit; }
  .cg-perfil-dialogo input[readonly] { background:#f2edf5; }
  .cg-perfil-dialogo textarea { min-height:5rem; }
  .cg-perfil-dialogo button[type=submit] { background:#4a235f; border-color:#4a235f; color:#fff; }
  .cg-perfil-dialogo [role=status]:empty { display:none; }
`;
document.head.append(estilosPerfil);
const cerrarPerfil = document.createElement('button');
cerrarPerfil.type = 'button';
cerrarPerfil.textContent = 'Cerrar perfil';
cerrarPerfil.addEventListener('click', () => panelPerfil.close());
const contenidoPerfil = document.createElement('section');
panelPerfil.append(cerrarPerfil, contenidoPerfil);
document.body.append(panelPerfil);
document.querySelector('main header').append(botonPerfil);
panelPerfil.addEventListener('close', () => contenidoPerfil.replaceChildren());
botonPerfil.addEventListener('click', () => {
  panelPerfil.showModal();
  void renderPerfilPortal(contenidoPerfil);
});
const ingreso = document.createElement('form');
ingreso.className = 'cg-ingreso';
ingreso.hidden = true;
const correoIngreso = document.createElement('input');
correoIngreso.type = 'email';
correoIngreso.autocomplete = 'username';
correoIngreso.required = true;
const claveIngreso = document.createElement('input');
claveIngreso.type = 'password';
claveIngreso.autocomplete = 'current-password';
claveIngreso.required = true;
const entrar = document.createElement('button');
entrar.textContent = 'Iniciar sesión';
const salir = document.createElement('button');
salir.type = 'button';
salir.textContent = 'Cerrar sesión';
salir.hidden = true;
for (const [nombre, campo] of [['Correo electrónico', correoIngreso], ['Contraseña', claveIngreso]]) {
  const label = document.createElement('label');
  label.textContent = nombre;
  label.append(campo);
  ingreso.append(label);
}
ingreso.append(entrar);
$('estado').after(ingreso);
botonPerfil.after(salir);
ingreso.addEventListener('submit', async event => {
  event.preventDefault();
  entrar.disabled = true;
  try {
    await signInWithEmailAndPassword(auth, correoIngreso.value.trim(), claveIngreso.value);
    ingreso.reset();
  } catch {
    $('estado').textContent = 'No se pudo iniciar sesión. Revisa el correo y la contraseña.';
  } finally {
    entrar.disabled = false;
  }
});
salir.addEventListener('click', async () => {
  try { await signOut(auth); }
  catch { $('estado').textContent = 'No se pudo cerrar la sesión. Intenta nuevamente.'; }
});

onAuthStateChanged(auth, async user => {
  const request = nuevaVista();
  $('portal').hidden = true;
  $('resultado').replaceChildren();
  $('perfil').textContent = '';
  if (panelPerfil.open) panelPerfil.close();
  account = user;
  botonPerfil.hidden = !user;
  ingreso.hidden = !!user;
  salir.hidden = !user;
  if (!user) {
    $('estado').textContent = 'Inicia sesión para entrar a Coordinación General.';
    return;
  }
  $('estado').textContent = 'Consultando asignaciones…';
  try {
    access = await api('generalAcceso');
    if (request !== generation) return;
    const { destinos } = access;
    $('perfil').textContent = access.perfil;
    $('estado').textContent = destinos.length ? '' : 'No tienes asignaciones habilitadas.';
    if (!destinos.length) return;
    const saved = remember();
    $('destino').replaceChildren();
    if (destinos.length > 1) $('destino').append(new Option('Selecciona un destino', ''));
    for (const item of destinos) $('destino').append(new Option(item.nombre, item.id));
    $('destino').value = destinos.some(item => item.id === saved)
      ? saved : destinos.length === 1 ? destinos[0].id : '';
    $('portal').hidden = false;
    renderMenu();
  } catch (error) {
    if (request === generation) $('estado').textContent = error.message;
  }
});

async function abrirGrabadorNfc() {
  const request = nuevaVista();
  const resultado = $('resultado');
  resultado.replaceChildren();
  const grupoId = $('grupo').value;
  if (!grupoId) return;
  const destino = destination().id;
  const usuario = account;
  resultado.textContent = 'Buscando lotes…';

  try {
    const { grupoVentasId } = await apiPasajeros('grupoNfcCoordinacionGeneral', { destino, grupoId });
    if (request !== generation) return;
    const lotes = collection(db, 'ventas_pulseras_nfc_lotes');
    const snapshot = await getDocs(query(lotes, where('groupDocId', '==', grupoVentasId)));
    if (request !== generation) return;

    const disponibles = snapshot.docs.filter(lote => lote.get('activo') !== false);
    if (!disponibles.length) {
      resultado.textContent = 'Este grupo todavía no tiene un lote activo.';
      return;
    }

    const selector = document.createElement('select');
    selector.id = 'nfc-lote';
    selector.append(new Option('Selecciona un lote', ''));
    for (const lote of disponibles) {
      const nombre = lote.get('modalidad') || 'Lote';
      selector.append(new Option(nombre + ' · ' + lote.id, lote.id));
    }

    const zona = document.createElement('div');
    zona.style.marginTop = '1rem';
    resultado.replaceChildren(etiquetaNfc('Lote', selector.id), selector, zona);
    selector.addEventListener('change', () => {
      detenerPulseras();
      detenerPulseras = () => {};
      zona.replaceChildren();
      if (selector.value) montarPulseras(doc(lotes, selector.value), zona, usuario, selector);
    });
    if (disponibles.length === 1) {
      selector.value = disponibles[0].id;
      selector.dispatchEvent(new Event('change'));
    }
  } catch (error) {
    if (request === generation) resultado.textContent = error.message;
  }
}

function etiquetaNfc(texto, id) {
  const label = document.createElement('label');
  label.htmlFor = id;
  label.textContent = texto;
  return label;
}

function montarPulseras(lote, zona, usuario, selectorLote) {
  const modo = document.createElement('select');
  modo.id = 'nfc-modo';
  modo.append(new Option('Grabar nueva', 'nueva'), new Option('Regrabar existente', 'regrabar'));

  const buscar = document.createElement('input');
  buscar.id = 'nfc-buscar';
  buscar.type = 'search';
  buscar.placeholder = 'Nombre o número';
  buscar.style.cssText = 'width:100%;min-height:48px;font:inherit;padding:.75rem';

  const persona = document.createElement('select');
  persona.id = 'nfc-persona';

  const progreso = document.createElement('p');
  progreso.setAttribute('role', 'status');
  progreso.textContent = 'Cargando pulseras…';
  const estado = document.createElement('p');
  estado.setAttribute('role', 'status');

  const grabar = document.createElement('button');
  grabar.type = 'button';
  grabar.textContent = 'Grabar pulsera';
  grabar.disabled = true;

  zona.append(
    etiquetaNfc('Acción', modo.id), modo,
    etiquetaNfc('Buscar pasajero o pulsera', buscar.id), buscar,
    etiquetaNfc('Pulsera', persona.id), persona, progreso, grabar, estado
  );

  let pulseras = [];
  let ocupado = false;
  let errorLectura = '';
  const normalizar = valor => String(valor ?? '').normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();

  function pintar() {
    const previo = persona.value;
    const filtro = normalizar(buscar.value);
    const grabadas = pulseras.filter(item => item.grabada === true).length;
    const visibles = pulseras.filter(item => (item.grabada === true) === (modo.value === 'regrabar'))
      .filter(item => normalizar(item.etiqueta).includes(filtro));
    persona.replaceChildren(new Option('Selecciona una pulsera', ''));
    for (const item of visibles) persona.append(new Option(item.etiqueta, item.id));
    if (visibles.some(item => item.id === previo)) persona.value = previo;
    progreso.textContent = errorLectura || `${grabadas} de ${pulseras.length} grabadas · ${visibles.length} disponibles`;
    grabar.textContent = modo.value === 'regrabar' ? 'Regrabar pulsera' : 'Grabar pulsera';
    grabar.disabled = ocupado || Boolean(errorLectura) || !persona.value;
  }

  modo.addEventListener('change', pintar);
  buscar.addEventListener('input', pintar);
  persona.addEventListener('change', pintar);

  const controlador = new AbortController();
  const detener = onSnapshot(collection(lote, 'pulseras'), snapshot => {
    errorLectura = '';
    pulseras = snapshot.docs.map(documento => {
      const datos = documento.data();
      const nombre = datos.nombre || datos.nombreCompleto || datos.nombrePasajero || datos.codigo;
      return { ...datos, id: documento.id, etiqueta: [datos.numero, nombre].filter(Boolean).join(' · ') };
    }).sort((a, b) => Number(a.numero) - Number(b.numero));
    pintar();
  }, error => {
    errorLectura = 'No se pudo leer el lote. ' + error.message;
    pintar();
  });
  detenerPulseras = () => {
    controlador.abort();
    detener();
  };

  grabar.addEventListener('click', async () => {
    const item = pulseras.find(pulsera => pulsera.id === persona.value);
    if (ocupado || !item?.codigo || account?.uid !== usuario.uid) return;
    if (!window.isSecureContext || !('NDEFReader' in window)) {
      estado.textContent = 'Para grabar, abre este portal en Chrome para Android con NFC activado.';
      return;
    }

    ocupado = true;
    for (const campo of [selectorLote, modo, buscar, persona, grabar]) campo.disabled = true;
    const ref = doc(lote, 'pulseras', item.id);
    const codigo = String(item.codigo);
    const url = 'https://comunicaciones-raitrai.vercel.app/?nfc=' + encodeURIComponent(codigo);
    let escrita = false;
    estado.textContent = 'Acerca la pulsera al teléfono y mantenla quieta.';

    try {
      await new NDEFReader().write({ records: [{ recordType: 'url', data: url }] }, {
        overwrite: true, signal: controlador.signal
      });
      escrita = true;
      await guardarGrabacionNfc(lote, ref, codigo, url, usuario);
      estado.textContent = 'Pulsera grabada correctamente.';
    } catch (error) {
      estado.textContent = escrita
        ? 'La pulsera se escribió, pero el lote no pudo actualizarse. Reintenta con la misma pulsera. ' + error.message
        : 'No se completó la escritura NFC. ' + error.message;
    } finally {
      ocupado = false;
      for (const campo of [selectorLote, modo, buscar, persona]) campo.disabled = false;
      pintar();
    }
  });
}

function guardarGrabacionNfc(lote, ref, codigo, url, usuario) {
  return runTransaction(db, async tx => {
    const [loteSnap, pulseraSnap] = await Promise.all([tx.get(lote), tx.get(ref)]);
    if (!loteSnap.exists() || loteSnap.get('activo') === false ||
        !pulseraSnap.exists() || String(pulseraSnap.get('codigo')) !== codigo) {
      throw new Error('El lote o el código cambió. Actualiza antes de seguir.');
    }

    const ahora = serverTimestamp();
    const correo = usuario.email || '';
    tx.update(ref, {
      estado: 'GRABADA', grabada: true, error: false, mensajeError: '',
      formatoNfc: 'URL', urlNfc: url, codigoNfc: codigo, verificada: true,
      intentos: Number(pulseraSnap.get('intentos') || 0) + 1,
      grabadaAt: ahora, grabadaPor: usuario.displayName || correo,
      grabadaPorCorreo: correo, actualizadoAt: ahora
    });

    if (pulseraSnap.get('grabada') === true) {
      tx.update(lote, { actualizadoAt: ahora });
      return;
    }

    const total = Number(loteSnap.get('total') || 0);
    const grabadas = Math.min(total, Number(loteSnap.get('grabadas') || 0) + 1);
    const pendientes = Math.max(0, total - grabadas);
    tx.update(lote, {
      grabadas, pendientes, estado: pendientes ? 'EN_PROCESO' : 'COMPLETADO',
      iniciadoAt: loteSnap.get('iniciadoAt') || ahora,
      completadoAt: pendientes ? loteSnap.get('completadoAt') || null : ahora,
      actualizadoAt: ahora
    });
  });
}
function mostrarHoy(vista, destino) {
  const resultado = $('resultado');
  const controles = document.createElement('div');
  const fecha = document.createElement('input');
  fecha.type = 'date';
  fecha.setAttribute('aria-label', 'Fecha de operación');
  const hoy = new Intl.DateTimeFormat('sv-SE', { timeZone: 'America/Santiago' }).format(new Date());
  fecha.value = hoy;
  const lista = document.createElement('div');
  let consulta = 0;

  async function consultar() {
    const numero = ++consulta;
    if (!fecha.value) {
      lista.textContent = 'Selecciona una fecha.';
      return;
    }
    lista.textContent = 'Cargando operación…';
    try {
      const datos = await api('generalHoy', { destino, fecha: fecha.value });
      if (vista !== generation || numero !== consulta) return;
      lista.textContent = datos.grupos.length ? '' : 'No hay actividades para este destino en la fecha seleccionada.';
      if (datos.actividadesSinTramo) {
        const aviso = document.createElement('p');
        aviso.textContent = `${datos.actividadesSinTramo} actividades de grupos mixtos tienen tramo por confirmar. Puedes consultarlas en Itinerarios.`;
        lista.append(aviso);
      }
      for (const grupo of datos.grupos) {
        const tarjeta = document.createElement('article');
        const titulo = document.createElement('h3');
        titulo.textContent = grupo.nombre;
        const detalle = document.createElement('ul');
        for (const actividad of grupo.actividades) {
          const item = document.createElement('li');
          item.textContent = [actividad.hora, actividad.nombre, actividad.ciudad].filter(Boolean).join(' · ');
          detalle.append(item);
        }
        tarjeta.append(titulo, detalle);
        lista.append(tarjeta);
      }
    } catch (error) {
      if (vista === generation && numero === consulta) lista.textContent = error.message;
    }
  }

  for (const [texto, pasos] of [['Anterior', -1], ['Hoy', 0], ['Siguiente', 1]]) {
    const boton = document.createElement('button');
    boton.type = 'button';
    boton.textContent = texto;
    boton.addEventListener('click', () => {
      fecha.value ||= hoy;
      if (pasos) fecha.stepUp(pasos);
      else fecha.value = hoy;
      void consultar();
    });
    controles.append(boton);
  }
  controles.style.cssText = 'display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:.5rem;margin:.75rem 0';
  fecha.style.cssText = 'width:100%;min-height:48px;font:inherit';
  fecha.addEventListener('change', consultar);
  resultado.replaceChildren(fecha, controles, lista);
  void consultar();
}
const RT_DOCUMENTOS_PERFIL = [
  ['foto', 'Foto de perfil'],
  ['antecedentes', 'Certificado de antecedentes'],
  ['inhabilidad', 'Certificado de inhabilidad para trabajar con menores'],
  ['primerosAuxilios', 'Certificado de primeros auxilios'],
  ['carnetAnverso', 'Carnet de identidad · anverso'],
  ['carnetReverso', 'Carnet de identidad · reverso']
];

const llamarApiPerfil = api;
const dmy = fecha => fecha.split('-').reverse().join('-');
function obtenerFechaChile() {
  const [ano, mes, dia] = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'America/Santiago'
  }).format(new Date()).split('-');
  return { ano, mes, dia };
}

function fechaPerfilISO(valor) {
  return String(valor ?? '').match(/^\d{4}-\d{2}-\d{2}/)?.[0] || '';
}

function textoVigenciaPerfil(meta, vencimiento) {
  if (!meta) return 'Faltante';

  const vence = fechaPerfilISO(vencimiento);
  if (!vence) return 'Sin vigencia informada';

  const { ano, mes, dia } = obtenerFechaChile();
  const hoy = `${ano}-${String(mes).padStart(2, '0')}-${String(dia).padStart(2, '0')}`;
  const dias = (Date.parse(vence) - Date.parse(hoy)) / 86400000;

  if (dias < 0) return `Vencido · ${dmy(vence)}`;
  if (dias <= 30) return `Por vencer · ${dmy(vence)}`;
  return `Vigente hasta ${dmy(vence)}`;
}

async function prepararArchivoPerfil(file, tipo) {
  const carnet = tipo.startsWith('carnet');
  const max = (carnet ? 12 : tipo === 'foto' ? 5 : 10) * 1024 * 1024;
  const imagenes = ['image/jpeg', 'image/png', 'image/webp'];
  const permitidos = carnet || tipo === 'foto'
    ? imagenes
    : [...imagenes, 'application/pdf'];

  if (!permitidos.includes(file.type)) {
    throw new Error('Selecciona un archivo en un formato admitido.');
  }

  if (!file.size || file.size > max) {
    throw new Error(`Selecciona un archivo de hasta ${max / 1024 / 1024} MB.`);
  }

  let contenido = file;
  let mime = file.type;

  if (carnet) {
    const url = URL.createObjectURL(file);

    try {
      const imagen = new Image();
      await new Promise((resolve, reject) => {
        imagen.onload = resolve;
        imagen.onerror = () => reject(new Error('No se pudo abrir la imagen.'));
        imagen.src = url;
      });

      const escala = Math.min(1, 1200 / Math.max(imagen.width, imagen.height));
      const canvas = document.createElement('canvas');
      canvas.width = Math.max(1, Math.round(imagen.width * escala));
      canvas.height = Math.max(1, Math.round(imagen.height * escala));

      const ctx = canvas.getContext('2d');
      ctx.fillStyle = '#fff';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(imagen, 0, 0, canvas.width, canvas.height);

      let comprimida = null;
      for (const calidad of [0.88, 0.75, 0.6, 0.45, 0.3]) {
        comprimida = await new Promise(resolve =>
          canvas.toBlob(resolve, 'image/jpeg', calidad)
        );
        if (comprimida?.size <= 180000) break;
      }

      if (!comprimida || comprimida.size > 180000) {
        throw new Error('La imagen del carnet es demasiado grande.');
      }

      contenido = comprimida;
      mime = 'image/jpeg';
    } finally {
      URL.revokeObjectURL(url);
    }
  }

  const base64 = await new Promise((resolve, reject) => {
    const lector = new FileReader();
    lector.onload = () => resolve(String(lector.result).split(',')[1]);
    lector.onerror = () => reject(new Error('No se pudo leer el archivo.'));
    lector.readAsDataURL(contenido);
  });

  return {
    nombre: carnet ? `${tipo}.jpg` : file.name,
    mime,
    base64
  };
}

function crearCampoPerfil(texto, tipo, valor = '') {
  const label = document.createElement('label');
  label.className = 'rtPerfilCampo';
  label.append(document.createTextNode(texto));

  const input = document.createElement(tipo === 'textarea' ? 'textarea' : 'input');

  if (tipo !== 'textarea') input.type = tipo;
  if (tipo !== 'file') input.value = valor;

  label.append(input);
  return { label, input };
}

async function abrirDocumentoPerfil(coordinadorId, tipo, nombre) {
  const resultado = await llamarApiPerfil('myFileRead', { coordinadorId, tipo });
  const admitidos = ['application/pdf', 'image/jpeg', 'image/png', 'image/webp'];
  const mime = admitidos.includes(resultado.mime)
    ? resultado.mime
    : 'application/octet-stream';

  const bytes = Uint8Array.from(atob(resultado.base64), letra => letra.charCodeAt(0));
  const url = URL.createObjectURL(new Blob([bytes], { type: mime }));
  const link = document.createElement('a');
  link.href = url;
  link.download = nombre || tipo;
  document.body.append(link);
  link.click();
  link.remove();

  setTimeout(() => URL.revokeObjectURL(url), 60000);
}

function calcularRenovacionPerfil(tipo, valorEmision) {
  const emision = fechaPerfilISO(valorEmision);
  if (!emision) return '';

  const fecha = new Date(`${emision}T12:00:00Z`);
  if (!Number.isFinite(fecha.getTime())) return '';

  if (tipo === 'antecedentes' || tipo === 'inhabilidad') {
    const dias = tipo === 'antecedentes' ? 60 : 90;
    fecha.setUTCDate(fecha.getUTCDate() + dias);
    return fecha.toISOString().slice(0, 10);
  }

  if (tipo === 'primerosAuxilios') {
    const ano = fecha.getUTCFullYear() + 2;
    const mes = fecha.getUTCMonth();
    const dia = fecha.getUTCDate();
    const ultimoDia = new Date(Date.UTC(ano, mes + 1, 0)).getUTCDate();

    return new Date(Date.UTC(ano, mes, Math.min(dia, ultimoDia)))
      .toISOString().slice(0, 10);
  }

  return '';
}

function crearDocumentoPerfil(perfil, tipo, titulo) {
  const foto = tipo === 'foto';
  const carnet = tipo.startsWith('carnet');
  const meta = perfil.documentos?.[tipo];
  const certificado = !foto && !carnet;
  const venceActual = certificado
    ? calcularRenovacionPerfil(tipo, meta?.emision)
    : carnet
      ? perfil.carnetVence || meta?.vence || ''
      : '';

  const form = document.createElement('form');
  form.className = 'card rtPerfilForm';

  const encabezado = document.createElement('h4');
  encabezado.textContent = titulo;
  form.append(encabezado);

  if (foto && perfil.fotoURL) {
    const imagen = document.createElement('img');
    imagen.className = 'rtPerfilFoto';
    imagen.alt = 'Foto de perfil';
    imagen.src = perfil.fotoURL;
    form.append(imagen);
  }

  const estado = document.createElement('p');
  estado.className = 'muted';
  estado.textContent = foto
    ? (perfil.fotoURL ? 'Foto cargada' : 'Sin foto')
    : certificado && meta && !fechaPerfilISO(meta.emision)
      ? 'Falta fecha de emisión'
      : textoVigenciaPerfil(meta, venceActual);
  form.append(estado);

  const mensaje = document.createElement('p');
  mensaje.setAttribute('role', 'status');

  if (meta) {
    const abrir = document.createElement('button');
    abrir.type = 'button';
    abrir.className = 'btn sec';
    abrir.textContent = 'Descargar archivo';

    abrir.onclick = async () => {
      abrir.disabled = true;
      mensaje.textContent = '';

      try {
        await abrirDocumentoPerfil(perfil.id, tipo, meta.nombre);
      } catch (error) {
        mensaje.textContent = error.message;
      } finally {
        abrir.disabled = false;
      }
    };

    form.append(abrir);
  }

  const archivo = crearCampoPerfil('Seleccionar archivo', 'file');
  archivo.input.accept = foto || carnet
    ? 'image/jpeg,image/png,image/webp'
    : 'application/pdf,image/jpeg,image/png,image/webp';
  archivo.input.required = foto || !meta;
  form.append(archivo.label);

  const emision = crearCampoPerfil(
    'Fecha de emisión',
    'date',
    fechaPerfilISO(meta?.emision)
  );

  const vence = crearCampoPerfil(
    carnet ? 'Vencimiento del carnet' : 'Próxima renovación',
    'date',
    fechaPerfilISO(venceActual)
  );

  if (certificado) {
    emision.input.required = true;
    vence.input.readOnly = true;

    const actualizarRenovacion = () => {
      vence.input.value = calcularRenovacionPerfil(tipo, emision.input.value);
    };

    emision.input.addEventListener('input', actualizarRenovacion);
    emision.input.addEventListener('change', actualizarRenovacion);
    form.append(emision.label, vence.label);
  }

  if (carnet) {
    vence.input.required = true;
    form.append(vence.label);
  }

  const guardar = document.createElement('button');
  guardar.type = 'submit';
  guardar.className = 'btn ok';
  guardar.textContent = 'Guardar';
  form.append(guardar, mensaje);

  form.onsubmit = async event => {
    event.preventDefault();
    guardar.disabled = true;
    mensaje.textContent = 'Guardando…';

    try {
      const data = {
        coordinadorId: perfil.id,
        tipo,
        emision: emision.input.value,
        vence: vence.input.value
      };

      const file = archivo.input.files[0];
      if (file) data.archivo = await prepararArchivoPerfil(file, tipo);

      await llamarApiPerfil('myAssetSave', data);

      archivo.input.value = '';
      archivo.input.required = foto;
      mensaje.textContent = 'Guardado. Abre Perfil nuevamente para ver el archivo actualizado.';

      if (!foto) {
        estado.textContent = textoVigenciaPerfil(
          { nombre: file?.name || meta?.nombre },
          vence.input.value
        );
      }
    } catch (error) {
      mensaje.textContent = error.message;
    } finally {
      guardar.disabled = false;
    }
  };

  return form;
}

function crearFormularioDatosPerfil(perfil) {
  const form = document.createElement('form');
  form.className = 'card rtPerfilForm';

  const campos = [
    ['nombre', 'Nombre', 'text'],
    ['correo', 'Correo electrónico', 'email'],
    ['rut', 'RUT', 'text'],
    ['fechaNacimiento', 'Fecha de nacimiento', 'date'],
    ['telefono', 'Teléfono', 'tel'],
    ['direccion', 'Dirección', 'text'],
    ['ciudadResidencia', 'Ciudad de residencia', 'text'],
    ['celularMarcaModelo', 'Marca y modelo del celular', 'text'],
    ['tallaPolera', 'Talla de polera', 'text'],
    ['sexo', 'Sexo', 'text'],
    ['notas', 'Información adicional', 'textarea'],
    ['banco', 'Banco', 'text'],
    ['tipoCuenta', 'Tipo de cuenta', 'text'],
    ['numeroCuenta', 'Número de cuenta', 'text']
  ];

  const inputs = {};
  let bloque;
  const secciones = {
    nombre: 'Identificación y contacto',
    celularMarcaModelo: 'Información adicional',
    banco: 'Datos bancarios'
  };

  for (const [campo, etiqueta, tipo] of campos) {
    const valor = tipo === 'date'
      ? fechaPerfilISO(perfil[campo])
      : perfil[campo] ?? '';
    const { label, input } = crearCampoPerfil(etiqueta, tipo, valor);

    if (campo === 'nombre' || campo === 'correo') input.readOnly = true;
    if (campo === 'tallaPolera') input.placeholder = 'XS, S, M, L, XL o XXL';
    if (campo === 'sexo') input.placeholder = 'hombre, mujer o sin_informar';

    inputs[campo] = input;
    if (secciones[campo]) {
      bloque = document.createElement('fieldset');
      const titulo = document.createElement('legend');
      titulo.textContent = secciones[campo];
      bloque.append(titulo);
      form.append(bloque);
    }
    bloque.append(label);
  }

  const guardar = document.createElement('button');
  guardar.type = 'submit';
  guardar.className = 'btn ok';
  guardar.textContent = 'Guardar mis datos';

  const mensaje = document.createElement('p');
  mensaje.setAttribute('role', 'status');
  form.append(guardar, mensaje);

  form.onsubmit = async event => {
    event.preventDefault();
    guardar.disabled = true;
    mensaje.textContent = 'Guardando…';

    try {
      const banco = ['banco', 'tipoCuenta', 'numeroCuenta'];
      const valores = {};
      const antes = {};

      for (const [campo, input] of Object.entries(inputs)) {
        if (input.readOnly || banco.includes(campo)) continue;

        const anterior = campo === 'fechaNacimiento'
          ? fechaPerfilISO(perfil[campo])
          : String(perfil[campo] ?? '');

        if (input.value.trim() === anterior) continue;

        valores[campo] = input.value.trim();
        antes[campo] = perfil[campo] ?? '';
      }

      const bancoEditado = banco.some(campo =>
        inputs[campo].value.trim() !== String(perfil[campo] ?? '')
      );

      if (bancoEditado) {
        for (const campo of banco) {
          valores[campo] = inputs[campo].value.trim();
          antes[campo] = perfil[campo] ?? '';
        }
        antes.datosTransferir = perfil.datosTransferir ?? '';
      }

      if (!Object.keys(valores).length) {
        mensaje.textContent = 'Tus datos están al día.';
        return;
      }

      await llamarApiPerfil('myProfileSave', {
        coordinadorId: perfil.id,
        valores,
        antes,
        bancoEditado
      });

      const resultado = await llamarApiPerfil('myProfileGet');
      Object.assign(perfil, resultado.perfil);

      for (const [campo, input] of Object.entries(inputs)) {
        input.value = campo === 'fechaNacimiento'
          ? fechaPerfilISO(perfil[campo])
          : perfil[campo] ?? '';
      }

      mensaje.textContent = 'Datos guardados.';
    } catch (error) {
      mensaje.textContent = error.message;
    } finally {
      guardar.disabled = false;
    }
  };

  return form;
}

async function renderPerfilPortal(panel) {
  panel.textContent = 'Cargando perfil…';

  try {
    const { perfil } = await llamarApiPerfil('myProfileGet');
    panel.replaceChildren();

    const titulo = document.createElement('h3');
    titulo.textContent = 'Mi perfil';
    panel.append(titulo, crearFormularioDatosPerfil(perfil));

    const documentos = document.createElement('h3');
    documentos.textContent = 'Foto y documentos';
    panel.append(documentos);

    for (const [tipo, nombre] of RT_DOCUMENTOS_PERFIL) {
      panel.append(crearDocumentoPerfil(perfil, tipo, nombre));
    }

  } catch (error) {
    panel.textContent = error.message;
  }
}
