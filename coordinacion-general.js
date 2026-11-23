import { auth, db } from './firebase-init-portal.js';
import { onAuthStateChanged } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';

import {
  collection, doc, getDocs, onSnapshot, query, runTransaction, serverTimestamp, where
} from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-firestore.js';

const API = 'https://southamerica-west1-sist-op-rt.cloudfunctions.net/controlCoordinadoresApi';
const $ = id => document.getElementById(id);
const labels = {
  itinerario: 'Itinerarios',
  nomina: 'Nóminas',
  fichas: 'Fichas médicas',
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
  const soloFichas = section === 'fichas';
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

  for (const id of selected.modulos) {
    const button = document.createElement('button');
    button.type = 'button';
    button.textContent = labels[id] || id;
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
  if (!['itinerario', 'nomina', 'fichas', 'nfc'].includes(section)) {
    $('resultado').textContent = 'Este módulo está pendiente de integración.';
    return;
  }
  try {
    const { grupos } = await api('generalGrupos', { destino: destination().id });
    if (request !== generation) return;
    $('resultado').textContent = grupos.length ? '' : 'No hay grupos para este destino.';
    if (!grupos.length) return;
    $('grupo').append(new Option('Selecciona un grupo', ''));
    for (const group of grupos) $('grupo').append(new Option(group.nombre, group.id));
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
  if (section === 'nfc') void abrirGrabadorNfc();
  else if (section === 'itinerario') void loadItinerary();
  else if (section === 'nomina' || section === 'fichas') void cargarPasajeros();
});

onAuthStateChanged(auth, async user => {
  const request = nuevaVista();
  $('portal').hidden = true;
  $('resultado').replaceChildren();
  $('perfil').textContent = '';
  account = user;
  if (!user) {
    $('estado').textContent = 'Inicia sesión desde el Portal de Coordinadores.';
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
