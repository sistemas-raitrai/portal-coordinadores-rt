import { auth } from './firebase-init-portal.js';
import { onAuthStateChanged } from 'https://www.gstatic.com/firebasejs/11.7.3/firebase-auth.js';

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
  generation++;
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
  const request = ++generation;
  $('grupo').hidden = $('label-grupo').hidden = true;
  $('grupo').replaceChildren();
  $('resultado').textContent = 'Cargando…';
  if (section !== 'itinerario') {
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
  const request = ++generation;
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
$('grupo').addEventListener('change', loadItinerary);

onAuthStateChanged(auth, async user => {
  const request = ++generation;
  $('portal').hidden = true;
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
