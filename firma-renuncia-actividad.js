const $ = id => document.getElementById(id);
const params = new URLSearchParams(location.hash.slice(1));
const id = params.get('id');
const token = params.get('token');
const canvas = $('firma');
const ctx = canvas.getContext('2d');
ctx.lineWidth = 3;
ctx.lineCap = 'round';
ctx.lineJoin = 'round';
let puntero = null;
let firmado = false;
function punto(evento) {
  const r = canvas.getBoundingClientRect();
  return [(evento.clientX - r.left) * canvas.width / r.width,
    (evento.clientY - r.top) * canvas.height / r.height];
}
canvas.addEventListener('pointerdown', e => {
  if (puntero !== null) return;
  puntero = e.pointerId;
  canvas.setPointerCapture(puntero);
  ctx.beginPath();
  ctx.moveTo(...punto(e));
});
canvas.addEventListener('pointermove', e => {
  if (puntero !== e.pointerId) return;
  ctx.lineTo(...punto(e));
  ctx.stroke();
  firmado = true;
});
for (const evento of ['pointerup', 'pointercancel', 'lostpointercapture']) {
  canvas.addEventListener(evento, e => { if (puntero === e.pointerId) puntero = null; });
}
$('limpiar').addEventListener('click', () => {
  ctx.clearRect(0, 0, canvas.width, canvas.height);
  firmado = false;
});
async function consultar(action, datos = {}) {
  const response = await fetch('https://southamerica-west1-sist-op-rt.cloudfunctions.net/firmarRenunciaActividad', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ action, id, token, ...datos })
  });
  const r = await response.json().catch(() => ({}));
  if (!response.ok || !r.ok) throw new Error(r.error || 'No se pudo consultar la constancia.');
  return r;
}
$('form').addEventListener('submit', async evento => {
  evento.preventDefault();
  if (!firmado) { $('estado').textContent = 'Dibuja tu firma antes de continuar.'; return; }
  $('enviar').disabled = true;
  try {
    await consultar('firmar', { firma: canvas.toDataURL('image/png'), acepta: $('acepta').checked });
    $('form').hidden = true;
    $('estado').textContent = 'Constancia firmada. Se prepararon las copias por correo.';
  } catch (error) {
    $('estado').textContent = error.message;
  } finally {
    $('enviar').disabled = false;
  }
});
try {
  const r = await consultar('leer');
  for (const [titulo, contenido] of [
    ['Grupo', r.grupo], ['Actividad', r.actividad.nombre], ['Fecha', r.actividad.fecha],
    ['Adulto acompañante', r.adulto], ['Motivo', r.motivo]
  ]) {
    const nodo = document.createElement('p');
    nodo.textContent = titulo + ': ' + contenido;
    $('datos').append(nodo);
  }
  $('form').hidden = r.estado !== 'pendiente';
  $('estado').textContent = r.estado === 'firmada' ? 'Esta constancia ya está firmada.' : '';
} catch (error) {
  $('estado').textContent = error.message;
}
