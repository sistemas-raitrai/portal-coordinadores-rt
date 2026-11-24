function elemento(tipo, texto = '') {
  const nodo = document.createElement(tipo);
  nodo.textContent = texto;
  return nodo;
}

function boton(texto, accion) {
  const nodo = elemento('button', texto);
  nodo.type = 'button';
  nodo.addEventListener('click', accion);
  return nodo;
}

function etiqueta(texto, campo) {
  const nodo = elemento('label', texto);
  nodo.append(campo);
  return nodo;
}

function fechaOperacion() {
  const campo = elemento('input');
  campo.type = 'date';
  campo.style.cssText = 'display:block;width:100%;min-height:48px;font:inherit';
  campo.value = new Intl.DateTimeFormat('sv-SE', { timeZone: 'America/Santiago' }).format(new Date());
  return campo;
}

const genero = datos => `Mujeres ${datos.mujeres} · Hombres ${datos.hombres} · Otro ${datos.otro} · Sin dato ${datos.sinDato}`;

export function mostrarHoteles({ api, destino, panel, vigente }) {
  const fecha = fechaOperacion();
  const detalle = elemento('div');
  let consulta = 0;
  let temporada = false;

  async function cargar() {
    const numero = ++consulta;
    detalle.textContent = 'Cargando hoteles…';
    try {
      const datos = await api('generalHoteles', { destino, fecha: temporada ? '' : fecha.value });
      if (!vigente() || numero !== consulta) return;
      detalle.textContent = datos.hoteles.length ? '' : 'No hay hoteles asignados para esta consulta.';
      const conflictos = new Set(datos.conflictos);
      for (const hotel of datos.hoteles) {
        const tarjeta = elemento('article');
        tarjeta.append(elemento('h3', hotel.nombre));
        const confirmadas = hotel.estadias.filter(fila => !conflictos.has(fila.grupoId));
        const grupos = new Set(confirmadas.map(fila => fila.grupoId));
        const coord = new Set(confirmadas.flatMap(fila => fila.coordinadores.map(c => c.id)));
        if (temporada) {
          tarjeta.append(elemento('p', `${grupos.size} grupos durante la temporada`));
        } else {
          const suma = campo => confirmadas.reduce((s, fila) => s + fila[campo], 0);
          tarjeta.append(elemento('p', [
            `${grupos.size} grupos`, `${suma('estudiantes')} estudiantes`,
            `${suma('adultos')} adultos`, `${suma('pasajeros')} pasajeros`,
            `${coord.size} coordinadores`, `${suma('pasajeros') + coord.size} personas`
          ].join(' · ')));
        }
        for (const fila of hotel.estadias) {
          const bloque = elemento('details');
          bloque.append(
            elemento('summary', fila.nombre),
            elemento('p', `Entrada ${fila.checkIn} · Salida ${fila.checkOut}`),
            elemento('p', `${fila.estudiantes} estudiantes · ${fila.adultos} adultos · ${fila.pasajeros} pasajeros`),
            elemento('p', 'Coordinadores: ' + (fila.coordinadores.map(x => x.nombre).join(', ') || 'Sin registro'))
          );
          if (conflictos.has(fila.grupoId)) bloque.append(elemento('p', 'Asignaciones cruzadas para esta noche. Grupo excluido del total.'));
          const resultado = elemento('p');
          const cargarDetalle = boton('Ver desglose por género', async () => {
            cargarDetalle.disabled = true;
            resultado.textContent = 'Cargando…';
            try {
              const info = await api('generalHotelDetalle', { destino, grupoId: fila.grupoId });
              if (!vigente() || numero !== consulta) return;
              const guias = { mujeres: 0, hombres: 0, otro: 0, sinDato: 0 };
              const porId = new Map(info.coordinadores.map(c => [c.id, c.genero]));
              for (const c of fila.coordinadores) guias[porId.get(c.id) || 'sinDato']++;
              resultado.replaceChildren(
                elemento('p', 'Pasajeros con inscripción activa · ' + genero(info.pasajeros)),
                elemento('p', 'Coordinadores · ' + genero(guias))
              );
            } catch (error) {
              resultado.textContent = error.message;
            } finally {
              cargarDetalle.disabled = false;
            }
          });
          bloque.append(cargarDetalle, resultado);
          tarjeta.append(bloque);
        }
        detalle.append(tarjeta);
      }
    } catch (error) {
      if (vigente() && numero === consulta) detalle.textContent = error.message;
    }
  }

  fecha.addEventListener('change', () => { temporada = false; void cargar(); });
  panel.replaceChildren(
    etiqueta('Noche', fecha),
    boton('Esta noche', () => { fecha.value = fechaOperacion().value; temporada = false; void cargar(); }),
    boton('Temporada', () => { temporada = true; void cargar(); }),
    detalle
  );
  void cargar();
}

export function mostrarComidas({ api, destino, grupoId, panel, vigente }) {
  panel.replaceChildren();
  if (!grupoId) return;
  const fecha = fechaOperacion();
  const detalle = elemento('div');
  panel.append(etiqueta('Fecha de servicio', fecha), detalle);
  let consulta = 0;
  async function cargar() {
    const numero = ++consulta;
    detalle.textContent = 'Cargando comidas…';
    try {
      const datos = await api('generalComidas', { destino, grupoId, fecha: fecha.value });
      if (!vigente() || numero !== consulta) return;
      detalle.replaceChildren(elemento('p', `${datos.personas.length} personas con alimentación especial informada.`));
      if (!datos.comidas.length) detalle.append(elemento('p', 'Sin comidas identificadas para ese día.'));
      for (const comida of datos.comidas) {
        const tarjeta = elemento('article');
        tarjeta.append(
          elemento('h3', [comida.hora, comida.tipo].filter(Boolean).join(' · ')),
          elemento('p', comida.actividad),
          elemento('p', comida.lugar || 'Lugar por confirmar')
        );
        if (comida.asistentesPorConfirmar) tarjeta.append(elemento('p', 'Asistencia individual por confirmar.'));
        detalle.append(tarjeta);
      }
      const lista = elemento('ul');
      for (const persona of datos.personas) {
        lista.append(elemento('li', [
          persona.nombre, ...persona.tipos, ...persona.restricciones,
          ...persona.alergias.map(a => 'Alergia: ' + a), persona.detalle
        ].filter(Boolean).join(' · ')));
      }
      detalle.append(lista);
    } catch (error) {
      if (vigente() && numero === consulta) detalle.textContent = error.message;
    }
  }
  fecha.addEventListener('change', cargar);
  void cargar();
}

function leerBase64(archivo) {
  return new Promise((resolver, rechazar) => {
    const lector = new FileReader();
    lector.onload = () => resolver(String(lector.result).split(',')[1]);
    lector.onerror = () => rechazar(lector.error);
    lector.readAsDataURL(archivo);
  });
}

export async function mostrarDocumentos({ api, destino, panel, vigente }) {
  panel.textContent = 'Cargando carpetas…';
  const seleccionar = elemento('select');
  const detalle = elemento('div');
  const mensaje = elemento('p');
  mensaje.setAttribute('role', 'status');
  let consulta = 0;

  async function cargarCarpetas(elegir) {
    const datos = await api('generalDocumentos', { destino });
    if (!vigente()) return datos;
    seleccionar.replaceChildren(new Option('Selecciona una carpeta', ''));
    for (const carpeta of datos.carpetas) seleccionar.append(new Option(carpeta.nombre, carpeta.id));
    if (elegir) seleccionar.value = elegir;
    return datos;
  }

  try {
    const datos = await cargarCarpetas();
    if (!vigente()) return;
    panel.replaceChildren(etiqueta('Carpeta', seleccionar));
    if (datos.administrador) {
      const formulario = elemento('form');
      const nombre = elemento('input');
      nombre.required = true;
      nombre.maxLength = 100;
      const crear = elemento('button', 'Crear carpeta');
      formulario.append(etiqueta('Nombre de carpeta', nombre), crear);
      formulario.addEventListener('submit', async evento => {
        evento.preventDefault();
        crear.disabled = true;
        const id = crypto.randomUUID();
        try {
          await api('generalCarpetaCrear', { destino, id, nombre: nombre.value });
          nombre.value = '';
          await cargarCarpetas(id);
          await abrirCarpeta();
        } catch (error) {
          mensaje.textContent = error.message;
        } finally {
          crear.disabled = false;
        }
      });
      panel.append(formulario);
    }
    panel.append(mensaje, detalle);
    seleccionar.addEventListener('change', abrirCarpeta);

    async function abrirCarpeta() {
      const actual = ++consulta;
      const carpetaId = seleccionar.value;
      detalle.replaceChildren();
      if (!carpetaId) return;
      try {
        const { archivos } = await api('generalArchivos', { destino, carpetaId });
        if (!vigente() || actual !== consulta) return;
        if (datos.administrador) prepararCarga(carpetaId);
        for (const archivo of archivos) {
          detalle.append(boton('Descargar ' + archivo.nombre, async () => {
            try {
              const datos = await api('generalDocumentoDescargar', { destino, carpetaId, id: archivo.id });
              if (!vigente() || actual !== consulta) return;
              const bytes = Uint8Array.from(atob(datos.base64), c => c.charCodeAt(0));
              const url = URL.createObjectURL(new Blob([bytes], { type: 'application/pdf' }));
              const enlace = elemento('a');
              enlace.href = url;
              enlace.download = datos.nombre;
              enlace.click();
              setTimeout(() => URL.revokeObjectURL(url), 1000);
            } catch (error) {
              mensaje.textContent = error.message;
            }
          }));
        }
      } catch (error) {
        if (vigente() && actual === consulta) mensaje.textContent = error.message;
      }
    }

    function prepararCarga(carpetaId) {
      const campo = elemento('input');
      campo.type = 'file';
      campo.accept = '.pdf,application/pdf';
      campo.multiple = true;
      let pendientes = [];
      const subir = boton('Subir PDF seleccionados', async () => {
        subir.disabled = campo.disabled = seleccionar.disabled = true;
        const lote = pendientes;
        const fallidos = [];
        let completados = 0;
        try {
          for (const item of lote) {
            if (!vigente()) break;
            try {
              if (item.archivo.size > 8 * 1024 * 1024) throw new Error('Supera 8 MB.');
              await api('generalDocumentoSubir', {
                destino, carpetaId, id: item.id, nombre: item.archivo.name,
                base64: await leerBase64(item.archivo)
              });
              completados++;
            } catch (error) {
              fallidos.push({ ...item, error: error.message });
            }
            mensaje.textContent = `${completados} subidos · ${fallidos.length} con error · ${lote.length} seleccionados`;
          }
          if (!vigente()) return;
          pendientes = fallidos;
          if (!fallidos.length) {
            await abrirCarpeta();
          } else {
            subir.textContent = 'Reintentar archivos fallidos';
            mensaje.textContent += '. ' + fallidos.map(x => `${x.archivo.name}: ${x.error}`).join(' · ');
          }
        } finally {
          campo.disabled = seleccionar.disabled = false;
          subir.disabled = !pendientes.length;
        }
      });
      subir.disabled = true;
      campo.addEventListener('change', () => {
        pendientes = Array.from(campo.files, archivo => ({ archivo, id: crypto.randomUUID() }));
        subir.disabled = !pendientes.length;
        subir.textContent = 'Subir PDF seleccionados';
      });
      detalle.append(etiqueta('Seleccionar PDF', campo), subir);
    }
  } catch (error) {
    if (vigente()) panel.textContent = error.message;
  }
}

export async function mostrarRenuncia({ api, destino, grupoId, panel, vigente }) {
  panel.replaceChildren();
  if (!grupoId) return;
  panel.textContent = 'Cargando actividades y adultos de la nómina…';
  try {
    const { adultos, actividades } = await api('generalRenunciaOpciones', { destino, grupoId });
    if (!vigente()) return;
    const actividad = elemento('select');
    actividad.append(new Option('Selecciona una actividad', ''));
    for (const item of actividades) actividad.append(new Option(`${item.fecha} · ${item.nombre}`, item.id));
    const adulto = elemento('select');
    adulto.append(new Option('Selecciona un adulto', ''));
    for (const item of adultos) adulto.append(new Option(item.nombre, item.id));
    const motivo = elemento('textarea');
    motivo.maxLength = 500;
    motivo.style.cssText = 'display:block;width:100%;font:inherit';
    const correo = elemento('input');
    correo.type = 'email';
    correo.style.cssText = 'display:block;width:100%;font:inherit';
    const estado = elemento('p');
    estado.setAttribute('role', 'status');
    const enlace = elemento('a', 'Abrir enlace de firma');
    enlace.hidden = true;
    const copiar = boton('Copiar enlace', async () => {
      try {
        await navigator.clipboard.writeText(enlace.href);
        estado.textContent = 'Enlace copiado.';
      } catch {
        estado.textContent = 'Abre el enlace y copia su dirección.';
      }
    });
    copiar.hidden = true;

    async function guardar(prueba) {
      pruebaBtn.disabled = emitirBtn.disabled = true;
      estado.textContent = 'Guardando…';
      try {
        const datos = await api(prueba ? 'generalRenunciaPrueba' : 'generalRenunciaCrear', {
          destino, grupoId, adultoId: adulto.value, actividadId: actividad.value,
          motivo: motivo.value, correoPrueba: correo.value, id: crypto.randomUUID()
        });
        if (!vigente()) return;
        if (prueba) {
          estado.textContent = 'Correo de prueba en cola. Revisa la casilla ingresada.';
        } else {
          const url = new URL('./firma-renuncia-actividad.html', location.href);
          url.hash = new URLSearchParams({ id: datos.id, token: datos.token });
          enlace.href = url.href;
          enlace.hidden = copiar.hidden = false;
          estado.textContent = 'Constancia pendiente de firma. Comparte el enlace con el adulto seleccionado.';
        }
      } catch (error) {
        if (vigente()) estado.textContent = error.message;
      } finally {
        pruebaBtn.disabled = false;
        emitirBtn.disabled = !enlace.hidden;
      }
    }

    const pruebaBtn = boton('Enviar prueba', () => guardar(true));
    const emitirBtn = boton('Generar enlace de firma', () => guardar(false));
    panel.replaceChildren(
      etiqueta('Actividad', actividad), etiqueta('Adulto acompañante', adulto),
      etiqueta('Motivo', motivo), etiqueta('Correo de prueba', correo),
      pruebaBtn, emitirBtn, estado, enlace, copiar
    );
  } catch (error) {
    if (vigente()) panel.textContent = error.message;
  }
}
