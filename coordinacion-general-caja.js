function nodo(etiqueta, texto = '') {
  const elemento = document.createElement(etiqueta);
  elemento.textContent = texto;
  return elemento;
}
function boton(texto, evento) {
  const elemento = nodo('button', texto);
  elemento.type = 'button';
  elemento.addEventListener('click', evento);
  return elemento;
}
function campo(titulo, elemento) {
  const etiqueta = nodo('label', titulo);
  elemento.style.cssText = 'display:block;width:100%;min-height:44px;font:inherit';
  etiqueta.append(elemento);
  return etiqueta;
}
function input(tipo = 'text', valor = '') {
  const elemento = nodo('input');
  elemento.type = tipo;
  elemento.value = valor;
  return elemento;
}
function select(opciones) {
  const elemento = nodo('select');
  for (const [valor, titulo] of opciones) elemento.append(new Option(titulo, valor));
  return elemento;
}
const dinero = (monto, moneda) => new Intl.NumberFormat('es-CL', {style:'currency', currency:moneda}).format(monto);
const normal = valor => String(valor ?? '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');
const MONEDAS = ['CLP','ARS','BRL','USD'];
const FONDOS = [['proveedores','Pago a proveedores'],['coordinacion','Gastos de coordinación']];
const nombreFondo = clave => FONDOS.find(([id]) => id === clave)?.[1] || 'Saldo anterior';
const etiquetaMovimiento = {
  abono:'Abono recibido', gasto:'Gasto', entrega:'Entrega de fondos', pago_delegado:'Pago delegado',
  pago_iniciado:'Pago iniciado', pago_ejecutado:'Pago ejecutado', devolucion:'Devolución'
};

async function archivoBase64(archivo) {
  if (!archivo || archivo.size > 8 * 1024 * 1024) throw new Error('Adjunta un PDF, PNG o JPG de hasta 8 MB.');
  return new Promise((resolve,reject) => {
    const lector = new FileReader();
    lector.onerror = () => reject(new Error('No se pudo leer el comprobante.'));
    lector.onload = () => resolve({nombre:archivo.name,mime:archivo.type,base64:String(lector.result).split(',')[1]});
    lector.readAsDataURL(archivo);
  });
}

function descargar(datos) {
  const bytes = Uint8Array.from(atob(datos.base64), c => c.charCodeAt(0));
  const url = URL.createObjectURL(new Blob([bytes], {type:datos.mime}));
  const enlace = nodo('a');
  enlace.download = datos.nombre;
  enlace.href = url;
  enlace.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export async function mostrarCaja({ api, destino = '', panel, vigente = () => true, soloPersonal = false }) {
  panel.textContent = 'Cargando Caja…';
  let catalogo;
  try {
    const caja = await api('generalCaja');
    if (!vigente()) return;
    const mostrarSaldos = saldos => Object.entries(saldos || {})
      .map(([moneda,monto]) => `${moneda} ${dinero(monto,moneda)}`).join(' · ') || 'Sin saldo registrado.';
    const saldoAnterior = Object.values(caja.saldoAnterior || {}).some(monto => monto > 0);
    const saldos = nodo('div');
    saldos.className = 'cg-caja-saldos';
    const tarjetas = !soloPersonal && caja.general && caja.saldosPorFondo
      ? [...FONDOS.map(([fondo,titulo]) => [titulo,caja.saldosPorFondo[fondo]]),
        ...(saldoAnterior ? [['Saldo anterior', caja.saldoAnterior]] : [])]
      : [['Saldo disponible', caja.saldos]];
    for (const [titulo, valores] of tarjetas) {
      const tarjeta = nodo('article');
      tarjeta.append(nodo('h3',titulo),nodo('p',mostrarSaldos(valores)));
      saldos.append(tarjeta);
    }
    const acciones = nodo('div');
    acciones.className = 'cg-caja-acciones';
    const secundarias = nodo('div');
    secundarias.className = 'cg-caja-secundarias';
    const formulario = nodo('div');
    const estado = nodo('p');
    estado.setAttribute('role','status');
    const lista = nodo('div');
    panel.replaceChildren(saldos,acciones,formulario,estado,lista);

    async function pedirCatalogo() {
      catalogo ||= await api('generalCajaCatalogo',{destino});
      return catalogo;
    }

    function guardarFormulario(items, etiqueta, enviar) {
      formulario.replaceChildren(...items);
      const form = nodo('form');
      form.append(...formulario.childNodes);
      const guardar = nodo('button',etiqueta);
      guardar.type = 'submit';
      form.append(guardar);
      formulario.append(form);
      const id = crypto.randomUUID();
      form.addEventListener('submit', async evento => {
        evento.preventDefault();
        guardar.disabled = true;
        estado.textContent = 'Guardando…';
        try {
          await enviar(id);
          if (vigente()) await mostrarCaja({api,destino,panel,vigente,soloPersonal});
        } catch (error) {
          if (vigente()) estado.textContent = error.message;
          guardar.disabled = false;
        }
      });
    }

    function buscadorPersonas(personas, vacio, titulo = 'Responsable') {
      const filtro = input('search');
      filtro.placeholder = 'Buscar por nombre';
      const persona = select([['',vacio]]);
      function filtrar() {
        const elegido = persona.value;
        persona.replaceChildren(new Option(vacio,''));
        for (const item of personas) if (normal(item.nombre).includes(normal(filtro.value))) {
          persona.append(new Option(item.nombre,item.id));
        }
        if ([...persona.options].some(x => x.value === elegido)) persona.value = elegido;
      }
      filtro.addEventListener('input',filtrar);
      filtrar();
      return {persona, filtros:[campo('Buscar persona',filtro), campo(titulo,persona)]};
    }

    async function abrir(tipo, entrega = null, abonoAdmin = false) {
      estado.textContent = '';
      formulario.replaceChildren(nodo('p','Cargando formulario…'));
      try {
        const requierePersona = abonoAdmin || ['entrega','pago'].includes(tipo) || (tipo === 'gasto' && !caja.general);
        const registrarOrigen = tipo === 'abono';
        const opcionesFondo = tipo === 'gasto' ? FONDOS.filter(([id]) => id === 'coordinacion')
          : tipo === 'pago' ? FONDOS.filter(([id]) => id === 'proveedores') : FONDOS;
        const fondo = select([...opcionesFondo,
          ...(saldoAnterior && ['gasto','pago','entrega'].includes(tipo) ? [['','Saldo anterior']] : [])]);
        const datos = requierePersona || registrarOrigen ? await pedirCatalogo() : null;
        if (!vigente()) return;
        const moneda = select((entrega ? [entrega.moneda] : MONEDAS).map(x=>[x,x]));
        const monto = input('number');
        monto.required = true;
        monto.min = '0.01';
        monto.step = '0.01';
        const concepto = input('text', entrega ? 'Devolución · ' + entrega.concepto : '');
        concepto.required = true;
        concepto.maxLength = 200;
        const grupo = select([['','Sin grupo asociado']]);
        if (tipo === 'pago' && destino) {
          const {grupos} = await api('generalGrupos',{destino});
          for (const item of grupos) grupo.append(new Option(item.nombre,item.id));
        }
        if (!vigente()) return;
        const comprobante = input('file');
        comprobante.accept = 'application/pdf,image/jpeg,image/png';
        comprobante.required = ['abono','gasto'].includes(tipo);
        const campos = [campo('Concepto',concepto)];
        if (tipo !== 'devolucion') campos.push(campo('Fondo',fondo));
        const campoMoneda = campo(tipo === 'pago' ? 'Moneda que sale de Caja' : 'Moneda',moneda);
        const campoMonto = campo(tipo === 'gasto' ? 'Monto descontado de Caja' : 'Monto',monto);
        campos.push(campoMoneda,campoMonto);
        const origen = registrarOrigen
          ? buscadorPersonas(datos.personas.filter(p => p.id !== caja.id), 'Rai Trai', 'Entregado por') : null;
        if (origen) campos.push(...origen.filtros);
        const monedaLocal = select(['BRL','ARS','CLP'].map(m => [m,m]));
        if (normal(destino).includes('bariloche')) monedaLocal.value = 'ARS';
        const montoLocal = input('number');
        const archivoBancario = input('file');
        archivoBancario.accept = comprobante.accept;
        const detalleGasto = nodo('div');
        detalleGasto.append(campo('Moneda de la compra',monedaLocal),campo('Monto en moneda local',montoLocal),
          campo('Comprobante bancario del cargo en CLP',archivoBancario));
        const etiquetaArchivo = campo(tipo === 'gasto' ? 'Comprobante de la compra' : 'Comprobante de recepción',comprobante);
        function configurarImportes() {
          const gastoCLP = tipo === 'gasto' && fondo.value === 'coordinacion';
          moneda.disabled = gastoCLP;
          if (gastoCLP) moneda.value = 'CLP';
          monto.min = monto.step = moneda.value === 'CLP' ? '1' : '0.01';
          montoLocal.min = montoLocal.step = monedaLocal.value === 'CLP' ? '1' : '0.01';
          detalleGasto.hidden = !gastoCLP;
          montoLocal.required = archivoBancario.required = gastoCLP;
        }
        fondo.addEventListener('change',configurarImportes);
        moneda.addEventListener('change',configurarImportes);
        monedaLocal.addEventListener('change',configurarImportes);
        configurarImportes();
        const fecha = input('date', new Intl.DateTimeFormat('sv-SE', {timeZone:'America/Santiago'}).format(new Date()));
        if (tipo === 'abono') {
          fecha.required = true;
          fecha.max = fecha.value;
          campos.push(campo('Fecha de recepción',fecha));
        }
        if (tipo === 'pago') campos.push(campo('Grupo asociado',grupo));
        if (comprobante.required) campos.push(etiquetaArchivo);
        if (tipo === 'gasto') campos.push(detalleGasto);
        let persona;
        if (requierePersona) {
          const personas = datos.personas.filter(p => p.id !== caja.id &&
            (abonoAdmin ? p.general : tipo === 'gasto' ? p.puedeRecibirBorrador : true));
          const selector = buscadorPersonas(personas, tipo === 'pago' ? 'Pago directo' : 'Selecciona una persona');
          persona = selector.persona;
          persona.required = tipo !== 'pago';
          campos.push(...selector.filtros);
        }
        const proveedor = select([['','Selecciona un proveedor'],['otro','Otro proveedor'],
          ...(datos?.proveedores || []).map(x=>[`${x.tipo}:${x.id}`, `${x.nombre} · ${x.servicio}`])]);
        const otro = input();
        const labelOtro = campo('Nombre del proveedor',otro);
        labelOtro.hidden = true;
        if (tipo === 'pago') {
          proveedor.required = true;
          proveedor.addEventListener('change',()=> {
            labelOtro.hidden = proveedor.value !== 'otro';
            otro.required = !labelOtro.hidden;
          });
          campos.push(campo('Proveedor / servicio',proveedor), labelOtro);
        }
        guardarFormulario(campos,'Guardar',async id => {
          const [proveedorTipo, proveedorId] = proveedor.value.split(/:(.*)/s);
          await api('generalCajaGuardar',{
            id,tipo,destino,moneda:moneda.value,monto:monto.value,concepto:concepto.value,
            fondo:tipo === 'devolucion' ? '' : fondo.value,
            entregadoPorId:origen?.persona.value || '', monedaLocal:monedaLocal.value, montoLocal:montoLocal.value,
            archivoBancario:archivoBancario.required ? await archivoBase64(archivoBancario.files[0]) : null,
            fecha:tipo === 'abono' ? fecha.value : '', personaId:persona?.value || '',grupoId:grupo.value,entregaId:entrega?.id || '',
            proveedorTipo,proveedorId,proveedorNombre:otro.value,
            archivo:comprobante.required ? await archivoBase64(comprobante.files[0]) : null
          });
        });
      } catch (error) {
        if (vigente()) formulario.textContent = error.message;
      }
    }

    if (!soloPersonal && (caja.general || caja.admin)) {
      const recibir = boton(caja.admin ? 'Abonar a otro general' : 'Registrar dinero recibido',
        () => abrir('abono',null,caja.admin));
      recibir.className = 'cg-caja-recibir';
      acciones.append(recibir);
    }
    if (!soloPersonal && caja.general) secundarias.append(
      boton('Registrar gasto',()=>abrir('gasto')),
      boton('Pagar proveedor',()=>abrir('pago')),
      boton('Entregar a coordinador',()=>abrir('entrega'))
    );
    if (!soloPersonal && caja.asistente && !caja.general) {
      secundarias.append(boton('Preparar gasto',()=>abrir('gasto')));
    }
    if (secundarias.childNodes.length) acciones.append(secundarias);

    for (const pago of caja.pagos.filter(x=>x.estado==='pendiente')) {
      const tarjeta = nodo('article');
      tarjeta.append(nodo('h3','Pago pendiente'), nodo('p', pago.concepto),
        nodo('p', dinero(pago.monto,pago.moneda) + ' · ' + (pago.proveedor.proveedorNombre || pago.proveedor.hotelNombre || 'Proveedor')));
      if (pago.responsableId === caja.id) tarjeta.append(boton('Registrar pago y comprobante',()=>{
        const monto = input('number',String(pago.monto));
        monto.required = true;
        monto.min = monto.step = pago.moneda === 'CLP' ? '1' : '0.01';
        monto.max = String(pago.monto);
        const monedaProveedor = select(MONEDAS.map(m => [m,m]));
        monedaProveedor.value = pago.moneda;
        const montoProveedor = input('number');
        const detalleProveedor = campo('Monto pagado al proveedor',montoProveedor);
        function configurarProveedor() {
          const distinta = monedaProveedor.value !== pago.moneda;
          detalleProveedor.hidden = !distinta;
          montoProveedor.required = distinta;
          montoProveedor.min = montoProveedor.step = monedaProveedor.value === 'CLP' ? '1' : '0.01';
        }
        monedaProveedor.addEventListener('change',configurarProveedor);
        configurarProveedor();
        const forma = select(['EFECTIVO','TRANSFERENCIA','Envío Global66','OTRO'].map(x=>[x,x]));
        const comprobante = input('file');
        comprobante.required = true;
        comprobante.accept = 'application/pdf,image/jpeg,image/png';
        guardarFormulario([campo('Monto que salió de Caja · ' + pago.moneda,monto),
          campo('Moneda del pago al proveedor',monedaProveedor),detalleProveedor,campo('Forma de pago',forma),
          campo('Comprobante',comprobante)], 'Confirmar pago', async id => {
          await api('generalCajaConfirmarPago',{id,pagoId:pago.id,monto:monto.value,
            monedaProveedor:monedaProveedor.value, montoProveedor:montoProveedor.required ? montoProveedor.value : monto.value,
            formaPago:forma.value,archivo:await archivoBase64(comprobante.files[0])});
        });
      }));
      lista.append(tarjeta);
    }

    if (!soloPersonal) for (const borrador of caja.borradores) {
      const tarjeta = nodo('article');
      tarjeta.append(nodo('h3','Gasto pendiente de aprobación'),
        nodo('p',borrador.concepto + ' · ' + dinero(borrador.monto,borrador.moneda)),
        ...[true,false].map(aprobar => boton(aprobar ? 'Aprobar' : 'Rechazar', async () => {
          try {
            await api('generalCajaResolverBorrador',{id:crypto.randomUUID(),borradorId:borrador.id,aprobar});
            if (vigente()) await mostrarCaja({api,destino,panel,vigente,soloPersonal});
          } catch(error) { estado.textContent = error.message; }
        })));
      lista.append(tarjeta);
    }

    lista.append(nodo('h3','Movimientos'));
    for (const mov of caja.movimientos) {
      const tarjeta = nodo('article');
      tarjeta.append(nodo('strong',etiquetaMovimiento[mov.tipo] || mov.tipo),
        nodo('p',`${mov.fecha} · ${dinero(mov.monto,mov.moneda)} · ${mov.concepto}`));
      if (mov.fondo) tarjeta.append(nodo('p',nombreFondo(mov.fondo)));
      if (mov.entregadoPor) tarjeta.append(nodo('p','Entregado por ' + mov.entregadoPor.nombre));
      if (mov.compraLocal) tarjeta.append(nodo('p','Compra local · ' + dinero(mov.compraLocal.monto,mov.compraLocal.moneda)));
      if (mov.monedaProveedor && mov.monedaProveedor !== mov.moneda) {
        tarjeta.append(nodo('p','Pagado al proveedor · ' + dinero(mov.montoProveedor,mov.monedaProveedor)));
      }
      for (const [visible,clase,titulo] of [
        [mov.comprobante,'principal',mov.compraLocal ? 'Ver comprobante de compra' : 'Ver comprobante'],
        [mov.comprobanteBancario,'bancario','Ver comprobante bancario']
      ]) if (visible) tarjeta.append(boton(titulo,async()=>{
        try { descargar(await api('generalCajaComprobante',{movimientoId:mov.id,clase})); }
        catch(error) { estado.textContent = error.message; }
      }));
      if (['entrega','pago_delegado'].includes(mov.tipo)) {
        const pago = caja.pagos.find(x=>x.id === mov.pagoId);
        const disponible = mov.unidades - (mov.unidadesDevueltas || 0) - (pago?.unidadesPagadas || 0);
        if (disponible > 0 && (!pago || pago.estado === 'pagado')) {
          tarjeta.append(boton('Registrar devolución',()=>abrir('devolucion',mov)));
        }
      }
      lista.append(tarjeta);
    }
    if (!caja.movimientos.length) lista.append(nodo('p','Sin movimientos registrados.'));
  } catch (error) {
    if (vigente()) panel.textContent = error.message;
  }
}
