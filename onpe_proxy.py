#!/usr/bin/env python3
"""
ONPE 2026 — Proxy v28 (Robusto: preserva caché, datos de respaldo, 27 regiones)
Uso local:  python3 onpe_proxy.py
Render.com: PORT env var tomada automáticamente
"""
import http.server, json, threading, time
import urllib.request, urllib.error, urllib.parse
import gzip, os, sys, concurrent.futures, unicodedata
from datetime import datetime, timezone

PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 30
CACHE_FILE  = "onpe_cache.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

EP_TOTALES    = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"
EP_MAP_JSON   = "https://resultadoelectoral.onpe.gob.pe/assets/lib/amcharts5/geodata/json/peruLow.json"

UBIGEOS = {
    "010000":"Amazonas",        "020000":"Áncash",          "030000":"Apurímac",
    "040000":"Arequipa",        "050000":"Ayacucho",        "060000":"Cajamarca",
    "070000":"Callao",          "080000":"Cusco",           "090000":"Huancavelica",
    "100000":"Huánuco",         "110000":"Ica",             "120000":"Junín",
    "130000":"La Libertad",     "140000":"Lambayeque",      "150000":"Lima Provincias",
    "150100":"Lima Metropolitana","160000":"Loreto",         "170000":"Madre De Dios",
    "180000":"Moquegua",        "190000":"Pasco",           "200000":"Piura",
    "210000":"Puno",            "220000":"San Martín",      "230000":"Tacna",
    "240000":"Tumbes",          "250000":"Ucayali",         "900000":"Extranjero",
}

MAX_VOTOS = {
    "150100":5_000_000,"150000":1_300_000,"200000":950_000,"130000":850_000,
    "060000":800_000,"120000":750_000,"040000":700_000,"210000":650_000,
    "140000":600_000,"080000":600_000,"100000":500_000,"220000":450_000,
    "020000":420_000,"160000":400_000,"070000":350_000,"050000":300_000,
    "110000":300_000,"010000":230_000,"030000":230_000,"090000":210_000,
    "250000":210_000,"190000":140_000,"180000":115_000,"230000":115_000,
    "240000":95_000,"170000":95_000,"900000":250_000,
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer":         "https://resultadoelectoral.onpe.gob.pe/",
    "Origin":          "https://resultadoelectoral.onpe.gob.pe",
    "Connection":      "keep-alive",
}

# Datos del último corte conocido — se muestran cuando ONPE no responde
# Actualiza estos valores manualmente si los tienes más recientes
RESPALDO = {
    "fuente":"respaldo_local","timestamp":"2026-04-14T20:00:00Z",
    "pctActas":89.5,"actasContabilizadas":83024,"actasTotales":92766,
    "actasJEE":280,"actasPendientes":9462,
    "votosEmitidos":19_800_000,"votosValidos":17_200_000,
    "votosBlancos":1_800_000,"votosNulos":800_000,"participacion":75.0,
    "candidatos":[
        {"nombre":"Keiko Fujimori",      "partido":"Fuerza Popular",          "votos":2_870_000,"pct":16.686},
        {"nombre":"Rafael López Aliaga", "partido":"Renovación Popular",      "votos":2_150_000,"pct":12.500},
        {"nombre":"Jorge Nieto",         "partido":"Partido Del Buen Gobierno","votos":1_950_000,"pct":11.337},
        {"nombre":"Ricardo Belmont",     "partido":"Partido Cívico Obras",    "votos":1_820_000,"pct":10.580},
        {"nombre":"Roberto Sánchez",     "partido":"Juntos Por El Perú",      "votos":1_740_000,"pct":10.116},
        {"nombre":"Carlos Álvarez",      "partido":"Partido País Para Todos", "votos":1_490_000,"pct":8.663},
        {"nombre":"Pablo López Chau",    "partido":"Ahora Nación - An",       "votos":1_310_000,"pct":7.616},
    ],
    "regiones":[],
}

_cache      = None
_lock       = threading.Lock()
_refreshing = False

def _sa(s):
    return ''.join(c for c in unicodedata.normalize('NFD',s) if unicodedata.category(c)!='Mn')

# ── HTTP ───────────────────────────────────────────────────────────────
def _get_json(url, retries=2):
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(retries+1):
        try:
            with urllib.request.urlopen(req, timeout=12) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding","") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as e:
            if e.code in (403, 404): break
        except Exception:
            pass
        if attempt < retries: time.sleep(1.5)
    return None

def _unwrap(raw):
    if isinstance(raw, dict) and "data" in raw: return raw["data"]
    return raw

# ── PARSERS ────────────────────────────────────────────────────────────
def parse_avance(raw):
    """
    Estructura real ONPE confirmada:
      data.actasContabilizadas   = 53.886  ← YA ES el porcentaje (float ≤100)
      data.contabilizadas        = 49988   ← valor absoluto
      data.totalActas            = 92766
      data.enviadasJee           = 155
      data.pendientesJee         = 42623
      data.totalVotosEmitidos    = 11488641
      data.totalVotosValidos     = 9847379
      data.participacionCiudadana = 42.044
    """
    if not raw: return {}
    d = _unwrap(raw)
    if isinstance(d, list): d = d[0] if d else {}
    if not isinstance(d, dict): return {}

    pct  = float(d.get("actasContabilizadas") or d.get("porcentajeActasContabilizadas") or d.get("porcentajeAvanceMesas") or 0)
    cont = int(d.get("contabilizadas") or d.get("mesasContabilizadas") or 0)
    tot  = int(d.get("totalActas") or d.get("totalMesas") or 92766)
    jee  = int(d.get("enviadasJee") or d.get("actasEnviadasJee") or 0)
    pend = int(d.get("pendientesJee") or d.get("actasPendientesJee") or max(0, tot-cont-jee))
    if pct == 0 and tot > 0 and cont > 0: pct = round((cont/tot)*100, 3)
    return {
        "pctActas":pct, "actasContabilizadas":cont, "actasTotales":tot,
        "actasJEE":jee, "actasPendientes":pend,
        "votosEmitidos":int(d.get("totalVotosEmitidos") or 0),
        "votosValidos":int(d.get("totalVotosValidos") or 0),
        "participacion":float(d.get("participacionCiudadana") or 0),
    }

def parse_candidatos(raw):
    if not raw: return [], 0, 0
    items = _unwrap(raw)
    if isinstance(items, dict):
        items = items.get("candidatos") or items.get("participantes") or items.get("data") or [items]
    if not isinstance(items, list): return [], 0, 0
    cands, vb, vn = [], 0, 0
    for item in items:
        if not isinstance(item, dict): continue
        cod    = str(item.get("codigoAgrupacionPolitica") or "")
        nombre = (item.get("nombreCandidato") or "").strip()
        partido= (item.get("nombreAgrupacionPolitica") or "").strip()
        votos  = int(item.get("totalVotosValidos") or item.get("votos") or 0)
        pct    = float(item.get("porcentajeVotosValidos") or item.get("pct") or 0)
        if cod == "80": vb = votos; continue
        if cod == "81": vn = votos; continue
        p = nombre.title().split()
        nf = (f"{p[0]} {p[2]}" if len(p)>=4 else f"{p[0]} {p[1]}" if len(p)>=2 else nombre.title())
        if nf and (votos>0 or pct>0):
            cands.append({"nombre":nf,"partido":partido.title(),"votos":votos,"pct":round(pct,3)})
    cands.sort(key=lambda x:-x["pct"])
    return cands, vb, vn

# ── FETCH REGIONAL ─────────────────────────────────────────────────────
def _region_urls(ubigeo):
    cod_int = str(int(ubigeo))
    cods    = [cod_int] if cod_int==ubigeo else [cod_int, ubigeo]
    ambitos = ["2","1"] if ubigeo=="900000" else ["1","3"] if ubigeo=="150100" else ["1"]
    variants = []
    for amb in ambitos:
        for cod in cods:
            variants.append((
                f"{BASE}/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico={amb}&ubigeoNivel1={cod}",
                f"{BASE}/resumen-general/totales?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico={amb}&idUbigeoDepartamento={cod}"
            ))
            variants.append((
                f"{BASE}/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={cod}",
                f"{BASE}/resumen-general/totales?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={cod}"
            ))
    return variants

def fetch_region(ubigeo, nombre):
    max_v = MAX_VOTOS.get(ubigeo, 700_000)
    for url_c, url_t in _region_urls(ubigeo):
        cands, _, _ = parse_candidatos(_get_json(url_c))
        avance      = parse_avance(_get_json(url_t))
        if not cands: continue
        tv = sum(c["votos"] for c in cands)
        if tv > max_v:
            print(f"    ⚠ FUGA {nombre}: {tv:,} > {max_v:,}"); continue
        aC  = avance.get("actasContabilizadas",0)
        aT  = avance.get("actasTotales",0)
        pct = avance.get("pctActas",0)
        if pct==0 and aT>0 and aC>0: pct = round((aC/aT)*100,3)
        vv  = avance.get("votosValidos",0)
        if vv==0 or vv>max_v: vv=tv
        print(f"    ✓ {nombre}: {tv:,} votos, {pct:.1f}% actas")
        return {"nombre":nombre,"pctActas":pct,"actasCont":aC,"actasTot":aT,
                "vValidos":vv,"lider":cands[0]["nombre"],"pctLider":cands[0]["pct"],
                "candidatos":cands}
    print(f"    ✗ {nombre} ({ubigeo}): todas las variantes fallaron")
    return None

# ── FETCH PRINCIPAL ────────────────────────────────────────────────────
def fetch_onpe():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando ONPE...")
    ok_count = 0

    avance = parse_avance(_get_json(EP_TOTALES))
    if avance.get("pctActas",0) > 0:
        ok_count += 1
        print(f"  ✓ Nacional: {avance['pctActas']:.3f}% actas")
    else:
        print("  ✗ Nacional: sin respuesta de ONPE")

    cands, blancos, nulos = parse_candidatos(_get_json(EP_CANDIDATOS))
    if cands:
        ok_count += 1
        print(f"  ✓ Candidatos: {len(cands)}")
    else:
        print("  ✗ Candidatos: sin respuesta de ONPE")

    # ── REGLA CLAVE: si ok_count==0, la ONPE no responde ──────────────
    # Devolvemos None para que _do_refresh NO sobreescriba el caché
    if ok_count == 0:
        print("  ✗ ONPE completamente no responde — caché preservado sin cambios")
        return None

    # ── Construir resultado desde el caché anterior (preserva regiones) ─
    with _lock:
        base = dict(_cache) if (_cache and _cache.get("pctActas",0)>0) else dict(RESPALDO)

    if avance.get("pctActas",0) > 0:
        base.update(avance)
    if cands:
        base["candidatos"]   = cands
        base["votosBlancos"] = blancos
        base["votosNulos"]   = nulos

    # ── Regiones ──────────────────────────────────────────────────────
    print(f"  → Descargando {len(UBIGEOS)} jurisdicciones...")
    nuevas = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(fetch_region,u,n):n for u,n in UBIGEOS.items()}
        for f in concurrent.futures.as_completed(futures):
            r = f.result()
            if r: nuevas.append(r)

    # Merge con anteriores (preserva regiones que no respondieron esta vez)
    prev = {r["nombre"]:r for r in base.get("regiones",[])}
    for r in nuevas: prev[r["nombre"]] = r
    base["regiones"] = sorted(prev.values(), key=lambda x:_sa(x["nombre"]))
    print(f"  ✓ Regiones: {len(base['regiones'])} en caché ({len(nuevas)} actualizadas ahora)")

    base["fuente"]    = "api_onpe" if ok_count>=2 else "api_onpe_parcial"
    base["timestamp"] = datetime.now(timezone.utc).isoformat()
    return base

# ── CACHÉ ──────────────────────────────────────────────────────────────
def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f: data = json.load(f)
            # Solo usar caché si tiene datos reales (no ceros)
            if data.get("pctActas",0) > 0 and data.get("candidatos"):
                _cache = data
                print(f"  Caché cargado: {data['pctActas']:.3f}% actas, {len(data.get('regiones',[]))} regiones")
                return
        except Exception: pass
    # Sin caché válido: usar datos de respaldo para que el frontend muestre algo
    _cache = dict(RESPALDO)
    print("  Caché: usando datos de respaldo (LAST_KNOWN)")

def _save_cache(data):
    try:
        with open(CACHE_FILE,"w") as f: json.dump(data,f,ensure_ascii=False,indent=2)
    except Exception as e: print(f"  ⚠ save_cache: {e}")

def _do_refresh():
    global _cache, _refreshing
    if _refreshing: return
    _refreshing = True
    try:
        result = fetch_onpe()
        # ── CRÍTICO: solo actualizar si fetch_onpe devolvió datos reales ──
        if result is not None:
            with _lock: _cache = result
            _save_cache(result)
            print(f"  → Caché actualizado: {result.get('pctActas',0):.3f}% actas, "
                  f"fuente={result.get('fuente')}, regiones={len(result.get('regiones',[]))}")
        # Si result is None: ONPE no respondió, _cache queda intacto
    except Exception as e:
        print(f"  ✗ _do_refresh: {e}")
    finally:
        _refreshing = False

def refresh_loop():
    while True:
        try: _do_refresh()
        except Exception as e: print(f"  ✗ loop: {e}")
        time.sleep(REFRESH_SEC)

# ── HTTP HANDLER ───────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if args and not any(s in str(args[0]) for s in ["/api/datos","/api/status"]):
            print(f"  GET {args[0]}")

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type")

    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()

    def _json(self, obj, status=200):
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type","application/json; charset=utf-8")
        self.send_header("Cache-Control","no-cache")
        self._cors(); self.end_headers(); self.wfile.write(payload)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/api/datos":
            with _lock: data = dict(_cache)
            self._json(data)

        elif path == "/api/refresh":
            threading.Thread(target=_do_refresh, daemon=True).start()
            with _lock: data = dict(_cache)
            self._json({"ok":True,"fuente":data.get("fuente"),
                        "pctActas":data.get("pctActas",0),
                        "nRegiones":len(data.get("regiones",[]))})

        elif path == "/api/status":
            with _lock: data = dict(_cache)
            self._json({"is_refreshing":_refreshing,
                        "pctActas":data.get("pctActas",0),
                        "nRegiones":len(data.get("regiones",[])),
                        "fuente":data.get("fuente","–"),
                        "timestamp":data.get("timestamp","–")})

        elif path == "/api/debug-regiones":
            with _lock: data = dict(_cache)
            regs  = data.get("regiones",[])
            found = {r["nombre"] for r in regs}
            miss  = sorted(n for n in UBIGEOS.values() if n not in found)
            self._json({
                "total_ubigeos":len(UBIGEOS),
                "en_cache":len(regs),
                "faltantes":miss,
                "detalle":[{
                    "nombre":r["nombre"],"pctActas":r.get("pctActas",0),
                    "vValidos":r.get("vValidos",0),"lider":r.get("lider","–"),
                    "nCands":len(r.get("candidatos",[]))
                } for r in regs],
            })

        elif path == "/api/mapa":
            try:
                req = urllib.request.Request(EP_MAP_JSON, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=10) as r:
                    raw = r.read()
                    if r.headers.get("Content-Encoding","")=="gzip": raw=gzip.decompress(raw)
                self.send_response(200)
                self.send_header("Content-Type","application/json; charset=utf-8")
                self._cors(); self.end_headers(); self.wfile.write(raw)
            except Exception:
                self.send_response(500); self._cors(); self.end_headers()

        elif path in ("/","/index.html"):
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE,"rb") as f: content=f.read()
                self.send_response(200)
                self.send_header("Content-Type","text/html; charset=utf-8")
                self.end_headers(); self.wfile.write(content)
            else:
                self.send_response(404); self.end_headers()
                self.wfile.write(b"onpe_2026.html no encontrado")

        else:
            self.send_response(404); self.end_headers()

# ── MAIN ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("="*55)
    print("  ONPE 2026 — Proxy v28")
    print("="*55)
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n  Dashboard:      http://localhost:{PORT}")
    print(f"  Estado:         http://localhost:{PORT}/api/status")
    print(f"  Debug regiones: http://localhost:{PORT}/api/debug-regiones\n")
    try: server.serve_forever()
    except KeyboardInterrupt: print("\n  Detenido."); sys.exit(0)
