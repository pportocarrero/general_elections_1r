#!/usr/bin/env python3
"""
ONPE 2026 — Proxy Local v11 (Endpoints Limpios + Anti Fuga Nacional)
Uso:  python3 onpe_proxy.py
Web:  http://localhost:8765
"""

import http.server, json, threading, time
import urllib.request, urllib.error, urllib.parse
import gzip, os, sys
from datetime import datetime, timezone

# Compatible con local y con despliegue en la nube (Render/Railway)
PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 60
CACHE_FILE  = "onpe_cache.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

# Endpoints Nacionales
EP_MESAS     = BASE + "/mesa/totales?tipoFiltro=eleccion"
EP_TOTALES   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS= BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"
EP_MAP_JSON  = "https://resultadoelectoral.onpe.gob.pe/assets/lib/amcharts5/geodata/json/peruLow.json"

# Endpoints Regionales (Ruta Estricta 1)
EP_REG_PART_1 = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={}"
EP_REG_TOT_1  = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={}"

# Endpoints Regionales (Ruta de Respaldo 2)
EP_REG_PART_2 = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&ubigeoNivel1={}"
EP_REG_TOT_2  = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&idUbigeoDepartamento={}"

# Diccionario Oficial de Ubigeos
UBIGEOS = {
    "010000": "Amazonas", "020000": "Áncash", "030000": "Apurímac",
    "040000": "Arequipa", "050000": "Ayacucho", "060000": "Cajamarca",
    "070000": "Callao", "080000": "Cusco", "090000": "Huancavelica",
    "100000": "Huánuco", "110000": "Ica", "120000": "Junín",
    "130000": "La Libertad", "140000": "Lambayeque", "150000": "Lima",
    "160000": "Loreto", "170000": "Madre de Dios", "180000": "Moquegua",
    "190000": "Pasco", "200000": "Piura", "210000": "Puno",
    "220000": "San Martín", "230000": "Tacna", "240000": "Tumbes",
    "250000": "Ucayali"
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer":         "https://resultadoelectoral.onpe.gob.pe/",
    "Origin":          "https://resultadoelectoral.onpe.gob.pe",
    "Connection":      "keep-alive",
}

FALLBACK = {
    "fuente": "respaldo_local",
    "timestamp": "2026-04-13T10:00:00",
    "pctActas": 53.886, "actasContabilizadas": 49988,
    "actasTotales": 92766, "actasJEE": 155, "actasPendientes": 42623,
    "votosEmitidos": 11488641, "votosValidos": 9847379,
    "votosBlancos": 1128877, "votosNulos": 512385,
    "participacion": 42.044,
    "candidatos": [],
    "regiones": [],
}

_cache = None
_lock  = threading.Lock()

def _get_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding", "") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw.decode("utf-8", errors="replace"))
    except Exception as e:
        return None

def _unwrap(raw):
    if isinstance(raw, dict) and "data" in raw:
        return raw["data"]
    return raw

def parse_avance(raw_totales):
    if not raw_totales: return {}
    d = _unwrap(raw_totales)
    
    # Manejar caso en el que la API devuelva una lista
    if isinstance(d, list):
        if len(d) > 0: d = d[0]
        else: return {}
    if not isinstance(d, dict): return {}

    # Prioridad 1: Buscar porcentajes explícitos
    pct = float(d.get("porcentajeActasContabilizadas") or d.get("porcentajeAvanceMesas") or 0)
    
    # Prioridad 2: Cálculo manual seguro para evitar confusión con números enteros
    cont = int(d.get("contabilizadas") or d.get("actasContabilizadas") or 0)
    tot  = int(d.get("totalActas") or 92766)
    jee  = int(d.get("enviadasJee") or 0)
    pend = int(d.get("pendientesJee") or max(0, tot - cont - jee))

    # Si el porcentaje falló pero tenemos datos crudos, lo calculamos
    if pct == 0 and tot > 0:
        pct = (cont / tot) * 100.0

    return {
        "pctActas": round(pct, 3), "actasContabilizadas": cont, "actasTotales": tot,
        "actasJEE": jee, "actasPendientes": pend,
        "votosEmitidos": int(d.get("totalVotosEmitidos") or 0),
        "votosValidos": int(d.get("totalVotosValidos") or 0),
        "participacion": float(d.get("participacionCiudadana") or 0),
    }

def parse_candidatos(raw):
    if not raw: return [], 0, 0
    items = _unwrap(raw)
    
    # Manejar variaciones en la estructura JSON
    if isinstance(items, dict):
        if "candidatos" in items: items = items["candidatos"]
        elif "participantes" in items: items = items["participantes"]
        else: items = [items]
        
    if not isinstance(items, list): return [], 0, 0

    candidatos = []
    votos_blancos = votos_nulos = 0

    for item in items:
        cod = str(item.get("codigoAgrupacionPolitica") or "")
        nombre = (item.get("nombreCandidato") or "").strip()
        partido= (item.get("nombreAgrupacionPolitica") or "").strip()
        
        # Buscar los votos en múltiples llaves posibles
        votos  = int(item.get("totalVotosValidos") or item.get("votos") or item.get("totalVotos") or 0)
        pct    = float(item.get("porcentajeVotosValidos") or item.get("porcentajeVotos") or item.get("pct") or 0)

        if cod == "80": votos_blancos = votos; continue
        if cod == "81": votos_nulos = votos; continue

        nombre_fmt = _fmt_nombre(nombre)
        if nombre_fmt and (votos > 0 or pct > 0):
            candidatos.append({
                "nombre": nombre_fmt, "partido": partido.title(),
                "votos": votos, "pct": round(pct, 3),
            })

    candidatos.sort(key=lambda x: -x["pct"])
    return candidatos, votos_blancos, votos_nulos

def _fmt_nombre(nombre_mayus):
    partes = nombre_mayus.strip().title().split()
    if len(partes) >= 4: return f"{partes[0]} {partes[2]}"
    if len(partes) == 3: return f"{partes[0]} {partes[1]}"
    if len(partes) == 2: return f"{partes[0]} {partes[1]}"
    return nombre_mayus.title()

def fetch_region(ubigeo, nombre):
    # Intento 1: Endpoints principales
    raw_part = _get_json(EP_REG_PART_1.format(ubigeo))
    raw_tot  = _get_json(EP_REG_TOT_1.format(ubigeo))
    
    cands, _, _ = parse_candidatos(raw_part)
    avance = parse_avance(raw_tot)

    # Validar "Fuga Nacional": Si el líder tiene > 2M de votos y no es Lima, 
    # significa que el servidor de la ONPE colapsó y mandó el total del país.
    is_national_leak = cands and cands[0]["votos"] > 2000000 and nombre != "Lima"

    # Si falló o hay fuga, aplicamos el endpoint de respaldo
    if not cands or is_national_leak or avance.get("pctActas", 0) == 0:
        raw_part = _get_json(EP_REG_PART_2.format(ubigeo))
        raw_tot  = _get_json(EP_REG_TOT_2.format(ubigeo))
        cands, _, _ = parse_candidatos(raw_part)
        avance = parse_avance(raw_tot)

    if not cands: return None

    return {
        "nombre": nombre,
        "pctActas": avance.get("pctActas", 0),
        "lider": cands[0]["nombre"],
        "pctLider": cands[0]["pct"],
        "candidatos": cands
    }

def fetch_onpe():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando ONPE...")
    result   = dict(FALLBACK)
    ok_count = 0

    # 1) Avance Nacional
    raw_tot = _get_json(EP_TOTALES)
    avance  = parse_avance(raw_tot)
    if avance.get("pctActas", 0) > 0:
        result.update(avance)
        ok_count += 1
        print(f"  ✓ avance   → {result['pctActas']:.3f}% actas")

    # 2) Candidatos Nacionales
    raw_cand = _get_json(EP_CANDIDATOS)
    if raw_cand:
        cands, blancos, nulos = parse_candidatos(raw_cand)
        if cands:
            result["candidatos"]  = cands
            result["votosBlancos"] = blancos
            result["votosNulos"]   = nulos
            ok_count += 1
            print(f"  ✓ candidatos → {len(cands)} participantes")

    # 3) Regiones Limpias
    print(f"  ✓ Extrayendo data de {len(UBIGEOS)} regiones...")
    regiones = []
    
    for ubigeo, nombre in UBIGEOS.items():
        res = fetch_region(ubigeo, nombre)
        if res:
            regiones.append(res)
            print(f"    - {nombre}: OK ({res['pctActas']}% actas)")
        else:
            print(f"    - {nombre}: Sin datos en este corte")
        # Pequeña pausa para no saturar la ONPE
        time.sleep(0.4) 
    
    if regiones:
        regiones.sort(key=lambda x: x["nombre"])
        result["regiones"] = regiones

    result["fuente"] = ("api_onpe" if ok_count >= 2 else "api_onpe_parcial" if ok_count == 1 else "respaldo_local")
    result["timestamp"] = datetime.now(timezone.utc).isoformat()
    return result

def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f: _cache = json.load(f)
            return
        except Exception: pass
    _cache = dict(FALLBACK)

def _save_cache(data):
    try:
        with open(CACHE_FILE, "w") as f: json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception: pass

def _do_refresh():
    global _cache
    data = fetch_onpe()
    with _lock: _cache = data
    _save_cache(data)
    return data

def refresh_loop():
    while True:
        try: _do_refresh()
        except Exception as e: print(f"  ✗ Error en ciclo: {e}")
        time.sleep(REFRESH_SEC)

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if args and not ("/api/datos" in str(args[0])): print(f"  GET {args[0]}")
    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()
    def _json(self, obj, status=200):
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self._cors(); self.end_headers(); self.wfile.write(payload)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/datos":
            with _lock: data = dict(_cache)
            self._json(data)
        elif path == "/api/refresh":
            data = _do_refresh()
            self._json({"ok": True, "fuente": data["fuente"], "pctActas": data.get("pctActas", 0)})
        elif path == "/api/mapa":
            # Extrae el mapa geoJSON de la web de ONPE directamente
            try:
                req = urllib.request.Request(EP_MAP_JSON, headers=HEADERS)
                with urllib.request.urlopen(req, timeout=10) as r:
                    raw = r.read()
                    if r.headers.get("Content-Encoding", "") == "gzip": raw = gzip.decompress(raw)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json; charset=utf-8")
                    self._cors(); self.end_headers(); self.wfile.write(raw)
            except Exception as e:
                self.send_response(500); self._cors(); self.end_headers()
        elif path in ("/", "/index.html"):
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, "rb") as f: content = f.read()
                self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers(); self.wfile.write(content)
            else:
                self.send_response(404); self.end_headers(); self.wfile.write(b"No se encontro onpe_2026.html")
        else:
            self.send_response(404); self.end_headers()

if __name__ == "__main__":
    print("=" * 60)
    print("  ONPE 2026 — Proxy Local v11 (Anti-Fuga)")
    print("=" * 60)
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n  Dashboard:  http://0.0.0.0:{PORT}")
    try: server.serve_forever()
    except KeyboardInterrupt: sys.exit(0)