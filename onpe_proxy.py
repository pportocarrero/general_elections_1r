#!/usr/bin/env python3
"""
ONPE 2026 — Proxy Local v20 (Purificador de Caché Corrupto + Anti-Fuga)
Uso:  python3 onpe_proxy.py
"""

import http.server, json, threading, time
import urllib.request, urllib.error, urllib.parse
import gzip, os, sys, concurrent.futures, unicodedata
from datetime import datetime, timezone

PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 25
CACHE_FILE  = "onpe_cache.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

# Endpoints Nacionales
EP_TOTALES   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS= BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"
EP_MAP_JSON  = "https://resultadoelectoral.onpe.gob.pe/assets/lib/amcharts5/geodata/json/peruLow.json"

# Endpoints Regionales (ÚNICOS Y ESTRICTOS)
EP_REG_PART  = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&ubigeoNivel1={}"
EP_REG_TOT   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&idUbigeoDepartamento={}"

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/",
    "Origin": "https://resultadoelectoral.onpe.gob.pe",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
}

FALLBACK = {
    "fuente": "respaldo_local", "timestamp": "2026-04-13T10:00:00",
    "pctActas": 53.886, "actasContabilizadas": 49988, "actasTotales": 92766,
    "votosEmitidos": 11488641, "votosValidos": 9847379, "candidatos": [], "regiones": []
}

_cache = None
_lock  = threading.Lock()
_is_refreshing = False

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def _get_json(url, retries=1):
    sep = "&" if "?" in url else "?"
    cb_url = f"{url}{sep}_={int(time.time()*1000)}"
    req = urllib.request.Request(cb_url, headers=HEADERS)
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding", "") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception:
            if attempt == retries: return None
            time.sleep(0.5)
    return None

def _unwrap(raw):
    if isinstance(raw, dict) and "data" in raw: return raw["data"]
    return raw

def parse_avance(raw_totales):
    if not raw_totales: return {}
    d = _unwrap(raw_totales)
    if isinstance(d, list):
        d = d[0] if d else {}
    if not isinstance(d, dict): return {}

    pct  = float(d.get("actasContabilizadas") or d.get("porcentajeActasContabilizadas") or d.get("porcentajeAvanceMesas") or 0)
    cont = d.get("contabilizadas")
    if cont is None:
        ac = d.get("actasContabilizadas", 0)
        if isinstance(ac, float) and ac <= 100.0:
            pct = pct or ac; cont = 0
        else: cont = ac
            
    cont = int(cont or 0)
    tot  = int(d.get("totalActas") or d.get("totalMesas") or 92766)
    jee  = int(d.get("enviadasJee") or d.get("actasEnviadasJee") or 0)
    pend = int(d.get("pendientesJee") or d.get("actasPendientesJee") or max(0, tot - cont - jee))

    if pct == 0 and tot > 0 and cont > 0: pct = round((cont / tot) * 100.0, 3)

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
        votos  = int(item.get("totalVotosValidos") or item.get("votos") or item.get("totalVotos") or 0)
        pct    = float(item.get("porcentajeVotosValidos") or item.get("porcentajeVotos") or item.get("pct") or 0)

        if cod == "80": votos_blancos = votos; continue
        if cod == "81": votos_nulos = votos; continue

        partes = nombre.title().split()
        nombre_fmt = f"{partes[0]} {partes[2]}" if len(partes) >= 4 else (f"{partes[0]} {partes[1]}" if len(partes) >= 2 else nombre.title())
        
        if nombre_fmt and (votos > 0 or pct > 0):
            candidatos.append({"nombre": nombre_fmt, "partido": partido.title(), "votos": votos, "pct": round(pct, 3)})

    candidatos.sort(key=lambda x: -x["pct"])
    return candidatos, votos_blancos, votos_nulos

def fetch_region_worker(ubigeo, nombre):
    url_part = EP_REG_PART.format(ubigeo)
    url_tot  = EP_REG_TOT.format(ubigeo)

    raw_part = _get_json(url_part)
    raw_tot  = _get_json(url_tot)

    cands, _, _ = parse_candidatos(raw_part)
    avance = parse_avance(raw_tot)

    if not cands: return None
    
    total_votos = sum(c["votos"] for c in cands)
    # ANTI-FUGA: Si una región reporta > 1.5M votos y NO es Lima, la ONPE se equivocó.
    if total_votos > 1500000 and ubigeo != "150000":
        return None
        
    return {
        "nombre":    nombre.title(),
        "pctActas":  avance.get("pctActas", 0),
        "actasCont": avance.get("actasContabilizadas", 0),
        "actasTot":  avance.get("actasTotales", 0),
        "lider":     cands[0]["nombre"],
        "pctLider":  cands[0]["pct"],
        "candidatos": cands,
    }

def fetch_onpe():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando ONPE...")
    result   = dict(FALLBACK)
    ok_count = 0

    raw_tot = _get_json(EP_TOTALES)
    avance  = parse_avance(raw_tot)
    if avance.get("pctActas", 0) > 0:
        result.update(avance)
        ok_count += 1

    raw_cand = _get_json(EP_CANDIDATOS)
    if raw_cand:
        cands, blancos, nulos = parse_candidatos(raw_cand)
        if cands:
            result["candidatos"]  = cands
            result["votosBlancos"] = blancos
            result["votosNulos"]   = nulos
            ok_count += 1

    print(f"  ✓ Extrayendo data de {len(UBIGEOS)} regiones...")
    regiones = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_region = {executor.submit(fetch_region_worker, u, n): n for u, n in UBIGEOS.items()}
        for future in concurrent.futures.as_completed(future_to_region):
            res = future.result()
            if res: regiones.append(res)
    
    if regiones:
        regiones.sort(key=lambda x: strip_accents(x["nombre"]))
        
        with _lock: old_regs = _cache.get("regiones", []) if _cache else []
        
        # PURIFICADOR DE CACHÉ: Si el caché tenía datos corruptos, los eliminamos.
        valid_old_regs = []
        for r in old_regs:
            v_totales = sum(c.get("votos",0) for c in r.get("candidatos",[]))
            if v_totales > 1500000 and r["nombre"] != "Lima":
                continue # Purgar región corrupta
            valid_old_regs.append(r)
            
        final_regs_dict = {r["nombre"]: r for r in valid_old_regs}
        for r in regiones: final_regs_dict[r["nombre"]] = r 
        
        result["regiones"] = sorted(final_regs_dict.values(), key=lambda x: strip_accents(x["nombre"]))
        print(f"  ✓ Regiones integradas → {len(result['regiones'])}/25")

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
    global _cache, _is_refreshing
    if _is_refreshing: return
    _is_refreshing = True
    try:
        data = fetch_onpe()
        with _lock: _cache = data
        _save_cache(data)
    except Exception:
        pass
    finally:
        _is_refreshing = False
    return _cache

def refresh_loop():
    while True:
        try: _do_refresh()
        except Exception: pass
        time.sleep(REFRESH_SEC)

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        if args and not any(x in str(args[0]) for x in ["/api/datos", "/api/status"]):
            print(f"  GET {args[0]}")
            
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
            threading.Thread(target=_do_refresh, daemon=True).start()
            with _lock: data = dict(_cache)
            self._json({"ok": True, "fuente": data.get("fuente", "actualizando")})

        elif path == "/api/status":
            self._json({"is_refreshing": _is_refreshing})
            
        elif path == "/api/mapa":
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
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

if __name__ == "__main__":
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n  Dashboard Listo -> http://0.0.0.0:{PORT}")
    try: server.serve_forever()
    except KeyboardInterrupt: sys.exit(0)