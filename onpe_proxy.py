#!/usr/bin/env python3
"""
ONPE 2026 — Proxy Nube v31 (Evasión WAF Cloudflare para Render)
"""

import http.server, json, threading, time
import urllib.request, urllib.error
import gzip, os, sys, concurrent.futures, unicodedata, ssl
from datetime import datetime, timezone

PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 25
CACHE_FILE  = "onpe_cache_v31.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

EP_TOTALES   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS= BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"

# 27 Jurisdicciones Oficiales Exactas
UBIGEOS = {
    "010000": "Amazonas", "020000": "Áncash", "030000": "Apurímac",
    "040000": "Arequipa", "050000": "Ayacucho", "060000": "Cajamarca",
    "070000": "Callao", "080000": "Cusco", "090000": "Huancavelica",
    "100000": "Huánuco", "110000": "Ica", "120000": "Junín",
    "130000": "La Libertad", "140000": "Lambayeque", "150000": "Lima Provincias",
    "150100": "Lima Metropolitana", "160000": "Loreto", "170000": "Madre de Dios", 
    "180000": "Moquegua", "190000": "Pasco", "200000": "Piura", "210000": "Puno",
    "220000": "San Martín", "230000": "Tacna", "240000": "Tumbes",
    "250000": "Ucayali", "900000": "Extranjero"
}

# Encabezados Stealth para engañar al Firewall de la ONPE desde Render
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es-419;q=0.9,es;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Connection": "keep-alive",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/",
    "Origin": "https://resultadoelectoral.onpe.gob.pe",
}

FALLBACK = {
    "fuente": "inicializando_render", "timestamp": datetime.now(timezone.utc).isoformat(),
    "pctActas": 0, "actasContabilizadas": 0, "actasTotales": 92766,
    "votosEmitidos": 0, "votosValidos": 0, "candidatos": [], "regiones": []
}

_cache = None
_lock  = threading.Lock()
_is_refreshing = False

# Contexto SSL ignorando validación estricta gubernamental
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def _get_json(url, retries=2):
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=12, context=CTX) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding", "") in ["gzip", "x-gzip"]:
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8", errors="replace"))
        except urllib.error.HTTPError as e:
            if attempt == retries:
                print(f"  [X] ONPE bloqueó la IP de Render (Error {e.code}) en: {url.split('/')[-1]}")
                return None
            time.sleep(1.5)
        except Exception as e:
            if attempt == retries: 
                return None
            time.sleep(1)
    return None

def _unwrap(raw):
    if isinstance(raw, dict) and "data" in raw: return raw["data"]
    return raw

def parse_avance(raw_totales):
    if not raw_totales: return {}
    d = _unwrap(raw_totales)
    if isinstance(d, list): d = d[0] if d else {}
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
    ambito = "2" if ubigeo == "900000" else "1"
    url_part = f"{BASE}/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico={ambito}&ubigeoNivel1={ubigeo}"
    url_tot  = f"{BASE}/resumen-general/totales?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico={ambito}&idUbigeoDepartamento={ubigeo}"

    cands, _, _ = parse_candidatos(_get_json(url_part))
    avance = parse_avance(_get_json(url_tot))

    if not cands or not avance: return None
    
    aT = avance.get("actasTotales", 0)
    vV = avance.get("votosValidos", 0)
    if vV == 0: vV = sum(c["votos"] for c in cands)
    
    # Filtro Anti-Cruce: Ninguna jurisdicción sola llega a 85,000 actas
    if aT > 85000: return None
        
    return {
        "nombre":    nombre.title(),
        "pctActas":  avance.get("pctActas", 0),
        "actasCont": avance.get("actasContabilizadas", 0),
        "actasTot":  aT,
        "vValidos":  vV,
        "lider":     cands[0]["nombre"],
        "pctLider":  cands[0]["pct"],
        "candidatos": cands,
    }

def fetch_onpe():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando ONPE desde Render...")
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

    if ok_count == 0:
        print("  ✗ Render bloqueado por Cloudflare ONPE. Usando Caché.")
        return None

    regiones = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_region = {executor.submit(fetch_region_worker, u, n): n for u, n in UBIGEOS.items()}
        for future in concurrent.futures.as_completed(future_to_region):
            res = future.result()
            if res: regiones.append(res)
    
    if regiones:
        regiones.sort(key=lambda x: strip_accents(x["nombre"]))
        with _lock: old_regs = _cache.get("regiones", []) if _cache else []
        final_regs_dict = {r["nombre"]: r for r in old_regs}
        for r in regiones: final_regs_dict[r["nombre"]] = r 
        result["regiones"] = sorted(final_regs_dict.values(), key=lambda x: strip_accents(x["nombre"]))

    result["fuente"] = ("api_onpe" if ok_count >= 2 else "api_onpe_parcial")
    result["timestamp"] = datetime.now(timezone.utc).isoformat()
    return result

def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f: 
                data = json.load(f)
                if data.get("pctActas", 0) > 0:
                    _cache = data
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
        if data is not None:
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
        pass # Silenciamos logs HTTP para no llenar los logs de Render
            
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
    # Limpia archivos conflictivos de versiones anteriores
    for cf in ["onpe_cache.json", "onpe_cache_v24.json", "onpe_cache_v25.json", "onpe_cache_v26.json"]:
        if os.path.exists(cf): os.remove(cf)
        
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n=============================================")
    print(f"  ONPE 2026 Proxy Corriendo en Puerto {PORT}")
    print(f"=============================================")
    try: server.serve_forever()
    except KeyboardInterrupt: sys.exit(0)