#!/usr/bin/env python3
"""
ONPE 2026 — Proxy Local v27 (Directo, Sin Bloqueos, Votos Válidos Exactos)
Uso:  python3 onpe_proxy.py
"""

import http.server, json, threading, time
import urllib.request, urllib.error
import gzip, os, sys, concurrent.futures, unicodedata
from datetime import datetime, timezone

PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 30
CACHE_FILE  = "onpe_cache_v27.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

EP_TOTALES   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS= BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"
EP_MAP_JSON  = "https://resultadoelectoral.onpe.gob.pe/assets/lib/amcharts5/geodata/json/peruLow.json"

# Las 26 Jurisdicciones Oficiales de ONPE (Lima dividida + Extranjero)
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-PE,es;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://resultadoelectoral.onpe.gob.pe/",
    "Origin": "https://resultadoelectoral.onpe.gob.pe",
}

FALLBACK = {
    "fuente": "respaldo_local", "timestamp": "2026-04-13T10:00:00",
    "pctActas": 0, "actasContabilizadas": 0, "actasTotales": 92766,
    "votosEmitidos": 0, "votosValidos": 0, "candidatos": [], "regiones": []
}

_cache = None
_lock  = threading.Lock()
_is_refreshing = False

def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

def _get_json(url, retries=2):
    # Ya NO inyectamos el "?_=timestamp" para que Cloudflare de ONPE no nos bloquee (Error 403)
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=12) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding", "") == "gzip":
                    raw = gzip.decompress(raw)
                return json.loads(raw.decode("utf-8", errors="replace"))
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
        "votosValidos": int(d.get("totalVotosValidos") or 0), # Extraemos el Voto Válido Real
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

# ── Límites de votos válidos por ubigeo ─────────────────────────
# Para detectar fugas (ONPE devuelve totales nacionales por error)
MAX_VOTOS = {
    "150100": 5_000_000,  # Lima Metropolitana
    "150000": 1_300_000,  # Lima Provincias
    "200000":   950_000,  # Piura
    "130000":   850_000,  # La Libertad
    "060000":   800_000,  # Cajamarca
    "120000":   750_000,  # Junín
    "040000":   700_000,  # Arequipa
    "210000":   650_000,  # Puno
    "140000":   600_000,  # Lambayeque
    "080000":   600_000,  # Cusco
    "100000":   500_000,  # Huánuco
    "220000":   450_000,  # San Martín
    "020000":   420_000,  # Áncash
    "160000":   400_000,  # Loreto
    "070000":   350_000,  # Callao
    "050000":   300_000,  # Ayacucho
    "110000":   300_000,  # Ica
    "010000":   230_000,  # Amazonas
    "030000":   230_000,  # Apurímac
    "090000":   210_000,  # Huancavelica
    "250000":   210_000,  # Ucayali
    "190000":   140_000,  # Pasco
    "180000":   115_000,  # Moquegua
    "230000":   115_000,  # Tacna
    "240000":    95_000,  # Tumbes
    "170000":    95_000,  # Madre de Dios
    "900000":   250_000,  # Extranjero
}

def _build_region_urls(ubigeo):
    """
    Genera todas las variantes de URL para una jurisdicción.
    Orden: de más específica a más genérica.
    La ONPE no es consistente — distintas regiones responden a distintos patrones.
    """
    variants = []
    cod_int = str(int(ubigeo))   # "040000" -> "40000", "150100" -> "150100"
    
    # Determinar ámbito: 1=nacional, 2=extranjero, 3=Lima Metro (a veces)
    if ubigeo == "900000":
        ambitos = ["2", "1"]
    elif ubigeo == "150100":
        ambitos = ["1", "3"]
    else:
        ambitos = ["1"]
    
    # Códigos a probar: int sin ceros y con ceros
    cods = [cod_int] if cod_int == ubigeo else [cod_int, ubigeo]
    
    for ambito in ambitos:
        for cod in cods:
            # Variante 1: ubigeoNivel1 (la que usa la web de ONPE)
            variants.append((
                f"{BASE}/eleccion-presidencial/participantes-ubicacion-geografica-nombre"
                f"?idEleccion=10&tipoFiltro=ubigeo_nivel_01"
                f"&idAmbitoGeografico={ambito}&ubigeoNivel1={cod}",
                f"{BASE}/resumen-general/totales"
                f"?idEleccion=10&tipoFiltro=ubigeo_nivel_01"
                f"&idAmbitoGeografico={ambito}&idUbigeoDepartamento={cod}"
            ))
            # Variante 2: tipoFiltro=departamento
            variants.append((
                f"{BASE}/eleccion-presidencial/participantes-ubicacion-geografica-nombre"
                f"?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={cod}",
                f"{BASE}/resumen-general/totales"
                f"?idEleccion=10&tipoFiltro=departamento&idUbigeoDepartamento={cod}"
            ))
    
    return variants


def fetch_region_worker(ubigeo, nombre):
    """
    Prueba todas las variantes de URL para la jurisdicción.
    Aplica validación anti-fuga antes de aceptar los datos.
    """
    max_v = MAX_VOTOS.get(ubigeo, 700_000)
    variants = _build_region_urls(ubigeo)
    
    for url_part, url_tot in variants:
        raw_part = _get_json(url_part)
        raw_tot  = _get_json(url_tot)

        cands, _, _ = parse_candidatos(raw_part)
        avance      = parse_avance(raw_tot)

        if not cands:
            continue

        # Anti-fuga: votos totales no pueden exceder el máximo de la jurisdicción
        total_votos = sum(c["votos"] for c in cands)
        if total_votos > max_v:
            print(f"    ⚠ FUGA {nombre}: {total_votos:,} > máx {max_v:,} ({url_part[-60:]})")
            continue

        # Calcular pctActas si no vino directo
        aC = avance.get("actasContabilizadas", 0)
        aT = avance.get("actasTotales", 0)
        pctAc = avance.get("pctActas", 0)
        if pctAc == 0 and aT > 0 and aC > 0:
            pctAc = round((aC / aT) * 100, 3)

        vv = avance.get("votosValidos", 0)
        if vv == 0 or vv > max_v:
            vv = total_votos  # fallback a suma de candidatos

        print(f"    ✓ {nombre}: {total_votos:,} votos, {pctAc:.1f}% actas")
        return {
            "nombre":    nombre.title(),
            "pctActas":  pctAc,
            "actasCont": aC,
            "actasTot":  aT,
            "vValidos":  vv,
            "lider":     cands[0]["nombre"],
            "pctLider":  cands[0]["pct"],
            "candidatos": cands,
        }

    print(f"    ✗ {nombre} ({ubigeo}): todas las variantes fallaron")
    return None

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

    print(f"  ✓ Descargando {len(UBIGEOS)} jurisdicciones (Rutas limpias)...")
    regiones = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_region = {executor.submit(fetch_region_worker, u, n): n for u, n in UBIGEOS.items()}
        for future in concurrent.futures.as_completed(future_to_region):
            res = future.result()
            if res: regiones.append(res)
    
    if regiones:
        # Orden estricto ignorando tildes
        regiones.sort(key=lambda x: strip_accents(x["nombre"]))
        with _lock: old_regs = _cache.get("regiones", []) if _cache else []
        final_regs_dict = {r["nombre"]: r for r in old_regs}
        for r in regiones: final_regs_dict[r["nombre"]] = r 
        result["regiones"] = sorted(final_regs_dict.values(), key=lambda x: strip_accents(x["nombre"]))
        print(f"  ✓ {len(result['regiones'])} jurisdicciones actualizadas.")

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
                
        elif path == "/api/debug-regiones":
            # Diagnóstico: muestra estado de cada jurisdicción en el caché
            with _lock: data = dict(_cache)
            regs = data.get("regiones", [])
            found = {r["nombre"] for r in regs}
            missing = {n for n in UBIGEOS.values() if n.title() not in found}
            self._json({
                "total_ubigeos": len(UBIGEOS),
                "regiones_en_cache": len(regs),
                "nombres_en_cache": sorted(found),
                "faltantes": sorted(missing),
                "detalle": [{
                    "nombre": r["nombre"],
                    "pctActas": r.get("pctActas", 0),
                    "actasCont": r.get("actasCont", 0),
                    "actasTot": r.get("actasTot", 0),
                    "vValidos": r.get("vValidos", 0),
                    "nCandidatos": len(r.get("candidatos", [])),
                    "lider": r.get("lider", "–"),
                } for r in regs]
            })

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