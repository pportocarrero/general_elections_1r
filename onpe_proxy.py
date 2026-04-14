#!/usr/bin/env python3
"""
ONPE 2026 — Proxy v17
Uso local:   python3 onpe_proxy.py
Render.com:  PORT env var tomada automáticamente
"""

import http.server, json, threading, time
import urllib.request, urllib.error, urllib.parse
import gzip, os, sys, concurrent.futures
from datetime import datetime, timezone

PORT        = int(os.environ.get("PORT", 8765))
REFRESH_SEC = 60
CACHE_FILE  = "onpe_cache.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

# ── Endpoints nacionales (confirmados via DevTools) ─────────────
EP_TOTALES    = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"

# ── Endpoints regionales ────────────────────────────────────────
# El parámetro ubigeoNivel1 usa el código sin ceros a la izquierda
# (040000 → 40000, 150000 → 150000) — ONPE usa int parseado
EP_REG_CANDS  = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&ubigeoNivel1={cod}"
EP_REG_TOTALES= BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=ubigeo_nivel_01&idAmbitoGeografico=1&idUbigeoDepartamento={cod}"

# Ubigeos: clave=código enviado a la API, valor=nombre legible
# Se prueba con int(cod) para eliminar ceros iniciales
DEPARTAMENTOS = {
    "010000": "Amazonas",    "020000": "Áncash",       "030000": "Apurímac",
    "040000": "Arequipa",    "050000": "Ayacucho",     "060000": "Cajamarca",
    "070000": "Callao",      "080000": "Cusco",        "090000": "Huancavelica",
    "100000": "Huánuco",     "110000": "Ica",          "120000": "Junín",
    "130000": "La Libertad", "140000": "Lambayeque",   "150000": "Lima",
    "160000": "Loreto",      "170000": "Madre de Dios","180000": "Moquegua",
    "190000": "Pasco",       "200000": "Piura",        "210000": "Puno",
    "220000": "San Martín",  "230000": "Tacna",        "240000": "Tumbes",
    "250000": "Ucayali",
}

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
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
    "candidatos": [], "regiones": [],
}

_cache = None
_lock  = threading.Lock()
_refreshing = False


# ══════════════════════════════════════════════════════════
#  HTTP
# ══════════════════════════════════════════════════════════
def _get_json(url):
    # Cache-bust para evitar CDN de ONPE
    sep = "&" if "?" in url else "?"
    url = f"{url}{sep}_={int(time.time()*1000)}"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding", "") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw.decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        print(f"    HTTP {e.code}: {url.split('?')[0].split('/')[-1]}")
        return None
    except Exception as e:
        print(f"    Error: {str(e)[:60]}")
        return None

def _unwrap(raw):
    """Extrae .data si existe."""
    if isinstance(raw, dict) and "data" in raw:
        return raw["data"]
    return raw


# ══════════════════════════════════════════════════════════
#  PARSERS — basados en estructura real confirmada por debug
# ══════════════════════════════════════════════════════════
def parse_avance_nacional(raw):
    """
    Estructura real confirmada:
    data.actasContabilizadas = 53.886  ← YA es porcentaje
    data.contabilizadas      = 49988   ← absoluto
    data.totalActas          = 92766
    data.enviadasJee         = 155
    data.pendientesJee       = 42623
    data.totalVotosEmitidos  = 11488641
    data.totalVotosValidos   = 9847379
    data.participacionCiudadana = 42.044
    """
    if not raw: return {}
    d = _unwrap(raw)
    if not isinstance(d, dict): return {}

    pct  = float(d.get("actasContabilizadas") or 0)   # YA es %
    cont = int(d.get("contabilizadas") or 0)
    tot  = int(d.get("totalActas") or 92766)
    jee  = int(d.get("enviadasJee") or 0)
    pend = int(d.get("pendientesJee") or max(0, tot - cont - jee))

    return {
        "pctActas":            pct,
        "actasContabilizadas": cont,
        "actasTotales":        tot,
        "actasJEE":            jee,
        "actasPendientes":     pend,
        "votosEmitidos":       int(d.get("totalVotosEmitidos") or 0),
        "votosValidos":        int(d.get("totalVotosValidos") or 0),
        "participacion":       float(d.get("participacionCiudadana") or 0),
    }


def parse_avance_regional(raw):
    """
    El endpoint regional puede devolver estructura ligeramente diferente.
    Intenta los mismos campos que el nacional, más variantes regionales.
    """
    if not raw: return {}
    d = _unwrap(raw)
    if isinstance(d, list):
        d = d[0] if d else {}
    if not isinstance(d, dict): return {}

    # El campo % puede llamarse distinto en la API regional
    pct = float(
        d.get("actasContabilizadas") or          # igual que nacional
        d.get("porcentajeActasContabilizadas") or
        d.get("porcentajeAvanceMesas") or
        d.get("avance") or 0
    )
    cont = int(d.get("contabilizadas") or d.get("mesasContabilizadas") or 0)
    tot  = int(d.get("totalActas") or d.get("totalMesas") or 0)
    jee  = int(d.get("enviadasJee") or 0)

    # Si el % no viene directo, calcularlo
    if pct == 0 and tot > 0 and cont > 0:
        pct = round((cont / tot) * 100, 3)

    return {"pctActas": pct, "actasContabilizadas": cont, "actasTotales": tot, "actasJEE": jee}


def parse_candidatos(raw):
    """
    Estructura real confirmada:
    data = [ {
        codigoAgrupacionPolitica: "8",  (80=blancos, 81=nulos)
        nombreAgrupacionPolitica: "FUERZA POPULAR",
        nombreCandidato:          "KEIKO SOFIA FUJIMORI HIGUCHI",
        totalVotosValidos:        1670250,
        porcentajeVotosValidos:   16.961,
        porcentajeVotosEmitidos:  14.538
    }, ... ]
    """
    if not raw: return [], 0, 0
    items = _unwrap(raw)
    if isinstance(items, dict):
        items = (items.get("candidatos") or items.get("participantes") or
                 items.get("data") or [items])
    if not isinstance(items, list): return [], 0, 0

    candidatos, votos_blancos, votos_nulos = [], 0, 0

    for item in items:
        if not isinstance(item, dict): continue
        cod    = str(item.get("codigoAgrupacionPolitica") or "")
        nombre = (item.get("nombreCandidato") or "").strip()
        partido= (item.get("nombreAgrupacionPolitica") or "").strip()
        votos  = int(item.get("totalVotosValidos") or item.get("votos") or 0)
        pct    = float(item.get("porcentajeVotosValidos") or item.get("porcentaje") or 0)

        if cod == "80": votos_blancos = votos; continue
        if cod == "81": votos_nulos   = votos; continue

        if nombre and (votos > 0 or pct > 0):
            candidatos.append({
                "nombre":  _fmt_nombre(nombre),
                "partido": partido.title(),
                "votos":   votos,
                "pct":     round(pct, 3),
            })

    candidatos.sort(key=lambda x: -x["pct"])
    return candidatos, votos_blancos, votos_nulos


def _fmt_nombre(n):
    """'KEIKO SOFIA FUJIMORI HIGUCHI' → 'Keiko Fujimori'"""
    p = n.strip().title().split()
    if len(p) >= 4: return f"{p[0]} {p[2]}"
    if len(p) == 3: return f"{p[0]} {p[2]}"
    return " ".join(p[:2]) if len(p) >= 2 else n.title()


# ══════════════════════════════════════════════════════════
#  FETCH REGIONAL — prueba dos formatos de ubigeo
# ══════════════════════════════════════════════════════════
def _fetch_region(ubigeo6, nombre):
    """
    Prueba el ubigeo en dos formatos:
      - Con ceros: "040000"  (como viene en el dict)
      - Sin cero inicial: "40000"  (int parseado, el más probable)
    """
    # Convertir a int y de vuelta a string para quitar ceros iniciales
    cod_int = str(int(ubigeo6))   # "040000" → "40000", "150000" → "150000"

    cands, avance = [], {}

    for cod in [cod_int, ubigeo6]:   # primero sin ceros, luego con
        url_c = EP_REG_CANDS.format(cod=cod)
        url_t = EP_REG_TOTALES.format(cod=cod)

        raw_c = _get_json(url_c)
        raw_t = _get_json(url_t)

        cands, _, _ = parse_candidatos(raw_c)
        avance = parse_avance_regional(raw_t)

        if cands:
            break   # encontramos el formato correcto

    if not cands:
        return None

    # Anti-fuga: Lima puede tener >2M, resto no
    total_votos = sum(c["votos"] for c in cands)
    if total_votos > 2_500_000 and ubigeo6 != "150000":
        print(f"    Anti-fuga activada: {nombre} reporta {total_votos:,} votos — descartado")
        return None

    return {
        "nombre":     nombre,
        "pctActas":   round(avance.get("pctActas", 0), 3),
        "actasTotal": avance.get("actasTotales", 0),
        "actasCont":  avance.get("actasContabilizadas", 0),
        "lider":      cands[0]["nombre"],
        "pctLider":   cands[0]["pct"],
        "candidatos": cands,
    }


# ══════════════════════════════════════════════════════════
#  FETCH PRINCIPAL
# ══════════════════════════════════════════════════════════
def fetch_onpe():
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n[{ts}] Consultando ONPE...")
    result   = dict(FALLBACK)
    ok_count = 0

    # 1) Avance nacional
    raw_tot = _get_json(EP_TOTALES)
    avance  = parse_avance_nacional(raw_tot)
    if avance.get("pctActas", 0) > 0:
        result.update(avance)
        ok_count += 1
        print(f"  ✓ Nacional → {result['pctActas']:.3f}% "
              f"({result['actasContabilizadas']:,}/{result['actasTotales']:,} actas)")

    # 2) Candidatos nacionales
    raw_cand = _get_json(EP_CANDIDATOS)
    if raw_cand:
        cands, blancos, nulos = parse_candidatos(raw_cand)
        if cands:
            result["candidatos"]   = cands
            result["votosBlancos"] = blancos
            result["votosNulos"]   = nulos
            ok_count += 1
            print(f"  ✓ Candidatos → {len(cands)}")

    # 3) Regiones — paralelo con ThreadPool
    print(f"  → Regiones ({len(DEPARTAMENTOS)} departamentos)...")
    regiones_nuevas = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        futures = {
            ex.submit(_fetch_region, ubigeo, nombre): nombre
            for ubigeo, nombre in DEPARTAMENTOS.items()
        }
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                regiones_nuevas.append(res)

    if regiones_nuevas:
        # Merge con caché anterior (preserva regiones que la API no devolvió esta vez)
        with _lock:
            prev = {r["nombre"]: r for r in (_cache.get("regiones") or [])}
        for r in regiones_nuevas:
            prev[r["nombre"]] = r
        result["regiones"] = sorted(prev.values(), key=lambda x: x["nombre"])
        print(f"  ✓ Regiones → {len(result['regiones'])} disponibles")
    else:
        # Mantener las del caché anterior
        with _lock:
            prev_regs = (_cache or {}).get("regiones", [])
        if prev_regs:
            result["regiones"] = prev_regs
            print(f"  ~ Regiones → sin nuevos datos, usando caché ({len(prev_regs)})")
        else:
            print("  ✗ Regiones → no disponibles")

    result["fuente"]    = ("api_onpe"         if ok_count >= 2 else
                           "api_onpe_parcial" if ok_count == 1 else "respaldo_local")
    result["timestamp"] = datetime.now(timezone.utc).isoformat()
    print(f"  → fuente: {result['fuente']}")
    return result


# ══════════════════════════════════════════════════════════
#  CACHE
# ══════════════════════════════════════════════════════════
def _load_cache():
    global _cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                _cache = json.load(f)
            n_regs = len(_cache.get("regiones", []))
            print(f"  Cache cargado: {_cache.get('pctActas',0):.3f}% actas, {n_regs} regiones")
            return
        except Exception:
            pass
    _cache = dict(FALLBACK)

def _save_cache(data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠ cache: {e}")

def _do_refresh():
    global _cache, _refreshing
    if _refreshing:
        return _cache
    _refreshing = True
    try:
        data = fetch_onpe()
        with _lock:
            _cache = data
        _save_cache(data)
        return data
    except Exception as e:
        print(f"  ✗ refresh: {e}")
        return _cache
    finally:
        _refreshing = False

def refresh_loop():
    while True:
        try:
            _do_refresh()
        except Exception as e:
            print(f"  ✗ loop: {e}")
        time.sleep(REFRESH_SEC)


# ══════════════════════════════════════════════════════════
#  HTTP HANDLER
# ══════════════════════════════════════════════════════════
class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if args and not any(s in str(args[0]) for s in ["/api/datos", "/api/status"]):
            print(f"  → {args[0]}")

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204); self._cors(); self.end_headers()

    def _json(self, obj, status=200):
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self._cors(); self.end_headers(); self.wfile.write(payload)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/api/datos":
            with _lock: data = dict(_cache)
            self._json(data)

        elif path == "/api/refresh":
            threading.Thread(target=_do_refresh, daemon=True).start()
            with _lock: data = dict(_cache)
            self._json({"ok": True, "fuente": data.get("fuente"),
                        "pctActas": data.get("pctActas", 0),
                        "nRegiones": len(data.get("regiones", []))})

        elif path == "/api/status":
            self._json({"refreshing": _refreshing,
                        "pctActas": (_cache or {}).get("pctActas", 0),
                        "nRegiones": len((_cache or {}).get("regiones", []))})

        elif path == "/api/debug":
            # Endpoint de diagnóstico: muestra raw de 1 región (Arequipa)
            test_cod_int = str(int("040000"))  # "40000"
            self._json({
                "EP_TOTALES":   _get_json(EP_TOTALES),
                "reg_arequipa_int": _get_json(EP_REG_CANDS.format(cod=test_cod_int)),
                "reg_arequipa_6d":  _get_json(EP_REG_CANDS.format(cod="040000")),
                "reg_arequipa_tot": _get_json(EP_REG_TOTALES.format(cod=test_cod_int)),
            })

        elif path in ("/", "/index.html"):
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, "rb") as f: content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers(); self.wfile.write(content)
            else:
                self.send_response(404); self.end_headers()
                self.wfile.write(f"onpe_2026.html no encontrado en: {HTML_FILE}".encode())

        else:
            self.send_response(404); self.end_headers()


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  ONPE 2026 — Proxy v17")
    print("=" * 60)
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"\n  Dashboard: http://localhost:{PORT}")
    print(f"  Debug:     http://localhost:{PORT}/api/debug")
    print(f"  Status:    http://localhost:{PORT}/api/status")
    print(f"  Polling:   cada {REFRESH_SEC}s | Ctrl+C para detener\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Detenido.")
        sys.exit(0)
