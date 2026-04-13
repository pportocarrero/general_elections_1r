#!/usr/bin/env python3
"""
ONPE 2026 — Proxy Local v5  (estructura real confirmada)
Uso:  python3 onpe_proxy.py
Web:  http://localhost:8765
"""

import http.server, json, threading, time
import urllib.request, urllib.error, urllib.parse
import gzip, os, sys
from datetime import datetime, timezone
from collections import defaultdict

PORT        = 8765
REFRESH_SEC = 60
CACHE_FILE  = "onpe_cache.json"
HTML_FILE   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "onpe_2026.html")
BASE        = "https://resultadoelectoral.onpe.gob.pe/presentacion-backend"

EP_MESAS     = BASE + "/mesa/totales?tipoFiltro=eleccion"
EP_TOTALES   = BASE + "/resumen-general/totales?idEleccion=10&tipoFiltro=eleccion"
EP_CANDIDATOS= BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=eleccion"
EP_GEO       = BASE + "/eleccion-presidencial/participantes-ubicacion-geografica-nombre?idEleccion=10&tipoFiltro=departamento"
EP_GEO2      = BASE + "/resumen-general/resultado-por-departamento?idEleccion=10&tipoFiltro=eleccion"
EP_GEO3      = BASE + "/eleccion-presidencial/resultado-departamental?idEleccion=10"
EP_GEO4      = BASE + "/resumen-departamental?idEleccion=10&tipoFiltro=eleccion"

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
    "candidatos": [],
    "regiones": [],
}

_cache = None
_lock  = threading.Lock()


def _get_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            if r.headers.get("Content-Encoding", "") == "gzip":
                raw = gzip.decompress(raw)
            return json.loads(raw.decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as e:
        print(f"    HTTP {e.code} → {url}")
        return None
    except Exception as e:
        print(f"    Error: {e}")
        return None

def _unwrap(raw):
    if isinstance(raw, dict) and "data" in raw:
        return raw["data"]
    return raw


# ── PARSER: avance de actas ─────────────────────────────────────
def parse_avance(raw_totales, raw_mesas):
    """
    EP_TOTALES.data:
      actasContabilizadas: 53.886   (% directo)
      contabilizadas: 49988
      totalActas: 92766
      enviadasJee: 155
      pendientesJee: 42623          (actas pendientes de procesar)
      totalVotosEmitidos: 11488641
      totalVotosValidos: 9847379
      participacionCiudadana: 42.044
    """
    if not raw_totales:
        return {}
    d = _unwrap(raw_totales)
    if not isinstance(d, dict):
        return {}

    pct  = float(d.get("actasContabilizadas") or 0)
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


# ── PARSER: candidatos nacionales ──────────────────────────────
def parse_candidatos(raw):
    """
    EP_CANDIDATOS.data = lista de objetos:
      nombreAgrupacionPolitica: "FUERZA POPULAR"
      nombreCandidato:          "KEIKO SOFIA FUJIMORI HIGUCHI"
      totalVotosValidos:        1670250
      porcentajeVotosValidos:   16.961
      porcentajeVotosEmitidos:  14.538
      codigoAgrupacionPolitica: "8"   (80=blancos, 81=nulos)
    """
    if not raw:
        return [], 0, 0

    items = _unwrap(raw)
    if not isinstance(items, list):
        return [], 0, 0

    candidatos   = []
    votos_blancos = 0
    votos_nulos   = 0

    for item in items:
        cod    = str(item.get("codigoAgrupacionPolitica") or "")
        nombre = (item.get("nombreCandidato") or "").strip()
        partido= (item.get("nombreAgrupacionPolitica") or "").strip()
        votos  = int(item.get("totalVotosValidos") or 0)
        pct    = float(item.get("porcentajeVotosValidos") or 0)

        if cod == "80":          # votos en blanco
            votos_blancos = votos
            continue
        if cod == "81":          # votos nulos
            votos_nulos = votos
            continue

        # Formatear nombre: "KEIKO SOFIA FUJIMORI HIGUCHI" → "Keiko Fujimori"
        nombre_fmt  = _fmt_nombre(nombre)
        partido_fmt = partido.title()

        if nombre_fmt and (votos > 0 or pct > 0):
            candidatos.append({
                "nombre":  nombre_fmt,
                "partido": partido_fmt,
                "votos":   votos,
                "pct":     round(pct, 3),
            })

    candidatos.sort(key=lambda x: -x["pct"])
    return candidatos, votos_blancos, votos_nulos


def _fmt_nombre(nombre_mayus):
    """'KEIKO SOFIA FUJIMORI HIGUCHI' → 'Keiko Fujimori' (primer + apellido paterno)."""
    partes = nombre_mayus.strip().title().split()
    if len(partes) >= 4:
        # Asume orden: Nombre1 Nombre2 Apellido1 Apellido2
        return f"{partes[0]} {partes[2]}"
    if len(partes) == 3:
        return f"{partes[0]} {partes[2]}"
    if len(partes) == 2:
        return f"{partes[0]} {partes[1]}"
    return nombre_mayus.title()


# ── PARSER: regiones ───────────────────────────────────────────
def parse_regiones(raw):
    """
    Intentamos EP_GEO con tipoFiltro=departamento.
    Si no, intentamos extraer info regional del mismo EP_CANDIDATOS
    usando el campo idUbigeoDepartamento o similar.
    """
    if not raw:
        return []
    items = _unwrap(raw)
    if not isinstance(items, list):
        return []

    regs = defaultdict(list)

    for item in items:
        cod_ag = str(item.get("codigoAgrupacionPolitica") or "")
        if cod_ag in ("80", "81"):   # blancos y nulos
            continue

        region  = (item.get("nombreUbigeo") or item.get("departamento") or
                   item.get("nombreDepartamento") or "").strip().title()
        nombre  = _fmt_nombre(item.get("nombreCandidato") or "")
        partido = (item.get("nombreAgrupacionPolitica") or "").strip().title()
        pct     = float(item.get("porcentajeVotosValidos") or 0)
        pct_ac  = float(item.get("porcentajeAvanceMesas") or
                        item.get("avanceMesas") or
                        item.get("porcentajeMesas") or 0)
        votos   = int(item.get("totalVotosValidos") or 0)

        if region and nombre and pct > 0:
            regs[region].append({
                "n": nombre, "p": partido,
                "pct": pct, "pctActas": pct_ac, "votos": votos
            })

    result = []
    for region, cands in regs.items():
        cands_s = sorted(cands, key=lambda x: -x["pct"])
        lider   = cands_s[0]
        result.append({
            "nombre":    region,
            "pctActas":  round(lider.get("pctActas", 0), 1),
            "lider":     lider["n"],
            "pctLider":  round(lider["pct"], 2),
            "candidatos":[{"nombre": c["n"], "partido": c["p"],
                           "votos": c["votos"], "pct": round(c["pct"], 2)}
                          for c in cands_s],
        })

    return sorted(result, key=lambda x: x["nombre"])


# ══════════════════════════════════════════════════════════
#  FETCH PRINCIPAL
# ══════════════════════════════════════════════════════════
def fetch_onpe():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Consultando ONPE...")
    result   = dict(FALLBACK)
    ok_count = 0

    # 1) Avance de actas
    raw_tot = _get_json(EP_TOTALES)
    raw_mes = _get_json(EP_MESAS)
    avance  = parse_avance(raw_tot, raw_mes)
    if avance.get("pctActas", 0) > 0:
        result.update(avance)
        ok_count += 1
        print(f"  ✓ avance   → {result['pctActas']:.3f}% "
              f"({result['actasContabilizadas']:,}/{result['actasTotales']:,} actas | "
              f"JEE: {result['actasJEE']:,} | pendientes: {result['actasPendientes']:,})")
    else:
        print("  ⚠ avance sin datos")

    # 2) Candidatos nacionales
    raw_cand = _get_json(EP_CANDIDATOS)
    if raw_cand is not None:
        cands, blancos, nulos = parse_candidatos(raw_cand)
        if cands:
            result["candidatos"]  = cands
            result["votosBlancos"] = blancos
            result["votosNulos"]   = nulos
            ok_count += 1
            print(f"  ✓ candidatos → {len(cands)} participantes")
        else:
            print("  ⚠ candidatos: sin datos parseados")

    # 3) Regiones — probar varios endpoints
    geo_endpoints = [EP_GEO, EP_GEO2, EP_GEO3, EP_GEO4]
    regiones = []
    for ep in geo_endpoints:
        raw_geo = _get_json(ep)
        if raw_geo is not None:
            regiones = parse_regiones(raw_geo)
            if regiones:
                print(f"  ✓ regiones  → {len(regiones)} departamentos (ep: {ep.split('/')[-1][:30]})")
                break
            else:
                # Guardar raw para diagnóstico
                result[f"_debug_geo_{ep.split('?')[0].split('/')[-1]}"] = str(_unwrap(raw_geo))[:200]
                print(f"  ~ regiones: {ep.split('?')[0].split('/')[-1][:30]} respondió pero sin datos: {str(_unwrap(raw_geo))[:100]}")
    if not regiones:
        # Último intento: extraer del EP_CANDIDATOS con ubigeo
        regiones = parse_regiones(raw_cand)
        if regiones:
            print(f"  ✓ regiones  → {len(regiones)} departamentos (desde candidatos)")
        else:
            print("  ~ regiones: no disponibles en este corte")
    if regiones:
        result["regiones"] = regiones

    result["fuente"]    = ("api_onpe" if ok_count >= 2 else
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
            print(f"  Cache cargado: {_cache.get('pctActas',0):.3f}% actas")
            return
        except Exception:
            pass
    _cache = dict(FALLBACK)
    _cache["timestamp"] = datetime.now(timezone.utc).isoformat()

def _save_cache(data):
    try:
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  ⚠ cache: {e}")

def _do_refresh():
    global _cache
    data = fetch_onpe()
    with _lock:
        _cache = data
    _save_cache(data)
    return data

def refresh_loop():
    while True:
        try:
            _do_refresh()
        except Exception as e:
            print(f"  ✗ {e}")
        time.sleep(REFRESH_SEC)


# ══════════════════════════════════════════════════════════
#  HTTP HANDLER
# ══════════════════════════════════════════════════════════
class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        if args and "/api/datos" not in str(args[0]):
            print(f"  GET {args[0]}")

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(204)
        self._cors()
        self.end_headers()

    def _json(self, obj, status=200):
        payload = json.dumps(obj, ensure_ascii=False, indent=2).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self._cors()
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path

        if path == "/api/datos":
            with _lock:
                data = dict(_cache)
            self._json(data)

        elif path == "/api/refresh":
            data = _do_refresh()
            self._json({"ok": True, "fuente": data["fuente"],
                        "pctActas": data.get("pctActas", 0),
                        "nCandidatos": len(data.get("candidatos", [])),
                        "timestamp": data.get("timestamp", "")})

        elif path == "/api/debug":
            print("  [DEBUG] fetching raw...")
            self._json({
                "EP_TOTALES":    _get_json(EP_TOTALES),
                "EP_MESAS":      _get_json(EP_MESAS),
                "EP_CANDIDATOS": _get_json(EP_CANDIDATOS),
                "EP_GEO":        _get_json(EP_GEO),
            })

        elif path in ("/", "/index.html"):
            if os.path.exists(HTML_FILE):
                with open(HTML_FILE, "rb") as f:
                    content = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(f"No se encontró onpe_2026.html\nBuscado en: {HTML_FILE}".encode())

        else:
            self.send_response(404)
            self.end_headers()


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("  ONPE 2026 — Proxy Local  v5")
    print("=" * 60)
    _load_cache()
    threading.Thread(target=_do_refresh, daemon=True).start()
    threading.Thread(target=refresh_loop, daemon=True).start()
    server = http.server.ThreadingHTTPServer(("localhost", PORT), Handler)
    print(f"\n  Dashboard:  http://localhost:{PORT}")
    print(f"  Debug raw:  http://localhost:{PORT}/api/debug")
    print(f"  Polling:    cada {REFRESH_SEC}s  |  Ctrl+C para detener\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Servidor detenido.")
        sys.exit(0)
