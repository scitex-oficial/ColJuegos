"""
IGT — Coljuegos Extractor de Establecimientos Autorizados
Streamlit Cloud App
"""

import streamlit as st
import threading
import requests
import pandas as pd
import io
import time
from bs4 import BeautifulSoup
from datetime import datetime

# ── Configuración ──────────────────────────────────────────
BASE_URL      = "https://www.coljuegos.gov.co"
OPERADORES_URL = f"{BASE_URL}/loader.php?lServicio=operadores"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": OPERADORES_URL,
    "X-Requested-With": "XMLHttpRequest",
}
DELAY       = 0.4
TIMEOUT     = 60
MAX_RETRIES = 3
RETRY_WAIT  = 3
TOTAL_DEP   = 33

# ── Paleta IGT ─────────────────────────────────────────────
CSS = """
<style>
/* Fuente y fondo */
@import url('https://fonts.googleapis.com/css2?family=Segoe+UI:wght@400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Segoe UI', sans-serif;
    background-color: #F0F4FA;
}

/* Header */
.igt-header {
    background: linear-gradient(135deg, #0051BA 80%, #003d8c 100%);
    border-top: 5px solid #FF6B35;
    padding: 24px 32px 20px 32px;
    border-radius: 10px;
    margin-bottom: 20px;
    color: white;
}
.igt-header h1 {
    font-size: 2rem;
    font-weight: 800;
    color: white;
    margin: 0;
    letter-spacing: 1px;
}
.igt-header p {
    color: #A8C8FF;
    font-size: 1rem;
    margin: 4px 0 0 0;
}

/* Tarjetas de estadísticas */
.stat-card {
    background: white;
    border-radius: 10px;
    padding: 16px;
    text-align: center;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}
.stat-value {
    font-size: 2rem;
    font-weight: 800;
    margin: 0;
}
.stat-label {
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #525252;
    margin: 4px 0 0 0;
}

/* Log */
.log-box {
    background: #0D1117;
    border-radius: 8px;
    padding: 14px 16px;
    font-family: 'Consolas', monospace;
    font-size: 0.82rem;
    color: #C9D1D9;
    max-height: 340px;
    overflow-y: auto;
    line-height: 1.6;
    border: 1px solid #30363D;
}
.log-dep  { color: #FFC220; font-weight: bold; }
.log-mun  { color: #C9D1D9; }
.log-ok   { color: #00A651; }
.log-err  { color: #E31C23; }
.log-info { color: #4A90E2; }
.log-retry{ color: #FF6B35; }

/* Badge de estado */
.badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 20px;
    font-size: 0.8rem;
    font-weight: 700;
    letter-spacing: 0.5px;
}
.badge-running  { background: #FFC220; color: #222; }
.badge-done     { background: #00A651; color: white; }
.badge-idle     { background: #525252; color: white; }
.badge-error    { background: #E31C23; color: white; }

/* Botón primario IGT */
div.stButton > button:first-child {
    background-color: #0051BA;
    color: white;
    border: none;
    border-radius: 6px;
    font-weight: 700;
    font-size: 1rem;
    padding: 10px 28px;
    transition: background 0.2s;
}
div.stButton > button:first-child:hover {
    background-color: #003d8c;
    color: white;
}

/* Footer */
.igt-footer {
    background: #003d8c;
    border-top: 3px solid #FF6B35;
    border-radius: 8px;
    padding: 10px 20px;
    color: #8899BB;
    font-size: 0.78rem;
    text-align: center;
    margin-top: 24px;
}

/* Ocultar menú streamlit */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
</style>
"""

# ══════════════════════════════════════════════════════════
#  Scraping
# ══════════════════════════════════════════════════════════

def parse_select(html):
    soup = BeautifulSoup(html, "html.parser")
    return [
        (o.get("value", "").strip(), o.get_text(strip=True))
        for o in soup.find_all("option")
        if o.get("value", "").strip()
        and o.get("value", "").strip() not in ("Seleccione...", "Seleccione....")
    ]


def request_with_retry(fn, label=""):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_WAIT * attempt)
    raise RuntimeError(f"Falló tras {MAX_RETRIES} intentos: {label}")


def get_tokens(session):
    resp = session.get(OPERADORES_URL, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    tokens = {}
    for i in range(1, 6):
        el = soup.find("input", {"id": f"sxToken{i}"})
        if el:
            tokens[f"sxToken{i}"] = el.get("value", "")
    if not tokens:
        raise ValueError("No se pudieron obtener los tokens de seguridad.")
    return tokens


def get_departamentos(session, tokens):
    def _r():
        r = session.post(f"{OPERADORES_URL}&lFuncion=cargar_departamentos_php",
                         data={"juego": "1", "sxToken": tokens["sxToken1"]},
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return parse_select(r.text)
    return request_with_retry(_r)


def get_municipios(session, cod, tokens):
    def _r():
        r = session.post(f"{OPERADORES_URL}&lFuncion=cargar_municipios_php",
                         data={"f": cod, "juego": "1", "sxToken": tokens["sxToken2"]},
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return parse_select(r.text)
    return request_with_retry(_r)


def get_establecimientos(session, cod, tokens):
    def _r():
        r = session.post(f"{OPERADORES_URL}&lFuncion=cargar_establecimientos_php",
                         data={"f": cod, "juego": "1", "sxToken": tokens["sxToken3"]},
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        return parse_select(r.text)
    return request_with_retry(_r)


def get_detalle(session, cod, tokens):
    def _r():
        r = session.post(f"{OPERADORES_URL}&lFuncion=cargar_detalle_establecimientos_php",
                         data={"f": cod, "sxToken": tokens["sxToken5"]},
                         headers=HEADERS, timeout=TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        total = ""
        txt = soup.get_text()
        if "Total instrumentos:" in txt:
            try:
                total = txt.split("Total instrumentos:")[1].strip().split()[0]
            except Exception:
                pass
        table = soup.find("table")
        if not table:
            return [], total
        hdrs = [th.get_text(strip=True) for th in table.find_all("th")]
        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            rows.append({hdrs[i]: tds[i].get_text(strip=True)
                         for i in range(min(len(hdrs), len(tds)))})
        return rows, total
    return request_with_retry(_r)


# ══════════════════════════════════════════════════════════
#  Hilo de scraping
# ══════════════════════════════════════════════════════════

def run_scraper(state: dict):
    """Corre en un thread separado. Actualiza `state` en tiempo real."""
    try:
        state["log"].append(("info", "Conectando con www.coljuegos.gov.co..."))
        session = requests.Session()
        session.headers.update(HEADERS)
        tokens = get_tokens(session)
        state["log"].append(("ok", "✓ Sesión iniciada — tokens obtenidos"))

        departamentos = get_departamentos(session, tokens)
        total = len(departamentos)
        state["total_dep"] = total
        state["log"].append(("ok", f"✓ {total} departamentos encontrados"))

        all_records = []

        for i, (cod_dep, nom_dep) in enumerate(departamentos, 1):
            if state.get("stop"):
                state["log"].append(("err", "⏹ Extracción detenida por el usuario."))
                break

            state["current_dep"] = nom_dep
            state["dep_done"]    = i
            state["log"].append(("dep", f"[{i}/{total}]  {nom_dep}"))
            time.sleep(DELAY)

            try:
                municipios = get_municipios(session, cod_dep, tokens)
            except Exception as e:
                state["log"].append(("err", f"  ✗ Error municipios {nom_dep}: {e}"))
                state["errors"] += 1
                continue

            for cod_mun, nom_mun in municipios:
                if state.get("stop"):
                    break
                time.sleep(DELAY)
                try:
                    ests = get_establecimientos(session, cod_mun, tokens)
                except Exception as e:
                    state["log"].append(("err", f"  ✗ {nom_mun}: {e}"))
                    state["errors"] += 1
                    continue

                if not ests:
                    continue

                state["mun_done"] += 1
                state["log"].append(("mun", f"  {nom_mun}: {len(ests)} establecimientos"))

                for cod_est, nom_est in ests:
                    if state.get("stop"):
                        break
                    time.sleep(DELAY)
                    try:
                        det, total_inst = get_detalle(session, cod_est, tokens)
                    except Exception as e:
                        state["log"].append(("retry", f"    ✗ {nom_est[:45]}..."))
                        state["errors"] += 1
                        det, total_inst = [], ""

                    state["est_done"] += 1
                    base = {
                        "Departamento":       nom_dep,
                        "Cod_Departamento":   cod_dep,
                        "Municipio":          nom_mun,
                        "Cod_Municipio":      cod_mun,
                        "Establecimiento":    nom_est,
                        "Cod_Establecimiento": cod_est,
                        "Total_Instrumentos": total_inst,
                    }
                    if det:
                        for row in det:
                            r = base.copy(); r.update(row)
                            all_records.append(r)
                    else:
                        all_records.append(base)

                    state["rows"] = len(all_records)

            state["log"].append(("ok", f"  ✓ {nom_dep} — {len(all_records):,} filas totales\n"))

        # Guardar en memoria
        if all_records:
            df = pd.DataFrame(all_records)
            buf = io.BytesIO()
            df.to_excel(buf, index=False, engine="openpyxl")
            buf.seek(0)
            state["excel_bytes"] = buf.read()
            state["excel_name"]  = f"coljuegos_casinos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            state["summary"]     = {
                "departamentos":    df["Departamento"].nunique(),
                "municipios":       df["Municipio"].nunique(),
                "establecimientos": df["Establecimiento"].nunique(),
                "filas":            len(df),
            }
            state["log"].append(("ok", "─" * 50))
            state["log"].append(("ok", "✅  EXTRACCIÓN COMPLETADA"))
            state["log"].append(("info", f"   Establecimientos: {df['Establecimiento'].nunique():,}"))
            state["log"].append(("info", f"   Total filas:      {len(df):,}"))
            state["log"].append(("ok", "─" * 50))
            state["status"] = "done"
        else:
            state["status"] = "error"

    except Exception as e:
        state["log"].append(("err", f"❌ Error crítico: {e}"))
        state["status"] = "error"


# ══════════════════════════════════════════════════════════
#  UI Streamlit
# ══════════════════════════════════════════════════════════

def init_state():
    defaults = dict(
        status="idle",   # idle | running | done | error
        stop=False,
        log=[],
        dep_done=0, mun_done=0, est_done=0, rows=0, errors=0,
        total_dep=TOTAL_DEP,
        current_dep="",
        excel_bytes=None, excel_name="", summary=None,
        thread=None,
    )
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def log_to_html(entries):
    css_map = {"dep": "log-dep", "ok": "log-ok", "err": "log-err",
               "info": "log-info", "mun": "log-mun", "retry": "log-retry"}
    lines = []
    for tag, msg in entries[-120:]:      # últimas 120 líneas
        cls = css_map.get(tag, "log-mun")
        safe = msg.replace("&", "&amp;").replace("<", "&lt;")
        lines.append(f'<span class="{cls}">{safe}</span>')
    return '<br>'.join(lines)


def render_stat(label, value, color):
    st.markdown(
        f"""<div class="stat-card">
              <p class="stat-value" style="color:{color};">{value:,}</p>
              <p class="stat-label">{label}</p>
            </div>""",
        unsafe_allow_html=True)


def main():
    st.set_page_config(
        page_title="IGT — Coljuegos Extractor",
        page_icon="🎰",
        layout="wide",
        initial_sidebar_state="collapsed",
    )
    st.markdown(CSS, unsafe_allow_html=True)
    init_state()
    s = st.session_state

    # ── Header ─────────────────────────────────────────────
    st.markdown("""
    <div class="igt-header">
        <h1>IGT &nbsp;|&nbsp; Coljuegos Extractor</h1>
        <p>Establecimientos de Juegos Localizados Autorizados — Colombia</p>
    </div>
    """, unsafe_allow_html=True)

    # ── Estado ─────────────────────────────────────────────
    badge_map = {
        "idle":    ("Listo para iniciar", "badge-idle"),
        "running": ("Extrayendo datos...", "badge-running"),
        "done":    ("Completado", "badge-done"),
        "error":   ("Error", "badge-error"),
    }
    badge_txt, badge_cls = badge_map.get(s.status, ("", "badge-idle"))
    st.markdown(f'<span class="badge {badge_cls}">{badge_txt}</span>',
                unsafe_allow_html=True)
    st.markdown("")

    # ── Tarjetas de estadísticas ────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: render_stat("Departamentos",    s.dep_done,  "#0051BA")
    with c2: render_stat("Municipios",       s.mun_done,  "#4A90E2")
    with c3: render_stat("Establecimientos", s.est_done,  "#FF6B35")
    with c4: render_stat("Total Filas",      s.rows,      "#00A651")
    with c5: render_stat("Errores",          s.errors,    "#E31C23")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Progreso ────────────────────────────────────────────
    pct = int((s.dep_done / s.total_dep) * 100)
    st.progress(pct / 100,
                text=f"Departamento {s.dep_done}/{s.total_dep}"
                     + (f"  —  {s.current_dep}" if s.current_dep else ""))

    # ── Botones ─────────────────────────────────────────────
    b1, b2, b3, _ = st.columns([1, 1, 1, 4])

    with b1:
        if st.button("▶  Iniciar", disabled=(s.status == "running"), use_container_width=True):
            # Reset
            for k in ("log", "dep_done", "mun_done", "est_done",
                      "rows", "errors", "excel_bytes", "summary"):
                s[k] = [] if k == "log" else (None if k in ("excel_bytes", "summary") else 0)
            s.stop   = False
            s.status = "running"
            s.current_dep = ""

            t = threading.Thread(target=run_scraper, args=(s,), daemon=True)
            s.thread = t
            t.start()
            st.rerun()

    with b2:
        if st.button("⏹  Detener", disabled=(s.status != "running"), use_container_width=True):
            s.stop = True
            st.rerun()

    with b3:
        if s.status == "done" and s.excel_bytes:
            st.download_button(
                label="📥  Descargar Excel",
                data=s.excel_bytes,
                file_name=s.excel_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Log ─────────────────────────────────────────────────
    st.markdown("**Actividad en tiempo real**")
    log_html = log_to_html(s.log)
    st.markdown(
        f'<div class="log-box">{log_html if log_html else "<span style=\'color:#555\'>Esperando inicio...</span>"}</div>',
        unsafe_allow_html=True)

    # ── Resumen final ───────────────────────────────────────
    if s.status == "done" and s.summary:
        st.markdown("<br>", unsafe_allow_html=True)
        st.success(
            f"✅  Extracción completada — "
            f"**{s.summary['establecimientos']:,}** establecimientos  |  "
            f"**{s.summary['filas']:,}** filas  |  "
            f"**{s.summary['departamentos']}** departamentos  |  "
            f"**{s.summary['municipios']}** municipios")

    # ── Footer ──────────────────────────────────────────────
    st.markdown(
        '<div class="igt-footer">© IGT — International Game Technology &nbsp;|&nbsp; Datos: Coljuegos Colombia</div>',
        unsafe_allow_html=True)

    # ── Auto-refresh mientras corre ─────────────────────────
    if s.status == "running":
        # Detectar si el thread terminó
        if s.thread and not s.thread.is_alive() and s.status == "running":
            if s.get("excel_bytes"):
                s.status = "done"
            else:
                s.status = "error"
        time.sleep(2)
        st.rerun()


if __name__ == "__main__":
    main()
