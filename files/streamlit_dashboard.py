import sqlite3
import html
import math
import pandas as pd
import streamlit as st
import numpy as np
import plotly.graph_objects as go

DB_PATH = "cell_counts.db"

POP_ORDER = ["b_cell", "cd8_t_cell", "cd4_t_cell", "nk_cell", "monocyte"]
POP_LABELS = {
    "b_cell": "B Cells",
    "cd8_t_cell": "CD8 T Cells",
    "cd4_t_cell": "CD4 T Cells",
    "nk_cell": "NK Cells",
    "monocyte": "Monocytes",
}

# Alternative table column widths: Sample, 5 pops, Total (sum=100)
COL_PCTS = [16, 14, 14, 14, 14, 14, 14]

# Required header/body widths (sum=100): idx | sample | total_count | population | count | percentage
REQ_IDX_PCT = 8
REQ_COL_PCTS = [24, 17, 17, 14, 20]  # sample...percentage (sum=92)
REQ_BODY_COL_PCTS = [REQ_IDX_PCT] + REQ_COL_PCTS

REQUIRED_COLS = ["sample", "total_count", "population", "count", "percentage"]

# Header-only horizontal buffers (pads affect header buttons, not the body tables)
OPT_LEFT_PAD, OPT_RIGHT_PAD = 1, 3.5
REQ_LEFT_PAD, REQ_RIGHT_PAD = 1, 3.5

PAGER_PULL_UP_PX = 80


# ----------------------------
# DB + queries (cached)
# ----------------------------
@st.cache_resource
def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@st.cache_data(show_spinner=False)
def load_filter_options(db_path: str) -> dict:
    conn = get_conn(db_path)

    def col(sql: str):
        return [r[0] for r in conn.execute(sql).fetchall()]

    return dict(
        projects=col("SELECT project_id FROM projects ORDER BY project_id"),
        conditions=col(
            "SELECT DISTINCT condition FROM subjects WHERE condition IS NOT NULL ORDER BY condition"
        ),
        responses=col(
            "SELECT DISTINCT response FROM subjects WHERE response IS NOT NULL ORDER BY response"
        ),
        treatments=col("SELECT name FROM treatments ORDER BY name"),
        sample_types=col(
            "SELECT DISTINCT sample_type FROM samples WHERE sample_type IS NOT NULL ORDER BY sample_type"
        ),
    )


@st.cache_data(show_spinner=False)
def query_part2_frequencies(
    db_path: str,
    project: str | None,
    condition: str | None,
    response: str | None,
    treatment: str | None,
    sample_type: str | None,
) -> pd.DataFrame:
    where, params = [], []
    if project:
        where.append("p.project_id = ?")
        params.append(project)
    if condition:
        where.append("sub.condition = ?")
        params.append(condition)
    if response:
        where.append("sub.response = ?")
        params.append(response)
    if treatment:
        where.append("t.name = ?")
        params.append(treatment)
    if sample_type:
        where.append("sa.sample_type = ?")
        params.append(sample_type)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"""
    WITH filtered_counts AS (
        SELECT
            sa.sample_id AS sample,
            cp.name AS population,
            cc.count AS count
        FROM cell_counts cc
        JOIN samples sa ON sa.sample_id = cc.sample_id
        JOIN subjects sub ON sub.subject_id = sa.subject_id
        JOIN projects p ON p.project_id = sub.project_id
        LEFT JOIN treatments t ON t.treatment_id = sa.treatment_id
        JOIN cell_populations cp ON cp.population_id = cc.population_id
        {where_sql}
    ),
    sample_totals AS (
        SELECT sample, SUM(count) AS total_count
        FROM filtered_counts
        GROUP BY sample
    )
    SELECT
        fc.sample AS sample,
        st.total_count AS total_count,
        fc.population AS population,
        fc.count AS count,
        CASE
            WHEN st.total_count > 0 THEN ROUND(100.0 * fc.count / st.total_count, 2)
            ELSE NULL
        END AS percentage
    FROM filtered_counts fc
    JOIN sample_totals st ON st.sample = fc.sample
    ORDER BY fc.sample, fc.population;
    """
    return pd.read_sql_query(sql, get_conn(db_path), params=params)


def norm(v: str) -> str | None:
    return None if v == "(All)" else v


# ----------------------------
# Required table prep
# ----------------------------
def make_required_df(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    out = df.copy()

    sample_order = out["sample"].drop_duplicates().tolist()
    out["sample"] = pd.Categorical(out["sample"], categories=sample_order, ordered=True)

    out["population"] = out["population"].astype(str)
    for c in ["total_count", "count", "percentage"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.sort_values(["sample", "population"], kind="stable").reset_index(drop=True)
    out["idx"] = out.index + 1
    return out[["idx"] + REQUIRED_COLS]


# ----------------------------
# Shared sort state + per-table paging
# ----------------------------
def init_state(page_key: str):
    st.session_state.setdefault("global_sort_key", "sample")
    st.session_state.setdefault("global_sort_dir", "asc")
    st.session_state.setdefault(f"{page_key}_page", 1)
    st.session_state.setdefault(f"{page_key}_page_input", int(st.session_state[f"{page_key}_page"]))


def clamp_page(p: int, total_pages: int) -> int:
    total_pages = max(1, int(total_pages))
    return max(1, min(int(p), total_pages))


def set_sort(new_key: str, page_key: str):
    init_state(page_key)
    if st.session_state.global_sort_key == new_key:
        st.session_state.global_sort_dir = "asc" if st.session_state.global_sort_dir == "desc" else "desc"
    else:
        st.session_state.global_sort_key = new_key
        st.session_state.global_sort_dir = "asc"

    st.session_state[f"{page_key}_page"] = 1
    st.session_state[f"{page_key}_page_input"] = 1


def render_pager(total_pages: int, page_key: str, pull_up_px: int = PAGER_PULL_UP_PX):
    init_state(page_key)
    page_state_key = f"{page_key}_page"
    page_input_key = f"{page_key}_page_input"

    def sync():
        st.session_state[page_input_key] = int(st.session_state[page_state_key])

    def prev_page():
        st.session_state[page_state_key] = clamp_page(st.session_state[page_state_key] - 1, total_pages)
        sync()

    def next_page():
        st.session_state[page_state_key] = clamp_page(st.session_state[page_state_key] + 1, total_pages)
        sync()

    def jump_page():
        desired = st.session_state.get(page_input_key, 1)
        st.session_state[page_state_key] = clamp_page(desired, total_pages)
        sync()

    st.markdown(
        f"""
        <style>
          .pager-anchor-{page_key} {{ margin-top: {-int(pull_up_px)}px; }}

          /* Keep the 4 widgets on one row and vertically centered */
          .pager-anchor-{page_key} + div [data-testid="stHorizontalBlock"] {{
            align-items: center !important;
          }}

          .pager-anchor-{page_key} + div [data-testid="stButton"] > button {{
            padding: 0px 10px !important;
            height: 28px !important;
            min-height: 28px !important;
            font-size: 12px !important;
            border-radius: 8px !important;
            background: #f0f0f0 !important;
            border: 1px solid #d5d5d5 !important;
            color: #444 !important;
            box-shadow: none !important;
          }}
          .pager-anchor-{page_key} + div [data-testid="stButton"] > button:hover {{
            background: #e9e9e9 !important;
            border-color: #cfcfcf !important;
          }}
          .pager-anchor-{page_key} + div [data-testid="stButton"] > button:disabled {{
            background: #f0f0f0 !important;
            border: 1px solid #d5d5d5 !important;
            color: #999 !important;
            opacity: 1 !important;
          }}

          .pager-anchor-{page_key} + div [data-testid="stNumberInput"] {{
            width: 70px !important;
            min-width: 70px !important;
            max-width: 70px !important;
          }}
          .pager-anchor-{page_key} + div [data-testid="stNumberInput"] input {{
            height: 28px !important;
            min-height: 28px !important;
            font-size: 12px !important;
            padding-top: 0px !important;
            padding-bottom: 0px !important;
            text-align: center !important;
          }}

          /* Make "/ 2100" align perfectly with the input + buttons */
          .pager-anchor-{page_key} + div .pager-total {{
            height: 28px !important;
            display: flex !important;
            align-items: center !important;
            justify-content: flex-start !important;
            margin: 0 !important;
            padding: 0 !important;
            font-size: 26px !important;
            font-weight: 450 !important;
            color: #444 !important;
            white-space: nowrap !important;
            line-height: 28px !important;
          }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f'<div class="pager-anchor-{page_key}"></div>', unsafe_allow_html=True)

    _, right = st.columns([12, 3], gap="small")
    with right:
        # Tuned widths to keep the number input visually centered between arrows
        c1, c2, c3, c4 = st.columns([0.9, 1.4, 1.0, 0.9], gap="small")

        with c1:
            st.button(
                "◀", on_click=prev_page, disabled=(st.session_state[page_state_key] <= 1), key=f"{page_key}_prev", use_container_width=True,
            )

        with c2:
            st.number_input(
                "Page", min_value=1, max_value=int(total_pages), step=1, label_visibility="collapsed", key=page_input_key, on_change=jump_page,
            )

        with c3:
            st.markdown(
                f'<div class="pager-total">/ {int(total_pages)}</div>', unsafe_allow_html=True,
            )

        with c4:
            st.button(
                "▶", on_click=next_page, disabled=(st.session_state[page_state_key] >= total_pages), key=f"{page_key}_next", use_container_width=True,
            )


# ----------------------------
# Required header + required HTML table
# ----------------------------
def render_required_sort_header(page_key: str):
    init_state(page_key)

    sort_key = st.session_state.global_sort_key
    sort_dir = st.session_state.global_sort_dir
    arrow = "▲" if sort_dir == "asc" else "▼"

    def label(k: str, text: str):
        return f"{text} {arrow}" if sort_key == k else text

    st.markdown(
        f"""
        <style>
        div:has(.req-sort-anchor-{page_key}) [data-testid="stHorizontalBlock"] {{ gap: 0.45rem !important; }}
        div:has(.req-sort-anchor-{page_key}) [data-testid="stButton"] {{ padding-left: 0 !important; padding-right: 0 !important; }}
        div:has(.req-sort-anchor-{page_key}) [data-testid="stButton"] > button {{
          width: 100% !important;
          border-radius: 10px !important;
          border: 1px solid #e6bcbc !important;
          background: #FFE1E1 !important;
          color: #333 !important;
          padding: 10px 12px !important;
          font-size: 16px !important;
          font-weight: 700 !important;
          text-align: center !important;
          box-shadow: none !important;
          white-space: nowrap !important;
          overflow: hidden !important;
          text-overflow: ellipsis !important;
        }}
        div:has(.req-sort-anchor-{page_key}) [data-testid="stButton"] > button:hover {{ filter: brightness(0.98); }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(f'<div class="req-sort-anchor-{page_key}"></div>', unsafe_allow_html=True)

    outer = st.columns([REQ_LEFT_PAD, 100, REQ_RIGHT_PAD], gap="small")
    with outer[0]:
        st.markdown("&nbsp;", unsafe_allow_html=True)

    with outer[1]:
        cols = st.columns(REQ_BODY_COL_PCTS, gap="medium")

        with cols[0]:
            st.markdown("&nbsp;", unsafe_allow_html=True)

        with cols[1]:
            st.button(label("sample", "sample"), on_click=set_sort, args=("sample", page_key), use_container_width=True, key=f"{page_key}_h_sample")
        with cols[2]:
            st.button(label("total_count", "total_count"), on_click=set_sort, args=("total_count", page_key), use_container_width=True, key=f"{page_key}_h_total")
        with cols[3]:
            st.button(label("population", "population"), on_click=set_sort, args=("population", page_key), use_container_width=True, key=f"{page_key}_h_population")
        with cols[4]:
            st.button(label("count", "count"), on_click=set_sort, args=("count", page_key), use_container_width=True, key=f"{page_key}_h_count")
        with cols[5]:
            st.button(label("percentage", "percentage"), on_click=set_sort, args=("percentage", page_key), use_container_width=True, key=f"{page_key}_h_percentage")

    with outer[2]:
        st.markdown("&nbsp;", unsafe_allow_html=True)

def render_required_long_table_html(df_required_page: pd.DataFrame, height_px: int = 560):
    colgroup = "<colgroup>" + "".join([f'<col style="width:{p}%">' for p in REQ_BODY_COL_PCTS]) + "</colgroup>"

    css = f"""
    <style>
      .req-wrap {{
        height: {int(height_px)}px;
        overflow: auto;
        scrollbar-gutter: stable;
        border: none;
        border-radius: 10px;
        background: white;
      }}
      table.req {{
        width: 100%;
        border-collapse: collapse;
        table-layout: fixed;
        font-size: 18px;
      }}
      table.req tbody td {{
        text-align: left;
        padding: 10px 12px;
        border-bottom: 1px solid #f0f0f0;
        vertical-align: top;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }}
      table.req tbody tr:hover td {{ background: #fff7f7; }}
      table.req tbody td.idx {{ color: #666; }}
    </style>
    """

    body_rows = []
    for row in df_required_page.itertuples(index=False, name=None):
        tds = []
        for j, val in enumerate(row):
            cls = "idx" if j == 0 else ""
            tds.append(f'<td class="{cls}">{html.escape(str(val))}</td>')
        body_rows.append("<tr>" + "".join(tds) + "</tr>")

    st.components.v1.html(
        f"""
        {css}
        <div class="req-wrap">
          <table class="req">
            {colgroup}
            <tbody>
              {''.join(body_rows)}
            </tbody>
          </table>
        </div>
        """,
        height=height_px + 20,
        scrolling=False,
    )


# ----------------------------
# Optional header + optional pretty table
# ----------------------------
def render_optional_sort_header(page_key: str):
    init_state(page_key)

    sort_key = st.session_state.global_sort_key
    sort_dir = st.session_state.global_sort_dir
    arrow = "▲" if sort_dir == "asc" else "▼"

    def label_for(key: str, base: str):
        return f"{base} {arrow}" if sort_key == key else base

    st.markdown(
        """
        <style>
        div:has(.sort-anchor) [data-testid="stHorizontalBlock"]{ gap: 0.45rem !important; }
        div:has(.sort-anchor) [data-testid="stButton"]{ padding-left: 0 !important; padding-right: 0 !important; }
        div:has(.sort-anchor) [data-testid="stButton"] > button{
          width: 100% !important;
          border-radius: 10px !important;
          border: 1px solid #e6bcbc !important;
          background: #FFE1E1 !important;
          color: #333 !important;
          padding: 8px 10px !important;
          font-size: 14px !important;
          font-weight: 700 !important;
          text-align: center !important;
          box-shadow: none !important;
          white-space: nowrap !important;
          overflow: hidden;
          text-overflow: ellipsis !important;
        }
        div:has(.sort-anchor) [data-testid="stButton"] > button:hover{ filter: brightness(0.98); }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="sort-anchor"></div>', unsafe_allow_html=True)

    outer = st.columns([OPT_LEFT_PAD, 100, OPT_RIGHT_PAD], gap="small")
    with outer[0]:
        st.markdown("&nbsp;", unsafe_allow_html=True)

    with outer[1]:
        cols = st.columns(COL_PCTS, gap="medium")

        with cols[0]:
            st.button(
                label_for("sample", "Sample"),
                on_click=set_sort,
                args=("sample", page_key),
                use_container_width=True,
                key=f"{page_key}_sortbtn_sample",
            )

        for i, p in enumerate(POP_ORDER, start=1):
            with cols[i]:
                st.button(
                    label_for(p, POP_LABELS.get(p, p)),
                    on_click=set_sort,
                    args=(p, page_key),
                    use_container_width=True,
                    key=f"{page_key}_sortbtn_{p}",
                )

        with cols[-1]:
            st.button(
                label_for("total_count", "Total count"),
                on_click=set_sort,
                args=("total_count", page_key),
                use_container_width=True,
                key=f"{page_key}_sortbtn_total",
            )

    with outer[2]:
        st.markdown("&nbsp;", unsafe_allow_html=True)


def render_pretty_rows(df_long: pd.DataFrame, height_px: int = 560):
    if df_long.empty:
        st.info("No data to display.")
        return

    totals = (
        df_long[["sample", "total_count"]]
        .drop_duplicates(subset=["sample"])
        .set_index("sample")["total_count"]
    )

    wide = df_long.pivot(index="sample", columns="population", values=["count", "percentage"])
    wide_count = wide.get("count")
    wide_pct = wide.get("percentage")

    samples = pd.Index(df_long["sample"]).drop_duplicates().tolist()
    colgroup = "<colgroup>" + "".join([f'<col style="width:{p}%">' for p in COL_PCTS]) + "</colgroup>"

    css = f"""
    <style>
    :root {{
        --pad-x: clamp(6px, 1.0vw, 12px);
        --pad-y: clamp(8px, 1.1vw, 12px);
        --radius: clamp(8px, 1.1vw, 10px);
        --box-min-w: clamp(86px, 10vw, 120px);
        --row-gap: clamp(6px, 1.0vw, 10px);
        --pct-size: clamp(15px, 2.0vw, 22px);
        --count-size: clamp(10px, 1.1vw, 12px);
        --pill-text: clamp(14px, 1.6vw, 20px);
        --tile-min-h: clamp(54px, 6.0vw, 76px);
        --accent-red: #FF4747;
    }}
    .wrap {{ height: {int(height_px)}px; overflow-y: auto; padding-right: 6px; }}
    table.pretty {{
        width: 100%;
        table-layout: fixed;
        border-collapse: separate;
        border-spacing: 0 var(--row-gap);
        margin: 0;
    }}
    tbody td {{ background: white; padding: 6px var(--pad-x); vertical-align: middle; overflow: hidden; }}
    tbody td:first-child {{ border-top-left-radius: 16px; border-bottom-left-radius: 16px; }}
    tbody td:last-child {{ border-top-right-radius: 16px; border-bottom-right-radius: 16px; }}

    .sample-pill, .total-pill {{
        border: 1px solid #e6e6e6;
        background: #F5F5F5;
        border-radius: var(--radius);
        padding: var(--pad-y) var(--pad-x);
        font-weight: 650;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        min-height: var(--tile-min-h);
        display: flex;
        align-items: center;
    }}
    .sample-pill {{ justify-content: flex-start; font-size: var(--pill-text); }}
    .total-pill {{ justify-content: flex-end; font-size: var(--pill-text); }}

    .box {{
        background: #FFF0F0;
        color: var(--accent-red);
        border-radius: var(--radius);
        padding: var(--pad-y) var(--pad-x);
        text-align: center;
        min-width: var(--box-min-w);
        min-height: var(--tile-min-h);
        overflow: hidden;
        border: 2px solid var(--accent-red);
        display: flex;
        flex-direction: column;
        justify-content: center;
    }}
    .pct {{ font-size: var(--pct-size); font-weight: 800; line-height: 1.05; color: var(--accent-red); }}
    .count {{
        margin-top: clamp(4px, 0.5vw, 6px);
        font-size: var(--count-size);
        opacity: 0.9;
        line-height: 1.1;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        color: var(--accent-red);
    }}
    </style>
    """

    body_rows = []
    for s in samples:
        total = totals.get(s, None)
        row = ["<tr>"]
        row.append(f'<td><div class="sample-pill">{html.escape(str(s))}</div></td>')

        for p in POP_ORDER:
            pct_val = None
            cnt_val = None

            if wide_pct is not None and p in getattr(wide_pct, "columns", []) and s in wide_pct.index:
                pv = wide_pct.at[s, p]
                if pd.notna(pv):
                    pct_val = float(pv)

            if wide_count is not None and p in getattr(wide_count, "columns", []) and s in wide_count.index:
                cv = wide_count.at[s, p]
                if pd.notna(cv):
                    cnt_val = cv

            if pct_val is None:
                pct_str, count_str = "—", ""
            else:
                pct_str = f"{pct_val:.2f}%"
                if cnt_val is None:
                    count_str = ""
                else:
                    try:
                        count_str = f"{int(cnt_val):,} cells"
                    except Exception:
                        count_str = f"{cnt_val} cells"

            row.append(
                f'<td><div class="box">'
                f'<div class="pct">{pct_str}</div>'
                f'<div class="count">{html.escape(count_str)}</div>'
                f"</div></td>"
            )

        total_str = "" if total is None or (isinstance(total, float) and pd.isna(total)) else f"{int(total):,}"
        row.append(f'<td><div class="total-pill">{total_str}</div></td>')
        row.append("</tr>")
        body_rows.append("".join(row))

    st.components.v1.html(
        f"""
        {css}
        <div class="wrap">
          <table class="pretty">
            {colgroup}
            <tbody>
              {''.join(body_rows)}
            </tbody>
          </table>
        </div>
        """,
        height=height_px,
        scrolling=False,
    )


# ----------------------------
# App
# ----------------------------
st.set_page_config(page_title="Loblaw Bio - Immune Dashboard", layout="wide")
st.title("Loblaw Bio - Immune Cell Dashboard")

# ----------------------------
# Sidebar Section Pager
# ----------------------------
SECTIONS = ["Part 2 - Overview", "Part 3 - Statistical Analysis", "Part 4 - Subset Analysis"]

# Persist selected section index
st.session_state.setdefault("section_idx", 0)

def _clamp_section(i: int) -> int:
    return max(0, min(int(i), len(SECTIONS) - 1))

def section_prev():
    st.session_state.section_idx = _clamp_section(st.session_state.section_idx - 1)

def section_next():
    st.session_state.section_idx = _clamp_section(st.session_state.section_idx + 1)

st.sidebar.markdown('<div class="section-pager-anchor"></div>', unsafe_allow_html=True)

st.sidebar.markdown("### Section")
b1, mid, b2 = st.sidebar.columns([1, 4, 1], gap="small")

with b1:
    st.button("◀", on_click=section_prev, disabled=(st.session_state.section_idx <= 0), key="section_prev")
with mid:
    st.markdown(
        f'<div style="text-align:center; font-weight:700; padding-top:2px;">{SECTIONS[st.session_state.section_idx]}</div>',
        unsafe_allow_html=True,
    )
with b2:
    st.button("▶", on_click=section_next, disabled=(st.session_state.section_idx >= len(SECTIONS) - 1), key="section_next")

page = SECTIONS[st.session_state.section_idx]

# Placeholders to prevent component flash across sections
st.session_state.setdefault("__p2_container_id", 0)
p2_container = st.empty()
p3_container = st.empty()
p4_container = st.empty()

if page == "Part 2 - Overview":
    # Clear other pages immediately
    p3_container.empty()
    p4_container.empty()

    with p2_container.container():
    
        st.caption("Part 2: Relative frequency (%) of each immune cell population per sample.")

        try:
            opts = load_filter_options(DB_PATH)
        except Exception as e:
            st.error(f"Could not load DB options from {DB_PATH}. Did you run Part 1 to create/load the DB?\n\nError: {e}")
            st.stop()

        with st.sidebar:
            st.sidebar.header("Filters (Part 2)")
            project = st.sidebar.selectbox("Project", ["(All)"] + opts["projects"], key="p2_project")
            condition = st.sidebar.selectbox("Condition", ["(All)"] + opts["conditions"], key="p2_condition")
            response = st.sidebar.selectbox("Response", ["(All)"] + opts["responses"], key="p2_response")
            treatment = st.sidebar.selectbox("Treatment", ["(All)"] + opts["treatments"], key="p2_treatment")
            sample_type = st.sidebar.selectbox("Sample Type", ["(All)"] + opts["sample_types"], key="p2_sample_type")

            st.sidebar.divider()
            st.sidebar.selectbox("Samples per page", [10, 25, 50, 100], index=1, key="p2_page_size_samples")
            show_pretty = st.sidebar.checkbox("Show Alternative Style Table", value=False, key="p2_show_pretty")

            st.sidebar.divider()

            page_size_samples = int(st.session_state.p2_page_size_samples)
            page_size_rows_required = page_size_samples


        with st.spinner("Querying database..."):
            df = query_part2_frequencies(
                DB_PATH,
                norm(project),
                norm(condition),
                norm(response),
                norm(treatment),
                norm(sample_type),
            )


        if df.empty:
            st.warning("No rows match your filters.")
            st.stop()

        c1, c2, c3 = st.columns(3)
        c1.metric("Rows (sample × population)", len(df))
        c2.metric("Unique samples", df["sample"].nunique())
        c3.metric("Populations", df["population"].nunique())

        st.divider()

        # ----------------------------
        # REQUIRED table
        # ----------------------------
        df_required = make_required_df(df)

        required_page_key = "part2_required"
        init_state(required_page_key)
        render_required_sort_header(required_page_key)

        sort_key = st.session_state.global_sort_key
        ascending = (st.session_state.global_sort_dir == "asc")

        df_sorted = df_required.copy()
        if sort_key in {"idx", "total_count", "count", "percentage"}:
            df_sorted[sort_key] = pd.to_numeric(df_sorted[sort_key], errors="coerce")
        df_sorted = df_sorted.sort_values(sort_key, ascending=ascending, kind="stable")

        total_pages = max(1, math.ceil(len(df_sorted) / page_size_rows_required))
        current_page = clamp_page(int(st.session_state[f"{required_page_key}_page"]), total_pages)
        st.session_state[f"{required_page_key}_page"] = current_page

        start = (current_page - 1) * page_size_rows_required
        end = start + page_size_rows_required
        df_required_page = df_sorted.iloc[start:end].copy()

        render_required_long_table_html(df_required_page, height_px=560)
        render_pager(total_pages, page_key=required_page_key, pull_up_px=PAGER_PULL_UP_PX)

        csv_required = df_required.drop(columns=["idx"]).to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Export table as CSV",
            data=csv_required,
            file_name="cell_frequencies_part2_required_long.csv",
            mime="text/csv",
        )

        st.caption(
            f"Sorted by **{sort_key}** ({st.session_state.global_sort_dir}). "
            f"Showing rows {start+1}-{min(end, len(df_sorted))} of {len(df_sorted)}."
        )

        # ----------------------------
        # OPTIONAL nicer table
        # ----------------------------
        if show_pretty:
            st.divider()
            st.subheader("Alternative Style Table")

            optional_page_key = "part2_optional"
            init_state(optional_page_key)

            render_optional_sort_header(optional_page_key)

            sample_index = df[["sample", "total_count"]].drop_duplicates(subset=["sample"]).copy()
            sample_index["__idx"] = range(len(sample_index))

            pct_wide = (
                df.pivot(index="sample", columns="population", values="percentage")
                .fillna(-1.0)
                .reset_index()
            )
            for p in POP_ORDER:
                if p not in pct_wide.columns:
                    pct_wide[p] = -1.0

            sample_index = sample_index.merge(pct_wide[["sample"] + POP_ORDER], on="sample", how="left")

            sort_key2 = st.session_state.global_sort_key
            ascending2 = (st.session_state.global_sort_dir == "asc")

            if sort_key2 in POP_ORDER:
                sample_index[sort_key2] = pd.to_numeric(sample_index[sort_key2], errors="coerce").fillna(-1.0)
            elif sort_key2 == "total_count":
                sample_index["total_count"] = pd.to_numeric(sample_index["total_count"], errors="coerce").fillna(-1.0)

            # Required-only keys can't sort the wide sample table; fall back to sample
            sort_key2_eff = "sample" if sort_key2 in {"idx", "population", "count", "percentage"} else sort_key2
            if sort_key2_eff not in sample_index.columns:
                sort_key2_eff = "sample"

            sample_index = sample_index.sort_values(sort_key2_eff, ascending=ascending2, kind="stable")
            all_samples_sorted2 = sample_index["sample"].tolist()

            total_pages2 = max(1, math.ceil(len(all_samples_sorted2) / page_size_samples))
            current_page2 = clamp_page(int(st.session_state[f"{optional_page_key}_page"]), total_pages2)
            st.session_state[f"{optional_page_key}_page"] = current_page2

            start2 = (current_page2 - 1) * page_size_samples
            end2 = start2 + page_size_samples
            page_samples2 = all_samples_sorted2[start2:end2]

            df_pretty_page = df[df["sample"].isin(page_samples2)].copy()
            df_pretty_page["__sample_order"] = pd.Categorical(df_pretty_page["sample"], categories=page_samples2, ordered=True)
            df_pretty_page = (
                df_pretty_page.sort_values(["__sample_order", "population"], kind="stable")
                            .drop(columns="__sample_order")
            )

            render_pretty_rows(df_pretty_page, height_px=560)
            render_pager(total_pages2, page_key=optional_page_key, pull_up_px=PAGER_PULL_UP_PX)

            wide = df.pivot(index=["sample", "total_count"], columns="population", values=["count", "percentage"])
            wide.columns = [f"{pop}_{metric}" for (metric, pop) in wide.columns]
            df_wide = wide.reset_index()

            ordered_cols = ["sample", "total_count"]
            for p in POP_ORDER:
                if f"{p}_count" in df_wide.columns:
                    ordered_cols.append(f"{p}_count")
                if f"{p}_percentage" in df_wide.columns:
                    ordered_cols.append(f"{p}_percentage")
            extras = [c for c in df_wide.columns if c not in ordered_cols]
            df_wide = df_wide[ordered_cols + extras]

            csv_wide = df_wide.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="Export alternative table as CSV",
                data=csv_wide,
                file_name="cell_frequencies_part2_wide.csv",
                mime="text/csv",
            )

            st.caption(
                "This is a dashboard-friendly *wide* view (one row per sample). "
                "The REQUIRED table is the long table above. "
                f"Sorted (globally) by **{sort_key2_eff}** ({st.session_state.global_sort_dir}). "
                f"Showing samples {start2+1}-{min(end2, len(all_samples_sorted2))} of {len(all_samples_sorted2)}."
            )

        st.divider()

        # ----------------------------
        # Plot
        # ----------------------------
        st.subheader("Population composition by sample (% stacked)")

        # Max samples in plot = Samples per page (tables)
        max_samples = int(st.session_state.p2_page_size_samples)

        # If the Alternative Style Table is displayed, mirror exactly the samples on that page.
        # Otherwise, default to first N samples alphabetically.
        if show_pretty and "page_samples2" in locals() and isinstance(page_samples2, list) and len(page_samples2) > 0:
            keep_samples = page_samples2
            st.caption("Plot is synced to the samples currently shown in the Alternative Style Table.")
        else:
            keep_samples = (
                df[["sample"]]
                .drop_duplicates()
                .sort_values("sample")
                .head(max_samples)["sample"]
                .tolist()
            )

        df_plot = df[df["sample"].isin(keep_samples)].copy()

        pivot = (
            df_plot.pivot(index="sample", columns="population", values="percentage")
                .fillna(0.0)
                .reindex(keep_samples)  # preserve table/alphabetical order
        )
        for p in POP_ORDER:
            if p not in pivot.columns:
                pivot[p] = 0.0

        st.bar_chart(pivot[POP_ORDER])

elif page == "Part 3 - Statistical Analysis":
    # Clear Part 2 + Part 4 immediately to prevent any leftover component iframe
    p2_container.empty()
    p4_container.empty()

    with p3_container.container():
        st.header("Part 3 - Statistical Analysis")
        st.caption(
            "Compare **responders** (response = yes) vs **non-responders** (response = no) "
            "using **relative frequencies (%)** per immune population."
        )

        # ----------------------------
        # Prevent Part 2 UI flash when entering Part 3
        # (clears any Part 2 widget/state keys so they never render here)
        # ----------------------------
        for k in list(st.session_state.keys()):
            if k.startswith("p2_") or k.startswith("part2_"):
                del st.session_state[k]

        # ----------------------------
        # Part 3 query (cached)
        # ----------------------------
        @st.cache_data(show_spinner=False)
        def query_part3_frequencies(
            db_path: str,
            project: str | None,
            condition: str | None,
            response: str | None,
            treatment: str | None,
            sample_type: str | None,
        ) -> pd.DataFrame:
            """
            Returns: sample | response | total_count | population | count | percentage
            """
            conn = get_conn(db_path)

            where, params = [], []

            if project:
                where.append("p.project_id = ?")
                params.append(project)
            if condition:
                where.append("sub.condition = ?")
                params.append(condition)
            if response:
                where.append("sub.response = ?")
                params.append(response)
            if treatment:
                where.append("t.name = ?")
                params.append(treatment)
            if sample_type:
                where.append("sa.sample_type = ?")
                params.append(sample_type)

            where_sql = ("WHERE " + " AND ".join(where)) if where else ""

            sql = f"""
            WITH filtered_counts AS (
                SELECT
                    sa.sample_id AS sample,
                    LOWER(COALESCE(sub.response,'')) AS response,
                    cp.name AS population,
                    cc.count AS count
                FROM cell_counts cc
                JOIN samples sa ON sa.sample_id = cc.sample_id
                JOIN subjects sub ON sub.subject_id = sa.subject_id
                JOIN projects p ON p.project_id = sub.project_id
                LEFT JOIN treatments t ON t.treatment_id = sa.treatment_id
                JOIN cell_populations cp ON cp.population_id = cc.population_id
                {where_sql}
            ),
            sample_totals AS (
                SELECT sample, SUM(count) AS total_count
                FROM filtered_counts
                GROUP BY sample
            )
            SELECT
                fc.sample AS sample,
                fc.response AS response,
                st.total_count AS total_count,
                fc.population AS population,
                fc.count AS count,
                CASE
                    WHEN st.total_count > 0 THEN (100.0 * fc.count / st.total_count)
                    ELSE NULL
                END AS percentage
            FROM filtered_counts fc
            JOIN sample_totals st ON st.sample = fc.sample
            ORDER BY fc.population, fc.sample;
            """
            return pd.read_sql_query(sql, conn, params=params)

        # ----------------------------
        # Sidebar controls (Part 3 only)
        # Defaults match the assignment: PBMC + melanoma + miraclib
        # ----------------------------
        try:
            opts = load_filter_options(DB_PATH)
        except Exception as e:
            st.error(f"Could not load DB options from {DB_PATH}. Did you run Part 1?\n\nError: {e}")
            st.stop()

        def _default_index(options: list[str], preferred: str) -> int:
            """Returns index of preferred if present, else 0."""
            lo = [str(x).lower() for x in options]
            return lo.index(preferred.lower()) if preferred.lower() in lo else 0

        with st.sidebar:
            st.header("Controls (Part 3)")

            project3 = st.selectbox("Project", ["(All)"] + opts["projects"], key="p3_project")

            condition_list = ["(All)"] + opts["conditions"]
            condition3 = st.selectbox(
                "Condition",
                condition_list,
                index=_default_index(condition_list, "melanoma"),
                key="p3_condition",
            )

            treatment_list = ["(All)"] + opts["treatments"]
            treatment3 = st.selectbox(
                "Treatment",
                treatment_list,
                index=_default_index(treatment_list, "miraclib"),
                key="p3_treatment",
            )

            sample_type_list = ["(All)"] + opts["sample_types"]
            sample_type3 = st.selectbox(
                "Sample Type",
                sample_type_list,
                index=_default_index(sample_type_list, "PBMC"),
                key="p3_sample_type",
            )

            show_points = st.checkbox("Overlay individual sample points", value=True, key="p3_show_points")


            st.divider()
            stat_test = st.selectbox(
                "Statistical test",
                ["Mann–Whitney U (nonparametric)", "Welch t-test (parametric)"],
                index=0,
                key="p3_stat_test",
            )
            alpha = st.number_input(
                "Significance level (α)",
                min_value=0.001,
                max_value=0.20,
                value=0.05,
                step=0.01,
                key="p3_alpha",
            )
            use_fdr = st.checkbox("Apply BH-FDR correction (recommended)", value=False, key="p3_use_fdr")

        # ----------------------------
        # Query + enforced response split
        # ----------------------------
        with st.spinner("Querying database for Part 3..."):
            df3 = query_part3_frequencies(
                DB_PATH,
                norm(project3),
                norm(condition3),
                None,  # do NOT filter response here; I need both yes/no
                norm(treatment3),
                norm(sample_type3),
            )

        if df3.empty:
            st.warning("No rows match your Part 3 filters.")
            st.stop()

        df3["response"] = df3["response"].astype(str).str.strip().str.lower()
        df3 = df3[df3["response"].isin(["yes", "no"])].copy()

        if df3.empty:
            st.warning("After filtering to response ∈ {yes, no}, no rows remain.")
            st.stop()

        n_samples = df3["sample"].nunique()
        n_yes = df3.loc[df3["response"] == "yes", "sample"].nunique()
        n_no = df3.loc[df3["response"] == "no", "sample"].nunique()

        c1, c2, c3 = st.columns(3)
        c1.metric("Samples", n_samples)
        c2.metric("Responders (yes)", n_yes)
        c3.metric("Non-responders (no)", n_no)

        if n_yes < 2 or n_no < 2:
            st.warning("Not enough samples in one group for reliable stats/boxplots (need at least 2 per group).")

        st.divider()

        # ----------------------------
        # Plotly boxplot: responders vs non-responders per population
        #   - points are transparent + different color
        #   - boxplots render OVER the points (points behind)
        #   - true jitter implemented via numeric x + random noise
        # ----------------------------
        st.subheader("Responder vs non-responder relative frequencies (boxplots)")

        df3["population"] = df3["population"].astype(str)
        df3["percentage"] = pd.to_numeric(df3["percentage"], errors="coerce")

        pops_present = sorted(df3["population"].unique().tolist())
        pop_order = [p for p in POP_ORDER if p in pops_present] + [p for p in pops_present if p not in POP_ORDER]
        pop_labels = [POP_LABELS.get(p, p) for p in pop_order]

        df_plot = df3[["sample", "response", "population", "percentage"]].dropna(subset=["percentage"]).copy()
        df_plot["response"] = df_plot["response"].astype(str).str.strip().str.lower()
        df_plot = df_plot[df_plot["response"].isin(["yes", "no"])].copy()

        df_plot["population_label"] = df_plot["population"].map(lambda p: POP_LABELS.get(p, p))
        df_plot["population_label"] = pd.Categorical(df_plot["population_label"], categories=pop_labels, ordered=True)

        # Map categories to numeric x positions so I can add jitter
        x_map = {lab: i for i, lab in enumerate(pop_labels)}
        df_plot["_x_base"] = df_plot["population_label"].map(x_map).astype(float)

        GROUP_OFFSET = {"yes": -0.18, "no": 0.18}

        JITTER = 0.10

        BOX_COLORS = {
            "yes": "rgba(31,119,180,1)",   # blue, mostly opaque
            "no":  "rgba(174,199,232,1)",  # light blue, mostly opaque
        }

        POINT_COLORS = {
            "yes": "rgba(31,119,180,0.18)",   # very transparent blue
            "no":  "rgba(174,199,232,0.18)",  # very transparent light blue
        }

        fig = go.Figure()

        # ----------------------------
        # 1) POINTS FIRST, ALWAYS BEHIND
        # ----------------------------
        if show_points:
            for resp in ["yes", "no"]:
                d = df_plot[df_plot["response"] == resp]
                if d.empty:
                    continue

                x_jit = (
                    d["_x_base"]
                    + GROUP_OFFSET[resp]
                    + (np.random.rand(len(d)) - 0.5) * (2 * JITTER)
                )

                fig.add_trace(
                    go.Scatter(
                        x=x_jit,
                        y=d["percentage"],
                        mode="markers",
                        marker=dict(
                            color=POINT_COLORS[resp],
                            size=7,
                            line=dict(width=0),
                        ),
                        showlegend=False,
                        hovertemplate=(
                            "Population: %{customdata[0]}<br>"
                            "Response: %{customdata[1]}<br>"
                            "Sample: %{customdata[2]}<br>"
                            "Percent: %{y:.2f}%<extra></extra>"
                        ),
                        customdata=np.stack(
                            [
                                d["population_label"].astype(str),
                                d["response"].astype(str),
                                d["sample"].astype(str),
                            ],
                            axis=1,
                        ),
                    )
                )

        # ----------------------------
        # 2) DRAW BOXPLOTS AS SHAPES ABOVE THE POINTS
        # ----------------------------

        BOX_HALF_WIDTH = 0.15

        shapes = []

        FILL_ALPHA = 0.75  # lower = more transparent

        def rgba_with_alpha(rgba: str, alpha: float) -> str:
            s = rgba.strip().lower()
            if s.startswith("rgba"):
                inner = s[s.find("(")+1:s.find(")")]
                parts = [p.strip() for p in inner.split(",")]
                r, g, b = parts[0], parts[1], parts[2]
                return f"rgba({r},{g},{b},{alpha})"
            if s.startswith("rgb"):
                inner = s[s.find("(")+1:s.find(")")]
                parts = [p.strip() for p in inner.split(",")]
                r, g, b = parts[0], parts[1], parts[2]
                return f"rgba({r},{g},{b},{alpha})"
            return rgba

        def _whiskers_iqr(series: pd.Series):
            s = pd.to_numeric(series, errors="coerce").dropna().astype(float)
            if len(s) == 0:
                return None
            q1 = float(s.quantile(0.25))
            med = float(s.quantile(0.50))
            q3 = float(s.quantile(0.75))
            iqr = q3 - q1
            lo_fence = q1 - 1.5 * iqr
            hi_fence = q3 + 1.5 * iqr
            lo = float(s[s >= lo_fence].min())
            hi = float(s[s <= hi_fence].max())
            return q1, med, q3, lo, hi

        # Adding legend entries separately (dummy traces) since shapes don't appear in legend.
        legend_added = {"yes": False, "no": False}

        for resp in ["yes", "no"]:
            for lab in pop_labels:
                d = df_plot[(df_plot["response"] == resp) & (df_plot["population_label"].astype(str) == str(lab))]
                if d.empty:
                    continue

                stats = _whiskers_iqr(d["percentage"])
                if stats is None:
                    continue
                q1, med, q3, wlo, whi = stats

                x_center = float(x_map[lab]) + GROUP_OFFSET[resp]
                x0 = x_center - BOX_HALF_WIDTH
                x1 = x_center + BOX_HALF_WIDTH

                fill = rgba_with_alpha(BOX_COLORS[resp], FILL_ALPHA)
                outline = "rgba(0,0,0,0.65)"

                # IQR box (filled rectangle) - ABOVE points
                shapes.append(dict(
                    type="rect",
                    xref="x", yref="y",
                    x0=x0, x1=x1,
                    y0=q1, y1=q3,
                    fillcolor=fill,
                    line=dict(color=outline, width=1.5),
                    layer="above",
                ))

                # Median line
                shapes.append(dict(
                    type="line",
                    xref="x", yref="y",
                    x0=x0, x1=x1,
                    y0=med, y1=med,
                    line=dict(color="rgba(0,0,0,0.75)", width=2),
                    layer="above",
                ))

                # Whiskers (drawn as two segments so they DON'T pass through the box)
                # Lower segment: wlo -> q1
                shapes.append(dict(
                    type="line",
                    xref="x", yref="y",
                    x0=x_center, x1=x_center,
                    y0=wlo, y1=q1,
                    line=dict(color=outline, width=1.5),
                    layer="above",
                ))
                # Upper segment: q3 -> whi
                shapes.append(dict(
                    type="line",
                    xref="x", yref="y",
                    x0=x_center, x1=x_center,
                    y0=q3, y1=whi,
                    line=dict(color=outline, width=1.5),
                    layer="above",
                ))

                # Whisker caps
                cap = BOX_HALF_WIDTH * 0.65
                shapes.append(dict(
                    type="line",
                    xref="x", yref="y",
                    x0=x_center - cap, x1=x_center + cap,
                    y0=wlo, y1=wlo,
                    line=dict(color=outline, width=1.5),
                    layer="above",
                ))
                shapes.append(dict(
                    type="line",
                    xref="x", yref="y",
                    x0=x_center - cap, x1=x_center + cap,
                    y0=whi, y1=whi,
                    line=dict(color=outline, width=1.5),
                    layer="above",
                ))

        # Adding legend entries (dummy traces) so legend still shows yes/no
        for resp in ["yes", "no"]:
            if not legend_added[resp]:
                fig.add_trace(go.Scatter(
                    x=[None], y=[None],
                    mode="markers",
                    marker=dict(size=10, color=BOX_COLORS[resp]),
                    name=("Responders (yes)" if resp == "yes" else "Non-responders (no)"),
                    showlegend=True,
                ))
                legend_added[resp] = True

        fig.update_layout(shapes=shapes)

        # Making the numeric x axis look categorical again
        fig.update_layout(
            title=dict(
                text="Responders (yes) vs Non-responders (no)",
                font=dict(size=22),   # <-- title text size
                x=0.5,
                xanchor="center",
            ),
            legend=dict(
                font=dict(size=16),   # <-- legend text size
                itemsizing="constant",
            ),
            legend_title_text="",
            boxmode="group",
            margin=dict(l=40, r=40, t=70, b=90),
            xaxis=dict(
                tickmode="array",
                tickvals=list(range(len(pop_labels))),
                ticktext=pop_labels,
                tickangle=25,
                tickfont=dict(size=16),      # <-- population label size
                title=dict(
                    text="",                 # no x-axis title
                    font=dict(size=18),
                ),
            ),
            yaxis=dict(
                title=dict(
                    text="Relative frequency (%)",
                    font=dict(size=18),       # <-- y-axis title size
                ),
                tickfont=dict(size=16),       # <-- y-axis tick label size
            ),
        )


        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # ----------------------------
        # Statistics: per-population test + (optional) BH-FDR correction
        # ----------------------------
        st.subheader("Significance testing by population")

        def bh_fdr(pvals: list[float]) -> list[float]:
            """Benjamini-Hochberg FDR-adjusted q-values."""
            m = len(pvals)
            order = sorted(range(m), key=lambda i: pvals[i])
            q = [1.0] * m
            prev = 1.0
            for rank, i in enumerate(reversed(order), start=1):
                p = float(pvals[i])
                fwd_rank = m - rank + 1
                val = min(prev, (p * m) / max(1, fwd_rank))
                prev = val
                q[i] = val
            return q

        try:
            from scipy import stats
            have_scipy = True
        except Exception:
            have_scipy = False

        results = []
        pvals = []

        for p in pop_order:
            yv = (
                df3[(df3["population"] == p) & (df3["response"] == "yes")]["percentage"]
                .dropna()
                .astype(float)
            )
            nv = (
                df3[(df3["population"] == p) & (df3["response"] == "no")]["percentage"]
                .dropna()
                .astype(float)
            )

            med_yes = float(yv.median()) if len(yv) else float("nan")
            med_no = float(nv.median()) if len(nv) else float("nan")

            pval = float("nan")
            test_name = None

            if have_scipy and len(yv) >= 2 and len(nv) >= 2:
                if stat_test.startswith("Mann"):
                    test_name = "Mann–Whitney U"
                    pval = float(stats.mannwhitneyu(yv, nv, alternative="two-sided").pvalue)
                else:
                    test_name = "Welch t-test"
                    pval = float(stats.ttest_ind(yv, nv, equal_var=False, nan_policy="omit").pvalue)

            results.append(
                dict(
                    population=POP_LABELS.get(p, p),
                    n_yes=int(len(yv)),
                    n_no=int(len(nv)),
                    median_yes=round(med_yes, 3) if not math.isnan(med_yes) else None,
                    median_no=round(med_no, 3) if not math.isnan(med_no) else None,
                    test=test_name,
                    p_value=None if math.isnan(pval) else pval,
                )
            )
            pvals.append(1.0 if math.isnan(pval) else float(pval))

        if not have_scipy:
            st.error(
                "SciPy is not available, so p-values cannot be computed. "
                "Install it with `pip install scipy` to enable the stats section."
            )

        if use_fdr and have_scipy:
            qvals = bh_fdr(pvals)
            for r, qv in zip(results, qvals):
                r["q_value_BH"] = float(qv)
                r["significant"] = (qv < float(alpha))
            out_df = pd.DataFrame(results).sort_values(["q_value_BH", "p_value"], ascending=[True, True], kind="stable")
        else:
            for r in results:
                r["q_value_BH"] = None
                r["significant"] = (r["p_value"] is not None and r["p_value"] < float(alpha))
            out_df = pd.DataFrame(results).sort_values(["p_value"], ascending=[True], na_position="last", kind="stable")

        st.dataframe(
            out_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "p_value": st.column_config.NumberColumn(format="%.3g"),
                "q_value_BH": st.column_config.NumberColumn(format="%.3g"),
            },
        )

        sig = out_df[out_df["significant"] == True]
        if have_scipy and len(sig) > 0:
            st.success(
                "Significant populations: " + ", ".join(sig["population"].tolist())
                + (f" (BH q < {alpha})." if (use_fdr and have_scipy) else f" (p < {alpha}).")
            )
        elif have_scipy:
            st.info("No populations reached significance at the selected threshold.")

elif page == "Part 4 - Subset Analysis":
    # Clear other pages’ containers to prevent any leftover component iframe
    p2_container.empty()
    p3_container.empty()

    with p4_container.container():
        st.header("Part 4 - Data Subset Analysis")

        # ----------------------------
        # Part 4 query (cached)
        # ----------------------------
        @st.cache_data(show_spinner=False)
        def query_part4_samples(
            db_path: str,
            condition: str,
            sample_type: str,
            treatment: str | None,
            time_from_start: int | None,
        ) -> pd.DataFrame:
            """
            Returns one row per (sample, population) at requested subset:
            project_id | subject_id | condition | sex | response | treatment | sample | sample_type | time_from_treatment_start |
            population | count
            """
            conn = get_conn(db_path)

            where = [
                "LOWER(sub.condition) = LOWER(?)",
                "LOWER(sa.sample_type) = LOWER(?)",
            ]
            params: list = [condition, sample_type]

            if treatment:
                where.append("LOWER(t.name) = LOWER(?)")
                params.append(treatment)

            if time_from_start is not None:
                where.append("sa.time_from_treatment_start = ?")
                params.append(int(time_from_start))

            where_sql = "WHERE " + " AND ".join(where)

            sql = f"""
            SELECT
                p.project_id AS project_id,
                sub.subject_id AS subject_id,
                sub.condition AS condition,
                sub.sex AS sex,
                LOWER(COALESCE(sub.response,'')) AS response,
                COALESCE(t.name,'') AS treatment,
                sa.sample_id AS sample,
                sa.sample_type AS sample_type,
                sa.time_from_treatment_start AS time_from_treatment_start,
                cp.name AS population,
                cc.count AS count
            FROM cell_counts cc
            JOIN samples sa ON sa.sample_id = cc.sample_id
            JOIN subjects sub ON sub.subject_id = sa.subject_id
            JOIN projects p ON p.project_id = sub.project_id
            LEFT JOIN treatments t ON t.treatment_id = sa.treatment_id
            JOIN cell_populations cp ON cp.population_id = cc.population_id
            {where_sql}
            ORDER BY p.project_id, sub.subject_id, sa.sample_id, cp.name;
            """
            return pd.read_sql_query(sql, conn, params=params)

        # ----------------------------
        # Controls (Part 4)
        # ----------------------------
        # Defaults required by prompt: melanoma + PBMC + miraclib + time=0
        try:
            opts = load_filter_options(DB_PATH)
        except Exception:
            opts = None

        def _default_index(options: list[str], preferred: str) -> int:
            lo = [str(x).lower() for x in options]
            return lo.index(preferred.lower()) if preferred.lower() in lo else 0

        with st.sidebar:
            st.header("Controls (Part 4)")

            condition4 = st.selectbox(
                "Condition",
                ["melanoma", "carcinoma", "healthy"],
                index=0,
                key="p4_condition",
            )

            sample_type4 = st.selectbox(
                "Sample Type",
                ["PBMC", "Tumor", "Serum", "Plasma"],
                index=0,
                key="p4_sample_type",
            )

            # Treatment selector (default miraclib)
            if opts and "treatments" in opts and opts["treatments"]:
                treatment_list = ["(All)"] + opts["treatments"]
                treatment4 = st.selectbox(
                    "Treatment",
                    treatment_list,
                    index=_default_index(treatment_list, "miraclib"),
                    key="p4_treatment",
                )
                treatment4 = None if treatment4 == "(All)" else treatment4
            else:
                treatment4 = st.text_input("Treatment (blank = all)", value="miraclib", key="p4_treatment_text").strip() or None

            # Time-from-treatment selector (default 0)
            # If possible, populate from DB options; otherwise just offer common values.
            if opts and "times" in opts and opts["times"]:
                # assumes load_filter_options returns numericish strings or ints
                times = sorted({int(x) for x in opts["times"]})
                time_choices = ["(All)"] + times
                time4 = st.selectbox(
                    "Time from treatment start",
                    time_choices,
                    index=(1 if 0 in times else 0),  # prefer 0 if present
                    key="p4_time",
                )
                time4 = None if time4 == "(All)" else int(time4)
            else:
                # common dataset values: 0/7/14. Include All.
                time4 = st.selectbox(
                    "Time from treatment start",
                    ["(All)", 0, 7, 14],
                    index=1,   # default = 0
                    key="p4_time_fallback",
                )
                time4 = None if time4 == "(All)" else int(time4)

        # ----------------------------
        # Run query
        # ----------------------------
        with st.spinner("Querying database for Part 4 subset..."):
            df4 = query_part4_samples(
                DB_PATH,
                condition=str(condition4),
                sample_type=str(sample_type4),
                treatment=treatment4,
                time_from_start=time4,
            )

        if df4.empty:
            st.warning("No rows match the Part 4 subset filters.")
            st.stop()

        # ----------------------------
        # 1) Matching samples
        # ----------------------------
        st.subheader("1) Matching samples (one row per sample)")
        samples_df = (
            df4[
                [
                    "project_id", "subject_id", "condition", "sex", "response",
                    "treatment", "sample", "sample_type", "time_from_treatment_start"
                ]
            ]
            .drop_duplicates()
            .sort_values(["project_id", "subject_id", "sample"], kind="stable")
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Samples", int(samples_df["sample"].nunique()))
        c2.metric("Subjects", int(samples_df["subject_id"].nunique()))
        c3.metric("Projects", int(samples_df["project_id"].nunique()))
        st.dataframe(samples_df, use_container_width=True, hide_index=True)

        st.divider()

        # ----------------------------
        # 2) Summaries within those samples
        # ----------------------------
        st.subheader("2) Subset summaries")

        st.markdown("**2.1 Samples by project**")
        by_project = (
            samples_df.groupby("project_id", as_index=False)
            .agg(n_samples=("sample", "nunique"))
            .sort_values(["n_samples", "project_id"], ascending=[False, True], kind="stable")
        )

        st.dataframe(by_project, use_container_width=True, hide_index=True)

        st.markdown("**2.2 Subjects by response (yes/no)**")
        resp_df = samples_df.copy()
        resp_df["response"] = resp_df["response"].astype(str).str.strip().str.lower()
        resp_df.loc[~resp_df["response"].isin(["yes", "no"]), "response"] = "unknown"
        by_response = (
            resp_df.groupby("response", as_index=False)
            .agg(n_subjects=("subject_id", "nunique"))
            .sort_values(["response"], kind="stable")
        )

        st.dataframe(by_response, use_container_width=True, hide_index=True)

        st.markdown("**2.3 Subjects by sex**")
        sex_df = samples_df.copy()
        sex_df["sex"] = sex_df["sex"].astype(str).str.strip()
        sex_df.loc[~sex_df["sex"].isin(["M", "F"]), "sex"] = "Unknown"
        by_sex = (
            sex_df.groupby("sex", as_index=False)
            .agg(n_subjects=("subject_id", "nunique"))
            .sort_values(["sex"], kind="stable")
        )

        st.dataframe(by_sex, use_container_width=True, hide_index=True)

        st.divider()

        # ----------------------------
        # Required question:
        # "Considering Melanoma males, what is the average number of B cells for responders at time=0?"
        # ----------------------------
        st.subheader("Required question")

        conn = get_conn(DB_PATH)

        params = [treatment4, treatment4]

        # ---- average ----
        avg_sql = """
        SELECT AVG(cc.count) AS avg_b_cells
        FROM cell_counts cc
        JOIN samples sa ON sa.sample_id = cc.sample_id
        JOIN subjects sub ON sub.subject_id = sa.subject_id
        JOIN cell_populations cp ON cp.population_id = cc.population_id
        LEFT JOIN treatments t ON t.treatment_id = sa.treatment_id
        WHERE LOWER(cp.name) = 'b_cell'
          AND LOWER(sub.condition) = 'melanoma'
          AND TRIM(sub.sex) = 'M'
          AND LOWER(COALESCE(sub.response,'')) = 'yes'
          AND sa.time_from_treatment_start = 0
          AND (? IS NULL OR LOWER(t.name) = LOWER(?));
        """

        avg_df = pd.read_sql_query(avg_sql, conn, params=params)

        avg_b = avg_df.loc[0, "avg_b_cells"]

        if pd.isna(avg_b):
            st.warning(
                "No rows found for: melanoma + males (M) + responders (yes) + time=0 + B Cells "
                f"{'(and treatment filter applied)' if treatment4 else '(any treatment)'}."
            )
        else:
            st.success(
                f"**Average # of B cells (melanoma, M, responders, time=0"
                f"{', ' + str(treatment4) if treatment4 else ''}): {avg_b:.2f}**"
            )

            # ---- contributing rows ----
            rows_sql = """
            SELECT
                sub.subject_id,
                sa.sample_id AS sample,
                cc.count
            FROM cell_counts cc
            JOIN samples sa ON sa.sample_id = cc.sample_id
            JOIN subjects sub ON sub.subject_id = sa.subject_id
            JOIN cell_populations cp ON cp.population_id = cc.population_id
            LEFT JOIN treatments t ON t.treatment_id = sa.treatment_id
            WHERE LOWER(cp.name) = 'b_cell'
              AND LOWER(sub.condition) = 'melanoma'
              AND TRIM(sub.sex) = 'M'
              AND LOWER(COALESCE(sub.response,'')) = 'yes'
              AND sa.time_from_treatment_start = 0
              AND (? IS NULL OR LOWER(t.name) = LOWER(?))
            ORDER BY sub.subject_id, sa.sample_id;
            """

            rows_df = pd.read_sql_query(rows_sql, conn, params=params)

            with st.expander("Show contributing rows"):
                st.dataframe(
                    rows_df,
                    use_container_width=True,
                    hide_index=True,
                )
