import os
import re
import sqlite3
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()

import streamlit as st
from langchain_community.utilities import SQLDatabase
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sqlalchemy import create_engine, text
from langchain_groq import ChatGroq

# ── Page configuration ───────────────────────────────────────────
st.set_page_config(
    page_title="SmartQuery - Chat with SQL DB",
    page_icon=":material/database:",
    layout="wide",
)

st.markdown(
    """
    <style>
    /* ── Force Dark Theme & Colors ── */
    :root {
        --primary-color: #1E88E5;
        --bg-color: #0E1117;
        --secondary-bg-color: #1A2332;
        --text-color: #E0E0E0;
    }

    [data-testid="stAppViewContainer"] {
        background-color: var(--bg-color) !important;
        color: var(--text-color) !important;
    }

    [data-testid="stHeader"] {
        background-color: var(--bg-color) !important;
    }

    [data-testid="stSidebar"] {
        background-color: var(--secondary-bg-color) !important;
    }

    /* Force text color in sidebar */
    [data-testid="stSidebar"] *, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span {
        color: var(--text-color) !important;
    }

    /* Force primary blue on buttons and links */
    button[kind="primary"] {
        background-color: var(--primary-color) !important;
        border-color: var(--primary-color) !important;
        color: white !important;
    }

    /* ── Custom chat bubbles ── */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(8px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    .chat-row {
        display: flex;
        margin: 0.4rem 0;
        animation: fadeInUp 0.3s ease-out;
    }
    .chat-row.user-row {
        justify-content: flex-end;
    }
    .chat-row.assistant-row {
        justify-content: flex-start;
    }
    .chat-bubble {
        max-width: 75%;
        padding: 0.7rem 1.15rem;
        line-height: 1.55;
        font-size: 1.05rem;
        word-wrap: break-word;
        color: #e5e5e7;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
    }
    .chat-bubble.user-bubble {
        background: #1c1c1e;
        border-radius: 20px 20px 4px 20px;
        margin-right: 4px;
    }
    .chat-bubble.assistant-bubble {
        background: #252a34;
        border-radius: 20px 20px 20px 4px;
        margin-left: 4px;
    }
    .chat-bubble p { margin: 0 0 0.4em 0; }
    .chat-bubble p:last-child { margin-bottom: 0; }
    .chat-label {
        font-size: 0.68rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 3px;
        padding: 0 8px;
        opacity: 0.6;
    }
    .chat-label.user-label {
        text-align: right;
        color: #aaaaaa;
    }
    .chat-label.assistant-label {
        text-align: left;
        color: #aaaaaa;
    }

    /* ── Code blocks inside chat bubbles ── */
    .chat-bubble pre {
        background: #0d1117;
        border-left: 3px solid #66bb6a;
        border-radius: 8px;
        padding: 0.65rem 1rem;
        margin: 0.5rem 0;
        overflow-x: auto;
    }
    .chat-bubble pre code {
        font-family: 'Consolas', 'Courier New', monospace;
        color: #66bb6a;
        background: none;
        padding: 0;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    .chat-bubble code {
        font-family: 'Consolas', 'Courier New', monospace;
        background: rgba(102,187,106,0.12);
        color: #66bb6a;
        padding: 0.12em 0.4em;
        border-radius: 4px;
        font-size: 0.9rem;
    }

    /* ── Tables inside chat bubbles ── */
    .chat-bubble table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0;
        margin: 0.5rem 0;
        border-radius: 8px;
        overflow: hidden;
        font-size: 0.9rem;
    }
    .chat-bubble table th {
        background: #1a73e8;
        color: #ffffff;
        font-weight: 600;
        padding: 0.55rem 0.75rem;
        text-align: left;
        white-space: nowrap;
    }
    .chat-bubble table td {
        padding: 0.5rem 0.75rem;
        border-bottom: 1px solid rgba(255,255,255,0.06);
        color: #e0e0e0;
    }
    .chat-bubble table tr:nth-child(even) td {
        background: rgba(255,255,255,0.03);
    }
    .chat-bubble table tr:hover td {
        background: rgba(26,115,232,0.12);
    }
    .chat-bubble table tr:last-child td {
        border-bottom: none;
    }
    .error-text {
        color: #ff6b6b;
        font-weight: 500;
    }

    /* ── Hide native st.chat_message elements ── */
    div[data-testid="stChatMessage"] {
        display: none !important;
    }

    /* ── Spinner accent ── */
    .stSpinner > div > div { border-top-color: #1E88E5 !important; }

    /* ── Sidebar brand + collapse icon on one line ── */
    section[data-testid="stSidebar"] [data-testid="stSidebarHeader"] {
        min-height: 2.8rem !important;
        display: flex !important;
        align-items: center !important;
        padding: 0.2rem 0.6rem !important;
        border-bottom: 2px solid #1E88E5;
    }
    section[data-testid="stSidebar"] [data-testid="stSidebarHeader"]::before {
        content: "SmartQuery";
        color: #90CAF9;
        font-size: 1.5rem;
        font-weight: 700;
        margin-right: auto;
        line-height: 1;
    }
    section[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"] {
        margin-left: auto !important;
    }
    /* tighten sidebar widget spacing */
    [data-testid="stSidebar"] [data-testid="stSelectbox"] { margin-bottom: 0.25rem !important; }
    [data-testid="stSidebar"] [data-testid="stToggle"] { margin-top: 0.1rem !important; margin-bottom: 0.35rem !important; }
    /* remove extra gap under header */
    [data-testid="stSidebarContent"] { padding-top: 0.2rem !important; }
    section[data-testid="stSidebar"] > div { padding-top: 0 !important; }
    /* reclaim main area space */
    .stMainBlockContainer { padding-top: 2.8rem !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

LOCALDB = "USE_LOCALDB"
MYSQL = "USE_MYSQL"
SQLITE_DIR = Path(__file__).parent / "databases"
SQLITE_DIR.mkdir(exist_ok=True)

WRITE_SQL_RE = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|REPLACE|MERGE)\b",
    re.IGNORECASE,
)
SCHEMA_CHANGE_RE = re.compile(
    r"^\s*(CREATE|ALTER|DROP|TRUNCATE)\b",
    re.IGNORECASE,
)


def is_write_query(sql: str) -> bool:
    return bool(WRITE_SQL_RE.match(sql.strip()))


def is_schema_change(sql: str) -> bool:
    return bool(SCHEMA_CHANGE_RE.match(sql.strip()))


def clean_sql(raw: str) -> str:
    """Strip markdown fences and whitespace from LLM-generated SQL."""
    s = raw.strip()
    if s.startswith("```"):
        s = s.split("\n", 1)[-1] if "\n" in s else s[3:]
    if s.endswith("```"):
        s = s[: -3]
    s = s.removeprefix("sql").strip()
    return s


def split_statements(sql: str) -> list[str]:
    """Split multi-statement SQL into individual statements."""
    return [s.strip() for s in sql.split(";") if s.strip()]


def execute_sql(engine, sql: str) -> int:
    """Execute one or more SQL statements, return total rows affected."""
    statements = split_statements(sql)
    total = 0
    with engine.begin() as conn:
        for stmt in statements:
            result = conn.execute(text(stmt))
            total += result.rowcount if result.rowcount > 0 else 0
    return total


def run_query_text(engine, sql: str) -> str:
    """Execute read/query statements and return text output."""
    statements = split_statements(sql)
    outputs = []
    with engine.connect() as conn:
        for stmt in statements:
            result = conn.execute(text(stmt))
            if result.returns_rows:
                outputs.append(str(result.fetchall()))
            else:
                outputs.append(f"{max(result.rowcount, 0)} row(s) affected.")
    return "\n".join(outputs)


# ── Sidebar ──────────────────────────────────────────────────────
radio_opt = [
    "Use SQLite Database",
    "Connect to your MySQL Database",
]
selected_opt = st.sidebar.radio("Choose a database", options=radio_opt)

mysql_host = mysql_port = mysql_user = mysql_password = mysql_db = None
sqlite_db_name = None

if radio_opt.index(selected_opt) == 1:
    db_uri = MYSQL
    mysql_host = st.sidebar.text_input("MySQL Host", value="localhost")
    mysql_port = st.sidebar.text_input("MySQL Port", value="3306")
    mysql_user = st.sidebar.text_input("MySQL User")
    mysql_password = st.sidebar.text_input("MySQL Password", type="password")
    mysql_db = st.sidebar.text_input("MySQL Database (optional)")
    if not mysql_db:
        st.sidebar.caption("Global mode: queries can target multiple DBs using `db_name.table_name`.")
else:
    db_uri = LOCALDB
    existing_dbs = sorted(p.name for p in SQLITE_DIR.glob("*.db"))
    CREATE_NEW = "+ Create new database"
    options = existing_dbs + [CREATE_NEW] if existing_dbs else [CREATE_NEW]
    choice = st.sidebar.selectbox("Select SQLite database", options)

    if choice == CREATE_NEW:
        new_name = st.sidebar.text_input("New database name (without .db)")
        if new_name:
            new_name = new_name.strip().replace(" ", "_")
            if not new_name.endswith(".db"):
                new_name += ".db"
            sqlite_db_name = new_name
        else:
            sqlite_db_name = None
    else:
        sqlite_db_name = choice

    if not sqlite_db_name:
        st.info("Enter a name to create a new database.")
        st.stop()

st.sidebar.markdown(
    "<hr style='margin:0.35rem 0 0.55rem 0; border:0; border-top:1px solid #2E4A73;'>",
    unsafe_allow_html=True,
)
allow_writes = st.sidebar.toggle(
    "Allow Write Operations",
    value=st.session_state.get("allow_writes", False),
    help="Enable to allow CREATE TABLE, INSERT, UPDATE, DELETE. "
    "Every write still requires your explicit confirmation.",
)
st.session_state["allow_writes"] = allow_writes

# ── API key ──────────────────────────────────────────────────────
api_key = os.getenv("GROQ_API_KEY")
if not api_key:
    st.info("GROQ_API_KEY not found. Add it to your .env file.")
    st.stop()

# ── LLM ──────────────────────────────────────────────────────────
llm = ChatGroq(
    groq_api_key=api_key,
    model_name="llama-3.1-8b-instant",
    streaming=False,
)


# ── Database connection ──────────────────────────────────────────
@st.cache_resource(ttl="2h")
def configure_db(
    db_uri,
    sqlite_name=None,
    mysql_host=None,
    mysql_port=None,
    mysql_user=None,
    mysql_password=None,
    mysql_db=None,
    _version=0,
):
    if db_uri == LOCALDB:
        db_path = (SQLITE_DIR / sqlite_name).absolute()
        sqlite3.connect(str(db_path)).close()
        engine = create_engine(f"sqlite:///{db_path}")
        return SQLDatabase(engine), engine

    if not (mysql_host and mysql_user and mysql_password):
        st.info("Please provide MySQL host, user, and password.")
        st.stop()

    port = mysql_port or "3306"
    db_suffix = f"/{mysql_db}" if mysql_db else ""
    conn_str = (
        f"mysql+mysqlconnector://"
        f"{quote_plus(mysql_user)}:{quote_plus(mysql_password)}"
        f"@{mysql_host}:{port}{db_suffix}"
    )
    engine = create_engine(conn_str, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        st.info(f"MySQL connection failed: {e}")
        st.stop()
    # When no default DB is selected, query across all schemas via information_schema.
    db_obj = SQLDatabase(engine) if mysql_db else None
    return db_obj, engine


if "db_version" not in st.session_state:
    st.session_state["db_version"] = 0

if db_uri == MYSQL:
    db, engine = configure_db(
        db_uri,
        mysql_host=mysql_host,
        mysql_port=mysql_port,
        mysql_user=mysql_user,
        mysql_password=mysql_password,
        mysql_db=mysql_db,
        _version=st.session_state["db_version"],
    )
else:
    db, engine = configure_db(
        db_uri,
        sqlite_name=sqlite_db_name,
        _version=st.session_state["db_version"],
    )

# Build table list for schema context (not shown in UI)
try:
    if db_uri == MYSQL and not mysql_db:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type='BASE TABLE'
                      AND table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
                    ORDER BY table_schema, table_name
                    LIMIT 300
                    """
                )
            ).fetchall()
        tables = [f"{r[0]}.{r[1]}" for r in rows]
    else:
        tables = db.get_usable_table_names()
except Exception:
    tables = []

try:
    if db_uri == MYSQL and not mysql_db:
        with engine.connect() as conn:
            col_rows = conn.execute(
                text(
                    """
                    SELECT table_schema, table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
                    ORDER BY table_schema, table_name, ordinal_position
                    LIMIT 1500
                    """
                )
            ).fetchall()
        grouped = {}
        for schema, table, col, dtype in col_rows:
            key = f"{schema}.{table}"
            grouped.setdefault(key, []).append(f"{col} ({dtype})")
        schema_info = "\n".join(
            [f"{k}: {', '.join(v)}" for k, v in grouped.items()]
        ) if grouped else "No tables exist yet."
    else:
        schema_info = db.get_table_info() if tables else "No tables exist yet."
except Exception:
    schema_info = "No tables exist yet."

dialect = "SQLite" if db_uri == LOCALDB else "MySQL"
scope_note = (
    "No default MySQL database is selected. "
    "Use fully qualified table names like database_name.table_name."
    if db_uri == MYSQL and not mysql_db
    else ""
)

# ── LLM chains (replace the slow agent) ─────────────────────────
SQL_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are a SQL expert for a {dialect} database.\n\n"
        "{scope_note}\n\n"
        "SCHEMA:\n{schema}\n\n"
        "Given the user's question, output ONLY the raw SQL query. "
        "No explanation, no markdown fences, no prefixes. Just the SQL.",
    ),
    ("human", "{question}"),
])

ANSWER_PROMPT = ChatPromptTemplate.from_messages([
    (
        "system",
        "You are a helpful data assistant. Given the user's question, "
        "the SQL that was run, and its result, write a concise natural-language answer. "
        "Format any tabular data as a markdown table.",
    ),
    ("human", "Question: {question}\nSQL: {sql}\nResult: {result}"),
])

sql_chain = SQL_PROMPT | llm | StrOutputParser()
answer_chain = ANSWER_PROMPT | llm | StrOutputParser()


def ask_database(question: str) -> str:
    """Generate SQL, execute, and format the answer. 2 LLM calls max."""
    try:
        sql = clean_sql(
            sql_chain.invoke(
                {
                    "dialect": dialect,
                    "scope_note": scope_note,
                    "schema": schema_info,
                    "question": question,
                }
            )
        )
    except Exception as e:
        if "rate_limit" in str(e).lower() or "413" in str(e):
            return '<span class="error-text">Rate limit exceeded. Please wait a moment and try again.</span>'
        raise

    if is_write_query(sql):
        if not st.session_state.get("allow_writes", False):
            return (
                "This requires a write operation but writes are disabled. "
                "Enable **Allow Write Operations** in the sidebar first."
            )
        st.session_state["pending_write"] = sql
        return (
            f"I've prepared this query that needs your confirmation:\n\n"
            f"```sql\n{sql}\n```\n\n"
            "Click **Confirm & Execute** to run it, or **Cancel** to discard."
        )

    try:
        result = run_query_text(engine, sql)
    except Exception as first_err:
        retry_question = (
            f"{question}\n\n"
            f"(Previous attempt failed with: {first_err}. Write a corrected query.)"
        )
        try:
            sql = clean_sql(
                sql_chain.invoke(
                    {
                        "dialect": dialect,
                        "scope_note": scope_note,
                        "schema": schema_info,
                        "question": retry_question,
                    }
                )
            )
        except Exception as e2:
            if "rate_limit" in str(e2).lower() or "413" in str(e2):
                return '<span class="error-text">Rate limit exceeded. Please wait a moment and try again.</span>'
            raise
        try:
            result = run_query_text(engine, sql)
        except Exception as second_err:
            return f"Could not execute query after retry.\n\n```\n{second_err}\n```"

    if not result or result.strip() == "":
        return "The query returned no results."

    try:
        return answer_chain.invoke({
            "question": question,
            "sql": sql,
            "result": result,
        })
    except Exception as e:
        if "rate_limit" in str(e).lower() or "413" in str(e):
            return '<span class="error-text">Rate limit exceeded. Please wait a moment and try again.</span>'
        raise


def render_message(role: str, content: str) -> None:
    """Render message as a custom HTML chat bubble — left/right aligned."""
    import html as _html
    import re as _re

    # Handle fenced code blocks: ```...``` -> styled code
    def _code_block(m):
        code = _html.escape(m.group(2))
        return f'<pre><code>{code}</code></pre>'

    processed = _re.sub(r'```(\w*)\n?(.*?)```', _code_block, content, flags=_re.DOTALL)

    # Handle markdown tables: | col | col |
    def _convert_md_table(text):
        lines = text.split('\n')
        result_lines = []
        table_lines = []
        in_table = False

        def flush_table():
            if len(table_lines) < 2:
                return '\n'.join(table_lines)
            rows = []
            for line in table_lines:
                cells = [c.strip() for c in line.strip('|').split('|')]
                rows.append(cells)
            # Skip separator row (---)
            data_rows = [r for r in rows if not all(_re.match(r'^[:\-\s]+$', c) for c in r)]
            if not data_rows:
                return '\n'.join(table_lines)
            html_parts = ['<table>']
            # First row as header
            html_parts.append('<thead><tr>')
            for cell in data_rows[0]:
                html_parts.append(f'<th>{_html.escape(cell)}</th>')
            html_parts.append('</tr></thead>')
            # Remaining as body
            if len(data_rows) > 1:
                html_parts.append('<tbody>')
                for row in data_rows[1:]:
                    html_parts.append('<tr>')
                    for cell in row:
                        html_parts.append(f'<td>{_html.escape(cell)}</td>')
                    html_parts.append('</tr>')
                html_parts.append('</tbody>')
            html_parts.append('</table>')
            return ''.join(html_parts)

        for line in lines:
            stripped = line.strip()
            if stripped.startswith('|') and stripped.endswith('|'):
                in_table = True
                table_lines.append(stripped)
            else:
                if in_table:
                    result_lines.append(flush_table())
                    table_lines = []
                    in_table = False
                result_lines.append(line)
        if in_table:
            result_lines.append(flush_table())
        return '\n'.join(result_lines)

    processed = _convert_md_table(processed)

    # Handle inline code: `...`
    processed = _re.sub(r'`([^`]+)`', lambda m: f'<code>{_html.escape(m.group(1))}</code>', processed)

    # Handle bold: **...**
    processed = _re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', processed)

    # Escape remaining plain text parts, convert newlines
    parts = _re.split(r'(<pre><code>.*?</code></pre>|<code>.*?</code>|<strong>.*?</strong>|<table>.*?</table>|<span.*?>.*?</span>)', processed, flags=_re.DOTALL)
    final = []
    for p in parts:
        if p.startswith(('<pre>', '<code>', '<strong>', '<table>', '<span')):
            final.append(p)
        else:
            final.append(_html.escape(p).replace('\n', '<br>'))
    formatted = ''.join(final)

    if role == "user":
        bubble_cls = "user-bubble"
        row_cls = "user-row"
    else:
        bubble_cls = "assistant-bubble"
        row_cls = "assistant-row"
    st.markdown(
        f'<div class="chat-row {row_cls}">'
        f'  <div class="chat-bubble {bubble_cls}">{formatted}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Chat state ───────────────────────────────────────────────────
st.sidebar.markdown(
    "<hr style='margin:0.45rem 0 0.55rem 0; border:0; border-top:1px solid #2E4A73;'>",
    unsafe_allow_html=True,
)
start_new_chat = st.sidebar.button("Start New Chat", use_container_width=True)
if "messages" not in st.session_state or start_new_chat:
    st.session_state["messages"] = [
        {"role": "assistant", "content": "How can I help you?"}
    ]
    st.session_state.pop("pending_write", None)

for msg in st.session_state["messages"]:
    render_message(msg["role"], msg["content"])

# ── Pending write confirmation UI ────────────────────────────────
if st.session_state.get("pending_write"):
    sql = st.session_state["pending_write"]
    st.info(f"**Write operation pending confirmation:**\n```sql\n{sql}\n```")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Confirm & Execute", type="primary", use_container_width=True):
            try:
                affected = execute_sql(engine, sql)
                msg = f"Write executed successfully. **{affected} row(s) affected.**"
            except Exception as e:
                msg = f"Write failed:\n\n```\n{e}\n```"
            st.session_state["messages"].append({"role": "assistant", "content": msg})
            del st.session_state["pending_write"]
            if is_schema_change(sql):
                st.session_state["db_version"] += 1
            st.rerun()
    with col2:
        if st.button("Cancel", use_container_width=True):
            st.session_state["messages"].append(
                {"role": "assistant", "content": "Write operation cancelled by user."}
            )
            del st.session_state["pending_write"]
            st.rerun()

# ── Chat input ───────────────────────────────────────────────────
user_query = st.chat_input(placeholder="Ask anything from the database")

if user_query:
    st.session_state["messages"].append({"role": "user", "content": user_query})
    render_message("user", user_query)

    with st.spinner("Thinking..."):
        try:
            output = ask_database(user_query)
        except Exception as e:
            output = f"An error occurred:\n\n```\n{e}\n```"

    st.session_state["messages"].append({"role": "assistant", "content": output})
    render_message("assistant", output)

    if st.session_state.get("pending_write"):
        st.rerun()
