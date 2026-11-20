"""AI in SDP Agent - Streamlit Databricks App over the ai-sdp AI Search index.

Light ChatGPT-style sidebar (native Material icon nav), chat bubbles with avatars,
a typing indicator, grouped source popover, and an action bar under each answer.
RAG_UI_ONLY=true renders the UI without hitting the index.
"""

from __future__ import annotations

import html
import json as _json
import os
import re
import time

import streamlit as st
import streamlit.components.v1 as components

UI_ONLY = os.environ.get("RAG_UI_ONLY", "false").lower() == "true"
INDEX = os.environ.get("RAG_INDEX", "(not connected)")
LLM_ENDPOINT = os.environ.get("RAG_LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")

st.set_page_config(page_title="AI in SDP Agent", page_icon=os.path.join(os.path.dirname(__file__), "favicon.png"),
                   layout="wide", initial_sidebar_state="expanded")

DBX_LOGO = (
    '<svg width="24" height="27" viewBox="0 0 300 331" fill="none" xmlns="http://www.w3.org/2000/svg">'
    '<path fill="#FF3621" d="M283.923 136.449L150.144 213.624L6.88995 131.168L0 134.982V194.844L150.144 281.115'
    'L283.923 204.234V235.926L150.144 313.1L6.88995 230.644L0 234.458V244.729L150.144 331L300 244.729V184.867'
    'L293.11 181.052L150.144 263.215L16.0766 186.334V154.643L150.144 231.524L300 145.253V86.2713L292.536 81.8697'
    'L150.144 163.739L22.9665 90.9663L150.144 17.8998L254.641 78.055L263.828 72.773V65.4371L150.144 0L0 86.2713'
    'V95.6613L150.144 181.933L283.923 104.758V136.449Z"/></svg>'
)
_LOGO_PATHS = ('<rect x="6" y="30" width="30" height="8" rx="4" fill="#F9D3CA"/>'
               '<rect x="6" y="19" width="30" height="8" rx="4" fill="#F4A897"/>'
               '<rect x="6" y="8" width="20" height="8" rx="4" fill="#EB5A46"/>'
               '<path d="M37 3c.9 5.4 2.6 7.1 8 8-5.4.9-7.1 2.6-8 8-.9-5.4-2.6-7.1-8-8 5.4-.9 7.1-2.6 8-8z" fill="#FF3621"/>')
# Agent logo: stacked chunks + sparkle (documents, chunked and embedded by the pipeline).
AGENT_LOGO = f'<svg width="44" height="44" viewBox="0 0 48 48" fill="none">{_LOGO_PATHS}</svg>'
BOT_ICON = f'<svg width="19" height="19" viewBox="0 0 48 48" fill="none">{_LOGO_PATHS}</svg>'
USER_ICON = ('<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="#6b7789" '
             'stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">'
             '<circle cx="12" cy="8" r="3.6"/><path d="M5 20c0-3.7 3.1-6 7-6s7 2.3 7 6"/></svg>')
TYPING = (f'<div class="chat-wrap"><div class="crow bot"><div class="mline">'
          f'<div class="avatar bot">{BOT_ICON}</div>'
          f'<div class="bubble"><span class="typing"><i></i><i></i><i></i></span></div></div></div></div>')

GREETING = ("Servus! I'm the AI in SDP agent. Ask me about passwords and sign-in, VPN, "
            "pricing, refunds, onboarding, SLAs, or the legal docs, and I'll answer straight "
            "from the indexed documents.")
SUGGESTIONS = [
    "How do I turn on two-factor authentication?",
    "How do I reset my password?",
    "How do I fix VPN connection problems?",
    "What are the enterprise pricing discounts?",
    "What does the data processing agreement cover?",
    "What is the refund policy?",
    "What happens when I exceed the API rate limit?",
]
TOPICS = ["product", "support", "billing", "legal", "other"]
NAV = [("chat", "New Chat", ":material/edit_square:"),
       ("documents", "Documents", ":material/description:"),
       ("skills", "Skills", ":material/auto_awesome:"),
       ("research", "Deep Research", ":material/travel_explore:")]

st.markdown('<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
            '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&display=swap" rel="stylesheet">',
            unsafe_allow_html=True)
st.markdown("""
<style>
#MainMenu, footer { visibility:hidden; }
[data-testid="stToolbar"] { visibility:hidden; }
[data-testid="stDecoration"] { display:none; }
/* keep the sidebar collapse/expand control usable so the menu can always be reopened
   (Streamlit renames this control across versions, so cover the known variants) */
[data-testid="stSidebarCollapsedControl"],
[data-testid="stExpandSidebarButton"],
[data-testid="stBaseButton-headerNoPadding"],
[data-testid="collapsedControl"] { visibility:visible !important; display:flex !important; opacity:1 !important; }
.block-container { padding-top:2rem; max-width:900px; }
:root { --ink:#1B3139; --muted:#6b7789; --lava:#FF3621; --font:"DM Sans",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
html, body, .stApp, .stApp p, .stApp li, .stApp span:not([data-testid="stIconMaterial"]), .stApp div, .stApp button, .stApp textarea, .stApp input, .stApp label { font-family:var(--font) !important; }
.stApp code, .bubble code { font-family:"SF Mono",Menlo,Consolas,monospace !important; }
/* light sidebar */
section[data-testid="stSidebar"] { background:#f7f7f5 !important; border-right:1px solid #ececea; }
section[data-testid="stSidebar"] * { color:#3a4658; }
.side-head { display:flex; align-items:center; gap:.55rem; padding:.4rem .2rem 2rem; margin-bottom:.4rem; }
.side-head .wm { font-weight:700; font-size:1.1rem; color:#1B3139; letter-spacing:-.2px; }
section[data-testid="stSidebar"] [data-testid="stVerticalBlock"] { gap:.2rem !important; }
section[data-testid="stSidebar"] .stButton button {
  display:flex !important; align-items:center !important; justify-content:flex-start !important; gap:.55rem !important;
  text-align:left !important; background-color:transparent !important; border:none !important; box-shadow:none !important;
  color:#3a4658 !important; padding:.45rem .7rem !important; border-radius:8px !important; min-height:0 !important; width:100% !important; }
/* Streamlit centers button content in an inner flex wrapper — force it left */
section[data-testid="stSidebar"] .stButton button > div,
section[data-testid="stSidebar"] .stButton button [data-testid="stMarkdownContainer"] {
  display:flex !important; align-items:center !important; justify-content:flex-start !important; width:100% !important; text-align:left !important; }
section[data-testid="stSidebar"] .stButton button:hover { background-color:#ececea !important; color:#16233a !important; }
section[data-testid="stSidebar"] .stButton button p { margin:0 !important; font-size:.95rem !important; font-weight:500 !important; text-align:left !important; }
section[data-testid="stSidebar"] .stButton button [data-testid="stIconMaterial"] { font-size:20px !important; color:#5a6474 !important; }
.side-cap { color:#8b929c; font-size:.75rem; padding:.4rem .6rem; }
.stApp { background: radial-gradient(ellipse 60% 40% at 50% 34%, rgba(255,54,33,.055), rgba(255,54,33,0) 70%), #faf9f7 !important; }
[data-testid="stHeader"] { background:transparent !important; }
/* hero (Genie-style entry page) */
.hero-logo { width:76px; height:76px; margin:11vh auto 1.3rem; background:#fff; border-radius:20px;
  box-shadow:0 6px 24px rgba(30,20,10,.08); display:flex; align-items:center; justify-content:center; }
.hero-title { text-align:center; font-family:"DM Sans",-apple-system,sans-serif; font-size:2.6rem; font-weight:500;
  letter-spacing:-.8px; color:#1f1f1f; margin:0 0 .35rem; }
.hero-sub { text-align:center; color:#8a8580; font-size:.95rem; margin-bottom:1.6rem; }
.st-key-hero_input [data-testid="stChatInput"] > div:first-child { padding:8px 8px 8px 16px !important;
  box-shadow:0 4px 18px rgba(30,20,10,.07) !important; border-color:#e7e3de !important; }
/* rotating suggestions: two rows visible, JS cycles through the pool */
[class*="st-key-sug_"] { margin-top:-.75rem; }
.st-key-sug_0 { margin-top:-.2rem; }
[data-testid="stBottom"] > div, [data-testid="stBottomBlockContainer"] { background:#faf9f7 !important; }
.st-key-hero_input textarea { font-size:1rem !important; }
/* rotating example in the hero input: fade the placeholder out, swap text, fade back in */
.st-key-hero_input textarea::placeholder { transition:opacity .35s ease; opacity:1; }
.st-key-hero_input textarea.ph-out::placeholder { opacity:0; }
[class*="st-key-sug_"] button { width:100% !important; justify-content:flex-start !important; background:transparent !important;
  border:none !important; box-shadow:none !important; border-radius:10px !important; padding:.62rem 2.6rem .62rem 2.6rem !important;
  color:#5f5a55 !important; position:relative; min-height:0 !important; }
[class*="st-key-sug_"] button > div, [class*="st-key-sug_"] button [data-testid="stMarkdownContainer"] { justify-content:flex-start !important; text-align:left !important; width:100%; }
[class*="st-key-sug_"] button p { font-size:1rem !important; text-align:left !important; }
[class*="st-key-sug_"] button::before { content:""; position:absolute; left:.85rem; top:50%; width:18px; height:18px; transform:translateY(-50%);
  background:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%237a746e' stroke-width='1.6' stroke-linejoin='round'%3E%3Cpath d='M12 3c.8 4.6 2.4 6.2 7 7-4.6.8-6.2 2.4-7 7-.8-4.6-2.4-6.2-7-7 4.6-.8 6.2-2.4 7-7z'/%3E%3C/svg%3E") no-repeat center/contain; }
[class*="st-key-sug_"] button::after { content:""; position:absolute; right:1rem; top:50%; width:18px; height:18px; transform:translateY(-50%);
  opacity:0; transition:opacity .15s;
  background:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='%23555' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpath d='M5 12h14M13 6l6 6-6 6'/%3E%3C/svg%3E") no-repeat center/contain; }
[class*="st-key-sug_"] button:hover { background:#efedea !important; color:#1f1f1f !important; }
[class*="st-key-sug_"] button:hover::after { opacity:1; }
/* live "Thinking" trace, Genie-style */
.trace { margin:6px 0 8px; animation:pop .3s ease; }
.think-head { display:flex; align-items:center; gap:10px; font-family:"DM Sans",-apple-system,sans-serif; font-size:1.05rem; font-weight:500; color:#6f6a65; }
.think-logo { display:flex; } .think-logo svg { animation:breathe 1.4s ease-in-out infinite; }
@keyframes breathe { 50% { transform:scale(.86); opacity:.7; } }
.shimmer { background:linear-gradient(90deg,#8a847e 0%,#8a847e 40%,#e2ddd8 50%,#8a847e 60%,#8a847e 100%); background-size:200% 100%;
  -webkit-background-clip:text; background-clip:text; color:transparent; animation:shimmer 1.6s linear infinite; }
@keyframes shimmer { from { background-position:100% 0; } to { background-position:-100% 0; } }
.tl { margin:12px 0 0 10px; padding-left:24px; border-left:1.5px solid #dcd8d3; position:relative; }
.tl::before { content:""; position:absolute; left:-5px; top:3px; width:8px; height:8px; border-radius:50%; background:#2a2a2a; }
.tl-intro { color:#2a2a2a; font-size:.98rem; margin-bottom:12px; }
.step { display:flex; gap:12px; align-items:flex-start; background:#fff; border:1px solid #e9e5e0; border-radius:10px;
  padding:10px 14px; margin-bottom:8px; animation:pop .3s ease; }
.step .sl { color:#2a2a2a; font-size:.95rem; } .step .sd { color:#8a847e; font-size:.82rem; margin-top:2px; }
.spin { flex:0 0 14px; width:14px; height:14px; margin-top:3px; border:2px solid #e3ded8; border-top-color:#FF3621; border-radius:50%; animation:spin .8s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
.step .ok { flex:0 0 16px; margin-top:2px; }
details.trace-done { margin:8px 0 4px; }
details.trace-done summary { cursor:pointer; list-style:none; display:inline-flex; align-items:center; gap:8px; color:#6f6a65; white-space:nowrap;
  font-family:"DM Sans",-apple-system,sans-serif; font-size:1rem; font-weight:500; }
details.trace-done summary .think-logo svg { animation:none; }
details.trace-done summary:hover { color:#2a2a2a; }
details.trace-done summary::-webkit-details-marker { display:none; }
details.trace-done summary .chev { transition:transform .2s; } details.trace-done[open] summary .chev { transform:rotate(90deg); }
details.trace-done .tl { margin-top:8px; }
/* markdown inside answer bubbles */
.bubble p { margin:0 0 .6em; } .bubble p:last-child { margin-bottom:0; }
.bubble ol, .bubble ul { margin:.1em 0 .7em; padding-left:1.35em; } .bubble li { margin:.3em 0; }
.bubble ol:last-child, .bubble ul:last-child { margin-bottom:0; }
.bubble strong { font-weight:650; } .bubble .h { font-weight:650; margin-top:.4em; }
.bubble code { background:#f3f5f8; border-radius:5px; padding:1px 5px; font-size:.88em; }
.crow.user .bubble code { background:#e3e0dc; }
.chips-label { color:var(--muted); font-size:.76rem; font-weight:700; letter-spacing:.05em; text-transform:uppercase; margin:1.1rem 0 .4rem; }
/* chat bubbles */
.chat-wrap { display:flex; flex-direction:column; gap:14px; padding:6px 2px 4px; }
.crow { animation:pop .32s cubic-bezier(.2,.8,.2,1); }
.mline { display:flex; align-items:flex-start; gap:11px; }
.crow.user .mline { flex-direction:row-reverse; }
.avatar { width:34px; height:34px; border-radius:50%; display:flex; align-items:center; justify-content:center; flex:0 0 34px; margin-top:2px; }
.avatar.bot { background:#fff; border:1px solid #f1e3df; }
.avatar.usr { background:#eef1f6; border:1px solid #e2e7ef; }
.bubble { padding:11px 15px; border-radius:16px; font-size:.98rem; line-height:1.5; word-wrap:break-word; max-width:76%; }
.crow.bot .bubble { background:transparent; border:none; box-shadow:none; color:#1f1f1f; padding:2px 0; max-width:100%; font-size:1rem; line-height:1.6; }
.crow.user .bubble { background:#f4f3f1; color:#1f1f1f; border-radius:18px; padding:11px 18px; }
.avatar { display:none !important; }
@keyframes pop { from{opacity:0; transform:translateY(8px);} to{opacity:1; transform:none;} }
.typing { display:inline-flex; gap:4px; padding:3px 0; }
.typing i { width:7px; height:7px; border-radius:50%; background:#c3ccd8; animation:blink 1.2s infinite; }
.typing i:nth-child(2){ animation-delay:.2s; } .typing i:nth-child(3){ animation-delay:.4s; }
@keyframes blink { 0%,60%,100%{opacity:.3; transform:translateY(0);} 30%{opacity:1; transform:translateY(-3px);} }
/* grouped sources popover trigger (indented under the bubble) */
.st-key-src_ { margin-left:0 !important; }
[class*="st-key-src_"] [data-testid="stPopover"] > div > button {
  background:#efedea !important; border:none !important; border-radius:999px !important; box-shadow:none !important;
  color:#1f1f1f !important; padding:.25rem .95rem !important; min-height:0 !important; margin-left:0 !important; }
[class*="st-key-src_"] [data-testid="stPopover"] > div > button:hover { background:#e5e2de !important; }
[class*="st-key-src_"] [data-testid="stPopover"] > div > button p { font-size:.95rem !important; font-weight:500 !important; }
[class*="st-key-src_"] [data-testid="stPopover"] > div > button [data-testid="stIconMaterial"] { display:none !important; }
[class*="st-key-act_"] [data-testid="stHorizontalBlock"] { gap:.2rem !important; }
/* sources dropdown, Genie-style: one row per file with a format icon */
[data-testid="stPopoverBody"] { padding:6px !important; border-radius:12px !important; border:1px solid #ebe8e4 !important;
  box-shadow:0 8px 28px rgba(30,20,10,.10) !important; min-width:340px !important; background:#fff !important; }
[data-testid="stPopoverBody"], [data-testid="stPopoverBody"] *:not([data-testid="stIconMaterial"]) { font-family:var(--font) !important; }
.src-list { display:flex; flex-direction:column; }
.src-row { display:flex; align-items:center; gap:12px; padding:9px 12px; border-radius:8px; color:#1f1f1f !important;
  text-decoration:none !important; font-size:.98rem; white-space:nowrap; }
.src-row:hover { background:#f2f1ef; }
.src-row .fi { flex:0 0 22px; }
/* rounded pill chat input, neutral border, red sized submit */
[data-testid="stChatInput"] { background:transparent !important; }
[data-testid="stChatInput"] > div:first-child { border:1px solid #dbe3ee !important; box-shadow:0 2px 10px rgba(20,35,58,.06) !important;
  border-radius:26px !important; background:#fff !important; padding:4px 6px 4px 10px !important; align-items:center !important; }
[data-testid="stChatInput"] > div:first-child:focus-within { border-color:#c7cdd4 !important; box-shadow:0 2px 12px rgba(20,35,58,.09) !important; }
[data-testid="stChatInput"] textarea { color:#1B3139 !important; }
[data-testid="stChatInputSubmitButton"] { background:#FF3621 !important; border-radius:50% !important;
  width:36px !important; height:36px !important; min-height:36px !important; align-self:center !important; margin:0 2px !important; }
[data-testid="stChatInputSubmitButton"] svg { color:#fff !important; fill:#fff !important; width:18px !important; height:18px !important; }
[data-testid="stChatInputSubmitButton"]:hover { background:#e02b17 !important; }
[data-testid="stChatInputSubmitButton"]:disabled { background:#f3c6bf !important; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_agent():
    if UI_ONLY:
        return None
    from rag_agent import RagAgent
    return RagAgent()


st.session_state.setdefault("view", "chat")
st.session_state.setdefault("messages", [])
st.session_state.setdefault("pending", None)
K = 4

SHOWN_SUGGESTIONS = 3  # fixed rows under the input; the rest rotate as the input placeholder
PLACEHOLDER_ROTATOR = """<script>
(function () {
  var w = window.parent, d = w.document, pool = %s, i = 0;
  if (w.__phTimer) clearInterval(w.__phTimer);
  w.__phTimer = setInterval(function () {
    var ta = d.querySelector('.st-key-hero_input textarea');
    if (!ta) { clearInterval(w.__phTimer); return; }
    if (ta.value) return;  // leave it alone while the user is typing
    ta.classList.add('ph-out');
    setTimeout(function () {
      i = (i + 1) %% pool.length;
      ta.setAttribute('placeholder', pool[i]);
      ta.classList.remove('ph-out');
    }, 350);
  }, 4000);
})();
</script>""" % _json.dumps(SUGGESTIONS[SHOWN_SUGGESTIONS:])

NOT_CONNECTED = ("The interface is deployed, but it isn't connected to the AI Search index yet. "
                 "Retrieval and answers light up once the index is wired in.")


def basename(path):
    return (path or "").rstrip("/").split("/")[-1] or "(unknown)"


def source_link(path):
    p = path or ""
    for pre in ("dbfs:/Volumes/", "/Volumes/"):
        if p.startswith(pre):
            return f"{HOST}/explore/data/volumes/{p[len(pre):]}" if HOST else None
    return None


def _inline(s):
    s = html.escape(s)
    s = re.sub(r"`([^`]+)`", r"<code>\1</code>", s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", s)
    return re.sub(r"(?<![*\w])\*([^*\n]+?)\*(?![*\w])", r"<em>\1</em>", s)


def md_html(text):
    """Small markdown renderer for answers: paragraphs, numbered and bullet lists, headings, inline styles."""
    out, para, lst = [], [], None

    def flush():
        if para:
            out.append("<p>" + "<br>".join(_inline(x) for x in para) + "</p>")
            para.clear()

    def close():
        nonlocal lst
        if lst:
            out.append(f"</{lst}>")
            lst = None

    for line in (text or "").split("\n"):
        ol = re.match(r"^\s*(\d+)[.)]\s+(.*)", line)
        ul = re.match(r"^\s*[-*\u2022]\s+(.*)", line)
        hd = re.match(r"^\s*#{1,6}\s+(.*)", line)
        if not line.strip():
            flush()  # keep an open list across blank lines so numbering continues
        elif ol or ul:
            flush()
            kind = "ol" if ol else "ul"
            if lst != kind:
                close()
                out.append(f'<ol start="{ol.group(1)}">' if ol else "<ul>")
                lst = kind
            out.append(f"<li>{_inline((ol or ul).group(2 if ol else 1))}</li>")
        elif lst and line[:1] in (" ", "\t") and out and out[-1].endswith("</li>"):
            out[-1] = out[-1][:-5] + "<br>" + _inline(line.strip()) + "</li>"
        elif hd:
            flush(); close()
            out.append(f'<p class="h">{_inline(hd.group(1))}</p>')
        else:
            close()
            para.append(line.strip())
    flush(); close()
    return "".join(out)


_OK = ('<svg class="ok" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#3b9b5a" stroke-width="2.2" '
       'stroke-linecap="round" stroke-linejoin="round"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg>')
_CHEV = ('<svg class="chev" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" '
         'stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"/></svg>')
TRACE_INTRO = "I'll search the indexed documents for passages about this, then write an answer grounded in them."


def _steps_html(steps):
    rows = []
    for label, state, detail in steps:
        icon = '<span class="spin"></span>' if state == "run" else _OK
        det = f'<div class="sd">{html.escape(detail)}</div>' if detail else ""
        rows.append(f'<div class="step">{icon}<div><div class="sl">{html.escape(label)}</div>{det}</div></div>')
    return "".join(rows)


def trace_live(steps):
    return (f'<div class="trace"><div class="think-head"><span class="think-logo">{BOT_ICON}</span>'
            f'<span class="shimmer">Thinking</span></div>'
            f'<div class="tl"><div class="tl-intro">{TRACE_INTRO}</div>{_steps_html(steps)}</div></div>')


def trace_done(steps):
    return (f'<details class="trace-done"><summary><span class="think-logo">{BOT_ICON}</span>'
            f'<span>Thought process</span>{_CHEV}</summary>'
            f'<div class="tl"><div class="tl-intro">{TRACE_INTRO}</div>{_steps_html(steps)}</div></details>')


def bubble(role, content):
    body = md_html(content) if role != "user" else html.escape(content or "").replace("\n", "<br>")
    cls, avatar = ("user", USER_ICON) if role == "user" else ("bot", BOT_ICON)
    acls = "usr" if role == "user" else "bot"
    return (f'<div class="chat-wrap"><div class="crow {cls}"><div class="mline">'
            f'<div class="avatar {acls}">{avatar}</div><div class="bubble">{body}</div></div></div></div>')


# Action bar under each assistant reply (copy is functional via an iframe; others are
# visual affordances; regenerate re-asks the last question via a query param).
_S = ('<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" '
      'stroke-linecap="round" stroke-linejoin="round">')
_ACTS = [
    ("up", "Good response", f'{_S}<path d="M7 10v11"/><path d="M15 5.9L14 10h5.8a2 2 0 0 1 2 2.3l-1.4 7A2 2 0 0 1 18.4 21H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h2.8a2 2 0 0 0 1.8-1.1L12 2a3.1 3.1 0 0 1 3 3.9z"/></svg>'),
    ("down", "Bad response", f'{_S}<path d="M17 14V3"/><path d="M9 18.1L10 14H4.2a2 2 0 0 1-2-2.3l1.4-7A2 2 0 0 1 5.6 3H20a2 2 0 0 1 2 2v7a2 2 0 0 1-2 2h-2.8a2 2 0 0 0-1.8 1.1L12 22a3.1 3.1 0 0 1-3-3.9z"/></svg>'),
    ("comment", "Give feedback", f'{_S}<path d="M7.9 20A9 9 0 1 0 4 16.1L2 22z"/><path d="M9.1 9a3 3 0 0 1 5.8 1c0 2-3 3-3 3"/><path d="M12 17h.01"/></svg>'),
    ("copy", "Copy", f'{_S}<rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>'),
    ("download", "Download", f'{_S}<path d="M12 3v12"/><path d="M7 10l5 5 5-5"/><path d="M5 21h14"/></svg>'),
]


def render_actions(text, is_last):
    tj = _json.dumps(text or "")
    btns = []
    for key, title, svg in _ACTS:
        if key == "copy":
            btns.append(f'<button class="a" id="cp" title="Copy" onclick="doCopy()">{svg}</button>')
        elif key == "download":
            btns.append(f'<button class="a" title="Download" onclick="doDownload()">{svg}</button>')
        else:
            btns.append(f'<button class="a" title="{title}" onclick="this.classList.toggle(\'on\')">{svg}</button>')
    doc = ("<!doctype html><html><head><meta charset='utf-8'><style>"
           "html,body{margin:0;padding:0;background:transparent;}"
           ".bar{display:flex;gap:2px;padding-left:0;}"
           ".a{width:34px;height:34px;border:none;background:transparent;color:#5f5a55;border-radius:8px;"
           "cursor:pointer;display:inline-flex;align-items:center;justify-content:center;padding:0;}"
           ".a:hover{background:#efedea;color:#1f1f1f;}.a.on{color:#FF3621;}.a svg{width:19px;height:19px;}"
           "</style></head><body><div class='bar'>" + "".join(btns) + "</div><script>"
           "function doCopy(){var t=" + tj + ";try{navigator.clipboard.writeText(t);}catch(e){}"
           "try{var ta=document.createElement('textarea');ta.value=t;ta.style.position='fixed';ta.style.opacity=0;"
           "document.body.appendChild(ta);ta.focus();ta.select();document.execCommand('copy');document.body.removeChild(ta);}catch(e){}"
           "var c=document.getElementById('cp');if(c){c.classList.add('on');setTimeout(function(){c.classList.remove('on');},1200);}}"
           "function doDownload(){var d=window.parent.document,a=d.createElement('a');"
           "a.href=URL.createObjectURL(new Blob([" + tj + "],{type:'text/markdown'}));a.download='answer.md';"
           "d.body.appendChild(a);a.click();a.remove();}"
           "</script></body></html>")
    components.html(doc, height=40)


_FMT = {  # extension -> (badge label, badge colour)
    "pdf": ("PDF", "#E5372B"), "doc": ("DOC", "#2B5FB4"), "docx": ("DOC", "#2B5FB4"),
    "xls": ("XLS", "#1E7B45"), "xlsx": ("XLS", "#1E7B45"), "csv": ("CSV", "#1E7B45"),
    "ppt": ("PPT", "#D2552B"), "pptx": ("PPT", "#D2552B"), "txt": ("TXT", "#6f6a65"), "md": ("MD", "#6f6a65"),
    "png": ("IMG", "#7B4FC9"), "jpg": ("IMG", "#7B4FC9"), "jpeg": ("IMG", "#7B4FC9"), "tif": ("IMG", "#7B4FC9"), "tiff": ("IMG", "#7B4FC9"),
}


def file_icon(name):
    label, colour = _FMT.get(name.rsplit(".", 1)[-1].lower() if "." in name else "", ("FILE", "#8a847e"))
    return ('<svg class="fi" width="22" height="22" viewBox="0 0 24 24" fill="none">'
            '<path d="M7 2.5h8l4.5 4.5v13.5a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1v-17a1 1 0 0 1 1-1z" fill="#fff" stroke="#9a948e" stroke-width="1.3"/>'
            '<path d="M15 2.5V7h4.5" stroke="#9a948e" stroke-width="1.3" stroke-linejoin="round"/>'
            f'<rect x="1.5" y="11.5" width="{5 + 3.6 * len(label)}" height="7.5" rx="1.6" fill="{colour}"/>'
            f'<text x="{1.5 + (5 + 3.6 * len(label)) / 2}" y="17.1" text-anchor="middle" font-size="5.4" font-weight="700" '
            f'font-family="DM Sans,Arial,sans-serif" fill="#fff">{label}</text></svg>')


def render_sources(sources, idx):
    """Grouped 'N sources' popover; clicking opens a panel listing the source files."""
    uniq = {}
    for s in sources or []:
        fn = basename(s.get("source"))
        if fn and fn not in uniq:
            uniq[fn] = source_link(s.get("source"))
    if not uniq:
        return
    with st.container(key=f"src_{idx}"):
        with st.popover(f"{len(uniq)} sources"):
            rows = "".join(
                (f'<a class="src-row" href="{html.escape(url)}" target="_blank" rel="noopener">' if url else '<div class="src-row">')
                + f'{file_icon(fn)}<span>{html.escape(fn)}</span>' + ("</a>" if url else "</div>")
                for fn, url in uniq.items())
            st.markdown(f'<div class="src-list">{rows}</div>', unsafe_allow_html=True)


def compute(prompt, deep, holder):
    if UI_ONLY:
        return {"answer": NOT_CONNECTED, "sources": []}
    from rag_agent import RESEARCH_PROMPT, ANSWER_PROMPT, EMBED_ENDPOINT
    steps = []

    def step(label, detail=""):
        if steps:
            steps[-1][1] = "done"
        steps.append([label, "run", detail])
        holder.markdown(trace_live(steps), unsafe_allow_html=True)
        time.sleep(0.35)  # let each step register visually before the next one replaces it

    try:
        agent = get_agent()
        k = 8 if deep else K
        step(f"Embedding your question with {EMBED_ENDPOINT}")
        vector = agent.embed(prompt)
        step(f'Searching AI Search for "{prompt}"')
        hits = agent.retrieve(prompt, k=k, vector=vector)
        docs = list(dict.fromkeys(basename(h.get("source")) for h in hits))
        more = f" +{len(docs) - 3} more" if len(docs) > 3 else ""
        steps[-1][2] = f"{len(hits)} passages from {', '.join(docs[:3])}{more}" if hits else "No matching passages"
        step("Writing a cited answer" if deep else "Writing the answer")
        out = agent.answer(prompt, k=k, hits=hits, system=RESEARCH_PROMPT if deep else ANSWER_PROMPT,
                           max_tokens=900 if deep else 400)
        steps[-1][1] = "done"
        out["trace"] = steps
        return out
    except Exception as e:
        return {"answer": f"Error: {e}", "sources": []}


# ---- sidebar ----
with st.sidebar:
    st.markdown(f'<div class="side-head">{DBX_LOGO}<span class="wm">AI in SDP Agent</span></div>',
                unsafe_allow_html=True)
    for key, label, icon in NAV:
        if st.button(label, key=f"nav_{key}", icon=icon, use_container_width=True):
            if key == "chat":
                st.session_state.messages = []
                st.session_state.pending = None
            st.session_state.view = key
            st.rerun()
    st.markdown(f'<style>section[data-testid="stSidebar"] .st-key-nav_{st.session_state.view} button {{ '
                f'background-color:#e7e4de !important; color:#16233a !important; }}</style>', unsafe_allow_html=True)
    if UI_ONLY:
        st.markdown('<div class="side-cap">UI preview — index not connected</div>', unsafe_allow_html=True)

view = st.session_state.view

page = st.empty()
if view in ("chat", "research"):
    with page.container():
        deep = view == "research"

        # Regenerate: drop last answer, re-ask the last question.
        if st.query_params.get("regen"):
            st.query_params.clear()
            msgs = st.session_state.messages
            if msgs and msgs[-1]["role"] == "assistant":
                msgs.pop()
            last_user = next((m["content"] for m in reversed(msgs) if m["role"] == "user"), None)
            if last_user:
                st.session_state.pending = (last_user, deep)
                st.rerun()

        click = None
        entry = not st.session_state.messages and not st.session_state.pending
        typed = None
        if entry:
            title = "How can I help you?" if not deep else "What should I research?"
            sub = ("Answers grounded in documents parsed, chunked, and embedded by a Lakeflow pipeline." if not deep
                   else "Pulls more passages across topics and synthesizes a cited answer.")
            st.markdown(f"<div class='hero-logo'>{AGENT_LOGO}</div><div class='hero-title'>{title}</div>"
                        f"<div class='hero-sub'>{sub}</div>", unsafe_allow_html=True)
            with st.container(key="hero_input"):  # chat_input inside a container renders inline, not pinned
                typed = st.chat_input(SUGGESTIONS[SHOWN_SUGGESTIONS])
            for i, q in enumerate(SUGGESTIONS[:SHOWN_SUGGESTIONS]):
                if st.button(q, key=f"sug_{i}"):
                    click = q
            components.html(PLACEHOLDER_ROTATOR, height=0)
        else:
            for i, m in enumerate(st.session_state.messages):
                if m.get("trace"):
                    st.markdown(trace_done(m["trace"]), unsafe_allow_html=True)
                st.markdown(bubble(m["role"], m["content"]), unsafe_allow_html=True)
                if m["role"] == "assistant":
                    with st.container(key=f"act_{i}"):
                        c1, c2 = st.columns([0.24, 0.76], vertical_alignment="center", gap="small")
                        with c1:
                            render_actions(m["content"], is_last=(i == len(st.session_state.messages) - 1))
                        with c2:
                            render_sources(m.get("sources"), i)

        # Thinking: show typing dots, compute, then replace with the answer.
        if st.session_state.pending:
            p, pdeep = st.session_state.pending
            st.markdown('<style>[data-stale="true"] { display:none !important; }</style>', unsafe_allow_html=True)
            holder = st.empty()
            holder.markdown(trace_live([]), unsafe_allow_html=True)
            out = compute(p, pdeep, holder)
            holder.empty()
            st.session_state.messages.append(
                {"role": "assistant", "content": out["answer"], "sources": out.get("sources", []),
                 "trace": out.get("trace")})
            st.session_state.pending = None
            st.rerun()

    if not entry:
        typed = st.chat_input("Message the AI in SDP Agent" if not deep
                              else "Ask a research question across the documents")
    prompt = typed or click
    if prompt and not st.session_state.pending:
        st.session_state.messages.append({"role": "user", "content": prompt})
        st.session_state.pending = (prompt, deep)
        st.rerun()

elif view == "documents":
    st.markdown("### Documents in the index")
    if UI_ONLY:
        st.info(NOT_CONNECTED)
    else:
        st.caption(f"Distinct source documents indexed in `{INDEX}`.")
        try:
            docs = get_agent().list_documents()
            st.metric("Documents indexed", len(docs))
            st.dataframe(
                [{"document": basename(d["source"]), "topic": d["topic"], "doc_id": d["doc_id"]} for d in docs],
                use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Could not list documents: {e}")

elif view == "skills":
    st.markdown("### Skills")
    st.markdown(
        "- **Grounded Q&A** over the indexed documents, with source citations.\n"
        "- **Topic labels** (product, support, billing, legal, other) on every document; "
        "filter retrieval by topic from the CLI agent (`--topic`).\n"
        "- **Deep Research** mode that pulls more passages and synthesizes across sources.")
    st.markdown('<div class="chips-label">Topics covered</div>', unsafe_allow_html=True)
    st.markdown(" ".join(f"`{t}`" for t in TOPICS))
    st.caption("Documents flow through a Lakeflow SDP pipeline: parse, chunk, embed, "
               "gold materialized view, AI Search index.")
