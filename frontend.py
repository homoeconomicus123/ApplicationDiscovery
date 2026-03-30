from pathlib import Path
import os
import time
import subprocess
import pandas as pd
import streamlit as st
import base64

st.set_page_config(
    page_title="Company A — Unified Software Discovery",
    page_icon="🟢",
    layout="wide"
)

ASSETS_DIR = Path("assets")
css_path = ASSETS_DIR / "styles.css"
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)

def get_base64_image(image_path):
    with open(image_path, "rb") as f:
        data = f.read()
    return base64.b64encode(data).decode()

logo_base64 = get_base64_image("assets/logo.png")

st.markdown(f"""
<div class="topbar">
  <div class="brand">
    <img src="data:image/png;base64,{logo_base64}" alt="Logo" width="32" style="margin-right:8px;"/>
    <span class="brand-text">CompanyA</span>
  </div>
</div>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero">
  <div class="hero-title">UNIFIED DISCOVERY.<br><span class="accent">EMAIL + SSO.</span></div>
  <div class="hero-sub">
    Upload your Microsoft Entra ID logs and Email CSVs to generate a single view of active software usage across your organization.
  </div>
</div>
""", unsafe_allow_html=True)

with st.container():
    st.markdown('<div class="upload-title">Upload Microsoft Entra ID logs</div>', unsafe_allow_html=True)
    sso_file = st.file_uploader("Choose SSO file", type=["csv", "xlsx"], key="sso", label_visibility="collapsed")
    st.markdown("</div>", unsafe_allow_html=True)

with st.container():
    st.markdown('<div class="upload-title">Upload Email Data</div>', unsafe_allow_html=True)
    email_file = st.file_uploader("Choose Email file", type=["csv", "xlsx"], key="email", label_visibility="collapsed")
    st.markdown("</div>", unsafe_allow_html=True)

run_btn = st.button("Run Unified Discovery", type="primary", disabled=(sso_file is None or email_file is None))

final_output = Path("final_output.csv")

if run_btn:
    uploads_dir = Path("data/uploads")
    uploads_dir.mkdir(parents=True, exist_ok=True)

    sso_path = uploads_dir / f"sso_{int(time.time())}_{sso_file.name}"
    with open(sso_path, "wb") as f:
        f.write(sso_file.getbuffer())

    email_path = uploads_dir / f"email_{int(time.time())}_{email_file.name}"
    with open(email_path, "wb") as f:
        f.write(email_file.getbuffer())

    os.environ["SSO_INPUT"] = str(sso_path)
    os.environ["SSO_OUTPUT"] = "SSO_OUTPUT.csv"
    os.environ["EMAIL_INPUT"] = str(email_path)
    os.environ["EMAIL_OUTPUT"] = "EMAIL_OUTPUT.csv"

    with st.spinner("Running pipelines…"):
        subprocess.run(["python", "sso.py"], check=True)
        subprocess.run(["python", "email_pipeline.py"], check=True)
        subprocess.run(["python", "combine.py"], check=True)

    # Feedback
    if final_output.exists():
        st.success("Unified discovery complete! 🎉")
    else:
        st.warning("No final output generated. Check logs.")

st.markdown('<div class="section-title">Results</div>', unsafe_allow_html=True)

if final_output.exists():
    df = pd.read_csv(final_output)

    c1, c2 = st.columns([1, 2])
    with c1:
        st.metric("Unique Applications", value=f"{df['Software'].nunique():,}")
    with c2:
        st.download_button(
            "Download Final CSV",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name="final_output.csv",
            mime="text/csv"
        )

    st.markdown('<div class="card table-card">', unsafe_allow_html=True)
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)
else:
    st.markdown("""
    <div class="card empty-card">
      <div class="empty-title">No Results Yet</div>
      <div class="empty-sub">Upload both SSO and Email files, then run the pipeline.</div>
    </div>
    """, unsafe_allow_html=True)
