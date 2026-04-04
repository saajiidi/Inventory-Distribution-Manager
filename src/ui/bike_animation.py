import os
import base64
import streamlit as st

def render_bike_animation():
    """
    Renders a full-screen right-to-left overlay animation of a delivery bike.
    This is extracted to a separate file to ensure it can be maintained easily.
    """
    # Load local bike image
    bike_uri = "https://cdn-icons-png.flaticon.com/512/2830/2830305.png"  # fallback
    
    # Adjust path for src/ui/ location
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    bike_path = os.path.join(base_dir, "assets", "bike.png")

    if os.path.exists(bike_path):
        with open(bike_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
            bike_uri = f"data:image/png;base64,{encoded}"

    st.markdown(f"""
    <style>
    /* Global Full-Screen Bike Animation (Right to Left) */
    @keyframes moveFullScreen {{
        0%   {{ transform: translateX(250px) scale(1); opacity: 0; }}
        10%  {{ opacity: 1; }}
        90%  {{ opacity: 1; }}
        100% {{ transform: translateX(-115vw) scale(1); opacity: 0; }}
    }}
    @keyframes smoke-puff {{
        0% {{ transform: scale(0.4); opacity: 0.8; }}
        100% {{ transform: scale(2) translate(15px, -10px); opacity: 0; }}
    }}
    .full-screen-bike {{
        position: fixed;
        top: 80px; /* Moved down to prevent header cutting */
        right: 0;
        z-index: 9999; /* Overlaps everything */
        pointer-events: none; /* Allows clicking through it */
        display: flex;
        align-items: center;
        flex-direction: row; /* Image then smoke */
        animation: moveFullScreen 18s linear infinite;
        filter: drop-shadow(0 5px 15px rgba(0,0,0,0.1));
    }}
    .bike-img {{
        width: 60px;
        z-index: 10000;
        display: block;
        /* Flips bike to point Left removed, as png natively handles it */
    }}
    .smoke-trail {{
        display: flex;
        /* Put smoke to the right of the left-facing bike */
        margin-left: -10px; 
    }}
    .smoke {{
        width: 12px;
        height: 12px;
        background: var(--text-dim);
        border-radius: 50%;
        animation: smoke-puff 0.8s ease-out infinite;
        margin-left: -6px;
    }}
    .smoke:nth-child(2) {{ animation-delay: 0.2s; }}
    .smoke:nth-child(3) {{ animation-delay: 0.4s; }}
    </style>
    
    <div class="full-screen-bike">
        <img src="{bike_uri}" class="bike-img">
        <div class="smoke-trail">
            <div class="smoke"></div>
            <div class="smoke"></div>
            <div class="smoke"></div>
        </div>
    </div>
    """, unsafe_allow_html=True)
