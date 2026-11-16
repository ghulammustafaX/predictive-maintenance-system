"""
Generates docs/repo-tree.png
Matches the reference image exactly:
  - Orange folder icons, blue/yellow Python icons, grey file icons
  - Clean tree connector lines (vertical + horizontal elbows)
  - Two-column layout: name on left, description right-aligned
  - White card with rounded corners, orange header bar
"""

from PIL import Image, ImageDraw, ImageFont
import os

# ── Canvas ────────────────────────────────────────────────────────────────────
W      = 1060
S      = 3           # 3× supersample → downsample for crisp text
SCALE  = S

# ── Colours ───────────────────────────────────────────────────────────────────
C_BG        = "#FFFFFF"
C_CARD      = "#FAFAFA"
C_BORDER    = "#E8E8E8"
C_HDR_BG    = "#FFF3E0"
C_HDR_FG    = "#BF360C"
C_DIR       = "#E65100"
C_PY_BOX    = "#1565C0"
C_PY_DOT1   = "#FFD600"
C_PY_DOT2   = "#29B6F6"
C_FILE_BG   = "#F5F5F5"
C_FILE_LINE = "#BDBDBD"
C_NAME_DIR  = "#E65100"
C_NAME_PY   = "#1A237E"
C_NAME_FILE = "#424242"
C_DESC      = "#9E9E9E"
C_DASH      = "#BDBDBD"
C_LINE      = "#E0E0E0"

# ── Tree entries ──────────────────────────────────────────────────────────────
# (indent, icon_type, name, description, bold)
ROWS = [
    (0, "dir",  "backend/",            "",                                        True),
    (1, "dir",  "app/",                "",                                        False),
    (2, "dir",  "routers/",            "",                                        False),
    (3, "py",   "auth.py",             "Authentication & token management",       False),
    (3, "py",   "fleet.py",            "Engine fleet status & telemetry",         False),
    (3, "py",   "alerts.py",           "Alert lifecycle management",              False),
    (3, "py",   "anomalies.py",        "Anomaly detection & history",             False),
    (3, "py",   "rul.py",              "RUL prediction queries",                  False),
    (2, "py",   "models.py",           "SQLAlchemy ORM models",                   False),
    (2, "py",   "schemas.py",          "Pydantic request/response schemas",       False),
    (2, "py",   "security.py",         "JWT, bcrypt, token utilities",            False),
    (2, "py",   "rbac.py",             "Role-based access control",               False),
    (2, "py",   "anomaly_detector.py", "Z-score & threshold detection logic",     False),
    (2, "py",   "database.py",         "DB session and connection setup",         False),
    (1, "py",   "main.py",             "App entry point, middleware, router reg", False),
    (1, "file", "Dockerfile",          "",                                        False),
    (1, "file", "requirements.txt",    "",                                        False),
    (0, "dir",  "frontend/",           "",                                        True),
    (1, "dir",  "authentication/",     "Login, register, password reset, OAuth",  False),
    (1, "dir",  "dashboard/",          "Live operations, alerts, anomalies, users",False),
    (0, "dir",  "ml/",                 "",                                        True),
    (1, "dir",  "prediction/",         "LSTM inference service (Kafka + REST)",   False),
    (1, "dir",  "streaming/",          "Real-time preprocessing service",         False),
    (1, "dir",  "data/",               "Preprocessing pipeline scripts",          False),
    (1, "dir",  "models/",             "Saved model checkpoints (not in git)",    False),
    (0, "dir",  "simulator/",          "Replays NASA C-MAPSS dataset into Kafka", False),
    (0, "dir",  "ingestion/",          "Kafka to InfluxDB consumer",              False),
    (0, "dir",  "tests/",              "Integration and unit tests",              False),
    (0, "dir",  "docs/",               "Architecture and API documentation",      False),
    (0, "file", "docker-compose.yml",  "Full stack definition",                   False),
]

# ── Layout constants (unscaled pixels) ───────────────────────────────────────
PAD_L     = 24    # left padding inside card
PAD_R     = 24
PAD_T     = 14    # top padding after header
HDR_H     = 38    # header bar height
INDENT_W  = 24    # indent per level
ROW_H     = 26    # row height
ICON_SZ   = 16    # icon bounding box
ICON_GAP  = 8     # gap between icon and text
DESC_X    = 590   # x start of description column
DASH_X    = DESC_X - 22

H = HDR_H + PAD_T + len(ROWS) * ROW_H + 24

# ── Create canvas ─────────────────────────────────────────────────────────────
img  = Image.new("RGB", (W * S, H * S), C_BG)
draw = ImageDraw.Draw(img)

def p(v):  return int(v * S)
def pp(*v): return tuple(int(x * S) for x in v)

# ── Fonts ─────────────────────────────────────────────────────────────────────
def _font(size, bold=False, mono=False):
    mono_fonts = ["C:/Windows/Fonts/consola.ttf",
                  "C:/Windows/Fonts/lucon.ttf",
                  "C:/Windows/Fonts/cour.ttf"]
    bold_fonts = ["C:/Windows/Fonts/arialbd.ttf",
                  "C:/Windows/Fonts/calibrib.ttf"]
    reg_fonts  = ["C:/Windows/Fonts/arial.ttf",
                  "C:/Windows/Fonts/calibri.ttf"]
    for path in (mono_fonts if mono else (bold_fonts if bold else reg_fonts)):
        if os.path.exists(path):
            return ImageFont.truetype(path, size * S)
    return ImageFont.load_default()

F_HDR   = _font(12, bold=True)
F_BOLD  = _font(11, bold=True,  mono=True)
F_MONO  = _font(11, mono=True)
F_DESC  = _font(10, mono=False)

# ── Card background ───────────────────────────────────────────────────────────
draw.rounded_rectangle(pp(2, 2, W-2, H-2),
                        radius=p(14), fill=C_CARD, outline=C_BORDER, width=p(1))

# ── Header bar ────────────────────────────────────────────────────────────────
draw.rounded_rectangle(pp(2, 2, W-2, HDR_H+2),
                        radius=p(14), fill=C_HDR_BG, outline=C_BORDER, width=p(1))
# Orange square icon in header
ix, iy = 18, (HDR_H - 18) // 2 + 2
draw.rounded_rectangle(pp(ix, iy, ix+18, iy+18),
                        radius=p(3), fill=C_HDR_FG)
# folder tab on icon
draw.rounded_rectangle(pp(ix, iy, ix+9, iy+6),
                        radius=p(2), fill="#FF8A65")
# minus sign
draw.line([pp(ix+4, iy+9), pp(ix+14, iy+9)],
          fill="#FFFFFF", width=p(2))

txt = "predictive-maintenance-system/"
bb  = draw.textbbox(pp(0,0), txt, font=F_HDR)
draw.text(pp(ix+24, (HDR_H - (bb[3]-bb[1])//S) // 2 + 2),
          txt, font=F_HDR, fill=C_HDR_FG)

# ── Helper: draw icons ────────────────────────────────────────────────────────
def folder_icon(x, y, color=C_DIR):
    # tab (top-left rounded)
    draw.rounded_rectangle(pp(x, y+1, x+9, y+6), radius=p(2), fill=color)
    # body
    draw.rounded_rectangle(pp(x, y+4, x+ICON_SZ, y+ICON_SZ+1),
                            radius=p(2), fill=color)

def py_icon(x, y):
    # blue rounded box
    draw.rounded_rectangle(pp(x, y, x+ICON_SZ, y+ICON_SZ),
                            radius=p(3), fill=C_PY_BOX)
    # two dots: yellow (Python logo style)
    draw.ellipse(pp(x+2, y+3, x+7, y+8),  fill=C_PY_DOT1)
    draw.ellipse(pp(x+9, y+8, x+14, y+13), fill=C_PY_DOT2)

def file_icon(x, y):
    # grey document with folded corner
    pts = [(x+1,y), (x+ICON_SZ-4,y), (x+ICON_SZ,y+5),
           (x+ICON_SZ,y+ICON_SZ), (x+1,y+ICON_SZ)]
    draw.polygon(pp(*[coord for pt in pts for coord in pt]),
                 fill=C_FILE_BG, outline=C_FILE_LINE)
    # folded corner triangle
    draw.polygon(pp(x+ICON_SZ-4, y,
                    x+ICON_SZ-4, y+5,
                    x+ICON_SZ,   y+5),
                 fill=C_FILE_LINE)
    # lines on doc
    for ly in [y+7, y+9, y+11]:
        draw.line(pp(x+3, ly, x+ICON_SZ-3, ly),
                  fill=C_FILE_LINE, width=max(1, p(0.5)))

# ── Draw rows ─────────────────────────────────────────────────────────────────
# Pre-compute "last child" at each level for elbows
# For each row, we need to know if there's any same-or-lower indent after it
def is_last_at_level(idx, level):
    for j in range(idx+1, len(ROWS)):
        if ROWS[j][0] < level:
            return True
        if ROWS[j][0] == level:
            return False
    return True

for i, (level, icon, name, desc, bold) in enumerate(ROWS):
    base_y = HDR_H + PAD_T + i * ROW_H

    # ── Connector lines ───────────────────────────────────────────────────────
    if level > 0:
        # Vertical lines for each ancestor level
        for lv in range(1, level + 1):
            vx = PAD_L + (lv - 1) * INDENT_W + ICON_SZ // 2
            # Draw vertical line segment for this row
            if lv < level:
                # Full vertical through this row if level is still open
                if not is_last_at_level(i, lv):
                    draw.line(pp(vx, base_y, vx, base_y + ROW_H),
                              fill=C_LINE, width=max(1, p(1)))
            else:
                # This level: elbow
                is_last = is_last_at_level(i, level)
                top_y   = base_y
                mid_y   = base_y + ROW_H // 2
                # Vertical part (down to mid if last, full row if not)
                v_end = mid_y if is_last else base_y + ROW_H
                draw.line(pp(vx, top_y, vx, v_end),
                          fill=C_LINE, width=max(1, p(1)))
                # Horizontal elbow
                draw.line(pp(vx, mid_y, vx + INDENT_W - 2, mid_y),
                          fill=C_LINE, width=max(1, p(1)))

    # ── Icon ──────────────────────────────────────────────────────────────────
    ix = PAD_L + level * INDENT_W
    iy = base_y + (ROW_H - ICON_SZ) // 2
    if icon == "dir":
        col = C_HDR_FG if bold else C_DIR
        folder_icon(ix, iy, col)
    elif icon == "py":
        py_icon(ix, iy)
    else:
        file_icon(ix, iy)

    # ── Name ──────────────────────────────────────────────────────────────────
    tx   = ix + ICON_SZ + ICON_GAP
    font = F_BOLD if bold else F_MONO
    bb   = draw.textbbox(pp(0,0), name, font=font)
    ty   = base_y + (ROW_H - (bb[3]-bb[1])//S) // 2
    ncol = (C_HDR_FG if bold else
            C_NAME_DIR if icon=="dir" else
            C_NAME_PY  if icon=="py"  else C_NAME_FILE)
    draw.text(pp(tx, ty), name, font=font, fill=ncol)

    # ── Description ───────────────────────────────────────────────────────────
    if desc:
        draw.text(pp(DASH_X, ty), "—", font=F_DESC, fill=C_DASH)
        draw.text(pp(DESC_X, ty), desc, font=F_DESC, fill=C_DESC)

# ── Downsample ────────────────────────────────────────────────────────────────
out = img.resize((W, H), Image.LANCZOS)
os.makedirs("docs", exist_ok=True)
out.save("docs/repo-tree.png", "PNG", optimize=True)
print(f"Saved docs/repo-tree.png  ({os.path.getsize('docs/repo-tree.png')//1024} KB)  {W}x{H}px")
