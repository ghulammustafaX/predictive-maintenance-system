"""
Generates docs/architecture.png — the PMS workflow architecture diagram.
Matches the reference image: colored rounded boxes, pill labels,
dashed outer border, L-shaped routed arrows.

Run from repo root:
    python scripts/gen_architecture.py
"""

from PIL import Image, ImageDraw, ImageFont
import math, os

# ── Canvas ────────────────────────────────────────────────────────────────────
W, H  = 1100, 980
S     = 3
WW, HH = W * S, H * S
bg    = "#F4F4F2"
img   = Image.new("RGB", (WW, HH), bg)
draw  = ImageDraw.Draw(img)

# ── Fonts ─────────────────────────────────────────────────────────────────────
def _font(size, bold=False):
    cands_b = ["C:/Windows/Fonts/arialbd.ttf", "C:/Windows/Fonts/calibrib.ttf",
               "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
               "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"]
    cands_r = ["C:/Windows/Fonts/arial.ttf", "C:/Windows/Fonts/calibri.ttf",
               "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
               "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"]
    for path in (cands_b if bold else cands_r):
        if os.path.exists(path):
            return ImageFont.truetype(path, size * S)
    return ImageFont.load_default()

FT = _font(14, bold=True)
FS = _font(11, bold=False)
FP = _font(10, bold=False)

# ── Colour palette ────────────────────────────────────────────────────────────
NODES = {
    "blue":   ("#CCDFF5", "#2E78B5", "#1A5C9A"),
    "orange": ("#F9D6B0", "#E07B2A", "#B85E10"),
    "green":  ("#C0E4C0", "#3A8A3A", "#276127"),
    "purple": ("#D5C5F0", "#6A45BF", "#4D2EA0"),
    "red":    ("#F5B8B8", "#D63B3B", "#B02020"),
    "yellow": ("#FDF0A8", "#B8930A", "#8A6B00"),
}
PILL_BG = "#E5E5E5"; PILL_FG = "#555555"; PILL_LINE = "#AAAAAA"
ARROW_COL = "#2A2A2A"; DASH_COL = "#AAAAAA"

def p(v):    return int(v * S)
def pp(*vs): return tuple(p(v) for v in vs)

# ── Primitives ────────────────────────────────────────────────────────────────
def rrect(x, y, w, h, r, fill, outline, lw=2):
    draw.rounded_rectangle([p(x), p(y), p(x+w), p(y+h)],
                            radius=p(r), fill=fill, outline=outline, width=p(lw))

def centered_text(cx, cy, lines):
    cx, cy = p(cx), p(cy)
    heights = [draw.textbbox((0,0), t, font=f)[3] - draw.textbbox((0,0), t, font=f)[1]
               for t, f, _ in lines]
    gap   = p(5)
    total = sum(heights) + gap * (len(lines)-1)
    y = cy - total // 2
    for (txt, fnt, col), h in zip(lines, heights):
        bb = draw.textbbox((0,0), txt, font=fnt)
        draw.text((cx - (bb[2]-bb[0])//2, y), txt, font=fnt, fill=col)
        y += h + gap

def pill(cx, cy, text):
    """Pill centred on unscaled (cx, cy)."""
    bb = draw.textbbox((0,0), text, font=FP)
    tw, th = bb[2]-bb[0], bb[3]-bb[1]
    px, py = p(9), p(4)
    pw, ph = tw+px*2, th+py*2
    x = p(cx) - pw//2;  y = p(cy) - ph//2
    draw.rounded_rectangle([x, y, x+pw, y+ph], radius=p(7),
                            fill=PILL_BG, outline=PILL_LINE, width=max(1,p(1)))
    draw.text((x+px, y+py), text, font=FP, fill=PILL_FG)

def arrowhead(x2, y2, angle_deg):
    a = math.radians(angle_deg); sz = p(10); sp = 0.38
    for da in (+sp, -sp):
        ax = p(x2) - int(sz * math.cos(a-da))
        ay = p(y2) - int(sz * math.sin(a-da))
        draw.line([(p(x2), p(y2)), (ax, ay)], fill=ARROW_COL, width=max(1,p(2)))

def straight_arrow(x1, y1, x2, y2, lbl_above=None, lbl_below=None):
    draw.line([pp(x1,y1), pp(x2,y2)], fill=ARROW_COL, width=max(1,p(2)))
    arrowhead(x2, y2, math.degrees(math.atan2(y2-y1, x2-x1)))
    mx, my = (x1+x2)/2, (y1+y2)/2
    if lbl_above: pill(mx, my-16, lbl_above)
    if lbl_below: pill(mx, my+16, lbl_below)

def L_right_then_down(x1, y1, x2, y2, hlbl1=None, hlbl2=None):
    """Right (x1,y1)→(x2,y1) then down (x2,y1)→(x2,y2). Arrow at bottom."""
    lw = max(1,p(2))
    draw.line([pp(x1,y1), pp(x2,y1)], fill=ARROW_COL, width=lw)
    draw.line([pp(x2,y1), pp(x2,y2)], fill=ARROW_COL, width=lw)
    arrowhead(x2, y2, 90)
    hx = (x1+x2)/2
    if hlbl1: pill(hx, y1-16, hlbl1)
    if hlbl2: pill(hx, y1+16, hlbl2)

def L_down_then_left(x1, y1, x2, y2, lbl=None, lbl_cx=None):
    """Down (x1,y1)→(x1,y2) then left (x1,y2)→(x2,y2). Arrow at left end."""
    lw = max(1,p(2))
    draw.line([pp(x1,y1), pp(x1,y2)], fill=ARROW_COL, width=lw)
    draw.line([pp(x1,y2), pp(x2,y2)], fill=ARROW_COL, width=lw)
    arrowhead(x2, y2, 180)
    if lbl:
        # pill on horizontal segment; lbl_cx lets us place it precisely
        cx = lbl_cx if lbl_cx is not None else (x1+x2)/2
        pill(cx, y2-16, lbl)

# ── Dashed outer border ───────────────────────────────────────────────────────
def dashed_border(x, y, w, h, r, color, lw=1.5, dash=12, gap=7):
    pts = [(x+r,y),(x+w-r,y),(x+w,y+r),(x+w,y+h-r),
           (x+w-r,y+h),(x+r,y+h),(x,y+h-r),(x,y+r),(x+r,y)]
    lw = max(1,p(lw)); dash = p(dash); gap = p(gap)
    drawing = True; bucket = 0
    for i in range(len(pts)-1):
        sx,sy = p(pts[i][0]),p(pts[i][1]); ex,ey = p(pts[i+1][0]),p(pts[i+1][1])
        seg = math.hypot(ex-sx, ey-sy); done = 0.0
        while done < seg:
            chunk = (dash if drawing else gap) - bucket
            step  = min(chunk, seg-done)
            t0,t1 = done/seg, (done+step)/seg
            if drawing:
                draw.line([(sx+t0*(ex-sx), sy+t0*(ey-sy)),
                           (sx+t1*(ex-sx), sy+t1*(ey-sy))], fill=color, width=lw)
            done += step; bucket += step
            if bucket >= (dash if drawing else gap):
                bucket = 0; drawing = not drawing

dashed_border(28, 28, W-56, H-56, 18, DASH_COL)

# ── Node positions ────────────────────────────────────────────────────────────
NW = 210; NH = 115; R = 14

SIM_X,  SIM_Y  = 185,  118
PREP_X, PREP_Y = 650,  118
PRED_X, PRED_Y = 870,  318
CONS_X, CONS_Y = 185,  318
INFL_X, INFL_Y = 185,  498
API_X,  API_Y  = 185,  668
DASH_X, DASH_Y = 580,  668
PG_X,   PG_Y   = 185,  848

def node(cx, cy, ckey, title, sub=None):
    fill, outline, tc = NODES[ckey]
    rrect(cx-NW//2, cy-NH//2, NW, NH, R, fill, outline, lw=2)
    lines = [(title, FT, tc)]
    if sub: lines.append((sub, FS, "#555555"))
    centered_text(cx, cy, lines)

node(SIM_X,  SIM_Y,  "blue",   "Simulator",          "(C-MAPSS)")
node(PREP_X, PREP_Y, "orange", "Preprocessor",       "(scaling, windowing)")
node(PRED_X, PRED_Y, "purple", "Prediction Service", "(LSTM)")
node(CONS_X, CONS_Y, "green",  "Consumer",           "(InfluxDB writer)")
node(INFL_X, INFL_Y, "blue",   "InfluxDB",           "(telemetry + RUL)")
node(API_X,  API_Y,  "red",    "FastAPI Backend")
node(DASH_X, DASH_Y, "yellow", "Dashboard",          "(Browser)")
node(PG_X,   PG_Y,   "blue",   "PostgreSQL",         "(users, alerts, anomalies)")

# ── Arrows ────────────────────────────────────────────────────────────────────

# 1  Simulator → Preprocessor  (straight horizontal)
straight_arrow(SIM_X+NW//2, SIM_Y, PREP_X-NW//2, PREP_Y,
               lbl_above="Kafka", lbl_below="sensor-telemetry")

# 2  Preprocessor → Prediction Service
#    Horizontal right to PRED_X, then vertical down to PRED top.
#    Pills on the horizontal part only.
L_right_then_down(PREP_X+NW//2, PREP_Y, PRED_X, PRED_Y-NH//2,
                  hlbl1="Kafka", hlbl2="preprocessed-telemetry")

# 3  Prediction Service → Consumer  (straight horizontal, same row)
straight_arrow(PRED_X-NW//2, PRED_Y, CONS_X+NW//2, CONS_Y,
               lbl_above="Kafka", lbl_below="sensor-telemetry")

# 4  Prediction Service → InfluxDB
#    Vertical down from PRED bottom to INFL_Y, then horizontal left to INFL right edge.
#    Place "RUL preds" pill on the horizontal segment, slightly right of its midpoint.
L_down_then_left(PRED_X, PRED_Y+NH//2, INFL_X+NW//2, INFL_Y,
                 lbl="RUL preds",
                 lbl_cx=(PRED_X + INFL_X+NW//2)/2 + 60)

# 5  Consumer → InfluxDB  (vertical)
straight_arrow(CONS_X, CONS_Y+NH//2, INFL_X, INFL_Y-NH//2)

# 6  InfluxDB → FastAPI  (vertical)
straight_arrow(INFL_X, INFL_Y+NH//2, API_X, API_Y-NH//2)

# 7  Dashboard → FastAPI  (horizontal left, REST above)
straight_arrow(DASH_X-NW//2, DASH_Y-22, API_X+NW//2, API_Y-22,
               lbl_above="REST")

# 8  FastAPI → Dashboard  (horizontal right, HTTP below)
straight_arrow(API_X+NW//2, API_Y+22, DASH_X-NW//2, DASH_Y+22,
               lbl_below="HTTP")

# 9  FastAPI → PostgreSQL  (vertical)
straight_arrow(API_X, API_Y+NH//2, PG_X, PG_Y-NH//2)

# ── Downsample & save ─────────────────────────────────────────────────────────
out = img.resize((W, H), Image.LANCZOS)
os.makedirs("docs", exist_ok=True)
out.save("docs/architecture.png", "PNG", optimize=True)
print(f"Saved docs/architecture.png  "
      f"({os.path.getsize('docs/architecture.png')//1024} KB)  {W}×{H}px")
