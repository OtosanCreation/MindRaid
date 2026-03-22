"""
generate_image.py
Hyperliquid の Funding Rate データからダッシュボード画像を生成する。
出力: images/dashboard.png  (Twitter 投稿用 1200×675)
"""

import os
from datetime import datetime, timezone

from PIL import Image, ImageDraw, ImageFont
from hyperliquid.info import Info

# ── 設定 ──────────────────────────────────────────────────
IMG_W, IMG_H = 1200, 675
OUT_DIR = os.path.join(os.path.dirname(__file__), "images")
OUT_PATH = os.path.join(OUT_DIR, "dashboard.png")

# フォントパス（macOS → Linux 優先順）
_FONT_CANDIDATES_EN = [
    "/System/Library/Fonts/HelveticaNeue.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]
_FONT_CANDIDATES_JP = [
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansCJKjp-Regular.otf",
]

def _first_existing(paths):
    for p in paths:
        if os.path.exists(p):
            return p
    return None

FONT_PATH_EN = _first_existing(_FONT_CANDIDATES_EN)
FONT_PATH_JP = _first_existing(_FONT_CANDIDATES_JP)

# カラーパレット
BG       = "#0a0a0a"
PANEL    = "#111111"
BORDER   = "#1e1e1e"
ORANGE   = "#f7a324"
GREEN    = "#00e676"
RED      = "#ff4444"
WHITE    = "#f0f0f0"
GRAY     = "#555555"
GRAY2    = "#888888"
YELLOW   = "#ffd700"

TAKER_RT = 0.00035 * 2
MAKER_RT = 0.00010 * 2
TOP_N    = 10
# ──────────────────────────────────────────────────────────


def load_fonts():
    def ttf_en(size, index=0):
        if FONT_PATH_EN:
            try:
                return ImageFont.truetype(FONT_PATH_EN, size, index=index)
            except Exception:
                pass
        return ImageFont.load_default()

    def ttf_jp(size):
        path = FONT_PATH_JP or FONT_PATH_EN
        if path:
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                pass
        return ImageFont.load_default()

    return {
        "title":   ttf_en(38),
        "section": ttf_jp(17),
        "header":  ttf_jp(14),
        "body":    ttf_en(14),
        "body_jp": ttf_jp(13),
        "small":   ttf_jp(12),
        "tag":     ttf_jp(11),
    }


def fetch_data():
    info = Info(skip_ws=True)
    raw = info.post("/info", {"type": "predictedFundings"})
    rows = []
    for item in raw:
        coin = item[0]
        for venue_name, data in item[1]:
            if venue_name == "HlPerp":
                rate = float(data["fundingRate"])
                interval = int(data["fundingIntervalHours"])
                rate_1h = rate / interval
                rows.append({
                    "coin": coin,
                    "rate_1h": rate_1h,
                    "rate_8h": rate_1h * 8,
                })
                break
    return rows


def pct_str(v, digits=4):
    sign = "+" if v >= 0 else ""
    return f"{sign}{v * 100:.{digits}f}%"


def classify(rate_1h):
    if abs(rate_1h) > TAKER_RT:
        return "TAKER"
    if abs(rate_1h) > MAKER_RT:
        return "MAKER"
    return ""


def draw_rounded_rect(draw, xy, radius, fill=None, outline=None, width=1):
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle([x1, y1, x2, y2], radius=radius, fill=fill,
                           outline=outline, width=width)


def draw_tag(draw, x, y, text, fonts, bg, fg):
    font = fonts["tag"]
    bbox = font.getbbox(text)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    pad_x, pad_y = 6, 3
    draw.rounded_rectangle(
        [x, y, x + tw + pad_x * 2, y + th + pad_y * 2],
        radius=4, fill=bg
    )
    draw.text((x + pad_x, y + pad_y), text, font=font, fill=fg)
    return tw + pad_x * 2


def render_table(draw, fonts, x, y, w, rows, is_long, total_coins):
    """片側テーブルを描画。is_long=True → ロング受取（rate負）"""
    color = GREEN if is_long else RED
    title = "LONG 受取 TOP10  (FR < 0)" if is_long else "SHORT 受取 TOP10  (FR > 0)"

    # セクションヘッダー
    draw.text((x, y), title, font=fonts["section"], fill=color)
    y += 26

    # 列ヘッダー
    col_coin = x
    col_rate = x + 80
    col_8h   = x + 170
    col_tag  = x + 250

    header_color = GRAY2
    draw.text((col_coin, y), "銘柄",   font=fonts["header"], fill=header_color)
    draw.text((col_rate, y), "1h",     font=fonts["header"], fill=header_color)
    draw.text((col_8h,   y), "8h",     font=fonts["header"], fill=header_color)
    y += 20

    # 区切り線
    draw.line([(x, y), (x + w - 10, y)], fill=BORDER, width=1)
    y += 8

    row_h = 28
    for i, r in enumerate(rows[:TOP_N]):
        rate = r["rate_1h"]

        # 行背景（偶数行）
        if i % 2 == 0:
            draw.rectangle([x - 4, y - 2, x + w - 14, y + row_h - 6],
                           fill="#131313")

        # ランク
        rank_color = YELLOW if i < 3 else GRAY2
        draw.text((col_coin - 20, y), f"{i+1}", font=fonts["small"], fill=rank_color)

        # 銘柄
        draw.text((col_coin, y), r["coin"], font=fonts["body"], fill=WHITE)

        # 1h rate
        draw.text((col_rate, y), pct_str(rate), font=fonts["body"], fill=color)

        # 8h rate
        draw.text((col_8h, y), pct_str(r["rate_8h"], digits=3),
                  font=fonts["body"], fill=color)

        # タグ
        tag = classify(rate)
        if tag == "TAKER":
            draw_tag(draw, col_tag, y, "TAKER", fonts, "#3a1a00", ORANGE)
        elif tag == "MAKER":
            draw_tag(draw, col_tag, y, "maker", fonts, "#1a2a1a", "#88cc88")

        y += row_h

    return y


def generate(rows, now_str):
    os.makedirs(OUT_DIR, exist_ok=True)
    fonts = load_fonts()

    img = Image.new("RGB", (IMG_W, IMG_H), BG)
    draw = ImageDraw.Draw(img)

    # ── ヘッダー ────────────────────────────────────────
    header_h = 80
    draw.rectangle([0, 0, IMG_W, header_h], fill="#0f0f0f")
    draw.line([(0, header_h), (IMG_W, header_h)], fill=ORANGE, width=2)

    # ロゴ・タイトル
    draw.text((24, 12), "⚡", font=fonts["title"], fill=ORANGE)
    draw.text((70, 14), "MindRaid", font=fonts["title"], fill=WHITE)

    subtitle = "Hyperliquid Funding Rate Dashboard"
    draw.text((70, 52), subtitle, font=fonts["body"], fill=GRAY2)

    # タイムスタンプ（右寄せ）
    ts_font = fonts["body"]
    ts_text = f"{now_str} UTC"
    ts_bbox = ts_font.getbbox(ts_text)
    ts_w = ts_bbox[2] - ts_bbox[0]
    draw.text((IMG_W - ts_w - 24, 28), ts_text, font=ts_font, fill=GRAY2)

    # アカウント名
    acct = "@logiQ_Alpha"
    ac_bbox = ts_font.getbbox(acct)
    ac_w = ac_bbox[2] - ac_bbox[0]
    draw.text((IMG_W - ac_w - 24, 48), acct, font=ts_font, fill=ORANGE)

    # ── 統計バー ─────────────────────────────────────────
    stat_y = header_h + 14
    total = len(rows)
    taker_n = sum(1 for r in rows if abs(r["rate_1h"]) > TAKER_RT)
    maker_n = sum(1 for r in rows if abs(r["rate_1h"]) > MAKER_RT)

    stats = [
        ("対象銘柄", f"{total}"),
        ("TAKER超え", f"{taker_n}銘柄"),
        ("MAKER超え", f"{maker_n}銘柄"),
        ("往復Taker手数料", "0.070%"),
        ("往復Maker手数料", "0.020%"),
    ]

    stat_x = 24
    for label, val in stats:
        draw_rounded_rect(draw, [stat_x, stat_y, stat_x + 160, stat_y + 40],
                          radius=6, fill=PANEL, outline=BORDER, width=1)
        draw.text((stat_x + 8, stat_y + 4), label, font=fonts["tag"], fill=GRAY2)
        val_color = ORANGE if label in ("TAKER超え", "MAKER超え") else WHITE
        draw.text((stat_x + 8, stat_y + 20), val, font=fonts["section"], fill=val_color)
        stat_x += 172

    # ── メインテーブル ───────────────────────────────────
    table_y = stat_y + 60
    pad_l = 44
    col_w = (IMG_W - pad_l * 2 - 20) // 2

    long_rows  = sorted(rows, key=lambda r: r["rate_1h"])[:TOP_N]
    short_rows = sorted(rows, key=lambda r: r["rate_1h"], reverse=True)[:TOP_N]

    # 左パネル（ロング受取）
    draw_rounded_rect(draw,
                      [pad_l - 8, table_y - 8,
                       pad_l + col_w, IMG_H - 50],
                      radius=8, fill=PANEL, outline=BORDER, width=1)
    render_table(draw, fonts, pad_l + 20, table_y + 8, col_w, long_rows, True, total)

    # 右パネル（ショート受取）
    right_x = pad_l + col_w + 20
    draw_rounded_rect(draw,
                      [right_x - 8, table_y - 8,
                       right_x + col_w, IMG_H - 50],
                      radius=8, fill=PANEL, outline=BORDER, width=1)
    render_table(draw, fonts, right_x + 20, table_y + 8, col_w, short_rows, False, total)

    # ── フッター ─────────────────────────────────────────
    footer_y = IMG_H - 44
    draw.line([(0, footer_y), (IMG_W, footer_y)], fill=BORDER, width=1)
    draw.text((24, footer_y + 10),
              "© MindRaid  |  非エンジニアが Claude Code で作るアービトラージシステム",
              font=fonts["small"], fill=GRAY)
    notice = "※ 投資助言ではありません"
    nb = fonts["small"].getbbox(notice)
    draw.text((IMG_W - (nb[2] - nb[0]) - 24, footer_y + 10),
              notice, font=fonts["small"], fill=GRAY)

    img.save(OUT_PATH, "PNG")
    print(f"✅  Saved: {OUT_PATH}")
    return OUT_PATH


def main():
    print("Fetching Hyperliquid funding rates …")
    rows = fetch_data()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    path = generate(rows, now_str)
    print(f"   {len(rows)} coins  →  {path}")


if __name__ == "__main__":
    main()
