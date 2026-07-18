"""Render seat-result card dicts to PNGs via headless Chromium (Playwright).

Reuses the ElectionData.MY frontend's compiled Tailwind CSS so the cards match
the live modal exactly.
"""

from __future__ import annotations

import base64
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape
from playwright.sync_api import sync_playwright

from cards.seatcard_data import FRONTEND

HERE = Path(__file__).resolve().parent
DIST = FRONTEND / "dist"
BRAND_LOGO = FRONTEND / "public/static/logo/logo-default.png"

CARD_WIDTH = 430  # mobile modal width (< 640px sm breakpoint), matches screenshots


def _brand_logo_uri() -> str | None:
    if not BRAND_LOGO.exists():
        return None
    return "data:image/png;base64," + base64.b64encode(BRAND_LOGO.read_bytes()).decode()


def _load_css() -> str:
    files = sorted(DIST.glob("_astro/*.css"))
    if not files:
        raise FileNotFoundError(
            f"No compiled CSS in {DIST}. Build the frontend (pnpm build) first."
        )
    # Concatenate all built stylesheets (there is normally one).
    return "\n".join(f.read_text(encoding="utf-8") for f in files)


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader(str(HERE)),
        autoescape=select_autoescape(["html", "j2"]),
    )


def render_cards(cards: list[dict], out_dir: str | Path, scale: int = 2) -> list[Path]:
    """Render each card dict to ``out_dir/<slug>.png``. Returns written paths."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    css = _load_css()
    brand_logo = _brand_logo_uri()
    template = _env().get_template("card.html.j2")

    written: list[Path] = []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(
            viewport={"width": CARD_WIDTH, "height": 1400},
            device_scale_factor=scale,
        )
        for card in cards:
            html = template.render(card=card, css=css, width=CARD_WIDTH, brand_logo=brand_logo)
            page.set_content(html, wait_until="networkidle")
            path = out_dir / f"{card['slug']}.png"
            page.locator("#card").screenshot(path=str(path))
            written.append(path)
        browser.close()
    return written
