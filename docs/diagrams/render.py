"""Render a diagram SVG to a 2x PNG for the docs.

The SVGs in this directory are the editable sources; the PNGs under
docs/images/ are what the Markdown embeds. After editing an SVG, re-render it:

    python -m venv /tmp/diagrams && /tmp/diagrams/bin/pip install playwright
    /tmp/diagrams/bin/playwright install chromium-headless-shell
    /tmp/diagrams/bin/python docs/diagrams/render.py \\
        docs/diagrams/architecture.svg docs/images/architecture.png

Playwright is deliberately not in requirements-dev.txt: it is only needed to
regenerate images, not to run or test the project. Fonts (Inter and JetBrains
Mono) are loaded from Google Fonts at render time, so rendering needs network
access.
"""

import pathlib
import re
import sys

from playwright.sync_api import sync_playwright

FONTS = (
    "https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700"
    "&family=JetBrains+Mono:wght@400;700&display=block"
)


def render(src: pathlib.Path, out: pathlib.Path, scale: int = 2) -> None:
    svg = src.read_text()
    width, height = map(int, re.search(r'viewBox="0 0 (\d+) (\d+)"', svg).groups())
    html = (
        f'<!doctype html><html><head><meta charset="utf-8">'
        f'<link href="{FONTS}" rel="stylesheet">'
        f"<style>html,body{{margin:0;background:#fff}} svg{{display:block}}</style>"
        f"</head><body>{svg}</body></html>"
    )
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(
            viewport={"width": width, "height": height}, device_scale_factor=scale
        )
        page.set_content(html)
        page.wait_for_load_state("networkidle")
        page.evaluate("document.fonts.ready")
        page.locator("svg").screenshot(path=str(out))
        browser.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: render.py <in.svg> <out.png>")
    render(pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]))
    print(f"wrote {sys.argv[2]}")
