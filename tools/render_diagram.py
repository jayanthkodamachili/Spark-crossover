"""Render the committed architecture SVG.

Hand written rather than produced by a diagramming library, for the reasons in ADR-004: a
committed SVG with intrinsic dimensions and an explicit light background renders the same
on GitHub in either theme, which a Mermaid fence does not reliably do.
"""

from __future__ import annotations

from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "docs" / "architecture.svg"

W, H = 960, 500
INK = "#1b1f23"
MUTED = "#57606a"
BAD = "#b42318"
GOOD = "#116329"
LINE = "#8c959f"


def box(x, y, w, h, title, subtitle="", stroke=MUTED, fill="#ffffff", bold=False):
    weight = "600" if bold else "400"
    parts = [
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="6" fill="{fill}" '
        f'stroke="{stroke}" stroke-width="1.5"/>',
        f'<text x="{x + w / 2}" y="{y + (20 if subtitle else h / 2 + 4)}" '
        f'text-anchor="middle" font-family="Helvetica,Arial,sans-serif" font-size="13" '
        f'font-weight="{weight}" fill="{INK}">{title}</text>',
    ]
    if subtitle:
        for index, line in enumerate(subtitle.split("|")):
            parts.append(
                f'<text x="{x + w / 2}" y="{y + 38 + index * 14}" text-anchor="middle" '
                f'font-family="Helvetica,Arial,sans-serif" font-size="11" '
                f'fill="{MUTED}">{line}</text>'
            )
    return "".join(parts)


def arrow(x1, y1, x2, y2, dashed=False):
    dash = ' stroke-dasharray="4 3"' if dashed else ""
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{LINE}" '
        f'stroke-width="1.5" marker-end="url(#a)"{dash}/>'
    )


def label(x, y, text, fill=MUTED, size=11, anchor="middle", weight="400"):
    return (
        f'<text x="{x}" y="{y}" text-anchor="{anchor}" '
        f'font-family="Helvetica,Arial,sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}">{text}</text>'
    )


def render() -> str:
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}" role="img" '
        f'aria-label="Architecture of spark-crossover">',
        '<defs><marker id="a" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" '
        f'markerHeight="6" orient="auto"><path d="M 0 0 L 10 5 L 0 10 z" fill="{LINE}"/>'
        "</marker></defs>",
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        label(W / 2, 26, "Four engines, one file, and a gate before any timing is read",
              INK, 14, weight="600"),
    ]

    parts.append(box(24, 150, 180, 70, "one Parquet file",
                     "written once,|read by all four", bold=True))

    engines = ["pandas", "polars", "duckdb", "spark"]
    for index, engine in enumerate(engines):
        y = 68 + index * 62
        parts.append(box(290, y, 190, 46, f"{engine}", "own process", stroke=MUTED))
        parts.append(arrow(204, 185, 290, y + 23))

    parts.append(label(385, 338, "setup timed separately from the work", MUTED, 10))

    parts.append(box(548, 120, 190, 74, "do the answers match?",
                     "counts exactly,|sums to a relative|tolerance",
                     stroke=GOOD, fill="#f2fbf4", bold=True))
    for index in range(len(engines)):
        parts.append(arrow(480, 91 + index * 62, 548, 157))

    parts.append(box(548, 232, 190, 56, "timings are read",
                     "medians, with spread", stroke=MUTED))
    parts.append(arrow(643, 194, 643, 232))
    parts.append(label(700, 218, "only if they match", GOOD, 10, anchor="start"))

    parts.append(box(790, 120, 146, 74, "refuse",
                     "exit 4: the engines|computed different|things",
                     stroke=BAD, fill="#fff5f4", bold=True))
    parts.append(arrow(738, 157, 790, 157))

    parts.append(box(790, 232, 146, 56, "a verdict",
                     "exit 0, 1 or 5", stroke=MUTED, bold=True))
    parts.append(arrow(738, 260, 790, 260))

    parts.append(
        box(130, 400, 700, 66,
            "a benchmark whose engines quietly disagree is worse than no benchmark",
            "it looks like evidence, so the comparison is gated on the answers before "
            "any timing is published", bold=True)
    )
    parts.append(arrow(863, 194, 700, 400))
    parts.append(arrow(643, 288, 480, 400))

    parts.append("</svg>")
    return "".join(parts)


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render() + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
