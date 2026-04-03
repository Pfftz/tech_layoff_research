import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib import rcParams

rcParams['font.family'] = 'DejaVu Serif'

data = {
    'Year': ['2020', '2022', '2022', '2022', '2023', '2020', '2022', '2023'],
    'Quarter': ['Q2', 'Q4', 'Q4', 'Q4', 'Q1', 'Q2', 'Q4', 'Q1'],
    'Industry': ['Transportation', 'Fintech', 'Retail', 'Transportation', 'Transportation',
                 'Transportation', 'Fintech', 'Retail'],
    'Region Scope': ['Indonesia', 'Indonesia', 'Indonesia', 'Indonesia', 'Indonesia',
                     'Global', 'Global', 'Global'],
    'Companies\nImpacted': ['1', '2', '3', '1', '1', '20', '34', '19'],
    'Total\nLayoffs': ['430', '137', '494', '1.300', '360', '12.424', '4.197', '20.234'],
}
df = pd.DataFrame(data)

# ── Figure ──────────────────────────────────────────────────────────────────
fig = plt.figure(figsize=(11, 4.6), dpi=300, facecolor='white')
ax = fig.add_axes([0, 0.08, 1, 0.82])   # leave room top + bottom for title/note
ax.set_xlim(0, 1)
ax.set_ylim(0, 1)
ax.axis('off')

# ── Layout constants ────────────────────────────────────────────────────────
n_rows = len(df)
n_cols = len(df.columns)
col_widths = [0.09, 0.09, 0.18, 0.16, 0.14, 0.14]   # must sum to ~0.80
col_starts = [0.02]
for w in col_widths[:-1]:
    col_starts.append(col_starts[-1] + w)

header_h   = 0.155
row_h      = 0.092
table_top  = 0.92

# Stripe colours
stripe_even = '#F4F4F4'
stripe_odd  = '#FFFFFF'
header_bg   = '#1A1A2E'   # dark navy
header_fg   = 'white'

# ── Draw rows ───────────────────────────────────────────────────────────────
def draw_cell(ax, x, y, w, h, text,
              bg='white', fg='black', bold=False,
              fontsize=8.5, valign='center', ha='center', wrap=True):
    rect = mpatches.FancyBboxPatch(
        (x, y), w, h,
        boxstyle="square,pad=0",
        linewidth=0,
        facecolor=bg, edgecolor='none',
        zorder=2
    )
    ax.add_patch(rect)
    weight = 'bold' if bold else 'normal'
    ax.text(x + w / 2, y + h / 2, text,
            ha=ha, va=valign,
            fontsize=fontsize, color=fg,
            fontweight=weight,
            wrap=wrap,
            zorder=3)

# Header row
y_top = table_top - header_h
for j, col in enumerate(df.columns):
    draw_cell(ax, col_starts[j], y_top,
              col_widths[j], header_h,
              col, bg=header_bg, fg=header_fg,
              bold=True, fontsize=8.2)

# Horizontal rule under header
ax.axhline(y=y_top, xmin=col_starts[0], xmax=col_starts[-1] + col_widths[-1],
           color='#1A1A2E', linewidth=1.4, zorder=4)

# Data rows
for i, (_, row) in enumerate(df.iterrows()):
    y = y_top - (i + 1) * row_h
    bg = stripe_even if i % 2 == 0 else stripe_odd

    # Indonesia rows get a subtle left accent
    is_indo = row['Region Scope'] == 'Indonesia'
    accent_color = '#3A86FF' if is_indo else '#FF6B6B'

    # Full-row background
    bg_rect = mpatches.FancyBboxPatch(
        (col_starts[0], y),
        sum(col_widths), row_h,
        boxstyle="square,pad=0",
        linewidth=0, facecolor=bg, zorder=1
    )
    ax.add_patch(bg_rect)

    # Accent bar on the left
    bar = mpatches.FancyBboxPatch(
        (col_starts[0], y), 0.004, row_h,
        boxstyle="square,pad=0",
        linewidth=0, facecolor=accent_color, zorder=2
    )
    ax.add_patch(bar)

    for j, val in enumerate(row):
        draw_cell(ax, col_starts[j], y,
                  col_widths[j], row_h,
                  str(val), bg='none', fg='#111111',
                  fontsize=8.2)

# Bottom rule
y_bottom = y_top - (n_rows) * row_h
ax.axhline(y=y_bottom, xmin=col_starts[0], xmax=col_starts[-1] + col_widths[-1],
           color='#1A1A2E', linewidth=1.0, zorder=4)

# ── Thin separator lines between rows ───────────────────────────────────────
for i in range(1, n_rows):
    yy = y_top - i * row_h
    ax.axhline(y=yy, xmin=col_starts[0], xmax=col_starts[-1] + col_widths[-1],
               color='#CCCCCC', linewidth=0.4, zorder=3)

# ── Legend (colour key) ──────────────────────────────────────────────────────
legend_x = col_starts[0]
legend_y = y_bottom - 0.055
for color, label in [('#3A86FF', 'Indonesia'), ('#FF6B6B', 'Global')]:
    dot = mpatches.Circle((legend_x + 0.009, legend_y + 0.013), 0.008,
                           color=color, zorder=5)
    ax.add_patch(dot)
    ax.text(legend_x + 0.024, legend_y + 0.013, label,
            va='center', fontsize=7.5, color='#444444')
    legend_x += 0.115

# ── Title & note ─────────────────────────────────────────────────────────────
fig.text(0.5, 0.975,
         'Tabel 1: Cuplikan Agregasi Komparasi Indonesia vs Global',
         ha='center', va='top',
         fontsize=11, fontweight='bold', color='#1A1A2E',
         fontfamily='DejaVu Sans')

fig.text(0.5, 0.04,
         'Sumber: Data agregasi internal.'
         'Total Layoffs dinyatakan dalam jumlah individu.',
         ha='center', va='bottom',
         fontsize=7, color='#777777', style='italic',
         fontfamily='DejaVu Serif')

plt.savefig('tabel_komparasi.png',
            dpi=300, bbox_inches='tight',
            facecolor='white', pad_inches=0.15)
print('Done.')