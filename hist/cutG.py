"""Interactive polygon cut generator for Spark DataFrames.
Provides a GUI to define polygonal cuts on 2D histograms plotted with matplotlib.
Generates Spark SQL expressions for point-in-polygon tests.

Usage:
1. Create a 2D histogram plot with matplotlib.
2. Import and use createCutG to attach the polygon cut GUI:
```python
from hist.cutG import createCutG
from hist.sparkHist2d import Hist2D
%matplotlib widget
d = Hist2D(dataFrame, colName=["varX", "varY"], nbins=[50, 50], range=[[xmin, xmax], [ymin, ymax]])
gui = createCutG(d.fig)
```
3. Click on the plot to define polygon vertices.
4. Click "Create cut" to finalize the polygon and generate the SQL expression.
Right click to remove the last point, and use "Clear" to start over.
5. Retrieve the SQL expression with gui.getSql() method.
"""
import matplotlib.pyplot as plt
from matplotlib.widgets import Button, TextBox
import numpy as np
from IPython.display import display, Markdown

# =========================
# Geometry utilities
# =========================

def polygon_gate_sql(vertices, x_col, y_col):
    """
    Generate Spark SQL expression for point-in-polygon test.

    vertices     : list of (x, y) tuples, ordered (CW or CCW)
    x_col, y_col : column names (strings)

    Returns: SQL boolean expression string
    """
    n = len(vertices)
    terms = []

    for i in range(n):
        x1, y1 = vertices[i]
        x2, y2 = vertices[(i + 1) % n]

        # skip horizontal edges
        if y1 == y2:
            continue

        cond = (
            f"(({y1} > {y_col}) != ({y2} > {y_col})) "
            f"AND "
            f"({x_col} < "
            f"({x1} + ({y_col} - {y1}) * ({x2} - {x1}) / ({y2} - {y1}))"
            f")"
        )

        terms.append(f"(CASE WHEN {cond} THEN 1 ELSE 0 END)")

    if not terms:
        raise ValueError("Polygon has no valid edges")

    return f"(({ ' + '.join(terms) }) % 2 = 1)"

# =========================
# Interactive cut class
# =========================

class PolygonCutGUI:
    def __init__(self, fig=None):
        self.fig = fig if fig is not None else plt.gcf()
        self.ax = self.fig.axes[0]

        self.points = []
        self.line = None
        self.vline = None
        self.closed = False
        self.sql = ""

        # infer Spark column names from axis labels
        self.xcol = self.ax.get_xlabel().strip()
        self.ycol = self.ax.get_ylabel().strip()

        if not self.xcol or not self.ycol:
            raise ValueError("Axis labels must be set (used as Spark column names).")

        self.cid = self.fig.canvas.mpl_connect(
            "button_press_event", self.onclick
        )

        self._add_button()

    # -------------------------

    def onclick(self, event):
        if self.closed:
            return
        if event.inaxes != self.ax:
            return
        if event.button != 1:
            if len(self.points) > 0:
                self.points.remove((self.points[-1]))
            self._update_plot()
            return

        self.points.append((event.xdata, event.ydata))
        self._update_plot()

    # -------------------------

    def _update_plot(self):
        xs, ys = zip(*self.points)

        if self.line is None:
            self.line, = self.ax.plot(xs, ys, "ro-", lw=1.5)
        else:
            self.line.set_data(xs, ys)

        self.fig.canvas.draw_idle()

    # -------------------------

    def _add_button(self):
        ax_btn = self.fig.add_axes([0.79, 0.0, 0.20, 0.07])
        self.btn = Button(ax_btn, "Create cut")
        self.btn.on_clicked(self.finalize)
        ax_btn2 = self.fig.add_axes([0.59, 0.0, 0.20, 0.07])
        self.btn2 = Button(ax_btn2, "Clear")
        self.btn2.on_clicked(self.clear)

    # -------------------------

    def clear(self, event=None):
        self.points = []
        self.closed = False
        self.sql = ""
        if self.line is not None:
            self.line.remove()
            self.line = None
        if self.vline is not None:
            self.vline.remove()
            self.vline = None
        self._update_plot()

    def finalize(self, event=None):
        if len(self.points) < 3:
            print("Need at least 3 points.")
            return

        self.closed = True

        # close polygon visually
        pts = self.points + [self.points[0]]
        xs, ys = zip(*pts)
        self.vline, = self.ax.plot(xs, ys, "r-", lw=2)
        self.fig.canvas.draw_idle()

        self.sql = polygon_gate_sql(
            self.points,
            self.xcol,
            self.ycol,
        )

    def getSql(self) -> str:
        """
        Returns the generated Spark SQL cut expression.
        """
        return self.sql

def createCutG(fig: plt.Figure = None) -> PolygonCutGUI:
    """
    Attach a PolygonCutGUI to the given figure (or current figure if None).
    Returns the PolygonCutGUI instance.
    """
    if fig is None:
        fig = plt.gcf()
    return PolygonCutGUI(fig)

