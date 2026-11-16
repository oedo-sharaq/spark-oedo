# interactive_line.py
"""Interactive line generator for Spark DataFrames.
Provides a GUI to define a slope on 2D histograms plotted with matplotlib.

Usage:
1. Create a 2D histogram plot with matplotlib.
2. Import and use Slo to define a slope on the figure:
```python
from hist.interactive_line import Slo
from hist.sparkHist2d import Hist2D
%matplotlib widget
d = Hist2D(dataFrame, colName=["varX", "varY"], nbins=[50, 50], range=[[xmin, xmax], [ymin, ymax]])
tool = Slo(d.fig,init_points=[[x1, y1], [x2, y2]])
```
3. Click on the plot to define a first point.
4. Click again to define a second point, which will draw the line and show the slope and intercept in the title.
5. Click and drag the points to adjust the line interactively.
"""
import numpy as np
import matplotlib.pyplot as plt

class InteractiveLine:
    def __init__(self, fig=None, init_points=None):
        self.fig = fig if fig is not None else plt.gcf()

        if len(self.fig.axes) == 0:
            self.ax = self.fig.add_subplot(111)
        else:
            self.ax = self.fig.axes[0]

        # 点の初期化
        if init_points is None:
            self.points = []  # ← 空スタート
        else:
            self.points = list(map(list, init_points))

        # 描画
        self.point_plot, = self.ax.plot([], [], 'ro', picker=5)
        self.line_plot, = self.ax.plot([], [], 'b-')
        self.title = self.ax.set_title("Click 2 points")

        self.dragging_idx = None

        # イベント
        self.cid_click = self.fig.canvas.mpl_connect('button_press_event', self.on_click)
        self.cid_pick = self.fig.canvas.mpl_connect('pick_event', self.on_pick)
        self.cid_motion = self.fig.canvas.mpl_connect('motion_notify_event', self.on_motion)
        self.cid_release = self.fig.canvas.mpl_connect('button_release_event', self.on_release)

        self.update_plot()

    def update_plot(self):
        if len(self.points) > 0:
            pts = np.array(self.points)
            self.point_plot.set_data(pts[:,0], pts[:,1])
        else:
            self.point_plot.set_data([], [])

        if len(self.points) == 2:
            self.update_line()

        self.fig.canvas.draw_idle()

    def update_line(self):
        (x1, y1), (x2, y2) = self.points

        if x1 != x2:
            a = (y2 - y1) / (x2 - x1)
            b = y1 - a * x1

            x_vals = np.array(self.ax.get_xlim())
            y_vals = a * x_vals + b

            self.title.set_text(f"y = {a:.3f}x + {b:.3f}")
            self.slope = a
            self.intercept = b
        else:
            x_vals = [x1, x1]
            y_vals = self.ax.get_ylim()

            self.title.set_text("x = constant")
            self.slope = None
            self.intercept = None

        self.line_plot.set_data(x_vals, y_vals)

    # 👇 クリックで点を追加
    def on_click(self, event):
        if event.inaxes != self.ax:
            return

        # 2点まで
        if len(self.points) >= 2:
            self.points = []

        self.points.append([event.xdata, event.ydata])
        self.update_plot()

    # 👇 ドラッグ開始
    def on_pick(self, event):
        if event.artist != self.point_plot:
            return

        mouse = event.mouseevent
        x, y = mouse.xdata, mouse.ydata

        pts = np.array(self.points)
        dists = np.hypot(pts[:,0] - x, pts[:,1] - y)
        self.dragging_idx = int(np.argmin(dists))

    # 👇 ドラッグ中
    def on_motion(self, event):
        if self.dragging_idx is None or event.inaxes != self.ax:
            return

        self.points[self.dragging_idx] = [event.xdata, event.ydata]
        self.update_plot()

    def on_release(self, event):
        self.dragging_idx = None


def Slo(fig=None):
    if fig is None:
        fig = plt.gcf()
    return InteractiveLine(fig)
