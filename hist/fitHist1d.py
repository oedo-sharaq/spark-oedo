import ipywidgets as widgets
from IPython.display import display
from matplotlib import pyplot as plt

from scipy.optimize import curve_fit
from scipy.optimize import minimize
import numpy as np

def gauss_plus_linear(x, A, mu, sigma, m, b):
    return A * np.exp(-0.5 * ((x - mu) / sigma)**2) + m * x + b

def FitHist1DGauss():
    """
    Add widgets to fit Gaussian+linear to a bar histogram.

    - Red fit line
    - Slider min/max & step fixed to full histogram range (good resolution even after zoom).
    - slider.value follows current x view when you zoom/pan.
    - Fit methods:
        * "chi2": weighted least squares, sigma = sqrt(N)
        * "poisson_ll": Poisson log-likelihood
    """
    fig = plt.gcf()
    ax = fig.gca()
    bars = ax.patches

    # Get bin centers and counts from bars
    xs = np.array([bar.get_x() + bar.get_width()/2 for bar in bars])
    ys = np.array([bar.get_height() for bar in bars])

    x_min_all = xs.min()
    x_max_all = xs.max()

    # Rough initial guesses
    A0 = ys.max()
    mu0 = xs[ys.argmax()]
    sigma0 = (x_max_all - x_min_all) / 6 if xs.size > 1 else 1.0
    m0 = (ys[-1] - ys[0]) / (xs[-1] - xs[0]) if xs[-1] != xs[0] else 0.0
    b0 = (ys[0] + ys[-1]) / 2
    p0 = np.array([A0, mu0, sigma0, m0, b0], dtype=float)

    # Slider: min/max fixed to full range, fine step from full range
    slider = widgets.FloatRangeSlider(
        value=[x_min_all, x_max_all],
        min=x_min_all,
        max=x_max_all,
        step=(x_max_all - x_min_all) / 500 if xs.size > 1 else 1.0,
        description="Fit range:",
        continuous_update=True,
        readout=True,
        layout=widgets.Layout(width="50%"),
    )

    # Select χ² or Poisson LL
    method_sel = widgets.ToggleButtons(
        options=[("χ²", "chi2"), ("Poisson LL", "poisson_ll")],
        value="chi2",
        description="Method:",
    )

    # Visual vertical lines for current fit range
    vmin_line = ax.axvline(slider.value[0], linestyle="--")
    vmax_line = ax.axvline(slider.value[1], linestyle="--")

    def _on_slider_change(change):
        x_min, x_max = change["new"]
        vmin_line.set_xdata([x_min, x_min])
        vmax_line.set_xdata([x_max, x_max])
        new_step = (x_max - x_min) / 500 if xs.size > 1 else 1.0
        slider.step = new_step
        fig.canvas.draw_idle()


    slider.observe(_on_slider_change, names="value")

    # Fit line (red)
    (fit_line,) = ax.plot([], [], color="red", linewidth=2)

    # Button + output
    button = widgets.Button(description="Fit")
    out = widgets.Output()

    def do_chi2_fit(xfit, yfit):
        """Weighted χ² fit with sigma = sqrt(N); N<=0 bins excluded."""
        mask = yfit > 0
        xw = xfit[mask]
        yw = yfit[mask]
        if xw.size < 5:
            raise RuntimeError("Not enough non-zero bins for χ² fit.")
        sigma = np.sqrt(yw)

        A0 = yfit.max()
        mu0 = xfit[yfit.argmax()]
        sigma0 = (xfit[-1] - xfit[0]) / 6 if xfit.size > 1 else 1.0
        m0 = (yfit[-1] - yfit[0]) / (xfit[-1] - xfit[0]) if xfit[-1] != xfit[0] else 0.0
        b0 = (yfit[0] + yfit[-1]) / 2
        p0 = np.array([A0, mu0, sigma0, m0, b0], dtype=float)
        popt, pcov = curve_fit(
            gauss_plus_linear,
            xw,
            yw,
            p0=p0,
            sigma=sigma,
            absolute_sigma=True,
            maxfev=10000,
            bounds=([0, xw[0], 0, -np.inf, -np.inf],   # lower bounds
            [np.inf, xw[-1], np.inf, np.inf, np.inf])  # upper bounds
        )
        perr = np.sqrt(np.diag(pcov))
        return popt, perr

    def do_poisson_ll_fit(xfit, yfit):
        """Poisson log-likelihood fit: minimize NLL = sum(model - N log model)."""
        def nll(params):
            A, mu, sigma, m, b = params
            model = gauss_plus_linear(xfit, A, mu, sigma, m, b)
            model = np.clip(model, 1e-12, None)  # avoid log(0)
            return np.sum(model - yfit * np.log(model))

        res = minimize(nll, p0, method="L-BFGS-B")
        if not res.success:
            raise RuntimeError(f"Poisson LL fit failed: {res.message}")

        popt = res.x

        # Approximate errors from inverse Hessian if available
        perr = np.full_like(popt, np.nan, dtype=float)
        try:
            Hinv = res.hess_inv
            if hasattr(Hinv, "todense"):
                Hinv = Hinv.todense()
            Hinv = np.asarray(Hinv)
            perr = np.sqrt(np.diag(Hinv))
        except Exception:
            pass

        return popt, perr

    def on_click(_):
        with out:
            out.clear_output()

            x_min, x_max = slider.value
            # Clamp to data range
            x_min = max(x_min_all, x_min)
            x_max = min(x_max_all, x_max)

            mask = (xs >= x_min) & (xs <= x_max)
            xfit = xs[mask]
            yfit = ys[mask]

            if xfit.size < 5:
                print("Not enough points in selected range to fit.")
                return

            try:
                if method_sel.value == "chi2":
                    popt, perr = do_chi2_fit(xfit, yfit)
                    method_name = "χ² (σ = √N)"
                else:
                    popt, perr = do_poisson_ll_fit(xfit, yfit)
                    method_name = "Poisson log-likelihood"
            except RuntimeError as e:
                print(e)
                return

            # Plot fit curve over selected slider range
            xx = np.linspace(x_min, x_max, 500)
            yy = gauss_plus_linear(xx, *popt)
            fit_line.set_data(xx, yy)
            ax.relim()
            ax.autoscale_view()
            fig.canvas.draw_idle()

            A, mu, sigma, m, b = popt
            dA, dmu, dsigma, dm, db = perr

            print(f"Fit result ({method_name}):")
            print(f"  A     = {A:.6g} ± {dA:.3g}")
            print(f"  mu    = {mu:.6g} ± {dmu:.3g}")
            print(f"  sigma = {sigma:.6g} ± {dsigma:.3g}")
            print(f"  m     = {m:.6g} ± {dm:.3g}")
            print(f"  b     = {b:.6g} ± {db:.3g}")
            if np.any(np.isnan(perr)) and method_sel.value == "poisson_ll":
                print("  (errors from Poisson LL are approximate; Hessian may be poor)")

    button.on_click(on_click)

    # When you zoom/pan, update only slider.value (not min/max)
    def on_xlim_change(ax_):
        xmin, xmax = ax_.get_xlim()
        # Clamped to data range for safety
        xmin = max(x_min_all, xmin)
        xmax = min(x_max_all, xmax)
        # Update slider boundaries but DO NOT change slider.step
        slider.min = xmin
        slider.max = xmax
        # Move selected range to match visible region
        slider.value = (xmin, xmax)

    ax.callbacks.connect("xlim_changed", on_xlim_change)

    xscale = widgets.ToggleButtons(
        options=["linear", "log"],
        description="X scale:"
    )

    yscale = widgets.ToggleButtons(
        options=["linear", "log"],
        description="Y scale:"
    )

    def update(xscale, yscale):
        ax.set_xscale(xscale)
        ax.set_yscale(yscale)
        fig.canvas.draw_idle()

    hbox1 = widgets.HBox([xscale, yscale])
    hbox2 = widgets.HBox([slider])
    hbox3 = widgets.HBox([method_sel, button])
    ui = widgets.VBox([hbox1, hbox2, hbox3])
    out = widgets.interactive_output(update, {"xscale": xscale, "yscale": yscale})

    display(ui, out)