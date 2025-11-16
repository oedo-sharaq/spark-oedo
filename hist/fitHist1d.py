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

    Features:
      - Fit range slider (min/max = current x-limits, step = (x-width)/500).
      - χ² (σ = √N) or Poisson log-likelihood fit.
      - A >= 0, sigma > 0.
      - Initial guess updated from current slider range before each fit.
      - Checkboxes + textboxes to FIX A, mu, sigma, m, b to given values.
      - X/Y log/lin toggle.
    """
    fig = plt.gcf()
    ax = fig.gca()
    bars = ax.patches

    if not bars:
        print("No bars found on current axes.")
        return

    # Get bin centers and counts from bars
    xs = np.array([bar.get_x() + bar.get_width()/2 for bar in bars])
    ys = np.array([bar.get_height() for bar in bars])

    x_min_all = xs.min()
    x_max_all = xs.max()

    # Rough initial guesses (full-range, only for textbox defaults)
    A0 = ys.max()
    mu0 = xs[ys.argmax()]
    sigma0 = (x_max_all - x_min_all) / 6 if xs.size > 1 else 1.0
    m0 = (ys[-1] - ys[0]) / (xs[-1] - xs[0]) if xs[-1] != xs[0] else 0.0
    b0 = (ys[0] + ys[-1]) / 2

    # -------- Slider for fit range --------
    init_step = (x_max_all - x_min_all) / 500 if xs.size > 1 else 1.0
    slider = widgets.FloatRangeSlider(
        value=[x_min_all, x_max_all],
        min=x_min_all,
        max=x_max_all,
        step=init_step,
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
        fig.canvas.draw_idle()

    slider.observe(_on_slider_change, names="value")

    # -------- Parameter fixing controls --------
    param_names = ["A", "mu", "sigma", "m", "b"]
    init_vals   = [A0,   mu0,   sigma0,   m0,   b0]

    fix_checkboxes = []
    param_boxes = []
    rows = []
    for name in param_names:
        cb = widgets.Checkbox(value=False, description=f"fix {name}")
        box = widgets.FloatText(
            value=float(0.),
            description=name + ":",
            layout=widgets.Layout(width="160px"),
            style={"description_width": "40px"},
        )
        fix_checkboxes.append(cb)
        param_boxes.append(box)
        if name == "m" or name == "b":
            cb.value = True
        rows.append(widgets.HBox([cb, box]))
    params_ui = widgets.VBox(rows)

    # -------- Fit line (red) --------
    (fit_line,) = ax.plot([], [], color="red", linewidth=2)

    # Outputs
    fit_out = widgets.Output()
    button = widgets.Button(description="Fit")

    # Common helper: bounds for current region
    def make_bounds_for_region(xfit):
        if xfit.size == 0:
            return None, None
        lower_full = np.array([0.0, xfit[0], 1e-12, -np.inf, -np.inf])   # A>=0, sigma>0, mu in range
        upper_full = np.array([np.inf, xfit[-1], np.inf, np.inf, np.inf])
        return lower_full, upper_full

    # ---- χ² fit with optional fixed params ----
    def do_chi2_fit(xfit, yfit, full_p0, fixed_mask):
        mask = yfit > 0
        xw = xfit[mask]
        yw = yfit[mask]
        if xw.size < 5:
            raise RuntimeError("Not enough non-zero bins for χ² fit.")

        sigma = np.sqrt(yw)
        lower_full, upper_full = make_bounds_for_region(xw)
        if lower_full is None:
            raise RuntimeError("No data in selected range.")

        idx_free = np.where(~fixed_mask)[0]
        if idx_free.size == 0:
            # All fixed: no fit needed
            popt_full = full_p0.copy()
            perr_full = np.zeros_like(full_p0)
            return popt_full, perr_full

        def model_wrapped(x, *theta_free):
            theta = full_p0.copy()
            theta[idx_free] = theta_free
            return gauss_plus_linear(x, *theta)

        p0_free = full_p0[idx_free]
        lower_free = lower_full[idx_free]
        upper_free = upper_full[idx_free]

        popt_free, pcov = curve_fit(
            model_wrapped,
            xw,
            yw,
            p0=p0_free,
            sigma=sigma,
            absolute_sigma=True,
            maxfev=10000,
            bounds=(lower_free, upper_free),
        )

        popt_full = full_p0.copy()
        popt_full[idx_free] = popt_free

        perr_full = np.zeros_like(full_p0)
        perr_free = np.sqrt(np.diag(pcov))
        perr_full[idx_free] = perr_free

        return popt_full, perr_full

    # ---- Poisson LL fit with optional fixed params ----
    def do_poisson_ll_fit(xfit, yfit, full_p0, fixed_mask):
        lower_full, upper_full = make_bounds_for_region(xfit)
        if lower_full is None:
            raise RuntimeError("No data in selected range.")

        idx_free = np.where(~fixed_mask)[0]
        if idx_free.size == 0:
            popt_full = full_p0.copy()
            perr_full = np.zeros_like(full_p0)
            return popt_full, perr_full

        def nll_full(theta):
            A, mu, sigma, m, b = theta
            model = gauss_plus_linear(xfit, A, mu, sigma, m, b)
            model = np.clip(model, 1e-12, None)
            return np.sum(model - yfit * np.log(model))

        def nll_free(theta_free):
            theta = full_p0.copy()
            theta[idx_free] = theta_free
            # Enforce bounds manually to be safe
            theta = np.maximum(theta, lower_full)
            theta = np.minimum(theta, upper_full)
            return nll_full(theta)

        p0_free = full_p0[idx_free]
        bounds_all = list(zip(lower_full, upper_full))
        bounds_free = [bounds_all[i] for i in idx_free]

        res = minimize(nll_free, p0_free, method="L-BFGS-B", bounds=bounds_free)
        if not res.success:
            raise RuntimeError(f"Poisson LL fit failed: {res.message}")

        popt_free = res.x
        popt_full = full_p0.copy()
        popt_full[idx_free] = popt_free

        # Approximate covariance from inverse Hessian if available
        perr_full = np.full_like(full_p0, np.nan)
        try:
            Hinv = res.hess_inv
            if hasattr(Hinv, "todense"):
                Hinv = Hinv.todense()
            Hinv = np.asarray(Hinv)
            perr_free = np.sqrt(np.diag(Hinv))
            perr_full[idx_free] = perr_free
        except Exception:
            pass

        return popt_full, perr_full

    # ---- Fit button callback ----
    def on_click(_):
        with fit_out:
            fit_out.clear_output()

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

            # Initial guesses from current slider region (this is what you wanted)
            A_init = yfit.max() if yfit.size > 0 else 1.0
            mu_init = xfit[np.argmax(yfit)] if yfit.size > 0 else (x_min + x_max) / 2
            sigma_init = (x_max - x_min) / 6 if (x_max > x_min) else 1.0
            m_init = (yfit[-1] - yfit[0]) / (xfit[-1] - xfit[0]) if xfit.size > 1 and xfit[-1] != xfit[0] else 0.0
            b_init = (yfit[0] + yfit[-1]) / 2 if yfit.size > 1 else (yfit[0] if yfit.size > 0 else 0.0)

            full_p0 = np.array([A_init, mu_init, sigma_init, m_init, b_init], dtype=float)

            # Fixed/free mask from checkboxes
            fixed_mask = np.array([cb.value for cb in fix_checkboxes], dtype=bool)

            # For FIXED parameters: override initial guess with textbox value
            for i, (fixed, box) in enumerate(zip(fixed_mask, param_boxes)):
                if fixed:
                    full_p0[i] = float(box.value)
                # if not fixed: keep slider-based initial guess

            try:
                if method_sel.value == "chi2":
                    popt, perr = do_chi2_fit(xfit, yfit, full_p0, fixed_mask)
                    method_name = "χ² (σ = √N)"
                else:
                    popt, perr = do_poisson_ll_fit(xfit, yfit, full_p0, fixed_mask)
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

            labels = ["A", "mu", "sigma", "m", "b"]
            print(f"Fit result ({method_name}):")
            for name, val, err, fixed in zip(labels, popt, perr, fixed_mask):
                if fixed:
                    print(f"  {name:6s} = {val:10.6g}  (fixed)")
                else:
                    print(f"  {name:6s} = {val:10.6g} ± {err:6.3g}")
            if np.any(np.isnan(perr)) and method_sel.value == "poisson_ll":
                print("  (errors for Poisson LL are approximate; Hessian may be poor)")

    button.on_click(on_click)

    # When you zoom/pan, update slider min/max and step to match visible range
    def on_xlim_change(ax_):
        xmin, xmax = ax_.get_xlim()
        xmin = max(x_min_all, xmin)
        xmax = min(x_max_all, xmax)
        if xmax <= xmin:
            return
        width = xmax - xmin
        slider.min = xmin
        slider.max = xmax
        slider.step = width / 500.0 if width > 0 else slider.step
        slider.value = (xmin, xmax)

    ax.callbacks.connect("xlim_changed", on_xlim_change)

    # -------- X/Y scale toggles --------
    xscale = widgets.ToggleButtons(
        options=["linear", "log"],
        description="X scale:"
    )

    yscale = widgets.ToggleButtons(
        options=["linear", "log"],
        description="Y scale:"
    )

    def update_scale(xscale_val, yscale_val):
        ax.set_xscale(xscale_val)
        ax.set_yscale(yscale_val)
        fig.canvas.draw_idle()

    scale_out = widgets.interactive_output(update_scale, {"xscale_val": xscale, "yscale_val": yscale})

    # Layout
    hbox1 = widgets.HBox([xscale, yscale])
    hbox2 = widgets.HBox([slider])
    hbox3 = widgets.HBox([method_sel, button])
    ui = widgets.VBox([hbox1, hbox2, hbox3, params_ui])

    display(ui, scale_out, fit_out)