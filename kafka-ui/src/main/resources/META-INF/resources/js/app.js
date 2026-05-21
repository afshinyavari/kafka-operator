// KafkaUI client glue — htmx + Bootstrap integration.
// Loaded with `defer` so htmx and Bootstrap are already parsed when this runs.

(function () {
    // Use template fragments so htmx can swap <tr>/<td> fragments inside tables.
    if (window.htmx) {
        htmx.config.useTemplateFragments = true;
    }

    // Show every newly-swapped Bootstrap toast (top-level swap or OOB).
    document.body.addEventListener('htmx:afterSwap', function (ev) {
        const root = ev.detail.target || document;
        const candidates = [];
        if (root.classList && root.classList.contains('toast')) candidates.push(root);
        candidates.push(...(root.querySelectorAll ? root.querySelectorAll('.toast:not(.show)') : []));
        // OOB swaps land directly in #toast-container; scan it explicitly.
        const tc = document.getElementById('toast-container');
        if (tc) candidates.push(...tc.querySelectorAll('.toast:not(.show)'));
        candidates.forEach(function (el) {
            const existing = bootstrap.Toast.getInstance(el);
            if (existing) return;
            new bootstrap.Toast(el).show();
        });
    });

    // HX-Trigger: closeModal — dismiss any visible Bootstrap modal.
    document.body.addEventListener('closeModal', function () {
        document.querySelectorAll('.modal.show').forEach(function (m) {
            const inst = bootstrap.Modal.getInstance(m);
            if (inst) inst.hide();
        });
    });

    // Session expired mid-request: reload so Quarkus OIDC kicks in and redirects.
    document.body.addEventListener('htmx:responseError', function (ev) {
        if (ev.detail.xhr && ev.detail.xhr.status === 401) {
            window.location.reload();
        }
    });
})();
