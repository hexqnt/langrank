(() => {
	try {
		const stored = localStorage.getItem("langrank-theme");
		const prefersDark = window.matchMedia(
			"(prefers-color-scheme: dark)",
		).matches;
		const theme =
			stored === "dark" || stored === "light"
				? stored
				: prefersDark
					? "dark"
					: "light";
		document.documentElement.dataset.theme = theme;
	} catch (_) {
		document.documentElement.dataset.theme = "light";
	}
})();
