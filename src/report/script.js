(() => {
	const themeToggle = document.querySelector("[data-theme-toggle]");
	const themeToggleLabel = document.querySelector("[data-theme-toggle-label]");

	const setThemeToggleState = (theme) => {
		if (!themeToggle) return;
		const isDark = theme === "dark";
		themeToggle.setAttribute("aria-pressed", isDark ? "true" : "false");
		themeToggle.setAttribute(
			"aria-label",
			isDark ? "Switch to light theme" : "Switch to dark theme",
		);
		if (themeToggleLabel) {
			themeToggleLabel.textContent = isDark ? "Light" : "Dark";
		}
	};

	const currentTheme =
		document.documentElement.dataset.theme === "dark" ? "dark" : "light";
	setThemeToggleState(currentTheme);

	if (themeToggle) {
		themeToggle.addEventListener("click", () => {
			const nextTheme =
				document.documentElement.dataset.theme === "dark" ? "light" : "dark";
			document.documentElement.dataset.theme = nextTheme;
			try {
				localStorage.setItem("langrank-theme", nextTheme);
			} catch (_) {
				// Хранилище может быть недоступно при строгих настройках браузера.
			}
			setThemeToggleState(nextTheme);
		});
	}

	const wrap = document.querySelector(".table-wrap");
	if (!wrap) return;

	const table = wrap.querySelector("table");
	if (!table) return;

	const tbody = table.querySelector("tbody");
	if (!tbody) return;

	const rows = Array.from(tbody.querySelectorAll("tr"));
	rows.forEach((row, index) => {
		row.dataset.index = String(index);
	});

	const headers = Array.from(table.querySelectorAll("thead th[data-sort]"));

	const updateStickyOffsets = () => {
		const headRow = table.querySelector("thead tr");
		if (!headRow || headRow.children.length < 2) return;
		const firstWidth = headRow.children[0].getBoundingClientRect().width;
		const secondWidth = headRow.children[1].getBoundingClientRect().width;
		if (!Number.isFinite(firstWidth) || !Number.isFinite(secondWidth)) return;
		wrap.style.setProperty("--sticky-col-2-left", `${firstWidth}px`);
		wrap.style.setProperty(
			"--sticky-cols-width",
			`${firstWidth + secondWidth}px`,
		);
	};

	const setToggleState = (button, isOn) => {
		button.classList.toggle("is-on", isOn);
		button.setAttribute("aria-pressed", isOn ? "true" : "false");
	};

	const toggles = Array.from(
		document.querySelectorAll(".table-controls [data-group]"),
	);
	toggles.forEach((button) => {
		const group = button.dataset.group;
		if (!group) return;
		const className = `show-${group}`;
		setToggleState(button, wrap.classList.contains(className));
		button.addEventListener("click", () => {
			const isOn = !wrap.classList.contains(className);
			wrap.classList.toggle(className, isOn);
			setToggleState(button, isOn);
			updateStickyOffsets();
		});
	});

	const parseNumber = (value) => {
		const cleaned = value.replace(/[%\s,]/g, "");
		if (!cleaned || cleaned === "-") return Number.NaN;
		const num = Number(cleaned);
		return Number.isFinite(num) ? num : Number.NaN;
	};

	const compareNumbers = (aVal, bVal, dir) => {
		const aInvalid = Number.isNaN(aVal);
		const bInvalid = Number.isNaN(bVal);
		if (aInvalid && bInvalid) return 0;
		if (aInvalid) return 1;
		if (bInvalid) return -1;
		return dir === "asc" ? aVal - bVal : bVal - aVal;
	};

	const compareText = (aVal, bVal, dir) => {
		const cmp = aVal.localeCompare(bVal, undefined, {
			numeric: true,
			sensitivity: "base",
		});
		return dir === "asc" ? cmp : -cmp;
	};

	const setActive = (activeTh, dir) => {
		headers.forEach((th) => {
			th.classList.remove("is-active", "is-asc", "is-desc");
			th.setAttribute("aria-sort", "none");
		});
		activeTh.classList.add("is-active");
		activeTh.classList.add(dir === "asc" ? "is-asc" : "is-desc");
		activeTh.setAttribute(
			"aria-sort",
			dir === "asc" ? "ascending" : "descending",
		);
	};

	const getCellText = (row, index) => {
		const cell = row.children[index];
		if (!cell) return "";
		return cell.textContent.trim();
	};

	if (headers.length > 0) {
		headers.forEach((th, index) => {
			const button = th.querySelector("button.sort-button");
			if (!button) return;
			button.addEventListener("click", () => {
				const sortType = th.dataset.sort;
				let dir = table.dataset.sortDir === "asc" ? "desc" : "asc";
				if (table.dataset.sortIndex !== String(index)) {
					dir = "asc";
				}
				if (sortType === "index") {
					dir = "asc";
				}

				table.dataset.sortIndex = String(index);
				table.dataset.sortDir = dir;
				setActive(th, dir);

				const sorted = rows.slice().sort((a, b) => {
					const aIndex = Number(a.dataset.index);
					const bIndex = Number(b.dataset.index);
					if (sortType === "index") {
						return aIndex - bIndex;
					}

					const aText = getCellText(a, index);
					const bText = getCellText(b, index);

					let cmp = 0;
					if (sortType === "num") {
						const aVal = parseNumber(aText);
						const bVal = parseNumber(bText);
						cmp = compareNumbers(aVal, bVal, dir);
					} else {
						cmp = compareText(aText, bText, dir);
					}

					if (cmp !== 0) return cmp;
					return aIndex - bIndex;
				});

				const fragment = document.createDocumentFragment();
				sorted.forEach((row) => {
					fragment.appendChild(row);
				});
				tbody.appendChild(fragment);
			});
		});
	}

	window.addEventListener("resize", () => {
		updateStickyOffsets();
	});

	updateStickyOffsets();
})();
