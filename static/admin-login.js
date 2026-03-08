const formEl = document.getElementById("login-form");
const passInputEl = document.getElementById("pass-input");
const loginBtnEl = document.getElementById("login-btn");
const msgEl = document.getElementById("msg");

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  const password = String(passInputEl.value || "").trim();
  if (!password) {
    setMessage("Wpisz haslo.", true);
    return;
  }

  setLoading(true);
  setMessage("Logowanie...", false);

  try {
    const response = await fetch("/api/admin/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || "Logowanie nieudane.");
    }
    window.location.href = "/admin";
  } catch (error) {
    setMessage(error.message || "Logowanie nieudane.", true);
  } finally {
    setLoading(false);
  }
});

function setLoading(isLoading) {
  passInputEl.disabled = isLoading;
  loginBtnEl.disabled = isLoading;
  loginBtnEl.textContent = isLoading ? "Sprawdzam..." : "Wejdz";
}

function setMessage(text, isError) {
  msgEl.textContent = text;
  msgEl.classList.toggle("err", Boolean(isError));
}
