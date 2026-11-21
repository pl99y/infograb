export function createLightboxModule() {
  const lightbox = document.getElementById("lightbox");
  const lightboxImg = document.getElementById("lightbox-img");

  function open(src) {
    if (!lightbox || !lightboxImg || !src) return;
    lightboxImg.src = src;
    lightbox.style.display = "flex";
  }

  function close() {
    if (!lightbox || !lightboxImg) return;
    lightbox.style.display = "none";
    lightboxImg.src = "";
  }

  return {
    open,
    close,
    init() {
      lightbox?.addEventListener("click", (event) => {
        if (event.target === lightbox || event.target === lightboxImg) {
          close();
        }
      });

      document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
          close();
        }
      });
    },
  };
}
