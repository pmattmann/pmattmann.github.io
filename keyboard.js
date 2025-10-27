let dotNetHelper = null;
let keydownHandler = null;

export function setupKeyboard(dotnetHelper) {
    dotNetHelper = dotnetHelper;

    // Event Handler für Tastatureingaben
    keydownHandler = (event) => {
        if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
            event.preventDefault(); // Verhindert Scrollen der Seite
            dotNetHelper.invokeMethodAsync('HandleKeyDown', event.key);
        }
    };

    // Füge Event Listener zum Document hinzu
    document.addEventListener('keydown', keydownHandler);
}

export function cleanupKeyboard() {
    if (keydownHandler) {
        document.removeEventListener('keydown', keydownHandler);
        keydownHandler = null;
    }
    dotNetHelper = null;
}
