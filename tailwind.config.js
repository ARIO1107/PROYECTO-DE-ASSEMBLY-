/**
 * Configuración de Tailwind v3 para BotMeter.
 *
 * Traducido tal cual del config que vivía inline en templates/index.html
 * cuando se usaba el Play CDN (`tailwind.config = {...}`), para que la
 * salida CSS sea idéntica a la anterior.
 *
 * El CSS resultante (static/tailwind.css) se commitea al repositorio:
 * Render solo ejecuta `pip install` en el deploy (ver render.yaml),
 * así que el build nunca corre en producción. Al cambiar clases en
 * templates/index.html, recompilar con `npm run build`.
 */
module.exports = {
  content: ['./templates/**/*.html'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['"Plus Jakarta Sans"', 'Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        display: ['"Outfit"', '"Plus Jakarta Sans"', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'monospace']
      },
      colors: {
        surface: { 50: '#fafaf9', 100: '#f5f5f4', 200: '#e7e5e4', 300: '#d6d3d1' },
        brand: {
          50: '#fff7ed', 100: '#ffedd5', 200: '#fed7aa', 300: '#fdba74',
          400: '#fb923c', 500: '#ff5c35', 600: '#ea580c', 700: '#c2410c',
          800: '#9a3412', 900: '#7c2d12', 950: '#431407'
        }
      }
    }
  }
};
