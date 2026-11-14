const fs = require('fs');
const path = require('path');

const root = path.resolve(__dirname, '..');

// Helper para copiar ficheros si existen
const copyFile = (src, dest) => {
    if (fs.existsSync(src)) {
        fs.copyFileSync(src, dest);
        console.log(`Copiado: ${path.relative(root, src)} -> ${path.relative(root, dest)}`);
    } else {
        console.warn(`No encontrado: ${path.relative(root, src)}`);
    }
};

// 1. Directorio public/static para que Vercel CDN sirva /static/...
const publicStatic = path.join(root, 'public', 'static');
fs.mkdirSync(publicStatic, { recursive: true });

copyFile(path.join(root, 'static', 'tailwind.css'), path.join(publicStatic, 'tailwind.css'));
copyFile(path.join(root, 'static', 'styless.css'), path.join(publicStatic, 'styless.css'));
// Copia de seguridad con una sola 's' por si acaso
copyFile(path.join(root, 'static', 'styless.css'), path.join(publicStatic, 'styles.css'));

// 2. public/index.html para que Vercel sirva la página inicial en la raíz /
const publicIndex = path.join(root, 'public', 'index.html');
const templateIndex = path.join(root, 'templates', 'index.html');
copyFile(templateIndex, publicIndex);

// 2b. index.html en la raíz: red de seguridad por si Vercel usa "." como
// Output Directory en lugar de "public". Así "/" y "/static/..." (que ya
// existen en la raíz) funcionan sin depender de la detección del framework.
copyFile(templateIndex, path.join(root, 'index.html'));

// 3. templates/static sincronizado (por si en Vercel el usuario configuró Root Directory como templates)
const templateStatic = path.join(root, 'templates', 'static');
fs.mkdirSync(templateStatic, { recursive: true });
copyFile(path.join(root, 'static', 'tailwind.css'), path.join(templateStatic, 'tailwind.css'));
copyFile(path.join(root, 'static', 'styless.css'), path.join(templateStatic, 'styless.css'));
copyFile(path.join(root, 'static', 'styless.css'), path.join(templateStatic, 'styles.css'));

console.log('Sincronización de recursos completada para Vercel, Render y local.');

