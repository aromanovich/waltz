// Builds the HTML edition of the handbook into site/ beside these chapters. The
// Markdown is the source; site/ is generated and safe to delete.
//
//   npm install && npm run build
//
// One page per chapter, which is the edition to browse. The single-file edition
// a reader downloads is build-single.mjs, and both render through render.mjs.
//
// Mermaid is vendored into site/assets/ from node_modules, so a built page opens
// over file:// with no network.

import { writeFileSync, mkdirSync, copyFileSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { slugger } from './slug.mjs'
import {
  BOOK,
  ui,
  chapters,
  escapeHTML,
  renderChapter,
  isChapterLink,
  isRelative,
  isAbsolute,
  navList,
  shell,
  titlesByFile,
} from './render.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const out = join(here, 'site')
const assets = join(out, 'assets')

mkdirSync(assets, { recursive: true })

// First pass: titles, so the navigation is complete on every page.
const pages = chapters(here).map((c) => ({
  ...c,
  href: c.file === 'README.md' ? 'index.html' : c.file.replace(/\.md$/, '.html'),
}))

const titleByFile = titlesByFile(pages)

// A sibling chapter is the same page set with a different extension; a path out
// of this directory gains one hop, because the built pages sit one level deeper
// than the Markdown.
const href = (target) => {
  if (isChapterLink(target)) return { href: target.replace(/\.md(?=$|#)/i, '.html') }
  if (isRelative(target)) return { href: `../${target}` }
  if (isAbsolute(target)) return { href: target, blank: true }
  return { href: target }
}

const searchIndex = []

pages.forEach((p, i) => {
  const id = slugger()
  const { html, headings } = renderChapter(p.src, { id, href, titleByFile })

  searchIndex.push({
    href: p.href,
    title: p.title,
    nav: p.navTitle,
    number: p.number,
    headings: headings.filter((h) => h.depth <= 3).map((h) => ({ id: h.id, text: h.text })),
  })

  const prev = pages[i - 1]
  const next = pages[i + 1]

  writeFileSync(
    join(out, p.href),
    shell({
      title: `${p.title} — ${BOOK}`,
      topbar: `${p.number ? `<span class="topbar-num">${p.number}</span>` : ''}${escapeHTML(p.title)}`,
      head: '<link rel="stylesheet" href="assets/style.css">',
      brandHref: 'index.html',
      nav: navList(
        pages.map((q) => ({
          href: q.href,
          number: q.file === 'README.md' ? '' : q.number,
          label: q.navTitle,
          active: q.file === p.file,
        })),
      ),
      body: html,
      pager: `  <nav class="pager" aria-label="${escapeHTML(ui.pager)}">
    ${prev ? `<a class="prev" href="${prev.href}"><span>${escapeHTML(ui.prev)}</span><strong>${escapeHTML(prev.navTitle)}</strong></a>` : '<span></span>'}
    ${next ? `<a class="next" href="${next.href}"><span>${escapeHTML(ui.next)}</span><strong>${escapeHTML(next.navTitle)}</strong></a>` : '<span></span>'}
  </nav>`,
      scripts: `<script src="assets/mermaid.min.js"></script>
<script src="assets/search-index.js"></script>
<script src="assets/app.js"></script>`,
    }),
  )
})

// A global rather than JSON fetched at run time, so search works over file://
// as well as over http.
writeFileSync(
  join(assets, 'search-index.js'),
  `window.HANDBOOK_INDEX = ${JSON.stringify(searchIndex)}\n`,
)

for (const a of ['style.css', 'app.js']) copyFileSync(join(here, 'assets', a), join(assets, a))

const mermaidSrc = join(here, 'node_modules/mermaid/dist/mermaid.min.js')
if (existsSync(mermaidSrc)) {
  copyFileSync(mermaidSrc, join(assets, 'mermaid.min.js'))
} else if (!existsSync(join(assets, 'mermaid.min.js'))) {
  console.warn('! mermaid not found in node_modules and none vendored: diagrams will not render.')
}

console.log(`built ${pages.length} pages into ${out}`)
