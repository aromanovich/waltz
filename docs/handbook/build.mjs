// Builds the HTML edition of the handbook into site/ beside these chapters. The
// Markdown is the source; site/ is generated and safe to delete.
//
//   npm install && npm run build
//
// The chapters are whatever Markdown sits beside this file: README.md is the
// index and the rest are ordered by their numeric prefix, so adding a chapter is
// adding a file.
//
// Mermaid is vendored into site/assets/ from node_modules, so a built page opens
// over file:// with no network.

import { readFileSync, writeFileSync, readdirSync, mkdirSync, copyFileSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { Marked } from 'marked'
import { slugger } from './slug.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const out = join(here, 'site')
const assets = join(out, 'assets')

const BOOK = 'The WAL Layer Handbook'
const SUBTITLE = 'waltz — a write-ahead log in front of a Temporal history shard'
const LANG = 'en'

// The chrome a reader clicks: the pager, the search box, the labels a screen
// reader announces. Chapter text is never here — that is whatever the Markdown
// says.
const ui = {
  skip: 'Skip to content',
  nav: 'Toggle navigation',
  chapters: 'Chapters',
  start: 'Start here',
  search: 'Search',
  searchWide: 'Search this book',
  searchLabel: 'Search headings and chapters…',
  theme: 'Switch theme',
  pager: 'Chapter navigation',
  prev: 'Previous',
  next: 'Next',
  hintMove: 'to move',
  hintOpen: 'to open',
  hintClose: 'to close',
}

// ---------------------------------------------------------------- chapter list

// README.md is the index; the rest are ordered by their numeric prefix.
const chapterFiles = readdirSync(here)
  .filter((f) => f.endsWith('.md') && f !== 'README.md')
  .sort()
const files = existsSync(join(here, 'README.md')) ? ['README.md', ...chapterFiles] : chapterFiles

const escapeHTML = (s) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

// Heading text arrives as rendered inline HTML: the tags come off, and the
// entities go back to their characters so that whatever re-escapes the text
// (the rail, the search index, the <title>) escapes it exactly once.
const stripTags = (s) =>
  s
    .replace(/<[^>]*>/g, '')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')

// ------------------------------------------------------------------- renderer

let headings = [] // collected per file for the search index
let nextID = slugger()
const titleByFile = {} // filled by the first pass, read by the link renderer

const marked = new Marked({ gfm: true, breaks: false })

marked.use({
  renderer: {
    heading({ tokens, depth }) {
      const html = this.parser.parseInline(tokens)
      const text = stripTags(html)
      const id = nextID(text) || `section-${headings.length + 1}`
      if (depth > 1) headings.push({ id, text, depth })
      const anchor = depth === 1 ? '' : `<a class="anchor" href="#${id}" aria-label="Link to this section">#</a>`
      return `<h${depth} id="${id}">${html}${anchor}</h${depth}>\n`
    },

    code({ text, lang }) {
      if ((lang || '').trim() === 'mermaid') {
        // Keep the raw source in a data attribute so the page can render the
        // diagram after Mermaid has loaded.
        return `<figure class="diagram"><div class="mermaid" data-src="${escapeHTML(text)}">${escapeHTML(text)}</div></figure>\n`
      }
      const cls = lang ? ` class="language-${escapeHTML(lang)}"` : ''
      return `<pre><code${cls}>${escapeHTML(text)}</code></pre>\n`
    },

    link({ href, title, tokens }) {
      let inner = this.parser.parseInline(tokens)
      let target = href || ''
      // A link whose visible text is the chapter's filename reads as a filename
      // on GitHub, where that is what it is, and as a stray path here. In the
      // built page it becomes the chapter's title.
      const file = target.replace(/#.*$/, '')
      if (titleByFile[file] && stripTags(inner).trim() === file) {
        inner = escapeHTML(titleByFile[file])
      }
      let extra = ''
      if (/^[a-z0-9-]*\.md(#.*)?$/i.test(target)) {
        // A sibling chapter: same page set, .html in the built site.
        target = target.replace(/\.md(?=$|#)/i, '.html')
      } else if (/^\.\.?\//.test(target)) {
        // A path out of the handbook directory. The built pages sit one level
        // deeper than the Markdown, so every such link gains one hop.
        target = `../${target}`
      } else if (/^https?:/i.test(target)) {
        extra = ' rel="noreferrer noopener" target="_blank"'
      }
      const t = title ? ` title="${escapeHTML(title)}"` : ''
      return `<a href="${escapeHTML(target)}"${t}${extra}>${inner}</a>`
    },
  },
})

// -------------------------------------------------------------------- shell

const navHTML = (pages, current) =>
  pages
    .map((p) => {
      const active = p.file === current ? ' class="active"' : ''
      const num = p.file === 'README.md' ? '' : `<span class="num">${p.number}</span>`
      return `<li${active}><a href="${p.href}">${num}<span class="label">${escapeHTML(p.navTitle)}</span></a></li>`
    })
    .join('\n')

const page = ({ p, pages, body, prev, next }) => `<!doctype html>
<html lang="${LANG}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHTML(p.title)} — ${escapeHTML(BOOK)}</title>
<meta name="description" content="${escapeHTML(SUBTITLE)}">
<link rel="stylesheet" href="assets/style.css">
</head>
<body>
<a class="skip" href="#content">${escapeHTML(ui.skip)}</a>

<input type="checkbox" id="nav-toggle" class="nav-toggle" hidden>

<header class="topbar">
  <label for="nav-toggle" class="burger" aria-label="${escapeHTML(ui.nav)}"><span></span><span></span><span></span></label>
  <span class="topbar-title">${p.number ? `<span class="topbar-num">${p.number}</span>` : ''}${escapeHTML(p.title)}</span>
  <button class="search-open" data-search-open aria-label="${escapeHTML(ui.search)}">${escapeHTML(ui.search)} <kbd>/</kbd></button>
</header>

<nav class="sidebar" aria-label="${escapeHTML(ui.chapters)}">
  <a class="brand" href="index.html">
    <span class="brand-mark" aria-hidden="true"></span>
    <span class="brand-text"><strong>${escapeHTML(BOOK)}</strong><em>${escapeHTML(SUBTITLE)}</em></span>
  </a>
  <button class="search-open wide" data-search-open>${escapeHTML(ui.searchWide)} <kbd>/</kbd></button>
  <ol class="chapters">
${navHTML(pages, p.file)}
  </ol>
</nav>

<main id="content">
  <article class="prose">
${body}
  </article>
  <nav class="pager" aria-label="${escapeHTML(ui.pager)}">
    ${prev ? `<a class="prev" href="${prev.href}"><span>${escapeHTML(ui.prev)}</span><strong>${escapeHTML(prev.navTitle)}</strong></a>` : '<span></span>'}
    ${next ? `<a class="next" href="${next.href}"><span>${escapeHTML(ui.next)}</span><strong>${escapeHTML(next.navTitle)}</strong></a>` : '<span></span>'}
  </nav>
</main>

<div class="search-overlay" data-search-overlay hidden>
  <div class="search-box" role="dialog" aria-modal="true" aria-label="${escapeHTML(ui.search)}">
    <input type="search" placeholder="${escapeHTML(ui.searchLabel)}" data-search-input autocomplete="off" spellcheck="false">
    <ul data-search-results></ul>
    <p class="search-hint"><kbd>↑</kbd><kbd>↓</kbd> ${escapeHTML(ui.hintMove)} · <kbd>Enter</kbd> ${escapeHTML(ui.hintOpen)} · <kbd>Esc</kbd> ${escapeHTML(ui.hintClose)}</p>
  </div>
</div>

<script src="assets/mermaid.min.js"></script>
<script src="assets/search-index.js"></script>
<script src="assets/app.js"></script>
</body>
</html>
`

// --------------------------------------------------------------------- build

mkdirSync(assets, { recursive: true })

// First pass: titles, so the navigation is complete on every page.
const pages = files.map((file) => {
  const src = readFileSync(join(here, file), 'utf8')
  const h1 = src.match(/^#\s+(.+)$/m)
  const title = h1 ? stripTags(h1[1]).trim() : file.replace(/\.md$/, '')
  const number = (file.match(/^(\d+)/) || [null, ''])[1]
  // The sidebar carries the chapter's own title, minus any "Chapter N — " noise.
  const navTitle = title.replace(/^\s*\d+[.)]?\s*[—-]?\s*/, '')
  return {
    file,
    number,
    title,
    navTitle: file === 'README.md' ? ui.start : navTitle,
    href: file === 'README.md' ? 'index.html' : file.replace(/\.md$/, '.html'),
    src,
  }
})

for (const p of pages) titleByFile[p.file] = p.title

const searchIndex = []

pages.forEach((p, i) => {
  headings = []
  nextID = slugger()

  let body = marked.parse(p.src)
  body = body
    .replace(/<table>/g, '<div class="table-wrap"><table>')
    .replace(/<\/table>/g, '</table></div>')

  searchIndex.push({
    href: p.href,
    title: p.title,
    nav: p.navTitle,
    number: p.number,
    headings: headings.filter((h) => h.depth <= 3).map((h) => ({ id: h.id, text: h.text })),
  })

  writeFileSync(
    join(out, p.href),
    page({ p, pages, body, prev: pages[i - 1], next: pages[i + 1] }),
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
