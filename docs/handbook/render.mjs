// What the two builds share: the chapter list, the Markdown renderer and the
// HTML shell. build.mjs writes the site a reader browses; build-single.mjs
// writes the one file a reader downloads. Everything they differ in is a
// parameter here, so a change to the prose rendering lands in both editions or
// in neither.

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'
import { Marked } from 'marked'

export const BOOK = 'The WAL Layer Handbook'
export const SUBTITLE = 'waltz — a write-ahead log in front of a Temporal history shard'
export const LANG = 'en'

// The chrome a reader clicks: the pager, the search box, the labels a screen
// reader announces. Chapter text is never here — that is whatever the Markdown
// says.
export const ui = {
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

export const escapeHTML = (s) =>
  s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

// Heading text arrives as rendered inline HTML: the tags come off, and the
// entities go back to their characters so that whatever re-escapes the text
// (the rail, the search index, the <title>) escapes it exactly once.
export const stripTags = (s) =>
  s
    .replace(/<[^>]*>/g, '')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')

// ---------------------------------------------------------------- chapter list

// README.md is the index; the rest are ordered by their numeric prefix, so
// adding a chapter is adding a file.
export const chapters = (here) => {
  const rest = readdirSync(here)
    .filter((f) => f.endsWith('.md') && f !== 'README.md')
    .sort()
  const files = existsSync(join(here, 'README.md')) ? ['README.md', ...rest] : rest

  return files.map((file) => {
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
      stem: file.replace(/\.md$/, '').toLowerCase(),
      src,
    }
  })
}

// ------------------------------------------------------------------- renderer

// One chapter of Markdown to HTML, plus the headings that go into the search
// index. The caller supplies the two things the editions disagree about:
// `id(text)` mints a heading's anchor, and `href(target)` decides what a link
// points at once the Markdown is no longer the edition being read.
export const renderChapter = (src, { id, href, titleByFile = {} }) => {
  const headings = []
  const marked = new Marked({ gfm: true, breaks: false })

  marked.use({
    renderer: {
      heading({ tokens, depth }) {
        const html = this.parser.parseInline(tokens)
        const text = stripTags(html)
        const anchorID = id(text) || `section-${headings.length + 1}`
        if (depth > 1) headings.push({ id: anchorID, text, depth })
        const anchor =
          depth === 1 ? '' : `<a class="anchor" href="#${anchorID}" aria-label="Link to this section">#</a>`
        return `<h${depth} id="${anchorID}">${html}${anchor}</h${depth}>\n`
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

      link({ href: target, title, tokens }) {
        let inner = this.parser.parseInline(tokens)
        const raw = target || ''
        // A link whose visible text is the chapter's filename reads as a filename
        // on GitHub, where that is what it is, and as a stray path here. In the
        // built page it becomes the chapter's title.
        const file = raw.replace(/#.*$/, '')
        if (titleByFile[file] && stripTags(inner).trim() === file) {
          inner = escapeHTML(titleByFile[file])
        }
        const resolved = href(raw)
        const t = title ? ` title="${escapeHTML(title)}"` : ''
        const extra = resolved.blank ? ' rel="noreferrer noopener" target="_blank"' : ''
        return `<a href="${escapeHTML(resolved.href)}"${t}${extra}>${inner}</a>`
      },
    },
  })

  const html = marked
    .parse(src)
    .replace(/<table>/g, '<div class="table-wrap"><table>')
    .replace(/<\/table>/g, '</table></div>')

  return { html, headings }
}

// A link out of this directory keeps its shape in the Markdown, which is the
// edition it was written for; each edition says where that lands.
export const isChapterLink = (target) => /^[a-z0-9-]*\.md(#.*)?$/i.test(target)
export const isRelative = (target) => /^\.\.?\//.test(target)
export const isAbsolute = (target) => /^https?:/i.test(target)

// -------------------------------------------------------------------- shell

export const navList = (items) =>
  items
    .map(({ href, number, label, active }) => {
      const num = number ? `<span class="num">${number}</span>` : ''
      return `<li${active ? ' class="active"' : ''}><a href="${href}">${num}<span class="label">${escapeHTML(label)}</span></a></li>`
    })
    .join('\n')

// The one page template. `head` and `scripts` are where the two editions part:
// the site links its assets and the single file carries them inline.
export const shell = ({ title, topbar, head, brandHref, nav, body, pager = '', scripts }) => `<!doctype html>
<html lang="${LANG}">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHTML(title)}</title>
<meta name="description" content="${escapeHTML(SUBTITLE)}">
${head}
</head>
<body>
<a class="skip" href="#content">${escapeHTML(ui.skip)}</a>

<input type="checkbox" id="nav-toggle" class="nav-toggle" hidden>

<header class="topbar">
  <label for="nav-toggle" class="burger" aria-label="${escapeHTML(ui.nav)}"><span></span><span></span><span></span></label>
  <span class="topbar-title">${topbar}</span>
  <button class="search-open" data-search-open aria-label="${escapeHTML(ui.search)}">${escapeHTML(ui.search)} <kbd>/</kbd></button>
</header>

<nav class="sidebar" aria-label="${escapeHTML(ui.chapters)}">
  <a class="brand" href="${brandHref}">
    <span class="brand-mark" aria-hidden="true"></span>
    <span class="brand-text"><strong>${escapeHTML(BOOK)}</strong><em>${escapeHTML(SUBTITLE)}</em></span>
  </a>
  <button class="search-open wide" data-search-open>${escapeHTML(ui.searchWide)} <kbd>/</kbd></button>
  <ol class="chapters">
${nav}
  </ol>
</nav>

<main id="content">
  <article class="prose">
${body}
  </article>
${pager}
</main>

<div class="search-overlay" data-search-overlay hidden>
  <div class="search-box" role="dialog" aria-modal="true" aria-label="${escapeHTML(ui.search)}">
    <input type="search" placeholder="${escapeHTML(ui.searchLabel)}" data-search-input autocomplete="off" spellcheck="false">
    <ul data-search-results></ul>
    <p class="search-hint"><kbd>↑</kbd><kbd>↓</kbd> ${escapeHTML(ui.hintMove)} · <kbd>Enter</kbd> ${escapeHTML(ui.hintOpen)} · <kbd>Esc</kbd> ${escapeHTML(ui.hintClose)}</p>
  </div>
</div>

${scripts}
</body>
</html>
`
