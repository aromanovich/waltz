// Builds the whole handbook as one self-contained HTML file, for attaching to a
// release. The stylesheet, the script, the search index and the Mermaid bundle
// are inlined, so the download is a single file that opens over file:// with no
// network and nothing installed.
//
//   npm install && npm run build:single
//
// Two things follow from there being one document instead of sixteen pages.
//
// Anchors are namespaced by chapter: two chapters may each have an `## Overview`
// and in one document those ids would collide. Every heading becomes
// `<chapter-stem>--<the slug GitHub would mint>`, which keeps the mapping from a
// cross-chapter link mechanical.
//
// Links out of the handbook become absolute GitHub URLs. In the Markdown and in
// site/ a `../../wal/wal.go` resolves against a checkout; in a file somebody
// downloaded it resolves against their home directory, so the only honest target
// is the repository. WALTZ_REF pins which ref they land on — a release build
// should pass its tag, so the links keep saying what the prose says.

import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import { dirname, join, posix } from 'node:path'
import { fileURLToPath } from 'node:url'
import { slugger } from './slug.mjs'
import {
  BOOK,
  chapters,
  escapeHTML,
  renderChapter,
  isChapterLink,
  isRelative,
  isAbsolute,
  navList,
  shell,
} from './render.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const out = join(here, 'site')
const file = join(out, 'waltz-handbook.html')

const REPO = 'https://github.com/aromanovich/waltz'
const REF = process.env.WALTZ_REF || 'main'
// Where these chapters sit in the repository, which is what a `../` in one of
// them is relative to.
const HOME = 'docs/handbook'

const pages = chapters(here)
const stemOf = Object.fromEntries(pages.map((p) => [p.file, p.stem]))

const titleByFile = {}
for (const p of pages) titleByFile[p.file] = p.title

// A `<script>` written into a document ends at the first `</script` in its
// source, wherever that appears — including inside a string literal. `<\/script`
// is the same string to a JavaScript parser and not a closing tag to an HTML
// one. No bundle here contains the sequence today; this is what keeps that from
// being a fact the next dependency bump can quietly change.
const inlineScript = (js) => `<script>\n${js.replace(/<\/script/gi, '<\\/script')}\n</script>`

const searchIndex = []
const bodies = []

// Sixteen chapters of cross-references collapse into one document's anchors,
// and a rewrite that lands on nothing is a link that silently does nothing when
// clicked. Every id minted and every in-document href are collected here and
// reconciled at the end, so the build is what catches it.
const ids = new Set()
const internal = []

for (const p of pages) {
  ids.add(p.stem)
  const localID = slugger()
  const id = (text) => {
    const anchorID = `${p.stem}--${localID(text)}`
    ids.add(anchorID)
    return anchorID
  }

  const href = (target) => {
    const resolved = resolve(target)
    if (resolved.href.startsWith('#')) internal.push({ file: p.file, target, href: resolved.href })
    return resolved
  }

  const resolve = (target) => {
    if (isAbsolute(target)) return { href: target, blank: true }
    if (isChapterLink(target)) {
      const [f, anchor] = target.split('#')
      const stem = stemOf[f]
      if (!stem) return { href: target }
      return { href: anchor ? `#${stem}--${anchor}` : `#${stem}` }
    }
    // An anchor with no file is a link inside the chapter being rendered, and
    // it was written against that chapter's own slugs.
    if (target.startsWith('#')) return { href: `#${p.stem}--${target.slice(1)}` }
    if (isRelative(target)) {
      const [path, anchor] = target.split('#')
      const inRepo = posix.normalize(posix.join(HOME, path))
      return { href: `${REPO}/blob/${REF}/${inRepo}${anchor ? `#${anchor}` : ''}`, blank: true }
    }
    return { href: target }
  }

  const { html, headings } = renderChapter(p.src, { id, href, titleByFile })

  searchIndex.push({
    href: `#${p.stem}`,
    title: p.title,
    nav: p.navTitle,
    number: p.number,
    headings: headings
      .filter((h) => h.depth <= 3)
      .map((h) => ({ id: h.id, text: h.text, href: `#${h.id}` })),
  })

  bodies.push(`<section id="${p.stem}">\n${html}</section>`)
}

const dangling = internal.filter((l) => !ids.has(l.href.slice(1)))
if (dangling.length) {
  for (const l of dangling) console.log(`✗ ${l.file}: ${l.target} → ${l.href} — no such anchor`)
  console.log(`\n${dangling.length} of ${internal.length} in-document links resolve to nothing`)
  process.exit(1)
}

const asset = (name) => readFileSync(join(here, 'assets', name), 'utf8')
const mermaid = readFileSync(join(here, 'node_modules/mermaid/dist/mermaid.min.js'), 'utf8')

mkdirSync(out, { recursive: true })
writeFileSync(
  file,
  shell({
    title: BOOK,
    topbar: escapeHTML(BOOK),
    head: `<style>\n${asset('style.css')}\n</style>`,
    brandHref: '#',
    nav: navList(
      pages.map((p) => ({ href: `#${p.stem}`, number: p.number, label: p.navTitle })),
    ),
    body: bodies.join('\n'),
    scripts: [
      inlineScript(mermaid),
      inlineScript(`window.HANDBOOK_INDEX = ${JSON.stringify(searchIndex)}`),
      inlineScript(asset('app.js')),
    ].join('\n'),
  }),
)

const bytes = readFileSync(file).length
console.log(
  `built ${pages.length} chapters into ${file} (${(bytes / 1e6).toFixed(1)} MB, ` +
    `${internal.length}/${internal.length} in-document links resolve, out-of-book links point at ${REF})`,
)
