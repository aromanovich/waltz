// Resolves every link the chapters make: chapter-to-chapter anchors, same-page
// anchors, and the `../../wal/...` paths each chapter's "Where this lives in
// the code" list is made of.
//
//   npm install && npm run check
//
// It exists because the failure is silent from both ends: renaming a heading or
// moving a file leaves a dead link, `npm run build` still succeeds, and site/ is
// gitignored, so the first person to find out is a reader. Anchors are slugged
// through slug.mjs, which is the rule the built pages use and the rule GitHub
// uses, so a link that passes here resolves in the Markdown and in the built page alike.

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { slugger, slugMarkdown } from './slug.mjs'

const here = dirname(fileURLToPath(import.meta.url))

const files = readdirSync(here)
  .filter((f) => f.endsWith('.md'))
  .sort()

const anchors = new Map(
  files.map((file) => {
    const next = slugger()
    const src = readFileSync(join(here, file), 'utf8')
    let inFence = false
    const ids = new Set()
    for (const line of src.split('\n')) {
      if (/^\s*```/.test(line)) inFence = !inFence
      if (inFence) continue
      const h = line.match(/^#{1,6}\s+(.+?)\s*$/)
      if (h) ids.add(next(slugMarkdown(h[1])))
    }
    return [file, ids]
  }),
)

let total = 0
const broken = []

for (const file of files) {
  const src = readFileSync(join(here, file), 'utf8')
  for (const m of src.matchAll(/\]\(([^)\s]*?)(?:#([^)\s]+))?\)/g)) {
    const [, target, anchor] = m
    if (/^(https?:|mailto:)/.test(target)) continue
    if (!target && !anchor) continue
    const line = src.slice(0, m.index).split('\n').length
    const at = `${file}:${line}`
    total++

    // A bare "#anchor" points into the chapter making the link.
    const path = target || file
    if (!existsSync(join(here, path))) {
      broken.push(`${at} → ${path} — no such file`)
      continue
    }
    if (!anchor) continue
    if (!path.endsWith('.md')) {
      broken.push(`${at} → ${path}#${anchor} — anchors are only resolved into chapters`)
      continue
    }
    if (!anchors.get(path)?.has(anchor)) {
      broken.push(`${at} → ${path}#${anchor} — no heading slugs to that`)
    }
  }
}

for (const b of broken) console.log(`✗ ${b}`)
console.log(`\n${total - broken.length}/${total} links resolve`)
process.exit(broken.length === 0 ? 0 : 1)
