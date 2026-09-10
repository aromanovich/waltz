// Parses every mermaid block in every chapter and reports the ones that will
// not render. A broken diagram is invisible in Markdown review — it shows up as
// an empty box in the built page, or as nothing at all on GitHub — so this runs
// as its own step:
//
//   npm install && npm run check
//
// It uses mermaid's own parser under jsdom. Parsing is not rendering: a diagram
// that parses here can still lay out badly, but one that fails here is broken
// everywhere.

import { dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { JSDOM } from 'jsdom'

import { chapters } from './render.mjs'

const here = dirname(fileURLToPath(import.meta.url))

const dom = new JSDOM('<!doctype html><body></body>', { pretendToBeVisual: true })
global.window = dom.window
global.document = dom.window.document
// node 22 defines navigator as a getter-only global, so it is redefined rather
// than assigned.
Object.defineProperty(global, 'navigator', { value: dom.window.navigator, configurable: true })
global.HTMLElement = dom.window.HTMLElement
global.SVGElement = dom.window.SVGElement

const mermaid = (await import('mermaid')).default
mermaid.initialize({ startOnLoad: false, securityLevel: 'strict' })

let total = 0
let bad = 0

for (const { file, src } of chapters(here)) {
  const blocks = [...src.matchAll(/```mermaid\n([\s\S]*?)```/g)]
  for (const [i, block] of blocks.entries()) {
    total++
    const line = src.slice(0, block.index).split('\n').length
    try {
      await mermaid.parse(block[1])
    } catch (e) {
      bad++
      const message = String((e && e.message) || e).split('\n').slice(0, 8).join('\n  ')
      console.log(`\n✗ ${file}:${line} (diagram ${i + 1})\n  ${message}`)
    }
  }
}

console.log(`\n${total - bad}/${total} diagrams parse`)
process.exit(bad === 0 ? 0 : 1)
