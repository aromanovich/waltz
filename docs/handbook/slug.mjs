// The one place a heading becomes an anchor, shared by the build and the link
// checker.
//
// The rule is GitHub's, because the Markdown is the edition most links are
// followed in: a link in a PR review or an issue lands on github.com, not in
// site/. Where two sluggers disagree a link resolves in one edition and 404s in
// the other, which is what every heading carrying an em dash did — GitHub turns
// each space into its own hyphen, so the two spaces around the dash become two
// hyphens, and a slugger that collapses whitespace after dropping punctuation
// writes one.
//
// Letters, digits, `-` and `_` survive; everything else is dropped, and each
// remaining space becomes a hyphen.

const slug = (text) =>
  text
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s_-]/gu, '')
    .replace(/\s/g, '-')

// Markdown as written rather than the plain text a renderer hands back: the
// inline markers around `code` and **bold** are consumed before a heading is
// slugged.
export const slugMarkdown = (heading) => slug(heading.replace(/[`*]/g, ''))

// GitHub numbers repeats from 1 (`foo`, `foo-1`, `foo-2`) in document order.
export const slugger = () => {
  const seen = new Map()
  return (text) => {
    const base = slug(text)
    const n = seen.get(base) ?? 0
    seen.set(base, n + 1)
    return n === 0 ? base : `${base}-${n}`
  }
}
