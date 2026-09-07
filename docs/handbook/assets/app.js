/* The WAL Layer Handbook — page behaviour: diagrams, navigation and the search
   overlay. No build step and no dependencies beyond the
   vendored mermaid bundle; everything works over file://. */

(function () {
  'use strict'

  /* --------------------------------------------------------------- diagrams */

  var diagrams = Array.prototype.slice.call(document.querySelectorAll('.mermaid'))

  // Pull Mermaid's colours from the page tokens so diagrams share the same
  // monochrome whitepaper palette as the prose.
  function diagramTheme() {
    var css = getComputedStyle(document.body)
    var v = function (name, fallback) {
      return (css.getPropertyValue(name) || '').trim() || fallback
    }
    var surface = v('--surface', '#fff')
    var surface2 = v('--surface-2', '#f2f2f2')
    var text = v('--text', '#111')
    var muted = v('--muted', '#666')
    var line = v('--line-strong', '#ccc')
    var accent = v('--accent', '#a4501e')
    var accentSoft = v('--accent-soft', '#f6ece3')
    return {
      background: 'transparent',
      fontSize: '14px',

      primaryColor: surface2,
      primaryTextColor: text,
      primaryBorderColor: line,
      secondaryColor: accentSoft,
      tertiaryColor: surface,
      lineColor: muted,
      textColor: text,
      mainBkg: surface2,
      nodeBorder: line,
      clusterBkg: 'transparent',
      clusterBorder: line,
      edgeLabelBackground: surface,
      titleColor: text,

      actorBkg: surface2,
      actorBorder: line,
      actorTextColor: text,
      actorLineColor: muted,
      signalColor: text,
      signalTextColor: text,
      labelBoxBkgColor: accentSoft,
      labelBoxBorderColor: accent,
      labelTextColor: text,
      loopTextColor: text,
      noteBkgColor: accentSoft,
      noteBorderColor: accent,
      noteTextColor: text,
      activationBkgColor: accentSoft,
      activationBorderColor: accent,
      sequenceNumberColor: surface,

      altBackground: surface,
      transitionColor: muted,
      transitionLabelColor: text,
      stateLabelColor: text,
      stateBkg: surface2,
      compositeBackground: surface,
      compositeBorder: line,
      compositeTitleBackground: surface2,
      innerEndBackground: line,
      specialStateColor: accent,

      classText: text,
      relationColor: muted,
      relationLabelColor: text,
    }
  }

  // Diagrams are fitted to the figure, but only so far: below this the text
  // stops being readable and scrolling is the better trade.
  var MIN_SCALE = 0.72

  function fitDiagrams() {
    diagrams.forEach(function (n) {
      var svg = n.querySelector('svg')
      if (!svg) return
      var natural = (svg.viewBox && svg.viewBox.baseVal && svg.viewBox.baseVal.width) || 0
      if (!natural) return
      var fig = n.closest('.diagram')
      var available = fig ? fig.clientWidth - 34 : natural
      var scale = Math.min(1, Math.max(MIN_SCALE, available / natural))
      svg.style.width = Math.round(natural * scale) + 'px'
      svg.style.height = 'auto'
      svg.style.maxWidth = 'none'
    })
  }

  // A diagram still wider than its figure scrolls; say so, because a reader who
  // cannot see the right-hand third has no way to know it is there.
  function markScrollable() {
    fitDiagrams()
    diagrams.forEach(function (n) {
      var fig = n.closest('.diagram')
      if (!fig) return
      var over = fig.scrollWidth - fig.clientWidth > 8
      fig.classList.toggle('scrollable', over)
      var hint = fig.querySelector('.diagram-hint')
      if (over && !hint) {
        hint = document.createElement('p')
        hint.className = 'diagram-hint'
        hint.textContent = 'wider than the page — scroll the diagram sideways'
        fig.appendChild(hint)
      } else if (!over && hint) {
        hint.remove()
      }
    })
  }

  window.addEventListener('resize', markScrollable)

  function renderDiagrams() {
    if (!window.mermaid || !diagrams.length) return
    diagrams.forEach(function (n) {
      n.removeAttribute('data-processed')
      n.removeAttribute('data-rendered')
      n.textContent = n.dataset.src || n.textContent
    })
    try {
      window.mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        themeVariables: diagramTheme(),
        securityLevel: 'strict',
        fontFamily: getComputedStyle(document.body).getPropertyValue('--font-sans') || 'system-ui, sans-serif',
        flowchart: { useMaxWidth: true, htmlLabels: true, curve: 'basis' },
        // Mermaid's stock sequence metrics are generous enough that a
        // seven-participant diagram is three times the column wide and gets
        // scaled down until its text is unreadable. These tighten the natural
        // size instead, so the fit costs little; wrap stays off so participant
        // names are not hyphenated.
        sequence: {
          // Not fitted to the column: a long-message diagram is ~2.5x the
          // column wide, and fitting it puts the text below reading size. It
          // keeps its own size and the figure scrolls, with a hint.
          useMaxWidth: false,
          wrap: false,
          width: 120,
          actorMargin: 28,
          boxMargin: 8,
          actorFontSize: 13,
          messageFontSize: 13,
          noteFontSize: 12,
        },
        state: { useMaxWidth: true },
        class: { useMaxWidth: true },
      })
      window.mermaid
        .run({ nodes: diagrams, suppressErrors: true })
        .then(function () {
          diagrams.forEach(function (n) {
            if (n.querySelector('svg')) n.setAttribute('data-rendered', '')
          })
          markScrollable()
        })
    } catch (e) {
      /* A diagram that will not parse stays on the page as its own source,
         which is more useful than an empty box. */
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', renderDiagrams)
  } else {
    renderDiagrams()
  }

  /* ----------------------------------------------------------------- topbar */

  var topbar = document.querySelector('.topbar')
  var onScroll = function () {
    if (topbar) topbar.classList.toggle('scrolled', window.scrollY > 4)
  }
  window.addEventListener('scroll', onScroll, { passive: true })
  onScroll()

  var navToggle = document.getElementById('nav-toggle')
  if (navToggle) {
    var desktopNav = window.matchMedia('(min-width: 62rem)')
    try { navToggle.checked = desktopNav.matches && localStorage.getItem('book-nav-collapsed') === 'true' }
    catch (e) { /* Storage can be unavailable for local files. */ }
    navToggle.addEventListener('change', function () {
      if (!desktopNav.matches) return
      try { localStorage.setItem('book-nav-collapsed', String(navToggle.checked)) }
      catch (e) { /* The navigation still works without persistence. */ }
    })
  }
  Array.prototype.forEach.call(document.querySelectorAll('.sidebar a'), function (a) {
    a.addEventListener('click', function () {
      if (navToggle && window.matchMedia('(max-width: 61.99rem)').matches) navToggle.checked = false
    })
  })

  /* ----------------------------------------------------------------- search */

  var overlay = document.querySelector('[data-search-overlay]')
  var input = document.querySelector('[data-search-input]')
  var results = document.querySelector('[data-search-results]')
  var entries = []
  var selected = 0

  ;(window.HANDBOOK_INDEX || []).forEach(function (page) {
    entries.push({ label: page.title, sub: page.number ? 'Chapter ' + page.number : 'Handbook', href: page.href })
    page.headings.forEach(function (h) {
      entries.push({ label: h.text, sub: page.nav, href: h.href || page.href + '#' + h.id })
    })
  })

  function score(entry, needle) {
    var label = entry.label.toLowerCase()
    var i = label.indexOf(needle)
    if (i === 0) return 0
    if (i > 0) return 1
    if (entry.sub.toLowerCase().indexOf(needle) >= 0) return 2
    return -1
  }

  function draw() {
    var needle = (input.value || '').trim().toLowerCase()
    var matched = needle
      ? entries
          .map(function (e) { return { e: e, s: score(e, needle) } })
          .filter(function (m) { return m.s >= 0 })
          .sort(function (a, b) { return a.s - b.s })
          .slice(0, 30)
          .map(function (m) { return m.e })
      : entries.filter(function (e) { return e.sub.indexOf('Chapter') === 0 || e.sub === 'Handbook' }).slice(0, 12)

    selected = 0
    results.innerHTML = matched
      .map(function (e, i) {
        return (
          '<li' + (i === 0 ? ' aria-selected="true"' : '') + '><a href="' + e.href + '">' +
          '<span>' + e.label.replace(/[<>&]/g, '') + '</span>' +
          '<small>' + e.sub.replace(/[<>&]/g, '') + '</small></a></li>'
        )
      })
      .join('')
  }

  function openSearch() {
    if (!overlay) return
    overlay.hidden = false
    input.value = ''
    draw()
    input.focus()
  }

  function closeSearch() { if (overlay) overlay.hidden = true }

  function move(delta) {
    var items = results.querySelectorAll('li')
    if (!items.length) return
    items[selected].removeAttribute('aria-selected')
    selected = (selected + delta + items.length) % items.length
    items[selected].setAttribute('aria-selected', 'true')
    items[selected].scrollIntoView({ block: 'nearest' })
  }

  Array.prototype.forEach.call(document.querySelectorAll('[data-search-open]'), function (b) {
    b.addEventListener('click', openSearch)
  })

  if (overlay) {
    overlay.addEventListener('click', function (e) { if (e.target === overlay) closeSearch() })
    input.addEventListener('input', draw)
    input.addEventListener('keydown', function (e) {
      if (e.key === 'ArrowDown') { e.preventDefault(); move(1) }
      else if (e.key === 'ArrowUp') { e.preventDefault(); move(-1) }
      else if (e.key === 'Enter') {
        var a = results.querySelector('li[aria-selected="true"] a')
        if (a) { e.preventDefault(); window.location.href = a.getAttribute('href') }
      }
    })
  }

  document.addEventListener('keydown', function (e) {
    var typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement.tagName)
    if (e.key === 'Escape') return closeSearch()
    if (typing) return
    if (e.key === '/' || ((e.metaKey || e.ctrlKey) && e.key === 'k')) {
      e.preventDefault()
      openSearch()
    }
  })
})()
