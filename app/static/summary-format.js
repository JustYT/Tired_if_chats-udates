// A small, safe subset shared with Messenger/YFM: links, **bold**, ++underline++.
const escapeHtml = value => String(value).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const literal = value => value.replace(/\\([\\`*_{}\[\]()<>#!|+])/g, '$1');
const token = /\\[\\`*_{}\[\]()<>#!|+]|\[((?:\\.|[^\]\\\n])+)\]\((https?:\/\/[^\s)]+)\)|\*\*((?:\\.|[^\n])*?)\*\*|\+\+((?:\\.|[^\n])*?)\+\+/g;

export function summaryMarkup(text, depth = 0) {
  if (depth > 8) return escapeHtml(literal(text));
  let html = '', end = 0;
  for (const match of text.matchAll(token)) {
    html += escapeHtml(text.slice(end, match.index));
    if (match[1] !== undefined) {
      let valid = false;
      try {
        const url = new URL(match[2]);
        valid = ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password;
      } catch {}
      html += valid ? `<a href="${escapeHtml(match[2])}" target="_blank" rel="noopener noreferrer">${escapeHtml(literal(match[1]))}</a>` : escapeHtml(literal(match[0]));
    } else if (match[3] !== undefined) html += `<strong>${summaryMarkup(match[3], depth + 1)}</strong>`;
    else if (match[4] !== undefined) html += `<u>${summaryMarkup(match[4], depth + 1)}</u>`;
    else html += escapeHtml(literal(match[0]));
    end = match.index + match[0].length;
  }
  return html + escapeHtml(text.slice(end));
}
