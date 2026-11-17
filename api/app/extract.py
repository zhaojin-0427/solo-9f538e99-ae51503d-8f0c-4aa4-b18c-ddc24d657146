"""HTML title/body extraction built on the stdlib html.parser (no extra deps)."""
from html.parser import HTMLParser

_BLOCK_TAGS = {
    "p", "div", "br", "li", "tr", "td", "th", "table", "ul", "ol",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "section", "article", "header", "footer", "blockquote", "pre", "hr",
}
_SKIP_TAGS = {"script", "style", "noscript", "template"}


class _Parser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.in_title = False
        self.title_parts: list[str] = []
        self.skip_depth = 0
        self.body_depth = 0
        self.seen_body = False
        self.body_parts: list[str] = []
        self.all_parts: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag == "title":
            self.in_title = True
        elif tag in _SKIP_TAGS:
            self.skip_depth += 1
        elif tag == "body":
            self.seen_body = True
            self.body_depth += 1
        if tag in _BLOCK_TAGS:
            self._emit("\n")

    def handle_endtag(self, tag):
        if tag == "title":
            self.in_title = False
        elif tag in _SKIP_TAGS:
            if self.skip_depth:
                self.skip_depth -= 1
        elif tag == "body":
            if self.body_depth:
                self.body_depth -= 1
        if tag in _BLOCK_TAGS:
            self._emit("\n")

    def handle_data(self, data):
        if self.skip_depth:
            return
        if self.in_title:
            self.title_parts.append(data)
            return
        self._emit(data)

    def _emit(self, text: str) -> None:
        self.all_parts.append(text)
        if self.body_depth:
            self.body_parts.append(text)


def _normalize(parts: list[str]) -> str:
    raw = "".join(parts)
    lines = [" ".join(line.split()) for line in raw.split("\n")]
    return "\n".join(line for line in lines if line)


def extract_document(html: str) -> tuple[str, str]:
    """Return (title, body_text). Body text excludes script/style content."""
    parser = _Parser()
    parser.feed(html)
    parser.close()
    title = " ".join("".join(parser.title_parts).split())
    body = _normalize(parser.body_parts)
    if not body:
        body = _normalize(parser.all_parts)
    return title, body
